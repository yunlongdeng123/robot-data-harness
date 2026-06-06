"""SLA：policy 加载、ads/ml_ready 校验、写 sla_checks。"""

from __future__ import annotations

import csv
import fnmatch
import io
import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    DimDatasetRow,
    MlReadyDatasetRow,
    QcContractRunRow,
    SlaCheckRow,
    SlaPolicyRow,
    WorkflowRunRow,
    ensure_lake_tables,
)
from robot_dh.warehouse_metrics.dates import parse_date_range

LOG = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


@dataclass
class SlaPolicy:
    policy_id: str
    policy_name: str
    dataset_pattern: str | None
    dataset_family: str | None
    deadline_hour: int | None
    required_outputs: list[str]
    min_qc_pass_rate: float | None
    min_etl_success_rate: float | None
    max_failed_workflows: int | None
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SlaPolicyDoc:
    """sla_policies.yaml 解析结果。"""

    policies: list[SlaPolicy] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"policies": [p.to_dict() for p in self.policies]}


@dataclass
class SlaCheckResult:
    check_id: str
    policy_id: str
    policy_name: str
    dt: str
    dataset_id: str
    version: str
    status: str
    qc_pass_rate: float | None
    etl_success_rate: float | None
    workflow_success_rate: float | None
    missing_outputs: list[str]
    failed_reason: str | None
    metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SlaReportArtifacts:
    sla_report_html: Path
    sla_report_json: Path
    sla_failed_datasets: Path

    def to_dict(self) -> dict[str, Any]:
        return {k: str(v) for k, v in asdict(self).items()}


def load_sla_policies(path: Path | str) -> SlaPolicyDoc:
    """加载 sla_policies.yaml。"""
    try:
        import yaml
    except ImportError as err:
        raise RuntimeError("PyYAML required for load_sla_policies") from err
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"SLA policies file not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    policies_raw = raw.get("policies") or []
    if not isinstance(policies_raw, list):
        raise ValueError(f"SLA policies file invalid: 'policies' must be a list, got {type(policies_raw)}")
    out: list[SlaPolicy] = []
    for entry in policies_raw:
        if not isinstance(entry, dict):
            continue
        required_outputs = entry.get("required_outputs") or []
        if not isinstance(required_outputs, list):
            required_outputs = [str(required_outputs)]
        out.append(
            SlaPolicy(
                policy_id=str(entry.get("policy_id") or ""),
                policy_name=str(entry.get("policy_name") or entry.get("policy_id") or ""),
                dataset_pattern=str(entry["dataset_pattern"]) if entry.get("dataset_pattern") else None,
                dataset_family=str(entry["dataset_family"]) if entry.get("dataset_family") else None,
                deadline_hour=int(entry["deadline_hour"]) if entry.get("deadline_hour") is not None else None,
                required_outputs=[str(o) for o in required_outputs],
                min_qc_pass_rate=float(entry["min_qc_pass_rate"]) if entry.get("min_qc_pass_rate") is not None else None,
                min_etl_success_rate=float(entry["min_etl_success_rate"]) if entry.get("min_etl_success_rate") is not None else None,
                max_failed_workflows=int(entry["max_failed_workflows"]) if entry.get("max_failed_workflows") is not None else None,
                enabled=bool(entry.get("enabled", True)),
            )
        )
    return SlaPolicyDoc(policies=out)


def perform_sla_checks(
    *,
    policies: SlaPolicyDoc,
    date_: str | date | None = None,
    db_uri: str | None = None,
    persist: bool = True,
) -> list[SlaCheckResult]:
    """对每个 enabled policy 跑 SLA 校验，可选持久化 sla_policies / sla_checks。"""
    target = parse_date_range(date_=date_).start
    engine = _ensure_engine(db_uri)
    existing = set(inspect(engine).get_table_names())

    results: list[SlaCheckResult] = []
    with Session(engine, expire_on_commit=False, future=True) as session:
        ads_rows = []
        if "ads_quality_dashboard" in existing:
            ads_rows = session.execute(
                select(AdsQualityDashboardRow).where(AdsQualityDashboardRow.dt == target)
            ).scalars().all()
        dim_rows = []
        if "dim_dataset" in existing:
            dim_rows = session.execute(select(DimDatasetRow)).scalars().all()
        ml_ready_rows = []
        if "ml_ready_datasets" in existing:
            ml_ready_rows = session.execute(select(MlReadyDatasetRow)).scalars().all()
        qc_rows = []
        if "qc_contract_runs" in existing:
            qc_rows = session.execute(
                select(QcContractRunRow).where(
                    QcContractRunRow.started_at.is_not(None),
                )
            ).scalars().all()
            qc_rows = [r for r in qc_rows if r.started_at and r.started_at.date() == target]
        workflow_rows = []
        if "workflow_runs" in existing:
            workflow_rows = session.execute(select(WorkflowRunRow)).scalars().all()
            workflow_rows = [r for r in workflow_rows if r.started_at and r.started_at.date() == target]

        # 索引：dataset -> ads 行
        ads_by_key = {(r.dataset_id, r.version): r for r in ads_rows}
        ml_ready_by_key: dict[tuple[str, str], MlReadyDatasetRow] = {}
        for r in ml_ready_rows:
            ml_ready_by_key[(r.dataset_id, r.version)] = r

        for policy in policies.policies:
            if not policy.enabled:
                continue
            if persist and "sla_policies" in existing:
                _upsert_policy(session, policy)

            datasets = _select_datasets(
                policy=policy,
                ads_rows=ads_rows,
                dim_rows=dim_rows,
            )
            if not datasets and policy.dataset_pattern is None and policy.dataset_family is None:
                results.append(_skipped_result(policy=policy, dt=target, reason="no datasets matched policy scope"))
                continue
            for dataset_id, version, family in datasets:
                ads_row = ads_by_key.get((dataset_id, version))
                qc_rate = ads_row.qc_pass_rate if ads_row else None
                etl_rate = ads_row.etl_success_rate if ads_row else None
                wf_rate = ads_row.workflow_success_rate if ads_row else None

                ml_ready = ml_ready_by_key.get((dataset_id, version))
                qc_runs_for_ds = [r for r in qc_rows if r.dataset_id == dataset_id and r.version == version]
                failed_workflows = sum(
                    1 for r in workflow_rows
                    if (r.status or "").upper() in ("FAILED", "ERROR")
                )

                missing_outputs: list[str] = []
                for required in policy.required_outputs:
                    name = required.lower()
                    present = _output_present(
                        name=name, ads_row=ads_row, ml_ready=ml_ready, qc_runs=qc_runs_for_ds,
                    )
                    if not present:
                        missing_outputs.append(required)

                # 状态判定
                status, reason = _evaluate(
                    policy=policy,
                    qc_rate=qc_rate,
                    etl_rate=etl_rate,
                    failed_workflows=failed_workflows,
                    missing_outputs=missing_outputs,
                )

                check = SlaCheckResult(
                    check_id=f"slc-{target.isoformat()}-{policy.policy_id}-{dataset_id}-{version}-{uuid.uuid4().hex[:6]}",
                    policy_id=policy.policy_id,
                    policy_name=policy.policy_name,
                    dt=target.isoformat(),
                    dataset_id=dataset_id,
                    version=version,
                    status=status,
                    qc_pass_rate=qc_rate,
                    etl_success_rate=etl_rate,
                    workflow_success_rate=wf_rate,
                    missing_outputs=missing_outputs,
                    failed_reason=reason,
                    metrics={
                        "qc_runs": len(qc_runs_for_ds),
                        "ml_ready_uri": ml_ready.output_uri if ml_ready else None,
                        "failed_workflows": failed_workflows,
                        "dataset_family": family,
                    },
                )
                results.append(check)
                if persist and "sla_checks" in existing:
                    _persist_check(session, check, target)

        if persist:
            session.commit()
    return results


def render_sla_report(
    *,
    checks: list[SlaCheckResult],
    output_dir: Path,
    date_: str | None,
) -> SlaReportArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = date_ or (checks[0].dt if checks else datetime.utcnow().date().isoformat())
    payload = [c.to_dict() for c in checks]
    json_path = output_dir / "sla_report.json"
    json_path.write_text(
        json.dumps({"dt": target, "checks": payload}, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR.as_posix()),
        autoescape=select_autoescape(["html", "j2"]),
    )
    env.globals["fmt_rate"] = lambda v: "-" if v is None else f"{v * 100:.1f}%"
    env.globals["group_by_status"] = _group_by_status
    html_path = output_dir / "sla_report.html"
    html_path.write_text(
        env.get_template("sla_report.html.j2").render(
            dt=target, checks=payload, generated_at=datetime.now(timezone.utc).isoformat(),
        ),
        encoding="utf-8",
    )

    failed_rows = [c.to_dict() for c in checks if c.status in ("FAIL", "WARN")]
    csv_path = output_dir / "sla_failed_datasets.csv"
    _write_csv(csv_path, failed_rows)

    return SlaReportArtifacts(
        sla_report_html=html_path,
        sla_report_json=json_path,
        sla_failed_datasets=csv_path,
    )


# ---------- internal ----------


def _ensure_engine(db_uri: str | None) -> Engine:
    resolved = resolve_db_uri(db_uri)
    engine = get_engine(resolved)
    if engine.dialect.name == "sqlite":
        init_db(resolved)
        ensure_lake_tables(engine)
    return engine


def _select_datasets(
    *,
    policy: SlaPolicy,
    ads_rows: Iterable[AdsQualityDashboardRow],
    dim_rows: Iterable[DimDatasetRow],
) -> list[tuple[str, str, str | None]]:
    """根据 policy 的 dataset_pattern / dataset_family 圈定 dataset 集合。

    优先用 ads_rows（当日有指标），其次用 dim_dataset 兜底。
    """
    out: list[tuple[str, str, str | None]] = []
    seen: set[tuple[str, str]] = set()

    def matches(dataset_id: str, family: str | None) -> bool:
        if policy.dataset_family and family != policy.dataset_family:
            return False
        if policy.dataset_pattern and not fnmatch.fnmatch(dataset_id, policy.dataset_pattern):
            return False
        return True

    for r in ads_rows:
        if r.dataset_id is None or r.version is None:
            continue
        if not matches(r.dataset_id, r.dataset_family):
            continue
        key = (r.dataset_id, r.version)
        if key in seen:
            continue
        seen.add(key)
        out.append((r.dataset_id, r.version, r.dataset_family))
    for r in dim_rows:
        if r.dataset_id is None or r.version is None:
            continue
        key = (r.dataset_id, r.version)
        if key in seen:
            continue
        if not matches(r.dataset_id, r.dataset_family):
            continue
        seen.add(key)
        out.append((r.dataset_id, r.version, r.dataset_family))
    return out


def _output_present(
    *,
    name: str,
    ads_row: AdsQualityDashboardRow | None,
    ml_ready: MlReadyDatasetRow | None,
    qc_runs: list[QcContractRunRow],
) -> bool:
    name = name.lower()
    if name in ("qc_contract", "qc"):
        return any((r.status or "").upper() in ("PASS", "WARN") for r in qc_runs)
    if name in ("ml_ready", "ml-ready"):
        return ml_ready is not None and ml_ready.output_uri is not None
    if name == "ads":
        return ads_row is not None
    if name in ("dwd", "dwd_bytes"):
        return ads_row is not None and (ads_row.dwd_bytes or 0) > 0
    if name in ("ods", "raw", "raw_bytes"):
        return ads_row is not None and (ads_row.raw_bytes or 0) > 0
    # 兜底：把 name 与已知字段做一次包含匹配
    if ads_row is None:
        return False
    return any(name in c for c in ("ads", "dwd", "ods", "raw"))


def _evaluate(
    *,
    policy: SlaPolicy,
    qc_rate: float | None,
    etl_rate: float | None,
    failed_workflows: int,
    missing_outputs: list[str],
) -> tuple[str, str | None]:
    """根据 policy 阈值给出 PASS / WARN / FAIL + 失败原因。"""
    reasons: list[str] = []
    failing = False
    warning = False

    if missing_outputs:
        reasons.append(f"missing_outputs={missing_outputs}")
        failing = True

    if policy.min_qc_pass_rate is not None:
        if qc_rate is None:
            reasons.append("qc_pass_rate=NULL")
            warning = True
        elif qc_rate < policy.min_qc_pass_rate:
            reasons.append(f"qc_pass_rate {qc_rate:.3f}<{policy.min_qc_pass_rate:.3f}")
            failing = True

    if policy.min_etl_success_rate is not None:
        if etl_rate is None:
            reasons.append("etl_success_rate=NULL")
            warning = True
        elif etl_rate < policy.min_etl_success_rate:
            reasons.append(f"etl_success_rate {etl_rate:.3f}<{policy.min_etl_success_rate:.3f}")
            failing = True

    if policy.max_failed_workflows is not None:
        if failed_workflows > policy.max_failed_workflows:
            reasons.append(f"failed_workflows {failed_workflows}>{policy.max_failed_workflows}")
            failing = True

    if failing:
        return "FAIL", "; ".join(reasons) if reasons else "policy violated"
    if warning:
        return "WARN", "; ".join(reasons) if reasons else "missing inputs"
    return "PASS", None


def _skipped_result(*, policy: SlaPolicy, dt: date, reason: str) -> SlaCheckResult:
    return SlaCheckResult(
        check_id=f"slc-{dt.isoformat()}-{policy.policy_id}-skipped-{uuid.uuid4().hex[:6]}",
        policy_id=policy.policy_id,
        policy_name=policy.policy_name,
        dt=dt.isoformat(),
        dataset_id="",
        version="",
        status="SKIPPED",
        qc_pass_rate=None,
        etl_success_rate=None,
        workflow_success_rate=None,
        missing_outputs=[],
        failed_reason=reason,
        metrics={"note": reason},
    )


def _upsert_policy(session: Session, policy: SlaPolicy) -> None:
    row = session.get(SlaPolicyRow, policy.policy_id) if policy.policy_id else None
    payload = dict(
        policy_id=policy.policy_id,
        policy_name=policy.policy_name,
        dataset_pattern=policy.dataset_pattern,
        dataset_family=policy.dataset_family,
        deadline_hour=policy.deadline_hour,
        required_outputs_json=list(policy.required_outputs),
        min_qc_pass_rate=policy.min_qc_pass_rate,
        min_etl_success_rate=policy.min_etl_success_rate,
        max_failed_workflows=policy.max_failed_workflows,
        enabled=policy.enabled,
        updated_at=datetime.now(timezone.utc),
    )
    if row is None:
        if not policy.policy_id:
            return
        session.add(SlaPolicyRow(**payload))
    else:
        for k, v in payload.items():
            setattr(row, k, v)


def _persist_check(session: Session, check: SlaCheckResult, target: date) -> None:
    row = SlaCheckRow(
        check_id=check.check_id,
        policy_id=check.policy_id,
        dt=target,
        dataset_id=check.dataset_id or None,
        version=check.version or None,
        status=check.status,
        qc_pass_rate=check.qc_pass_rate,
        etl_success_rate=check.etl_success_rate,
        workflow_success_rate=check.workflow_success_rate,
        missing_outputs_json=list(check.missing_outputs),
        failed_reason=check.failed_reason,
        metrics_json=dict(check.metrics),
    )
    session.add(row)


def _group_by_status(checks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for c in checks:
        out.setdefault(c.get("status") or "UNKNOWN", []).append(c)
    return out


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                keys.append(k)
    if not keys:
        path.write_text("", encoding="utf-8")
        return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=keys)
    writer.writeheader()
    for r in rows:
        writer.writerow({k: _csv_value(r.get(k)) for k in keys})
    path.write_text(buf.getvalue(), encoding="utf-8")


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, dict, set)):
        return json.dumps(value, ensure_ascii=False)
    return value
