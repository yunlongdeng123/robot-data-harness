"""quality summary：从 ads_quality_dashboard / dws_rule_failure_daily / ads_workflow_ops_dashboard 抽取一个日度 summary。"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    AdsWorkflowOpsDashboardRow,
    DistillationDatasetRow,
    DwsInferenceJobDailyRow,
    DwsRuleFailureDailyRow,
    DwsDatasetQualityDailyRow,
    FactWorkflowStepRow,
    InferenceBenchmarkRunRow,
    ensure_lake_tables,
)
from robot_dh.warehouse_metrics.dates import parse_date_range

LOG = logging.getLogger(__name__)


@dataclass
class TopFailedRule:
    contract_id: str
    rule_id: str
    severity: str
    dataset_family: str | None
    fail_count: int
    run_count: int
    fail_rate: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "rule_id": self.rule_id,
            "severity": self.severity,
            "dataset_family": self.dataset_family,
            "fail_count": self.fail_count,
            "run_count": self.run_count,
            "fail_rate": self.fail_rate,
        }


@dataclass
class QualitySummary:
    """日度 quality summary。

    覆盖 promptB 第七节"报告内容"。
    """

    dt: str
    dataset_count: int = 0
    qc_pass_rate: float | None = None
    etl_success_rate: float | None = None
    workflow_success_rate: float | None = None
    top_failed_rules: list[TopFailedRule] = field(default_factory=list)
    p95_step_duration_sec: float | None = None
    stale_heartbeat_count: int = 0
    ml_ready_rows: int = 0
    raw_bytes: int = 0
    dwd_bytes: int = 0
    alert_level: str = "OK"
    archive_log_uris: list[str] = field(default_factory=list)
    workflow_ops: list[dict[str, Any]] = field(default_factory=list)
    dashboards: list[dict[str, Any]] = field(default_factory=list)
    # v1.9 推理运营指标。
    inference_job_count: int = 0
    inference_success_rate: float | None = None
    inference_samples_per_sec: float | None = None
    distillation_dataset_count: int = 0
    benchmark_p95_latency_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dt": self.dt,
            "dataset_count": self.dataset_count,
            "qc_pass_rate": self.qc_pass_rate,
            "etl_success_rate": self.etl_success_rate,
            "workflow_success_rate": self.workflow_success_rate,
            "top_failed_rules": [r.to_dict() for r in self.top_failed_rules],
            "p95_step_duration_sec": self.p95_step_duration_sec,
            "stale_heartbeat_count": self.stale_heartbeat_count,
            "ml_ready_rows": self.ml_ready_rows,
            "raw_bytes": self.raw_bytes,
            "dwd_bytes": self.dwd_bytes,
            "alert_level": self.alert_level,
            "archive_log_uris": list(self.archive_log_uris),
            "workflow_ops": list(self.workflow_ops),
            "dashboards": list(self.dashboards),
            "inference_job_count": self.inference_job_count,
            "inference_success_rate": self.inference_success_rate,
            "inference_samples_per_sec": self.inference_samples_per_sec,
            "distillation_dataset_count": self.distillation_dataset_count,
            "benchmark_p95_latency_ms": self.benchmark_p95_latency_ms,
        }


def _ensure_engine(db_uri: str | None) -> Engine:
    resolved = resolve_db_uri(db_uri)
    engine = get_engine(resolved)
    if engine.dialect.name == "sqlite":
        init_db(resolved)
        ensure_lake_tables(engine)
    return engine


def build_quality_summary(
    *,
    date_: str | date | None = None,
    db_uri: str | None = None,
    top_n: int = 10,
) -> QualitySummary:
    window = parse_date_range(date_=date_)
    target = window.start
    engine = _ensure_engine(db_uri)
    existing = set(inspect(engine).get_table_names())
    needed = (
        "ads_quality_dashboard",
        "ads_workflow_ops_dashboard",
        "dws_rule_failure_daily",
        "dws_dataset_quality_daily",
        "fact_workflow_step",
    )
    if not all(t in existing for t in needed):
        LOG.warning("quality summary: some v1.8 tables missing; returning empty summary. missing=%s",
                    [t for t in needed if t not in existing])
        empty = QualitySummary(dt=target.isoformat())
        _apply_inference_metrics(empty, engine, target)
        return empty

    with Session(engine, expire_on_commit=False, future=True) as session:
        ads_rows = session.execute(
            select(AdsQualityDashboardRow).where(AdsQualityDashboardRow.dt == target)
        ).scalars().all()
        dws_rows = session.execute(
            select(DwsDatasetQualityDailyRow).where(DwsDatasetQualityDailyRow.dt == target)
        ).scalars().all()
        rule_rows = session.execute(
            select(DwsRuleFailureDailyRow)
            .where(DwsRuleFailureDailyRow.dt == target)
            .where(DwsRuleFailureDailyRow.fail_count > 0)
            .order_by(DwsRuleFailureDailyRow.fail_count.desc(), DwsRuleFailureDailyRow.rule_id.asc())
            .limit(top_n)
        ).scalars().all()
        workflow_ops = session.execute(
            select(AdsWorkflowOpsDashboardRow).where(AdsWorkflowOpsDashboardRow.dt == target)
        ).scalars().all()
        step_rows = session.execute(
            select(FactWorkflowStepRow.duration_sec, FactWorkflowStepRow.archive_log_uri)
            .where(FactWorkflowStepRow.dt == target)
        ).all()

    qc_rates = [a.qc_pass_rate for a in ads_rows if a.qc_pass_rate is not None]
    etl_rates = [a.etl_success_rate for a in ads_rows if a.etl_success_rate is not None]
    wf_rates = [a.workflow_success_rate for a in ads_rows if a.workflow_success_rate is not None]
    raw_bytes = sum(int(a.raw_bytes or 0) for a in ads_rows)
    dwd_bytes = sum(int(a.dwd_bytes or 0) for a in ads_rows)
    ml_ready_rows = sum(int(a.ml_ready_rows or 0) for a in ads_rows)
    alert_levels = {(a.alert_level or "OK") for a in ads_rows}
    if "CRITICAL" in alert_levels:
        overall_alert = "CRITICAL"
    elif "WARN" in alert_levels:
        overall_alert = "WARN"
    else:
        overall_alert = "OK"

    durations = [d for d, _ in step_rows if d is not None]
    p95 = _percentile(durations, 0.95)
    archive_uris = sorted({u for _, u in step_rows if u})

    summary = QualitySummary(
        dt=target.isoformat(),
        dataset_count=len({(a.dataset_id, a.version) for a in ads_rows}),
        qc_pass_rate=_average(qc_rates),
        etl_success_rate=_average(etl_rates),
        workflow_success_rate=_average(wf_rates),
        top_failed_rules=[
            TopFailedRule(
                contract_id=r.contract_id,
                rule_id=r.rule_id,
                severity=r.severity,
                dataset_family=r.dataset_family,
                fail_count=int(r.fail_count or 0),
                run_count=int(r.run_count or 0),
                fail_rate=r.fail_rate,
            )
            for r in rule_rows
        ],
        p95_step_duration_sec=p95,
        stale_heartbeat_count=sum(int(d.stale_heartbeat_count or 0) for d in dws_rows),
        ml_ready_rows=ml_ready_rows,
        raw_bytes=raw_bytes,
        dwd_bytes=dwd_bytes,
        alert_level=overall_alert,
        archive_log_uris=archive_uris,
        workflow_ops=[
            {
                "workflow_type": w.workflow_type,
                "workflow_count": w.workflow_count,
                "success_count": w.success_count,
                "failed_count": w.failed_count,
                "success_rate": w.success_rate,
                "p95_duration_sec": w.p95_duration_sec,
                "alert_level": w.alert_level,
                "alert_reason": w.alert_reason,
            }
            for w in workflow_ops
        ],
        dashboards=[
            {
                "dataset_id": a.dataset_id,
                "version": a.version,
                "dataset_family": a.dataset_family,
                "overall_status": a.overall_status,
                "quality_score": a.quality_score,
                "qc_pass_rate": a.qc_pass_rate,
                "etl_success_rate": a.etl_success_rate,
                "workflow_success_rate": a.workflow_success_rate,
                "top_failed_rule": a.top_failed_rule,
                "top_failed_rule_count": a.top_failed_rule_count,
                "p95_duration_sec": a.p95_duration_sec,
                "ml_ready_rows": a.ml_ready_rows,
                "raw_bytes": a.raw_bytes,
                "dwd_bytes": a.dwd_bytes,
                "alert_level": a.alert_level,
                "alert_reason": a.alert_reason,
            }
            for a in ads_rows
        ],
    )
    _apply_inference_metrics(summary, engine, target)
    return summary


def _apply_inference_metrics(summary: QualitySummary, engine: Engine, target: date) -> None:
    """补充 v1.9 推理运营指标（表不存在时静默跳过，保持向后兼容）。"""
    existing = set(inspect(engine).get_table_names())
    if "dws_inference_job_daily" in existing:
        with Session(engine, expire_on_commit=False, future=True) as session:
            dws = session.execute(
                select(DwsInferenceJobDailyRow).where(DwsInferenceJobDailyRow.dt == target)
            ).scalars().all()
        job_count = sum(int(d.job_count or 0) for d in dws)
        success = sum(int(d.success_count or 0) for d in dws)
        sps = [d.samples_per_sec for d in dws if d.samples_per_sec is not None]
        summary.inference_job_count = job_count
        summary.inference_success_rate = (success / job_count) if job_count else None
        summary.inference_samples_per_sec = _average(sps) if sps else None

    if "distillation_datasets" in existing:
        with Session(engine, expire_on_commit=False, future=True) as session:
            distills = session.execute(select(DistillationDatasetRow)).scalars().all()
        summary.distillation_dataset_count = sum(
            1 for d in distills if d.created_at and d.created_at.date() == target
        )

    if "inference_benchmark_runs" in existing:
        with Session(engine, expire_on_commit=False, future=True) as session:
            benches = session.execute(select(InferenceBenchmarkRunRow)).scalars().all()
        p95s = [
            b.p95_latency_ms for b in benches
            if b.p95_latency_ms is not None and b.created_at and b.created_at.date() == target
        ]
        summary.benchmark_p95_latency_ms = _average(p95s) if p95s else None


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    sorted_vals = sorted(v for v in values if v is not None)
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac)


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))
