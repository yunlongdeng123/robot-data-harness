"""warehouse build：dim/fact/dws/ads 串接的物化 builder。

两套执行路径：

1. PostgreSQL（远端、生产）
   - 直接执行 warehouse/sql/dml/build_*.sql。
   - 需要 v1.8 schema 已 apply（infra 项目执行）；缺表抛 ``WarehouseSchemaMissingError``。
   - SQL 内部用 ON CONFLICT DO UPDATE 做 UPSERT，幂等。

2. SQLite（本地测试 / make test）
   - SQL 大量 PostgreSQL 特性（jsonb_array_elements / LATERAL / PERCENTILE_CONT），
     SQLite 不支持 → 走 Python 端简化口径：用 ORM 直接聚合。
   - 简化口径仅覆盖 promptB 第十一节列出的"空数据也能跑通 + 关键指标算对"。

层级顺序：dim → fact → dws → ads（promptB 第四节）。
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from sqlalchemy import delete, func, inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsInferenceDashboardRow,
    AdsQualityDashboardRow,
    AdsWorkflowOpsDashboardRow,
    AssetProfileRow,
    DatasetVersionRow,
    DimDatasetRow,
    DwsDatasetQualityDailyRow,
    DwsInferenceJobDailyRow,
    DwsRuleFailureDailyRow,
    DwsWorkflowOpsDailyRow,
    EtlPerfRunRow,
    FactAssetProfileRow,
    FactEtlRunRow,
    FactQcRuleResultRow,
    FactWorkflowStepRow,
    INFERENCE_PLANE_TABLES,
    InferenceFailureRow,
    InferenceJobRow,
    InferenceOutputRow,
    MlReadyDatasetRow,
    ModelRegistryRow,
    QcContractRunRow,
    QualitySnapshotRow,
    WAREHOUSE_METRICS_TABLES,
    WorkflowRunRow,
    WorkflowStepRow,
    ensure_lake_tables,
)
from robot_dh.warehouse_metrics.config import WarehouseMetricsConfig
from robot_dh.warehouse_metrics.dates import DateRange
from robot_dh.warehouse_metrics.models import (
    LayerBuildResult,
    WarehouseBuildReport,
)
from robot_dh.warehouse_metrics.sql_runner import SqlExecution, SqlTemplateRunner

LOG = logging.getLogger(__name__)

BUILD_SQL_SEQUENCE: list[tuple[str, str]] = [
    ("dim", "warehouse/sql/dml/build_dim_dataset.sql"),
    ("fact", "warehouse/sql/dml/build_fact_etl_run.sql"),
    ("fact", "warehouse/sql/dml/build_fact_qc_rule_result.sql"),
    ("fact", "warehouse/sql/dml/build_fact_workflow_step.sql"),
    ("fact", "warehouse/sql/dml/build_fact_asset_profile.sql"),
    ("dws", "warehouse/sql/dml/build_dws_dataset_quality_daily.sql"),
    ("dws", "warehouse/sql/dml/build_dws_rule_failure_daily.sql"),
    ("dws", "warehouse/sql/dml/build_dws_workflow_ops_daily.sql"),
    ("ads", "warehouse/sql/dml/build_ads_quality_dashboard.sql"),
    ("ads", "warehouse/sql/dml/build_ads_workflow_ops_dashboard.sql"),
    # v1.9 推理运营层（仅在 layers 含 inference 时执行）。
    ("inference", "warehouse/sql/dml/build_dws_inference_job_daily.sql"),
    ("inference", "warehouse/sql/dml/build_ads_inference_dashboard.sql"),
]

# warehouse build 支持的层级（v1.8 四层 + v1.9 inference）。
SUPPORTED_BUILD_LAYERS: tuple[str, ...] = ("dim", "fact", "dws", "ads", "inference")


class WarehouseSchemaMissingError(RuntimeError):
    """v1.8 表缺失时抛出，提示先在 infra 项目执行 migration。"""


@dataclass
class WarehouseInitReport:
    backend: str
    schema: str
    existing_tables: list[str]
    missing_tables: list[str]
    applied_ddl: bool = False
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "schema": self.schema,
            "existing_tables": list(self.existing_tables),
            "missing_tables": list(self.missing_tables),
            "applied_ddl": self.applied_ddl,
            "notes": list(self.notes),
        }


class WarehouseBuilder:
    """v1.8 warehouse build 入口。

    Args:
        config: warehouse 配置（schema / sql_root）。
        db_uri: 显式 DB URI；空则走 ROBOT_DH_DB_URI / SQLite 默认。
    """

    def __init__(
        self,
        *,
        config: WarehouseMetricsConfig | None = None,
        db_uri: str | None = None,
    ) -> None:
        self._config = config or WarehouseMetricsConfig()
        self._db_uri = db_uri
        self._engine: Engine | None = None

    @property
    def config(self) -> WarehouseMetricsConfig:
        return self._config

    def get_engine(self) -> Engine:
        if self._engine is None:
            resolved = resolve_db_uri(self._db_uri)
            engine = get_engine(resolved)
            if engine.dialect.name == "sqlite":
                init_db(resolved)
                ensure_lake_tables(engine)
            self._engine = engine
        return self._engine

    def get_sql_runner(self) -> SqlTemplateRunner:
        return SqlTemplateRunner(
            engine=self.get_engine(),
            sql_root=self._config.sql_root,
            default_params={"schema": self._config.schema},
        )

    # ---------- init / check ----------

    def init_check(self, *, apply_ddl: bool = False) -> WarehouseInitReport:
        """检查 v1.8 表是否齐全；apply_ddl=True 时按顺序执行 DDL 文件（仅对 SQLite 推荐）。"""
        engine = self.get_engine()
        existing = set(inspect(engine).get_table_names())
        missing = [t for t in WAREHOUSE_METRICS_TABLES if t not in existing]
        notes: list[str] = []
        applied = False

        if missing:
            hint = (
                "请先在 infra 项目执行 ./scripts/39_pg_apply_v1_8_schema.sh "
                "（或在本地 SQLite 用 robot-dh warehouse init --apply-ddl 建简化表）。"
            )
            notes.append(f"missing v1.8 tables: {', '.join(missing)}; {hint}")

        if apply_ddl and missing:
            ddl_dir = (self._config.sql_root / "ddl").resolve()
            if ddl_dir.exists():
                runner = self.get_sql_runner()
                for sql_path in sorted(ddl_dir.glob("*.sql")):
                    rel = sql_path.relative_to(self._config.sql_root.parent.resolve()) if sql_path.is_absolute() else sql_path
                    exec_res = runner.execute(str(rel), params={"schema": self._config.schema})
                    if exec_res.status == "error":
                        notes.append(f"ddl {sql_path.name} failed: {exec_res.error}")
                    else:
                        notes.append(f"ddl {sql_path.name} applied (rows={exec_res.affected_rows})")
                existing = set(inspect(engine).get_table_names())
                missing = [t for t in WAREHOUSE_METRICS_TABLES if t not in existing]
                applied = True

        return WarehouseInitReport(
            backend=engine.dialect.name,
            schema=self._config.schema,
            existing_tables=sorted(t for t in WAREHOUSE_METRICS_TABLES if t in existing),
            missing_tables=missing,
            applied_ddl=applied,
            notes=notes,
        )

    # ---------- build ----------

    def build(
        self,
        *,
        window: DateRange,
        layers: Iterable[str] | None = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> WarehouseBuildReport:
        engine = self.get_engine()
        backend = engine.dialect.name
        layer_filter = tuple((l or "").lower() for l in layers) if layers else self._config.default_layers
        layer_set = {l for l in layer_filter if l in SUPPORTED_BUILD_LAYERS}
        if not layer_set:
            raise ValueError(f"no valid layers selected: {layers}")
        started = datetime.now(timezone.utc)
        results: list[LayerBuildResult] = []
        warnings: list[str] = []

        if backend == "postgresql":
            results, warnings = self._build_postgres(
                window=window,
                layer_set=layer_set,
                dry_run=dry_run,
                force=force,
            )
        elif backend == "sqlite":
            results, warnings = self._build_sqlite(
                window=window,
                layer_set=layer_set,
                dry_run=dry_run,
                force=force,
            )
        else:
            raise WarehouseSchemaMissingError(
                f"unsupported backend '{backend}'; only PostgreSQL / SQLite supported"
            )

        finished = datetime.now(timezone.utc)
        status = "ok"
        if any(r.status == "error" for r in results):
            status = "fail"
        elif any(r.status == "warn" for r in results) or warnings:
            status = "warn"

        return WarehouseBuildReport(
            start_date=window.start.isoformat(),
            end_date=window.end.isoformat(),
            layers=sorted(layer_set),
            backend=backend,
            schema=self._config.schema,
            dry_run=dry_run,
            started_at=started.isoformat(),
            finished_at=finished.isoformat(),
            duration_sec=(finished - started).total_seconds(),
            status=status,
            results=results,
            warnings=warnings,
        )

    # ---------- PostgreSQL 路径 ----------

    def _build_postgres(
        self,
        *,
        window: DateRange,
        layer_set: set[str],
        dry_run: bool,
        force: bool,
    ) -> tuple[list[LayerBuildResult], list[str]]:
        engine = self.get_engine()
        warnings: list[str] = []
        results: list[LayerBuildResult] = []
        existing = set(inspect(engine).get_table_names())
        # 仅校验本次请求层级真正依赖的表。
        v18_layers = {"dim", "fact", "dws", "ads"}
        if layer_set & v18_layers:
            missing = [t for t in WAREHOUSE_METRICS_TABLES if t not in existing]
            if missing and not dry_run:
                raise WarehouseSchemaMissingError(
                    "v1.8 表缺失，请先在 infra 项目执行 v1.8 schema migration: "
                    f"missing={missing}"
                )
        if "inference" in layer_set:
            missing_inf = [t for t in INFERENCE_PLANE_TABLES if t not in existing]
            if missing_inf and not dry_run:
                raise WarehouseSchemaMissingError(
                    "v1.9 推理表缺失，请先在 infra 项目执行 "
                    "./scripts/45_pg_apply_inference_schema.sh: "
                    f"missing={missing_inf}"
                )

        runner = self.get_sql_runner()
        params = {
            "schema": self._config.schema,
            "start_date": window.start.isoformat(),
            "end_date": window.end.isoformat(),
        }
        for layer, sql_file in BUILD_SQL_SEQUENCE:
            if layer not in layer_set:
                continue
            exec_res = runner.execute(sql_file, params=params, dry_run=dry_run)
            results.append(_to_layer_result(layer, exec_res))
            if exec_res.status == "error":
                LOG.error("warehouse build %s FAILED: %s", sql_file, exec_res.error)
        return results, warnings

    # ---------- SQLite 路径（Python 端聚合） ----------

    def _build_sqlite(
        self,
        *,
        window: DateRange,
        layer_set: set[str],
        dry_run: bool,
        force: bool,
    ) -> tuple[list[LayerBuildResult], list[str]]:
        engine = self.get_engine()
        warnings: list[str] = []
        results: list[LayerBuildResult] = []

        steps: list[tuple[str, str, Callable[[Session, DateRange], int]]] = []
        if "dim" in layer_set:
            steps.append(("dim", "build_dim_dataset.sql (sqlite simplified)", self._sqlite_build_dim_dataset))
        if "fact" in layer_set:
            steps.extend([
                ("fact", "build_fact_etl_run.sql (sqlite simplified)", self._sqlite_build_fact_etl_run),
                ("fact", "build_fact_qc_rule_result.sql (sqlite simplified)", self._sqlite_build_fact_qc_rule_result),
                ("fact", "build_fact_workflow_step.sql (sqlite simplified)", self._sqlite_build_fact_workflow_step),
                ("fact", "build_fact_asset_profile.sql (sqlite simplified)", self._sqlite_build_fact_asset_profile),
            ])
        if "dws" in layer_set:
            steps.extend([
                ("dws", "build_dws_dataset_quality_daily.sql (sqlite simplified)", self._sqlite_build_dws_dataset_quality_daily),
                ("dws", "build_dws_rule_failure_daily.sql (sqlite simplified)", self._sqlite_build_dws_rule_failure_daily),
                ("dws", "build_dws_workflow_ops_daily.sql (sqlite simplified)", self._sqlite_build_dws_workflow_ops_daily),
            ])
        if "ads" in layer_set:
            steps.extend([
                ("ads", "build_ads_quality_dashboard.sql (sqlite simplified)", self._sqlite_build_ads_quality_dashboard),
                ("ads", "build_ads_workflow_ops_dashboard.sql (sqlite simplified)", self._sqlite_build_ads_workflow_ops_dashboard),
            ])
        if "inference" in layer_set:
            steps.extend([
                ("inference", "build_dws_inference_job_daily.sql (sqlite simplified)", self._sqlite_build_dws_inference_job_daily),
                ("inference", "build_ads_inference_dashboard.sql (sqlite simplified)", self._sqlite_build_ads_inference_dashboard),
            ])

        for layer, sql_file, fn in steps:
            started = datetime.now(timezone.utc)
            if dry_run:
                results.append(
                    LayerBuildResult(
                        layer=layer, sql_file=sql_file, status="dry-run",
                        duration_sec=0.0, affected_rows="unknown",
                        note="sqlite-simplified path; would run Python aggregation",
                    )
                )
                continue
            try:
                with Session(engine, expire_on_commit=False, future=True) as session:
                    rows = fn(session, window)
                    session.commit()
                duration = (datetime.now(timezone.utc) - started).total_seconds()
                results.append(
                    LayerBuildResult(
                        layer=layer, sql_file=sql_file, status="ok",
                        duration_sec=duration, affected_rows=rows,
                        note="sqlite-simplified path",
                    )
                )
            except SQLAlchemyError as err:
                duration = (datetime.now(timezone.utc) - started).total_seconds()
                results.append(
                    LayerBuildResult(
                        layer=layer, sql_file=sql_file, status="error",
                        duration_sec=duration, error=f"{type(err).__name__}: {err}",
                    )
                )
                LOG.error("sqlite warehouse build %s FAILED: %s", sql_file, err)

        return results, warnings

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    # 关键聚合函数（Python 端，单元测试可断言确定性输出）。

    def _sqlite_build_dim_dataset(self, session: Session, window: DateRange) -> int:
        affected = 0
        dataset_versions = session.execute(select(DatasetVersionRow)).scalars().all()
        for dv in dataset_versions:
            key = f"dataset:{dv.dataset_id}:{dv.version}"
            latest_qs = session.execute(
                select(QualitySnapshotRow)
                .where(QualitySnapshotRow.dataset_id == dv.dataset_id)
                .where(QualitySnapshotRow.version == dv.version)
                .order_by(QualitySnapshotRow.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            latest_mlr = session.execute(
                select(MlReadyDatasetRow)
                .where(MlReadyDatasetRow.dataset_id == dv.dataset_id)
                .where(MlReadyDatasetRow.version == dv.version)
                .order_by(MlReadyDatasetRow.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()
            family = None
            if latest_qs and isinstance(latest_qs.metrics_json, dict):
                family = latest_qs.metrics_json.get("dataset_family")
            row = session.get(DimDatasetRow, key)
            if row is None:
                row = DimDatasetRow(
                    dataset_key=key,
                    dataset_id=dv.dataset_id,
                    version=dv.version,
                    dataset_family=family,
                    source_uri=dv.raw_uri,
                    raw_uri=dv.raw_uri,
                    ods_uri=dv.ods_uri,
                    dwd_uri=dv.dwd_uri,
                    ml_ready_uri=latest_mlr.output_uri if latest_mlr else None,
                    first_seen_at=dv.created_at,
                    latest_status=(latest_qs.quality_status if latest_qs else dv.status),
                    latest_quality_score=(latest_qs.quality_score if latest_qs else None),
                    is_active=True,
                    updated_at=self._utcnow(),
                )
                session.add(row)
            else:
                row.dataset_family = family or row.dataset_family
                row.raw_uri = dv.raw_uri or row.raw_uri
                row.ods_uri = dv.ods_uri or row.ods_uri
                row.dwd_uri = dv.dwd_uri or row.dwd_uri
                if latest_mlr is not None:
                    row.ml_ready_uri = latest_mlr.output_uri
                if latest_qs is not None:
                    row.latest_status = latest_qs.quality_status
                    row.latest_quality_score = latest_qs.quality_score
                else:
                    row.latest_status = row.latest_status or dv.status
                row.updated_at = self._utcnow()
            affected += 1
        return affected

    def _sqlite_build_fact_etl_run(self, session: Session, window: DateRange) -> int:
        affected = 0
        rows = session.execute(
            select(EtlPerfRunRow).where(
                EtlPerfRunRow.started_at.is_not(None),
                func.date(EtlPerfRunRow.started_at) >= window.start.isoformat(),
                func.date(EtlPerfRunRow.started_at) <= window.end.isoformat(),
            )
        ).scalars().all()
        for r in rows:
            key = _md5_pipe(r.job_id, r.run_id, r.phase, r.dataset_id, r.version)
            dt_val = r.started_at.date() if r.started_at else None
            existing = session.get(FactEtlRunRow, key)
            archive_log = None
            if isinstance(r.metrics_json, dict):
                archive_log = r.metrics_json.get("archive_log_uri")
            payload = dict(
                run_key=key,
                job_id=r.job_id,
                run_id=r.run_id,
                dataset_id=r.dataset_id,
                version=r.version,
                phase=r.phase,
                status=r.status,
                started_at=r.started_at,
                finished_at=r.finished_at,
                dt=dt_val,
                duration_sec=r.duration_sec,
                input_bytes=r.input_bytes,
                output_bytes=r.output_bytes,
                input_rows=r.input_rows,
                output_rows=r.output_rows,
                peak_memory_mb=r.peak_memory_mb,
                error_message=r.error_message,
                archive_log_uri=archive_log,
            )
            if existing is None:
                session.add(FactEtlRunRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_fact_qc_rule_result(self, session: Session, window: DateRange) -> int:
        affected = 0
        runs = session.execute(
            select(QcContractRunRow).where(
                QcContractRunRow.started_at.is_not(None),
                func.date(QcContractRunRow.started_at) >= window.start.isoformat(),
                func.date(QcContractRunRow.started_at) <= window.end.isoformat(),
            )
        ).scalars().all()
        for run in runs:
            dt_val = run.started_at.date() if run.started_at else None
            failed = run.failed_rules_json if isinstance(run.failed_rules_json, list) else []
            warning = run.warning_rules_json if isinstance(run.warning_rules_json, list) else []
            buckets: list[tuple[str, dict[str, Any]]] = [
                *(("FAIL", item) for item in failed if isinstance(item, dict)),
                *(("WARN", item) for item in warning if isinstance(item, dict)),
            ]
            for status, item in buckets:
                rule_id = str(item.get("rule_id") or "")
                if not rule_id:
                    continue
                severity = str(item.get("severity") or "unknown")
                metric = item.get("metric")
                op = item.get("op")
                threshold = item.get("threshold")
                actual = item.get("actual")
                rule_key = _md5_pipe(run.run_id, severity, rule_id, str(metric) if metric else "")
                payload = dict(
                    rule_result_key=rule_key,
                    run_id=run.run_id,
                    contract_id=run.contract_id,
                    dataset_id=run.dataset_id,
                    version=run.version,
                    dataset_family=run.dataset_family,
                    rule_id=rule_id,
                    severity=severity,
                    status=status,
                    metric=str(metric) if metric is not None else None,
                    op=str(op) if op is not None else None,
                    threshold_value=str(threshold) if threshold is not None else None,
                    actual_value=str(actual) if actual is not None else None,
                    dt=dt_val,
                )
                existing = session.get(FactQcRuleResultRow, rule_key)
                if existing is None:
                    session.add(FactQcRuleResultRow(**payload))
                else:
                    for k, v in payload.items():
                        setattr(existing, k, v)
                affected += 1
            # 总是写一条 contract_status summary 行
            summary_key = _md5_pipe(run.run_id, "summary", "contract_status", "")
            summary_payload = dict(
                rule_result_key=summary_key,
                run_id=run.run_id,
                contract_id=run.contract_id,
                dataset_id=run.dataset_id,
                version=run.version,
                dataset_family=run.dataset_family,
                rule_id="contract_status",
                severity="summary",
                status=run.status,
                metric=None,
                op=None,
                threshold_value=None,
                actual_value=None,
                dt=dt_val,
            )
            existing_sum = session.get(FactQcRuleResultRow, summary_key)
            if existing_sum is None:
                session.add(FactQcRuleResultRow(**summary_payload))
            else:
                for k, v in summary_payload.items():
                    setattr(existing_sum, k, v)
            affected += 1
        return affected

    def _sqlite_build_fact_workflow_step(self, session: Session, window: DateRange) -> int:
        affected = 0
        steps = session.execute(
            select(WorkflowStepRow).where(
                WorkflowStepRow.started_at.is_not(None),
                func.date(WorkflowStepRow.started_at) >= window.start.isoformat(),
                func.date(WorkflowStepRow.started_at) <= window.end.isoformat(),
            )
        ).scalars().all()
        for ws in steps:
            dt_val = ws.started_at.date() if ws.started_at else None
            wr = session.execute(
                select(WorkflowRunRow).where(
                    WorkflowRunRow.workflow_name == ws.workflow_name,
                    WorkflowRunRow.workflow_namespace.is_(ws.workflow_namespace) if ws.workflow_namespace is None else WorkflowRunRow.workflow_namespace == ws.workflow_namespace,
                ).limit(1)
            ).scalar_one_or_none()
            workflow_type = wr.workflow_type if wr is not None else None
            metrics = ws.metrics_json if isinstance(ws.metrics_json, dict) else {}
            key = _md5_pipe(ws.workflow_namespace, ws.workflow_name, ws.step_name, ws.pod_name)
            payload = dict(
                step_key=key,
                workflow_name=ws.workflow_name,
                workflow_namespace=ws.workflow_namespace,
                workflow_type=workflow_type,
                step_name=ws.step_name,
                template_name=ws.template_name,
                pod_name=ws.pod_name,
                phase=ws.phase,
                dataset_id=ws.dataset_id,
                version=ws.version,
                dataset_family=ws.dataset_family,
                started_at=ws.started_at,
                finished_at=ws.finished_at,
                dt=dt_val,
                duration_sec=ws.duration_sec,
                exit_code=_safe_int(metrics.get("exit_code")),
                container_reason=_safe_str(metrics.get("container_reason")),
                archive_log_uri=_safe_str(metrics.get("archive_log_uri")),
                archive_log_url=_safe_str(metrics.get("archive_log_url")),
            )
            existing = session.get(FactWorkflowStepRow, key)
            if existing is None:
                session.add(FactWorkflowStepRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_fact_asset_profile(self, session: Session, window: DateRange) -> int:
        affected = 0
        profiles = session.execute(
            select(AssetProfileRow).where(
                AssetProfileRow.created_at.is_not(None),
                func.date(AssetProfileRow.created_at) >= window.start.isoformat(),
                func.date(AssetProfileRow.created_at) <= window.end.isoformat(),
            )
        ).scalars().all()
        for ap in profiles:
            key = hashlib.md5((ap.profile_id or "").encode()).hexdigest()
            dt_val = ap.created_at.date() if ap.created_at else None
            payload = dict(
                asset_profile_key=key,
                profile_id=ap.profile_id,
                dataset_id=ap.dataset_id,
                version=ap.version,
                dataset_family=ap.dataset_family,
                asset_uri=ap.asset_uri,
                asset_format=ap.asset_format,
                layer=ap.layer,
                bytes=ap.bytes,
                rows=ap.rows,
                files_count=ap.files_count,
                episodes_count=ap.episodes_count,
                videos_count=ap.videos_count,
                schema_hash=ap.schema_hash,
                null_rate=ap.null_rate,
                status=ap.status,
                dt=dt_val,
            )
            existing = session.get(FactAssetProfileRow, key)
            if existing is None:
                session.add(FactAssetProfileRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_dws_dataset_quality_daily(self, session: Session, window: DateRange) -> int:
        from collections import defaultdict

        etl_rows = session.execute(
            select(FactEtlRunRow).where(
                FactEtlRunRow.dt.is_not(None),
                FactEtlRunRow.dt >= window.start,
                FactEtlRunRow.dt <= window.end,
            )
        ).scalars().all()
        qc_rows = session.execute(
            select(FactQcRuleResultRow).where(
                FactQcRuleResultRow.dt.is_not(None),
                FactQcRuleResultRow.dt >= window.start,
                FactQcRuleResultRow.dt <= window.end,
                FactQcRuleResultRow.rule_id == "contract_status",
            )
        ).scalars().all()
        wf_rows = session.execute(
            select(FactWorkflowStepRow).where(
                FactWorkflowStepRow.dt.is_not(None),
                FactWorkflowStepRow.dt >= window.start,
                FactWorkflowStepRow.dt <= window.end,
            )
        ).scalars().all()
        mlr_rows = session.execute(
            select(MlReadyDatasetRow).where(MlReadyDatasetRow.created_at.is_not(None))
        ).scalars().all()
        quality_rows = session.execute(
            select(QualitySnapshotRow).where(QualitySnapshotRow.created_at.is_not(None))
        ).scalars().all()

        agg: dict[tuple[date, str, str], dict[str, Any]] = defaultdict(lambda: {
            "dataset_family": None,
            "etl_runs": 0, "etl_success": 0, "etl_fail": 0,
            "etl_durations": [], "input_bytes": 0, "output_bytes": 0,
            "qc_runs": 0, "qc_pass": 0, "qc_warn": 0, "qc_fail": 0,
            "wf_names": set(), "wf_success": set(), "wf_fail": set(),
            "wf_durations": [],
            "ml_rows": 0, "quality_scores": [],
        })

        def _key(dt_val: date | None, dataset_id: str | None, version: str | None) -> tuple[date, str, str] | None:
            if dt_val is None or dataset_id is None or version is None:
                return None
            return (dt_val, dataset_id, version)

        for r in etl_rows:
            k = _key(r.dt, r.dataset_id, r.version)
            if k is None:
                continue
            status_u = (r.status or "").upper()
            # v1.8 修复：与 warehouse/sql/dml/build_dws_dataset_quality_daily.sql 同款口径
            # 1) RUNNING / PENDING / STARTED 不进分母（兜底历史 normalize EtlProfiler
            #    退出时机错位残留的孤儿，以及未来任何 early-write）；
            # 2) WARN 是 "带警告的成功"（runner.py / cli.py 一致），必须计入 success。
            if status_u in ("RUNNING", "PENDING", "STARTED"):
                continue
            b = agg[k]
            b["dataset_family"] = b["dataset_family"] or r.dataset_family
            b["etl_runs"] += 1
            if status_u in ("OK", "WARN", "SUCCESS", "SUCCEEDED"):
                b["etl_success"] += 1
            elif status_u in ("FAIL", "FAILED", "ERROR"):
                b["etl_fail"] += 1
            if r.duration_sec is not None:
                b["etl_durations"].append(r.duration_sec)
            b["input_bytes"] += int(r.input_bytes or 0)
            b["output_bytes"] += int(r.output_bytes or 0)

        for r in qc_rows:
            k = _key(r.dt, r.dataset_id, r.version)
            if k is None:
                continue
            b = agg[k]
            b["dataset_family"] = b["dataset_family"] or r.dataset_family
            b["qc_runs"] += 1
            status_u = (r.status or "").upper()
            if status_u == "PASS":
                b["qc_pass"] += 1
            elif status_u == "WARN":
                b["qc_warn"] += 1
            elif status_u == "FAIL":
                b["qc_fail"] += 1

        for r in wf_rows:
            k = _key(r.dt, r.dataset_id, r.version)
            if k is None:
                continue
            b = agg[k]
            b["dataset_family"] = b["dataset_family"] or r.dataset_family
            wf_name = r.workflow_name or ""
            b["wf_names"].add(wf_name)
            phase_u = (r.phase or "").upper()
            if phase_u == "SUCCEEDED":
                b["wf_success"].add(wf_name)
            elif phase_u in ("FAILED", "ERROR"):
                b["wf_fail"].add(wf_name)
            if r.duration_sec is not None:
                b["wf_durations"].append(r.duration_sec)

        for r in mlr_rows:
            if r.created_at is None:
                continue
            d = r.created_at.date()
            if d < window.start or d > window.end:
                continue
            k = _key(d, r.dataset_id, r.version)
            if k is None:
                continue
            b = agg[k]
            b["ml_rows"] = max(b["ml_rows"], int((r.num_train or 0) + (r.num_val or 0) + (r.num_test or 0)))

        for r in quality_rows:
            if r.created_at is None:
                continue
            d = r.created_at.date()
            if d < window.start or d > window.end:
                continue
            k = _key(d, r.dataset_id, r.version)
            if k is None:
                continue
            if r.quality_score is not None:
                agg[k]["quality_scores"].append(r.quality_score)

        affected = 0
        for (dt_val, dataset_id, version), b in agg.items():
            payload = dict(
                dt=dt_val, dataset_id=dataset_id, version=version,
                dataset_family=b["dataset_family"],
                qc_run_count=b["qc_runs"], qc_pass_count=b["qc_pass"],
                qc_warn_count=b["qc_warn"], qc_fail_count=b["qc_fail"],
                qc_pass_rate=(b["qc_pass"] / b["qc_runs"]) if b["qc_runs"] else None,
                etl_run_count=b["etl_runs"], etl_success_count=b["etl_success"],
                etl_fail_count=b["etl_fail"],
                etl_success_rate=(b["etl_success"] / b["etl_runs"]) if b["etl_runs"] else None,
                workflow_count=len(b["wf_names"]),
                workflow_success_count=len(b["wf_success"]),
                workflow_fail_count=len(b["wf_fail"]),
                workflow_success_rate=(len(b["wf_success"]) / len(b["wf_names"])) if b["wf_names"] else None,
                avg_quality_score=(sum(b["quality_scores"]) / len(b["quality_scores"])) if b["quality_scores"] else None,
                ml_ready_rows=b["ml_rows"],
                total_input_bytes=b["input_bytes"],
                total_output_bytes=b["output_bytes"],
                p95_etl_duration_sec=_percentile(b["etl_durations"], 0.95),
                p95_workflow_step_duration_sec=_percentile(b["wf_durations"], 0.95),
                stale_heartbeat_count=0,
                updated_at=self._utcnow(),
            )
            existing = session.get(DwsDatasetQualityDailyRow, (dt_val, dataset_id, version))
            if existing is None:
                session.add(DwsDatasetQualityDailyRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_dws_rule_failure_daily(self, session: Session, window: DateRange) -> int:
        from collections import defaultdict

        rows = session.execute(
            select(FactQcRuleResultRow).where(
                FactQcRuleResultRow.dt.is_not(None),
                FactQcRuleResultRow.dt >= window.start,
                FactQcRuleResultRow.dt <= window.end,
                FactQcRuleResultRow.rule_id.is_not(None),
                FactQcRuleResultRow.rule_id != "contract_status",
            )
        ).scalars().all()
        agg: dict[tuple, dict[str, int]] = defaultdict(lambda: {"run": 0, "pass": 0, "warn": 0, "fail": 0})
        for r in rows:
            key = (r.dt, r.dataset_family or "unknown", r.contract_id or "unknown", r.rule_id, r.severity or "unknown")
            b = agg[key]
            b["run"] += 1
            status_u = (r.status or "").upper()
            if status_u == "PASS":
                b["pass"] += 1
            elif status_u == "WARN":
                b["warn"] += 1
            elif status_u == "FAIL":
                b["fail"] += 1
        affected = 0
        for (dt_val, family, contract_id, rule_id, severity), b in agg.items():
            run_count = b["run"]
            payload = dict(
                dt=dt_val,
                dataset_family=family,
                contract_id=contract_id,
                rule_id=rule_id,
                severity=severity,
                run_count=run_count,
                pass_count=b["pass"],
                warn_count=b["warn"],
                fail_count=b["fail"],
                fail_rate=(b["fail"] / run_count) if run_count else None,
                updated_at=self._utcnow(),
            )
            existing = session.get(
                DwsRuleFailureDailyRow,
                (dt_val, family, contract_id, rule_id, severity),
            )
            if existing is None:
                session.add(DwsRuleFailureDailyRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_dws_workflow_ops_daily(self, session: Session, window: DateRange) -> int:
        from collections import defaultdict

        rows = session.execute(
            select(FactWorkflowStepRow).where(
                FactWorkflowStepRow.dt.is_not(None),
                FactWorkflowStepRow.dt >= window.start,
                FactWorkflowStepRow.dt <= window.end,
            )
        ).scalars().all()
        agg: dict[tuple[date, str], dict[str, Any]] = defaultdict(lambda: {
            "wf_names": set(), "wf_success": set(), "wf_failed": set(), "wf_running": set(),
            "durations": [], "deadline_exceeded": 0, "oom": 0, "nonzero_exit": 0,
        })
        for r in rows:
            workflow_type = r.workflow_type or "unknown"
            key = (r.dt, workflow_type)
            b = agg[key]
            wf_name = r.workflow_name or ""
            b["wf_names"].add(wf_name)
            phase_u = (r.phase or "").upper()
            if phase_u == "SUCCEEDED":
                b["wf_success"].add(wf_name)
            elif phase_u in ("FAILED", "ERROR"):
                b["wf_failed"].add(wf_name)
            elif phase_u in ("RUNNING", "PENDING"):
                b["wf_running"].add(wf_name)
            if r.duration_sec is not None:
                b["durations"].append(r.duration_sec)
            cr_u = (r.container_reason or "").upper()
            if cr_u == "DEADLINEEXCEEDED":
                b["deadline_exceeded"] += 1
            if cr_u == "OOMKILLED":
                b["oom"] += 1
            if (r.exit_code or 0) != 0:
                b["nonzero_exit"] += 1

        affected = 0
        for (dt_val, workflow_type), b in agg.items():
            wf_count = len(b["wf_names"])
            payload = dict(
                dt=dt_val,
                workflow_type=workflow_type,
                workflow_count=wf_count,
                success_count=len(b["wf_success"]),
                failed_count=len(b["wf_failed"]),
                running_count=len(b["wf_running"]),
                success_rate=(len(b["wf_success"]) / wf_count) if wf_count else None,
                avg_duration_sec=(sum(b["durations"]) / len(b["durations"])) if b["durations"] else None,
                p95_duration_sec=_percentile(b["durations"], 0.95),
                deadline_exceeded_count=b["deadline_exceeded"],
                oom_count=b["oom"],
                nonzero_exit_count=b["nonzero_exit"],
                updated_at=self._utcnow(),
            )
            existing = session.get(DwsWorkflowOpsDailyRow, (dt_val, workflow_type))
            if existing is None:
                session.add(DwsWorkflowOpsDailyRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_ads_quality_dashboard(self, session: Session, window: DateRange) -> int:
        rows = session.execute(
            select(DwsDatasetQualityDailyRow).where(
                DwsDatasetQualityDailyRow.dt >= window.start,
                DwsDatasetQualityDailyRow.dt <= window.end,
            )
        ).scalars().all()
        rule_failures = session.execute(
            select(DwsRuleFailureDailyRow).where(
                DwsRuleFailureDailyRow.dt >= window.start,
                DwsRuleFailureDailyRow.dt <= window.end,
                DwsRuleFailureDailyRow.fail_count > 0,
            )
        ).scalars().all()
        top_rule_by_key: dict[tuple[date, str | None], DwsRuleFailureDailyRow] = {}
        for rf in rule_failures:
            k = (rf.dt, rf.dataset_family)
            cur = top_rule_by_key.get(k)
            if cur is None or (rf.fail_count or 0) > (cur.fail_count or 0):
                top_rule_by_key[k] = rf

        fact_assets = session.execute(
            select(FactAssetProfileRow).where(
                FactAssetProfileRow.dt.is_not(None),
                FactAssetProfileRow.dt >= window.start,
                FactAssetProfileRow.dt <= window.end,
            )
        ).scalars().all()
        raw_bytes_agg: dict[tuple, int] = {}
        dwd_bytes_agg: dict[tuple, int] = {}
        for fa in fact_assets:
            k = (fa.dt, fa.dataset_id, fa.version)
            layer = (fa.layer or "").lower()
            if layer in ("raw", "ods"):
                raw_bytes_agg[k] = raw_bytes_agg.get(k, 0) + int(fa.bytes or 0)
            elif layer == "dwd":
                dwd_bytes_agg[k] = dwd_bytes_agg.get(k, 0) + int(fa.bytes or 0)

        affected = 0
        for d in rows:
            qc_rate = d.qc_pass_rate if d.qc_pass_rate is not None else 1.0
            etl_rate = d.etl_success_rate if d.etl_success_rate is not None else 1.0
            wf_rate = d.workflow_success_rate if d.workflow_success_rate is not None else 1.0
            if qc_rate < 0.8 or etl_rate < 0.8:
                overall = "FAIL"
                alert = "CRITICAL"
            elif qc_rate < 0.95:
                overall = "WARN"
                alert = "WARN"
            else:
                overall = "PASS"
                alert = "OK"
            if qc_rate < 0.8:
                reason = "qc_pass_rate<0.8"
            elif etl_rate < 0.8:
                reason = "etl_success_rate<0.8"
            elif qc_rate < 0.95:
                reason = "qc_pass_rate<0.95"
            else:
                reason = None
            quality_score = (100.0 * qc_rate * 0.5) + (100.0 * etl_rate * 0.3) + (100.0 * wf_rate * 0.2)
            top_rule = top_rule_by_key.get((d.dt, d.dataset_family))
            payload = dict(
                dt=d.dt, dataset_id=d.dataset_id, version=d.version,
                dataset_family=d.dataset_family,
                overall_status=overall, quality_score=quality_score,
                qc_pass_rate=d.qc_pass_rate, etl_success_rate=d.etl_success_rate,
                workflow_success_rate=d.workflow_success_rate,
                top_failed_rule=top_rule.rule_id if top_rule else None,
                top_failed_rule_count=top_rule.fail_count if top_rule else None,
                p95_duration_sec=d.p95_workflow_step_duration_sec,
                ml_ready_rows=d.ml_ready_rows,
                raw_bytes=raw_bytes_agg.get((d.dt, d.dataset_id, d.version)),
                dwd_bytes=dwd_bytes_agg.get((d.dt, d.dataset_id, d.version)),
                alert_level=alert, alert_reason=reason,
                updated_at=self._utcnow(),
            )
            existing = session.get(AdsQualityDashboardRow, (d.dt, d.dataset_id, d.version))
            if existing is None:
                session.add(AdsQualityDashboardRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_ads_workflow_ops_dashboard(self, session: Session, window: DateRange) -> int:
        rows = session.execute(
            select(DwsWorkflowOpsDailyRow).where(
                DwsWorkflowOpsDailyRow.dt >= window.start,
                DwsWorkflowOpsDailyRow.dt <= window.end,
            )
        ).scalars().all()
        affected = 0
        for d in rows:
            sr = d.success_rate if d.success_rate is not None else 1.0
            oom = d.oom_count or 0
            ded = d.deadline_exceeded_count or 0
            if sr < 0.8 or oom > 0 or ded > 0:
                alert, reason = "CRITICAL", ("success_rate<0.8" if sr < 0.8 else ("oom_kill" if oom > 0 else "deadline_exceeded"))
            elif sr < 0.95:
                alert, reason = "WARN", "success_rate<0.95"
            else:
                alert, reason = "OK", None
            payload = dict(
                dt=d.dt, workflow_type=d.workflow_type,
                workflow_count=d.workflow_count, success_count=d.success_count,
                failed_count=d.failed_count, success_rate=d.success_rate,
                avg_duration_sec=d.avg_duration_sec, p95_duration_sec=d.p95_duration_sec,
                stale_heartbeat_count=0, oom_count=oom,
                deadline_exceeded_count=ded,
                alert_level=alert, alert_reason=reason,
                updated_at=self._utcnow(),
            )
            existing = session.get(AdsWorkflowOpsDashboardRow, (d.dt, d.workflow_type))
            if existing is None:
                session.add(AdsWorkflowOpsDashboardRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    # ---------- v1.9 推理运营层（sqlite 简化口径） ----------

    @staticmethod
    def _job_dt(job: InferenceJobRow) -> date | None:
        ts = job.finished_at or job.started_at or job.created_at
        return ts.date() if ts is not None else None

    def _sqlite_build_dws_inference_job_daily(self, session: Session, window: DateRange) -> int:
        """从 inference_jobs (+ inference_outputs 时延) 聚合到 dws_inference_job_daily。"""
        from robot_dh.inference.metrics import percentile

        jobs = session.execute(select(InferenceJobRow)).scalars().all()
        backends = {
            m.model_id: m.backend
            for m in session.execute(select(ModelRegistryRow)).scalars().all()
        }
        # 每个 job 的 latency 列表（来自 inference_outputs）。
        lat_by_job: dict[str, list[float]] = {}
        for jid, lat in session.execute(
            select(InferenceOutputRow.job_id, InferenceOutputRow.latency_ms)
        ).all():
            if lat is not None:
                lat_by_job.setdefault(jid, []).append(float(lat))

        groups: dict[tuple[date, str, str], dict[str, Any]] = {}
        for j in jobs:
            d = self._job_dt(j)
            if d is None or d < window.start or d > window.end:
                continue
            key = (d, j.model_id, j.task_type or "unknown")
            g = groups.setdefault(
                key,
                {
                    "job_count": 0, "success_count": 0, "fail_count": 0,
                    "total_samples": 0, "processed_samples": 0, "failed_samples": 0,
                    "duration_sec": 0.0, "latencies": [], "backend": backends.get(j.model_id),
                },
            )
            g["job_count"] += 1
            status = (j.status or "").upper()
            if status in ("SUCCEEDED", "OK"):
                g["success_count"] += 1
            elif status in ("FAILED", "DEAD_LETTER"):
                g["fail_count"] += 1
            g["total_samples"] += int(j.total_samples or 0)
            g["processed_samples"] += int(j.processed_samples or 0)
            g["failed_samples"] += int(j.failed_samples or 0)
            g["duration_sec"] += float(j.duration_sec or 0.0)
            g["latencies"].extend(lat_by_job.get(j.job_id, []))
            if g["backend"] is None:
                g["backend"] = backends.get(j.model_id)

        affected = 0
        for (d, model_id, task_type), g in groups.items():
            job_count = g["job_count"]
            total_samples = g["total_samples"]
            latencies = g["latencies"]
            payload = dict(
                dt=d, model_id=model_id, backend=g["backend"], task_type=task_type,
                job_count=job_count, success_count=g["success_count"], fail_count=g["fail_count"],
                success_rate=(g["success_count"] / job_count) if job_count else None,
                total_samples=total_samples,
                samples_per_sec=(g["processed_samples"] / g["duration_sec"]) if g["duration_sec"] > 0 else None,
                avg_latency_ms=(sum(latencies) / len(latencies)) if latencies else None,
                p95_latency_ms=percentile(latencies, 0.95),
                error_rate=(g["failed_samples"] / total_samples) if total_samples else 0.0,
                updated_at=self._utcnow(),
            )
            existing = session.get(DwsInferenceJobDailyRow, (d, model_id, task_type))
            if existing is None:
                session.add(DwsInferenceJobDailyRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected

    def _sqlite_build_ads_inference_dashboard(self, session: Session, window: DateRange) -> int:
        """从 dws_inference_job_daily 推导看板 + 告警 + top_error_type。"""
        dws_rows = session.execute(
            select(DwsInferenceJobDailyRow).where(
                DwsInferenceJobDailyRow.dt >= window.start,
                DwsInferenceJobDailyRow.dt <= window.end,
            )
        ).scalars().all()

        # 计算每个 (dt, model_id, task_type) 的 top_error_type。
        jobs = {j.job_id: j for j in session.execute(select(InferenceJobRow)).scalars().all()}
        err_counts: dict[tuple[date, str, str], dict[str, int]] = {}
        for f in session.execute(select(InferenceFailureRow)).scalars().all():
            job = jobs.get(f.job_id)
            if job is None:
                continue
            d = self._job_dt(job)
            if d is None or d < window.start or d > window.end:
                continue
            key = (d, job.model_id, job.task_type or "unknown")
            bucket = err_counts.setdefault(key, {})
            et = f.error_type or "UNKNOWN"
            bucket[et] = bucket.get(et, 0) + 1

        affected = 0
        for d in dws_rows:
            sr = d.success_rate if d.success_rate is not None else 1.0
            er = d.error_rate if d.error_rate is not None else 0.0
            if sr < 0.8 or er > 0.2:
                overall, alert = "FAIL", "CRITICAL"
                reason = "success_rate<0.8" if sr < 0.8 else "error_rate>0.2"
            elif sr < 0.95:
                overall, alert, reason = "WARN", "WARN", "success_rate<0.95"
            else:
                overall, alert, reason = "PASS", "OK", None
            counts = err_counts.get((d.dt, d.model_id, d.task_type))
            top_error = max(counts.items(), key=lambda kv: kv[1])[0] if counts else None
            payload = dict(
                dt=d.dt, model_id=d.model_id, backend=d.backend, task_type=d.task_type,
                overall_status=overall, job_count=d.job_count, success_rate=d.success_rate,
                total_samples=d.total_samples, samples_per_sec=d.samples_per_sec,
                p95_latency_ms=d.p95_latency_ms, error_rate=d.error_rate,
                top_error_type=top_error, alert_level=alert, alert_reason=reason,
                updated_at=self._utcnow(),
            )
            existing = session.get(AdsInferenceDashboardRow, (d.dt, d.model_id, d.task_type))
            if existing is None:
                session.add(AdsInferenceDashboardRow(**payload))
            else:
                for k, v in payload.items():
                    setattr(existing, k, v)
            affected += 1
        return affected


def _md5_pipe(*parts: Any) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


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


def _to_layer_result(layer: str, ex: SqlExecution) -> LayerBuildResult:
    return LayerBuildResult(
        layer=layer,
        sql_file=ex.sql_file,
        status=ex.status,
        duration_sec=ex.duration_sec,
        affected_rows=ex.affected_rows,
        error=ex.error,
    )
