"""WarehouseService：v1.4 数据湖元数据访问层。

写方法默认可降级：DB 不可用（如本地 CLI 未配 ROBOT_DH_DB_URI）时只打日志并返回 None，
本地 ETL 仍可产出 parquet + manifest。面向 `lake audit` 等运维路径时通过 LakeMetadataUnavailableError 暴露错误。
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from psycopg import errors as psycopg_errors
from sqlalchemy import inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    ArgoWorkflowRow,
    BenchmarkCaseRow,
    BenchmarkRunRow,
    DatasetVersionRow,
    EtlJobRow,
    EtlPerfRunRow,
    EtlShardRow,
    LakeAssetRow,
    LineageEdgeRow,
    QualitySnapshotRow,
    RuntimeEventRow,
    ensure_lake_tables,
)

LOG = logging.getLogger(__name__)

REQUIRED_TABLES = (
    "lake_assets",
    "etl_jobs",
    "lineage_edges",
    "dataset_versions",
    "quality_snapshots",
)

# v1.5 表分离对待：缺失时给出明确提示，但不会阻塞 soft 写入。
V1_5_TABLES = (
    "etl_perf_runs",
    "etl_shards",
    "benchmark_runs",
    "benchmark_cases",
    "runtime_events",
    "argo_workflow_runs",
)


class V15SchemaMissingError(RuntimeError):
    """v1.5 表缺失时（严格模式）抛出，提示先在 infra 项目执行 schema 脚本。"""


class LakeMetadataUnavailableError(RuntimeError):
    """API 端点 / lake audit 在 warehouse 不可达时抛出。"""


SCHEMA_DRIFT_ERRORS: tuple[type[BaseException], ...] = (
    psycopg_errors.UndefinedColumn,
    psycopg_errors.UndefinedTable,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_schema_drift_error(err: Exception) -> bool:
    """识别远端 schema 与当前模型不一致的确定性错误。"""
    current: BaseException | None = err
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, SCHEMA_DRIFT_ERRORS):
            return True
        for attr in ("orig", "__cause__"):
            next_err = getattr(current, attr, None)
            if isinstance(next_err, BaseException):
                current = next_err
                break
        else:
            current = None
    return False


class WarehouseService:
    def __init__(self, *, db_uri: str | None = None, soft: bool = True) -> None:
        """
        Args:
            db_uri: 显式 DB URI；否则从 ROBOT_DH_DB_URI 解析
            soft: True 时写操作吞异常并返回 None；False（API 层）抛 LakeMetadataUnavailableError，原因为 __cause__
        """
        self._db_uri = db_uri
        self._soft = soft
        self._engine: Engine | None = None

    def _get_engine(self) -> Engine:
        if self._engine is None:
            resolved = resolve_db_uri(self._db_uri)
            engine = get_engine(resolved)
            if engine.dialect.name == "sqlite":
                init_db(resolved)
                ensure_lake_tables(engine)
            else:
                existing = set(inspect(engine).get_table_names())
                missing = [t for t in REQUIRED_TABLES if t not in existing]
                if missing:
                    raise LakeMetadataUnavailableError(
                        "PostgreSQL lake metadata tables are missing: "
                        f"{', '.join(missing)}. Apply the lake schema in the infra "
                        "project first (for example: scripts/21_pg_apply_lake_schema.sh)."
                    )
            self._engine = engine
        return self._engine

    def _session(self) -> Session:
        return Session(self._get_engine(), expire_on_commit=False, future=True)

    def _handle_write_error(self, op: str, err: Exception) -> None:
        if _is_schema_drift_error(err):
            msg = (
                f"warehouse {op} schema mismatch: {err}. "
                "Apply the matching schema migration in the infra project first."
            )
            if self._soft:
                LOG.error(msg)
                raise V15SchemaMissingError(msg) from err
            raise LakeMetadataUnavailableError(msg) from err
        if self._soft:
            LOG.warning("warehouse %s failed (continuing in soft mode): %s", op, err)
            return
        raise LakeMetadataUnavailableError(f"warehouse {op} failed: {err}") from err

    def tables_present(self) -> dict[str, bool]:
        try:
            engine = self._get_engine()
            existing = set(inspect(engine).get_table_names())
            return {t: t in existing for t in REQUIRED_TABLES}
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"warehouse inspect failed: {err}") from err

    # 写入

    def record_lake_asset(
        self,
        *,
        dataset_id: str,
        version: str,
        layer: str,
        asset_type: str,
        uri: str,
        format: str | None = None,
        size_bytes: int | None = None,
        row_count: int | None = None,
        checksum: str | None = None,
    ) -> int | None:
        """按 uri 唯一性插入或 upsert 一条 lake_assets 行。"""
        try:
            with self._session() as session:
                existing = session.scalar(select(LakeAssetRow).where(LakeAssetRow.uri == uri))
                if existing is not None:
                    existing.dataset_id = dataset_id
                    existing.version = version
                    existing.layer = layer
                    existing.asset_type = asset_type
                    existing.format = format
                    existing.size_bytes = size_bytes
                    existing.row_count = row_count
                    existing.checksum = checksum
                    existing.updated_at = _utcnow()
                    session.commit()
                    return existing.id
                row = LakeAssetRow(
                    dataset_id=dataset_id,
                    version=version,
                    layer=layer,
                    asset_type=asset_type,
                    uri=uri,
                    format=format,
                    size_bytes=size_bytes,
                    row_count=row_count,
                    checksum=checksum,
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, LakeMetadataUnavailableError) as err:
            self._handle_write_error("record_lake_asset", err)
            return None

    def record_etl_job_start(
        self,
        *,
        job_id: str,
        job_type: str,
        input_uri: str | None,
        output_uri: str | None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            with self._session() as session:
                row = EtlJobRow(
                    job_id=job_id,
                    job_type=job_type,
                    input_uri=input_uri,
                    output_uri=output_uri,
                    status="RUNNING",
                    started_at=_utcnow(),
                    metrics_json=dict(metrics) if metrics else None,
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, LakeMetadataUnavailableError) as err:
            self._handle_write_error("record_etl_job_start", err)
            return None

    def record_etl_job_finish(
        self,
        *,
        job_id: str,
        status: str,
        metrics: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> int | None:
        try:
            with self._session() as session:
                row = session.scalar(select(EtlJobRow).where(EtlJobRow.job_id == job_id))
                now = _utcnow()
                if row is None:
                    row = EtlJobRow(
                        job_id=job_id,
                        job_type="unknown",
                        status=status,
                        started_at=now,
                        finished_at=now,
                        duration_sec=0.0,
                        error_message=error_message,
                        metrics_json=dict(metrics) if metrics else None,
                    )
                    session.add(row)
                else:
                    row.status = status
                    row.finished_at = now
                    if row.started_at is not None:
                        started = row.started_at
                        if started.tzinfo is None:
                            started = started.replace(tzinfo=timezone.utc)
                        row.duration_sec = (now - started).total_seconds()
                    row.error_message = error_message
                    if metrics is not None:
                        existing = dict(row.metrics_json) if row.metrics_json else {}
                        existing.update(metrics)
                        row.metrics_json = existing
                session.commit()
                return row.id
        except (SQLAlchemyError, LakeMetadataUnavailableError) as err:
            self._handle_write_error("record_etl_job_finish", err)
            return None

    def record_lineage_edge(
        self,
        *,
        source_uri: str,
        target_uri: str,
        job_id: str,
        job_type: str,
        run_id: str | None = None,
    ) -> int | None:
        try:
            with self._session() as session:
                row = LineageEdgeRow(
                    source_uri=source_uri,
                    target_uri=target_uri,
                    job_id=job_id,
                    job_type=job_type,
                    run_id=run_id,
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, LakeMetadataUnavailableError) as err:
            self._handle_write_error("record_lineage_edge", err)
            return None

    def record_lineage_edges(self, edges: Iterable[dict[str, Any]]) -> int:
        """批量插入血缘边，返回实际插入行数。"""
        count = 0
        try:
            with self._session() as session:
                for edge in edges:
                    session.add(
                        LineageEdgeRow(
                            source_uri=edge["source_uri"],
                            target_uri=edge["target_uri"],
                            job_id=edge["job_id"],
                            job_type=edge["job_type"],
                            run_id=edge.get("run_id"),
                        )
                    )
                    count += 1
                session.commit()
            return count
        except (SQLAlchemyError, LakeMetadataUnavailableError) as err:
            self._handle_write_error("record_lineage_edges", err)
            return count

    def upsert_dataset_version(
        self,
        *,
        dataset_id: str,
        version: str,
        raw_uri: str | None = None,
        ods_uri: str | None = None,
        dwd_uri: str | None = None,
        status: str | None = None,
    ) -> int | None:
        try:
            with self._session() as session:
                row = session.scalar(
                    select(DatasetVersionRow).where(
                        DatasetVersionRow.dataset_id == dataset_id,
                        DatasetVersionRow.version == version,
                    )
                )
                if row is None:
                    row = DatasetVersionRow(
                        dataset_id=dataset_id,
                        version=version,
                        raw_uri=raw_uri,
                        ods_uri=ods_uri,
                        dwd_uri=dwd_uri,
                        status=status,
                    )
                    session.add(row)
                else:
                    if raw_uri is not None:
                        row.raw_uri = raw_uri
                    if ods_uri is not None:
                        row.ods_uri = ods_uri
                    if dwd_uri is not None:
                        row.dwd_uri = dwd_uri
                    if status is not None:
                        row.status = status
                    row.updated_at = _utcnow()
                session.commit()
                return row.id
        except (SQLAlchemyError, LakeMetadataUnavailableError) as err:
            self._handle_write_error("upsert_dataset_version", err)
            return None

    def record_quality_snapshot(
        self,
        *,
        dataset_id: str,
        version: str,
        run_id: str | None,
        quality_status: str | None,
        quality_score: float | None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            with self._session() as session:
                row = QualitySnapshotRow(
                    dataset_id=dataset_id,
                    version=version,
                    run_id=run_id,
                    quality_status=quality_status,
                    quality_score=quality_score,
                    metrics_json=dict(metrics) if metrics else None,
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, LakeMetadataUnavailableError) as err:
            self._handle_write_error("record_quality_snapshot", err)
            return None

    # 读取

    def list_lake_assets(
        self,
        *,
        layer: str | None = None,
        dataset_id: str | None = None,
        version: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = select(LakeAssetRow).order_by(LakeAssetRow.created_at.desc()).limit(limit)
                if layer is not None:
                    stmt = stmt.where(LakeAssetRow.layer == layer)
                if dataset_id is not None:
                    stmt = stmt.where(LakeAssetRow.dataset_id == dataset_id)
                if version is not None:
                    stmt = stmt.where(LakeAssetRow.version == version)
                rows = session.scalars(stmt).all()
                return [_lake_asset_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_lake_assets failed: {err}") from err

    def list_lineage(
        self,
        *,
        uri: str,
        limit: int = 200,
    ) -> dict[str, list[dict[str, Any]]]:
        """返回给定 URI 的入边与出边。"""
        try:
            with self._session() as session:
                inbound = session.scalars(
                    select(LineageEdgeRow)
                    .where(LineageEdgeRow.target_uri == uri)
                    .order_by(LineageEdgeRow.created_at.desc())
                    .limit(limit)
                ).all()
                outbound = session.scalars(
                    select(LineageEdgeRow)
                    .where(LineageEdgeRow.source_uri == uri)
                    .order_by(LineageEdgeRow.created_at.desc())
                    .limit(limit)
                ).all()
                return {
                    "inbound": [_lineage_edge_to_dict(r) for r in inbound],
                    "outbound": [_lineage_edge_to_dict(r) for r in outbound],
                }
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_lineage failed: {err}") from err

    def list_etl_jobs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                rows = session.scalars(
                    select(EtlJobRow).order_by(EtlJobRow.created_at.desc()).limit(limit)
                ).all()
                return [_etl_job_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_etl_jobs failed: {err}") from err

    def get_etl_job(self, job_id: str) -> dict[str, Any] | None:
        try:
            with self._session() as session:
                row = session.scalar(select(EtlJobRow).where(EtlJobRow.job_id == job_id))
                return _etl_job_to_dict(row) if row is not None else None
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"get_etl_job failed: {err}") from err

    # v1.5：性能 / shard / benchmark / runtime events 写入

    def _v1_5_missing_tables(self) -> list[str]:
        engine = self._get_engine()
        existing = set(inspect(engine).get_table_names())
        return [t for t in V1_5_TABLES if t not in existing]

    def _ensure_v1_5_tables_or_warn(self, op: str) -> bool:
        try:
            missing = self._v1_5_missing_tables()
        except SQLAlchemyError as err:
            self._handle_write_error(op, err)
            return False
        if not missing:
            return True
        msg = (
            "v1.5 PostgreSQL tables missing: "
            + ", ".join(missing)
            + ". Apply v1.5 schema in the infra project first "
            "(e.g. scripts/29_pg_apply_v1_5_schema.sh)."
        )
        if self._soft:
            LOG.warning("%s skipped: %s", op, msg)
            return False
        raise V15SchemaMissingError(msg)

    def record_etl_perf_run(self, record: Any) -> int | None:
        """写一条 etl_perf_runs；record 是 PerfRecord（重复 import 时只取 to_dict）。

        与远端 schema 解耦：先用 ``inspect`` 嗅探 ``etl_perf_runs`` 真实列集合，
        然后用 SQLAlchemy core ``insert(...).values(...)`` **只**带 PG 上真实存在的列；
        ORM 模型多出来的列（典型如 v1.5 远端只有 ``created_at``，缺
        ``started_at`` / ``finished_at``）走 ``metrics_json`` 兜底携带，
        既不破坏 INSERT 也不丢数据；下游 DML（``build_fact_etl_run.sql``）已经会
        从 ``metrics_json`` 取这两个时间字段，pass-through 完整。
        """
        try:
            from robot_dh.perf.profiler import PerfRecord  # 延迟导入避免循环
        except Exception:
            PerfRecord = None  # type: ignore

        try:
            if not self._ensure_v1_5_tables_or_warn("record_etl_perf_run"):
                return None

            from sqlalchemy import insert as _sa_insert

            engine = self._get_engine()
            existing_cols = {
                c["name"]
                for c in inspect(engine).get_columns("etl_perf_runs")
            }
            payload = record.to_dict() if hasattr(record, "to_dict") else dict(record)

            started_at = _parse_dt(payload.get("started_at"))
            finished_at = _parse_dt(payload.get("finished_at"))
            metrics_json = dict(payload.get("metrics") or {})
            if "started_at" not in existing_cols and started_at is not None:
                metrics_json["started_at"] = started_at.isoformat()
            if "finished_at" not in existing_cols and finished_at is not None:
                metrics_json["finished_at"] = finished_at.isoformat()

            full_values: dict[str, Any] = dict(
                job_id=str(payload.get("job_id", "")),
                run_id=str(payload.get("run_id", "")),
                dataset_id=str(payload.get("dataset_id", "")),
                version=str(payload.get("version", "")),
                phase=str(payload.get("phase", "")),
                input_uri=payload.get("input_uri"),
                output_uri=payload.get("output_uri"),
                input_bytes=int(payload.get("input_bytes") or 0),
                output_bytes=int(payload.get("output_bytes") or 0),
                input_rows=int(payload.get("input_rows") or 0),
                output_rows=int(payload.get("output_rows") or 0),
                duration_sec=float(payload.get("duration_sec") or 0.0),
                download_duration_sec=float(payload.get("download_duration_sec") or 0.0),
                upload_duration_sec=float(payload.get("upload_duration_sec") or 0.0),
                compute_duration_sec=float(payload.get("compute_duration_sec") or 0.0),
                peak_memory_mb=float(payload.get("peak_memory_mb") or 0.0),
                worker_id=payload.get("worker_id"),
                status=str(payload.get("status") or "OK"),
                error_message=payload.get("error_message"),
                started_at=started_at,
                finished_at=finished_at,
                metrics_json=metrics_json,
            )
            # 只保留 PG 实际有的列，避免 UndefinedColumn 把整批 INSERT 拒绝。
            filtered = {k: v for k, v in full_values.items() if k in existing_cols}

            with self._session() as session:
                result = session.execute(
                    _sa_insert(EtlPerfRunRow.__table__).values(**filtered).returning(EtlPerfRunRow.id)
                )
                row_id = int(result.scalar_one())
                session.commit()
                return row_id
        except (SQLAlchemyError, V15SchemaMissingError) as err:
            self._handle_write_error("record_etl_perf_run", err)
            return None

    def record_etl_shard(
        self,
        *,
        plan_id: str,
        shard_id: str,
        shard_index: int,
        status: str,
        dataset_count: int | None = None,
        input_bytes: int | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_sec: float | None = None,
        succeeded: int | None = None,
        failed: int | None = None,
        skipped: int | None = None,
        summary_uri: str | None = None,
        error_message: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_v1_5_tables_or_warn("record_etl_shard"):
                return None
            with self._session() as session:
                row = session.scalar(
                    select(EtlShardRow).where(
                        EtlShardRow.plan_id == plan_id,
                        EtlShardRow.shard_id == shard_id,
                    )
                )
                if row is None:
                    row = EtlShardRow(
                        plan_id=plan_id,
                        shard_id=shard_id,
                        shard_index=int(shard_index),
                        status=status,
                        dataset_count=dataset_count,
                        input_bytes=input_bytes,
                        started_at=started_at,
                        finished_at=finished_at,
                        duration_sec=duration_sec,
                        succeeded=succeeded,
                        failed=failed,
                        skipped=skipped,
                        summary_uri=summary_uri,
                        error_message=error_message,
                        metrics_json=dict(metrics) if metrics else None,
                    )
                    session.add(row)
                else:
                    row.status = status
                    if dataset_count is not None:
                        row.dataset_count = int(dataset_count)
                    if input_bytes is not None:
                        row.input_bytes = int(input_bytes)
                    if started_at is not None:
                        row.started_at = started_at
                    if finished_at is not None:
                        row.finished_at = finished_at
                    if duration_sec is not None:
                        row.duration_sec = float(duration_sec)
                    if succeeded is not None:
                        row.succeeded = int(succeeded)
                    if failed is not None:
                        row.failed = int(failed)
                    if skipped is not None:
                        row.skipped = int(skipped)
                    if summary_uri is not None:
                        row.summary_uri = summary_uri
                    if error_message is not None:
                        row.error_message = error_message
                    if metrics is not None:
                        existing = dict(row.metrics_json) if row.metrics_json else {}
                        existing.update(metrics)
                        row.metrics_json = existing
                session.commit()
                return row.id
        except (SQLAlchemyError, V15SchemaMissingError) as err:
            self._handle_write_error("record_etl_shard", err)
            return None

    def record_benchmark_run(
        self,
        *,
        benchmark_id: str,
        suite_name: str,
        suite_path: str | None,
        total_cases: int | None,
        passed: int | None,
        failed: int | None,
        mismatched: int | None,
        status: str,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_sec: float | None = None,
        report_uri: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_v1_5_tables_or_warn("record_benchmark_run"):
                return None
            with self._session() as session:
                row = session.scalar(select(BenchmarkRunRow).where(BenchmarkRunRow.benchmark_id == benchmark_id))
                if row is None:
                    row = BenchmarkRunRow(
                        benchmark_id=benchmark_id,
                        suite_name=suite_name,
                        suite_path=suite_path,
                        total_cases=total_cases,
                        passed=passed,
                        failed=failed,
                        mismatched=mismatched,
                        status=status,
                        started_at=started_at,
                        finished_at=finished_at,
                        duration_sec=duration_sec,
                        report_uri=report_uri,
                        metrics_json=dict(metrics) if metrics else None,
                    )
                    session.add(row)
                else:
                    row.suite_name = suite_name
                    row.suite_path = suite_path
                    row.total_cases = total_cases
                    row.passed = passed
                    row.failed = failed
                    row.mismatched = mismatched
                    row.status = status
                    if started_at is not None:
                        row.started_at = started_at
                    if finished_at is not None:
                        row.finished_at = finished_at
                    if duration_sec is not None:
                        row.duration_sec = float(duration_sec)
                    if report_uri is not None:
                        row.report_uri = report_uri
                    if metrics is not None:
                        existing = dict(row.metrics_json) if row.metrics_json else {}
                        existing.update(metrics)
                        row.metrics_json = existing
                session.commit()
                return row.id
        except (SQLAlchemyError, V15SchemaMissingError) as err:
            self._handle_write_error("record_benchmark_run", err)
            return None

    def record_benchmark_case(
        self,
        *,
        benchmark_id: str,
        case_id: str,
        mutation: str | None,
        expected_status: str | None,
        actual_status: str | None,
        expected_failed_validators: list[str] | None,
        actual_failed_validators: list[str] | None,
        match: bool | None,
        duration_sec: float | None = None,
        error_message: str | None = None,
        metrics: dict[str, Any] | None = None,
        dataset_uri: str | None = None,
        artifacts_uri: str | None = None,
    ) -> int | None:
        try:
            if not self._ensure_v1_5_tables_or_warn("record_benchmark_case"):
                return None
            with self._session() as session:
                row = BenchmarkCaseRow(
                    benchmark_id=benchmark_id,
                    case_id=case_id,
                    dataset_uri=dataset_uri,
                    mutation=mutation,
                    expected_status=expected_status,
                    actual_status=actual_status,
                    expected_failed_validators={"items": list(expected_failed_validators or [])},
                    actual_failed_validators={"items": list(actual_failed_validators or [])},
                    match=match,
                    duration_sec=duration_sec,
                    error_message=error_message,
                    metrics_json=dict(metrics) if metrics else None,
                    artifacts_uri=artifacts_uri,
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, V15SchemaMissingError) as err:
            self._handle_write_error("record_benchmark_case", err)
            return None

    def record_runtime_event(self, event: Any) -> int | None:
        try:
            if not self._ensure_v1_5_tables_or_warn("record_runtime_event"):
                return None
            payload = event.to_dict() if hasattr(event, "to_dict") else dict(event)
            with self._session() as session:
                row = RuntimeEventRow(
                    event_id=str(payload.get("event_id")),
                    event_type=str(payload.get("event_type")),
                    job_id=payload.get("job_id"),
                    run_id=payload.get("run_id"),
                    dataset_id=payload.get("dataset_id"),
                    version=payload.get("version"),
                    payload_json=dict(payload.get("payload") or {}),
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, V15SchemaMissingError) as err:
            self._handle_write_error("record_runtime_event", err)
            return None

    def record_argo_workflow(
        self,
        *,
        workflow_name: str,
        workflow_template: str | None = None,
        status: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_sec: float | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_v1_5_tables_or_warn("record_argo_workflow"):
                return None
            with self._session() as session:
                row = session.scalar(select(ArgoWorkflowRow).where(ArgoWorkflowRow.workflow_name == workflow_name))
                if row is None:
                    row = ArgoWorkflowRow(
                        workflow_name=workflow_name,
                        workflow_template=workflow_template,
                        status=status,
                        started_at=started_at,
                        finished_at=finished_at,
                        duration_sec=duration_sec,
                        metrics_json=dict(metrics) if metrics else None,
                    )
                    session.add(row)
                else:
                    if workflow_template is not None:
                        row.workflow_template = workflow_template
                    if status is not None:
                        row.status = status
                    if started_at is not None:
                        row.started_at = started_at
                    if finished_at is not None:
                        row.finished_at = finished_at
                    if duration_sec is not None:
                        row.duration_sec = float(duration_sec)
                    if metrics is not None:
                        existing = dict(row.metrics_json) if row.metrics_json else {}
                        existing.update(metrics)
                        row.metrics_json = existing
                session.commit()
                return row.id
        except (SQLAlchemyError, V15SchemaMissingError) as err:
            self._handle_write_error("record_argo_workflow", err)
            return None

    # v1.5 读取

    def list_etl_perf_runs(
        self,
        *,
        dataset_id: str | None = None,
        version: str | None = None,
        phase: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = select(EtlPerfRunRow).order_by(EtlPerfRunRow.created_at.desc()).limit(limit)
                if dataset_id is not None:
                    stmt = stmt.where(EtlPerfRunRow.dataset_id == dataset_id)
                if version is not None:
                    stmt = stmt.where(EtlPerfRunRow.version == version)
                if phase is not None:
                    stmt = stmt.where(EtlPerfRunRow.phase == phase)
                if status is not None:
                    stmt = stmt.where(EtlPerfRunRow.status == status)
                rows = session.scalars(stmt).all()
                return [_etl_perf_run_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_etl_perf_runs failed: {err}") from err

    def list_etl_shards(
        self,
        *,
        plan_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = select(EtlShardRow).order_by(EtlShardRow.created_at.desc()).limit(limit)
                if plan_id is not None:
                    stmt = stmt.where(EtlShardRow.plan_id == plan_id)
                if status is not None:
                    stmt = stmt.where(EtlShardRow.status == status)
                rows = session.scalars(stmt).all()
                return [_etl_shard_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_etl_shards failed: {err}") from err

    def list_benchmark_runs(self, *, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                rows = session.scalars(
                    select(BenchmarkRunRow).order_by(BenchmarkRunRow.created_at.desc()).limit(limit)
                ).all()
                return [_benchmark_run_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_benchmark_runs failed: {err}") from err

    def get_benchmark_run(self, benchmark_id: str) -> dict[str, Any] | None:
        try:
            with self._session() as session:
                row = session.scalar(
                    select(BenchmarkRunRow).where(BenchmarkRunRow.benchmark_id == benchmark_id)
                )
                if row is None:
                    return None
                cases = session.scalars(
                    select(BenchmarkCaseRow).where(BenchmarkCaseRow.benchmark_id == benchmark_id)
                ).all()
                payload = _benchmark_run_to_dict(row)
                payload["cases"] = [_benchmark_case_to_dict(c) for c in cases]
                return payload
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"get_benchmark_run failed: {err}") from err

    def list_runtime_events(
        self,
        *,
        event_type: str | None = None,
        run_id: str | None = None,
        job_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = select(RuntimeEventRow).order_by(RuntimeEventRow.created_at.desc()).limit(limit)
                if event_type is not None:
                    stmt = stmt.where(RuntimeEventRow.event_type == event_type)
                if run_id is not None:
                    stmt = stmt.where(RuntimeEventRow.run_id == run_id)
                if job_id is not None:
                    stmt = stmt.where(RuntimeEventRow.job_id == job_id)
                rows = session.scalars(stmt).all()
                return [_runtime_event_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_runtime_events failed: {err}") from err

    def latest_quality_summary(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """每个 (dataset_id, version) 取最近一条 quality_snapshots。"""
        try:
            with self._session() as session:
                rows = session.scalars(
                    select(QualitySnapshotRow)
                    .order_by(QualitySnapshotRow.created_at.desc())
                    .limit(limit * 4)
                ).all()
                seen: set[tuple[str, str]] = set()
                out: list[dict[str, Any]] = []
                for r in rows:
                    key = (r.dataset_id, r.version)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(_quality_snapshot_to_dict(r))
                    if len(out) >= limit:
                        break
                return out
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"latest_quality_summary failed: {err}") from err


def _lake_asset_to_dict(row: LakeAssetRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "layer": row.layer,
        "asset_type": row.asset_type,
        "uri": row.uri,
        "format": row.format,
        "size_bytes": row.size_bytes,
        "row_count": row.row_count,
        "checksum": row.checksum,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _lineage_edge_to_dict(row: LineageEdgeRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "source_uri": row.source_uri,
        "target_uri": row.target_uri,
        "job_id": row.job_id,
        "job_type": row.job_type,
        "run_id": row.run_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _etl_job_to_dict(row: EtlJobRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "job_id": row.job_id,
        "job_type": row.job_type,
        "input_uri": row.input_uri,
        "output_uri": row.output_uri,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_sec": row.duration_sec,
        "error_message": row.error_message,
        "metrics_json": row.metrics_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _quality_snapshot_to_dict(row: QualitySnapshotRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "run_id": row.run_id,
        "quality_status": row.quality_status,
        "quality_score": row.quality_score,
        "metrics_json": row.metrics_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _parse_dt(value: Any) -> datetime | None:
    """容忍 ISO 字符串或 datetime；带 'Z' 后缀也能解析。"""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _etl_perf_run_to_dict(row: EtlPerfRunRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "job_id": row.job_id,
        "run_id": row.run_id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "phase": row.phase,
        "input_uri": row.input_uri,
        "output_uri": row.output_uri,
        "input_bytes": row.input_bytes,
        "output_bytes": row.output_bytes,
        "input_rows": row.input_rows,
        "output_rows": row.output_rows,
        "duration_sec": row.duration_sec,
        "download_duration_sec": row.download_duration_sec,
        "upload_duration_sec": row.upload_duration_sec,
        "compute_duration_sec": row.compute_duration_sec,
        "peak_memory_mb": row.peak_memory_mb,
        "worker_id": row.worker_id,
        "status": row.status,
        "error_message": row.error_message,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "metrics_json": row.metrics_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _etl_shard_to_dict(row: EtlShardRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "plan_id": row.plan_id,
        "shard_id": row.shard_id,
        "shard_index": row.shard_index,
        "dataset_count": row.dataset_count,
        "input_bytes": row.input_bytes,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_sec": row.duration_sec,
        "succeeded": row.succeeded,
        "failed": row.failed,
        "skipped": row.skipped,
        "summary_uri": row.summary_uri,
        "error_message": row.error_message,
        "metrics_json": row.metrics_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _benchmark_run_to_dict(row: BenchmarkRunRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "benchmark_id": row.benchmark_id,
        "suite_name": row.suite_name,
        "suite_path": row.suite_path,
        "total_cases": row.total_cases,
        "passed": row.passed,
        "failed": row.failed,
        "mismatched": row.mismatched,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_sec": row.duration_sec,
        "report_uri": row.report_uri,
        "metrics_json": row.metrics_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _benchmark_case_to_dict(row: BenchmarkCaseRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "benchmark_id": row.benchmark_id,
        "case_id": row.case_id,
        "dataset_uri": row.dataset_uri,
        "mutation_type": row.mutation_type,
        "mutation": row.mutation,
        "expected_status": row.expected_status,
        "actual_status": row.actual_status,
        "expected_failed_validators": (
            row.expected_failed_validators.get("items") if isinstance(row.expected_failed_validators, dict) else None
        ),
        "actual_failed_validators": (
            row.actual_failed_validators.get("items") if isinstance(row.actual_failed_validators, dict) else None
        ),
        "passed": row.passed,
        "match": row.match,
        "duration_sec": row.duration_sec,
        "error_message": row.error_message,
        "metrics_json": row.metrics_json,
        "artifacts_uri": row.artifacts_uri,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _runtime_event_to_dict(row: RuntimeEventRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "event_id": row.event_id,
        "event_type": row.event_type,
        "job_id": row.job_id,
        "run_id": row.run_id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "payload_json": row.payload_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }
