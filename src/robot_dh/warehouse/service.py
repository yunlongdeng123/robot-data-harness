"""WarehouseService：v1.4 数据湖元数据访问层。

写方法默认可降级：DB 不可用（如本地 CLI 未配 ROBOT_DH_DB_URI）时只打日志并返回 None，
本地 ETL 仍可产出 parquet + manifest。面向 `lake audit` 等运维路径时通过 LakeMetadataUnavailableError 暴露错误。
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    DatasetVersionRow,
    EtlJobRow,
    LakeAssetRow,
    LineageEdgeRow,
    QualitySnapshotRow,
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


class LakeMetadataUnavailableError(RuntimeError):
    """API 端点 / lake audit 在 warehouse 不可达时抛出。"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
