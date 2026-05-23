"""v1.4 数据湖元数据表的 SQLAlchemy 模型。

字段与云端 Postgres 一致（见 postgres/migrations/001_lake_metadata.reconstructed.sql）；
如无 lineage_edges.job_id -> etl_jobs.job_id 外键、quality_snapshots 无唯一约束等差异均保留。

5 张表共用 WarehouseBase，避免 v1.3 Base.metadata.create_all() 误建 v1.4 表；
v1.4 表仅由 ensure_lake_tables() 创建（SQLite 测试、WarehouseService 连 SQLite 时）。

Postgres 上 ensure_lake_tables() 为 no-op 安全的 create_all(checkfirst=True)，不改动云端已管表。
"""

from __future__ import annotations

from datetime import datetime, timezone

from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Engine,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON, TypeDecorator

# 跨后端 BIGSERIAL：PG 用 BigInteger 自增；SQLite 用 INTEGER 才能走 ROWID 自增
BigSerial = BigInteger().with_variant(Integer(), "sqlite")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _JsonB(TypeDecorator):
    """跨后端 JSON 列：Postgres 用 JSONB，其它方言用 JSON。"""

    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(JSON())


class WarehouseBase(DeclarativeBase):
    pass


class LakeAssetRow(WarehouseBase):
    __tablename__ = "lake_assets"
    __table_args__ = (
        Index("idx_lake_assets_dataset_version_layer", "dataset_id", "version", "layer"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    layer: Mapped[str] = mapped_column(Text, nullable=False)
    asset_type: Mapped[str] = mapped_column(Text, nullable=False)
    uri: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    format: Mapped[str | None] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    checksum: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )


class EtlJobRow(WarehouseBase):
    __tablename__ = "etl_jobs"
    __table_args__ = (
        Index("idx_etl_jobs_job_type_status_created_at", "job_type", "status", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    input_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class LineageEdgeRow(WarehouseBase):
    __tablename__ = "lineage_edges"
    __table_args__ = (
        Index("idx_lineage_edges_source_uri", "source_uri"),
        Index("idx_lineage_edges_target_uri", "target_uri"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    source_uri: Mapped[str] = mapped_column(Text, nullable=False)
    target_uri: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[str] = mapped_column(Text, nullable=False)
    job_type: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DatasetVersionRow(WarehouseBase):
    __tablename__ = "dataset_versions"
    __table_args__ = (
        UniqueConstraint("dataset_id", "version", name="dataset_versions_dataset_id_version_key"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    raw_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    ods_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    dwd_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )


class QualitySnapshotRow(WarehouseBase):
    __tablename__ = "quality_snapshots"
    __table_args__ = (
        Index("idx_quality_snapshots_dataset_version_created_at", "dataset_id", "version", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    quality_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


# v1.5 新增表：scale benchmark + sharded etl + runtime profiling


class EtlPerfRunRow(WarehouseBase):
    __tablename__ = "etl_perf_runs"
    __table_args__ = (
        Index("idx_etl_perf_runs_phase_status", "phase", "status"),
        Index("idx_etl_perf_runs_dataset_version", "dataset_id", "version"),
        Index("idx_etl_perf_runs_run_id", "run_id"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    input_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    output_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    input_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    output_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    download_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    upload_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    compute_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    peak_memory_mb: Mapped[float | None] = mapped_column(Float, nullable=True)
    worker_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="OK")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class EtlShardRow(WarehouseBase):
    __tablename__ = "etl_shards"
    __table_args__ = (
        UniqueConstraint("plan_id", "shard_id", name="etl_shards_plan_shard_key"),
        Index("idx_etl_shards_status", "status"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    plan_id: Mapped[str] = mapped_column(Text, nullable=False)
    shard_id: Mapped[str] = mapped_column(Text, nullable=False)
    shard_index: Mapped[int] = mapped_column(Integer, nullable=False)
    dataset_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="PENDING")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    succeeded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    failed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    skipped: Mapped[int | None] = mapped_column(Integer, nullable=True)
    summary_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class BenchmarkRunRow(WarehouseBase):
    __tablename__ = "benchmark_runs"
    __table_args__ = (
        Index("idx_benchmark_runs_suite_status", "suite_name", "status"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    benchmark_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    suite_name: Mapped[str] = mapped_column(Text, nullable=False)
    suite_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    total_cases: Mapped[int | None] = mapped_column(Integer, nullable=True)
    passed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    failed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mismatched: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="RUNNING")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    report_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class BenchmarkCaseRow(WarehouseBase):
    __tablename__ = "benchmark_cases"
    __table_args__ = (
        Index("idx_benchmark_cases_benchmark_case", "benchmark_id", "case_id"),
        Index("idx_benchmark_cases_benchmark_match", "benchmark_id", "match"),
        UniqueConstraint("benchmark_id", "case_id", name="uq_benchmark_cases_benchmark_case"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    benchmark_id: Mapped[str] = mapped_column(Text, nullable=False)
    case_id: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    mutation_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    mutation: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    actual_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_failed_validators: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    actual_failed_validators: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    match: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    artifacts_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class RuntimeEventRow(WarehouseBase):
    __tablename__ = "runtime_events"
    __table_args__ = (
        Index("idx_runtime_events_event_type_created_at", "event_type", "created_at"),
        Index("idx_runtime_events_run_id", "run_id"),
        Index("idx_runtime_events_job_id", "job_id"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class ArgoWorkflowRow(WarehouseBase):
    """v1.5 Argo workflow 元数据；非强制 schema，本仓库的写入路径主要在 CLI 与 exporter 读取。"""

    __tablename__ = "argo_workflow_runs"
    __table_args__ = (
        Index("idx_argo_workflow_runs_status", "status"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    workflow_name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    workflow_template: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


def ensure_lake_tables(engine: Engine) -> None:
    """若不存在则创建 v1.4 + v1.5 元数据表；生产 Postgres 通常已有，checkfirst 保证幂等；SQLite 测试依赖此建表。"""
    WarehouseBase.metadata.create_all(engine, checkfirst=True)
