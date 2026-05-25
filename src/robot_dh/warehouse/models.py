"""v1.4/v1.5/v1.6 数据湖元数据表的 SQLAlchemy 模型。

所有表共用 WarehouseBase；ensure_lake_tables() 在 SQLite 测试与新 Postgres 上做 checkfirst create_all。
真生产 Postgres 由 infra/migrations 严格管理，本模块仅作为 ORM 反射层。
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


# v1.6 新增表：QC contract / workflow / asset profile / ml-ready / partition / heartbeat / openlineage


class QcContractRow(WarehouseBase):
    """qc_contracts：dataset_family 级别的 contract 定义。"""

    __tablename__ = "qc_contracts"

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    contract_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    dataset_family: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    rules_json: Mapped[dict] = mapped_column(_JsonB, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, nullable=False
    )


class QcContractRunRow(WarehouseBase):
    """qc_contract_runs：单次 contract 执行结果（含 metrics / failed / warning rules）。"""

    __tablename__ = "qc_contract_runs"
    __table_args__ = (
        Index("idx_qc_contract_runs_contract_status", "contract_id", "status"),
        Index("idx_qc_contract_runs_dataset_version", "dataset_id", "version", "status"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    contract_id: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    failed_rules_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    warning_rules_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    artifacts_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class WorkflowRunRow(WarehouseBase):
    """workflow_runs：v1.6 通用 workflow 元数据（与 v1.5 argo_workflow_runs 并存）。"""

    __tablename__ = "workflow_runs"
    __table_args__ = (
        UniqueConstraint("workflow_namespace", "workflow_name", name="uq_workflow_runs_ns_name"),
        Index("idx_workflow_runs_status", "status"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    workflow_name: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_uid: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_namespace: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_template: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    parameters_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    workflow_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class WorkflowStepRow(WarehouseBase):
    """workflow_steps：workflow 内 step / template 的细粒度状态。"""

    __tablename__ = "workflow_steps"
    __table_args__ = (
        UniqueConstraint(
            "workflow_namespace", "workflow_name", "step_name",
            name="uq_workflow_steps_ns_wf_step",
        ),
        Index("idx_workflow_steps_workflow_phase", "workflow_name", "phase"),
        Index("idx_workflow_steps_dataset_version", "dataset_id", "version"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    workflow_name: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_namespace: Mapped[str | None] = mapped_column(Text, nullable=True)
    step_name: Mapped[str] = mapped_column(Text, nullable=False)
    template_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    pod_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class AssetProfileRow(WarehouseBase):
    """asset_profiles：单个 asset 的画像。"""

    __tablename__ = "asset_profiles"
    __table_args__ = (
        Index("idx_asset_profiles_dataset_version_family", "dataset_id", "version", "dataset_family"),
        Index("idx_asset_profiles_format_status", "asset_format", "status"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    profile_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    asset_uri: Mapped[str] = mapped_column(Text, nullable=False)
    asset_format: Mapped[str | None] = mapped_column(Text, nullable=True)
    layer: Mapped[str | None] = mapped_column(Text, nullable=True)
    bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    files_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    episodes_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    videos_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schema_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    null_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    profile_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class MlReadyDatasetRow(WarehouseBase):
    """ml_ready_datasets：训练就绪数据集元数据。"""

    __tablename__ = "ml_ready_datasets"
    __table_args__ = (
        Index("idx_ml_ready_datasets_dataset_version_status", "dataset_id", "version", "status"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_uri: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    train_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    val_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    test_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_card_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    feature_schema_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    quality_filter_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    lineage_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    quality_threshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    num_train: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    num_val: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    num_test: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DatasetPartitionRow(WarehouseBase):
    """dataset_partitions：partial resume 用的 partition 登记。"""

    __tablename__ = "dataset_partitions"
    __table_args__ = (
        Index("idx_dataset_partitions_dataset_version_type", "dataset_id", "version", "partition_type"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    partition_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_uri: Mapped[str] = mapped_column(Text, nullable=False)
    partition_type: Mapped[str] = mapped_column(Text, nullable=False)
    partition_index: Mapped[int] = mapped_column(Integer, nullable=False)
    partition_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    estimated_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class TaskHeartbeatRow(WarehouseBase):
    """task_heartbeats：长任务心跳；同一 task_id 允许重复行（按 updated_at DESC 取最新）。"""

    __tablename__ = "task_heartbeats"
    __table_args__ = (
        Index("idx_task_heartbeats_task_id", "task_id"),
        Index("idx_task_heartbeats_workflow_step", "workflow_name", "step_name"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    step_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    progress_current: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    progress_total: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    progress_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class OpenLineageEventRow(WarehouseBase):
    """openlineage_events：OpenLineage 风格事件表。"""

    __tablename__ = "openlineage_events"
    __table_args__ = (
        Index("idx_openlineage_events_type_time", "event_type", "event_time"),
    )

    id: Mapped[int] = mapped_column(BigSerial, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    job_namespace: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    inputs_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    outputs_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    facets_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    raw_event_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


def ensure_lake_tables(engine: Engine) -> None:
    """若不存在则创建 v1.4 + v1.5 + v1.6 元数据表；生产 Postgres 通常已有，checkfirst 保证幂等；SQLite 测试依赖此建表。"""
    WarehouseBase.metadata.create_all(engine, checkfirst=True)
