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
    Date,
    DateTime,
    Engine,
    Float,
    Index,
    Integer,
    PrimaryKeyConstraint,
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


# v1.8 warehouse 分层表：DIM / FACT / DWS / ADS / Backfill / SLA
#
# 仅作 ORM 反射 + 本地 SQLite 测试建表使用；远端 PostgreSQL schema 由
# postgres/migrations/006_v1_8_warehouse_quality_ops.sql 维护，infra 项目执行。


class DimDatasetRow(WarehouseBase):
    """dim_dataset：dataset 维度宽表，单 (dataset_id, version) 一条最新画像。"""

    __tablename__ = "dim_dataset"
    __table_args__ = (
        Index("idx_dim_dataset_dataset_id_version", "dataset_id", "version"),
        Index("idx_dim_dataset_dataset_family", "dataset_family"),
        Index("idx_dim_dataset_latest_status", "latest_status"),
    )

    dataset_key: Mapped[str] = mapped_column(Text, primary_key=True)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    ods_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    dwd_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    ads_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    ml_ready_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    latest_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    latest_quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class FactEtlRunRow(WarehouseBase):
    """fact_etl_run：单次 etl run 事实表。"""

    __tablename__ = "fact_etl_run"
    __table_args__ = (
        Index("idx_fact_etl_run_dt_dataset_phase", "dt", "dataset_id", "phase"),
        Index("idx_fact_etl_run_status_dt", "status", "dt"),
        Index("idx_fact_etl_run_family_dt", "dataset_family", "dt"),
    )

    run_key: Mapped[str] = mapped_column(Text, primary_key=True)
    job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    input_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    output_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    input_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    output_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    peak_memory_mb: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    archive_log_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class FactQcRuleResultRow(WarehouseBase):
    """fact_qc_rule_result：QC contract 单条规则结果。"""

    __tablename__ = "fact_qc_rule_result"
    __table_args__ = (
        Index("idx_fact_qc_rule_result_dt_dataset", "dt", "dataset_id"),
        Index("idx_fact_qc_rule_result_contract_rule_status", "contract_id", "rule_id", "status"),
        Index("idx_fact_qc_rule_result_family_status_dt", "dataset_family", "status", "dt"),
    )

    rule_result_key: Mapped[str] = mapped_column(Text, primary_key=True)
    run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    contract_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    metric: Mapped[str | None] = mapped_column(Text, nullable=True)
    op: Mapped[str | None] = mapped_column(Text, nullable=True)
    threshold_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    actual_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class FactWorkflowStepRow(WarehouseBase):
    """fact_workflow_step：workflow step 级事实表。"""

    __tablename__ = "fact_workflow_step"
    __table_args__ = (
        Index("idx_fact_workflow_step_dt_workflow", "dt", "workflow_name"),
        Index("idx_fact_workflow_step_phase_dt", "phase", "dt"),
        Index("idx_fact_workflow_step_dataset_version_dt", "dataset_id", "version", "dt"),
        Index("idx_fact_workflow_step_step_phase", "step_name", "phase"),
    )

    step_key: Mapped[str] = mapped_column(Text, primary_key=True)
    workflow_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_namespace: Mapped[str | None] = mapped_column(Text, nullable=True)
    workflow_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    step_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    template_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    pod_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    exit_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    container_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    archive_log_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    archive_log_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class FactAssetProfileRow(WarehouseBase):
    """fact_asset_profile：asset 画像事实表。"""

    __tablename__ = "fact_asset_profile"
    __table_args__ = (
        Index("idx_fact_asset_profile_dt_dataset_layer", "dt", "dataset_id", "layer"),
        Index("idx_fact_asset_profile_format_status", "asset_format", "status"),
        Index("idx_fact_asset_profile_family_dt", "dataset_family", "dt"),
    )

    asset_profile_key: Mapped[str] = mapped_column(Text, primary_key=True)
    profile_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    asset_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    asset_format: Mapped[str | None] = mapped_column(Text, nullable=True)
    layer: Mapped[str | None] = mapped_column(Text, nullable=True)
    bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    files_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    episodes_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    videos_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    schema_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    null_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DwsDatasetQualityDailyRow(WarehouseBase):
    """dws_dataset_quality_daily：dataset 日度宽表，主键 (dt, dataset_id, version)。"""

    __tablename__ = "dws_dataset_quality_daily"
    __table_args__ = (
        PrimaryKeyConstraint("dt", "dataset_id", "version"),
        Index("idx_dws_dataset_quality_daily_family_dt", "dataset_family", "dt"),
        Index("idx_dws_dataset_quality_daily_qc_pass_rate", "qc_pass_rate"),
        Index("idx_dws_dataset_quality_daily_etl_success_rate", "etl_success_rate"),
    )

    dt: Mapped[datetime] = mapped_column(Date, nullable=False)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    qc_run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qc_pass_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qc_warn_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qc_fail_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qc_pass_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    etl_run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    etl_success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    etl_fail_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    etl_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    workflow_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    workflow_success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    workflow_fail_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    workflow_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    ml_ready_rows: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_input_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_output_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    p95_etl_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_workflow_step_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    stale_heartbeat_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DwsRuleFailureDailyRow(WarehouseBase):
    """dws_rule_failure_daily：按规则维度的失败率聚合。"""

    __tablename__ = "dws_rule_failure_daily"
    __table_args__ = (
        PrimaryKeyConstraint("dt", "dataset_family", "contract_id", "rule_id", "severity"),
        Index("idx_dws_rule_failure_daily_dt_fail_rate", "dt", "fail_rate"),
        Index("idx_dws_rule_failure_daily_rule_dt", "rule_id", "dt"),
    )

    dt: Mapped[datetime] = mapped_column(Date, nullable=False)
    dataset_family: Mapped[str] = mapped_column(Text, nullable=False)
    contract_id: Mapped[str] = mapped_column(Text, nullable=False)
    rule_id: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    run_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pass_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    warn_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fail_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fail_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DwsWorkflowOpsDailyRow(WarehouseBase):
    """dws_workflow_ops_daily：workflow_type 维度日度运营指标。"""

    __tablename__ = "dws_workflow_ops_daily"
    __table_args__ = (PrimaryKeyConstraint("dt", "workflow_type"),)

    dt: Mapped[datetime] = mapped_column(Date, nullable=False)
    workflow_type: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    running_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    deadline_exceeded_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    oom_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    nonzero_exit_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class AdsQualityDashboardRow(WarehouseBase):
    """ads_quality_dashboard：质量看板表。"""

    __tablename__ = "ads_quality_dashboard"
    __table_args__ = (PrimaryKeyConstraint("dt", "dataset_id", "version"),)

    dt: Mapped[datetime] = mapped_column(Date, nullable=False)
    dataset_id: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    overall_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    quality_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    qc_pass_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    etl_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    workflow_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    top_failed_rule: Mapped[str | None] = mapped_column(Text, nullable=True)
    top_failed_rule_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    p95_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    ml_ready_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    raw_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    dwd_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    alert_level: Mapped[str | None] = mapped_column(Text, nullable=True)
    alert_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class AdsWorkflowOpsDashboardRow(WarehouseBase):
    """ads_workflow_ops_dashboard：workflow 运营看板。"""

    __tablename__ = "ads_workflow_ops_dashboard"
    __table_args__ = (PrimaryKeyConstraint("dt", "workflow_type"),)

    dt: Mapped[datetime] = mapped_column(Date, nullable=False)
    workflow_type: Mapped[str] = mapped_column(Text, nullable=False)
    workflow_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    success_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    failed_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    stale_heartbeat_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    oom_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deadline_exceeded_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    alert_level: Mapped[str | None] = mapped_column(Text, nullable=True)
    alert_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class BackfillPlanRow(WarehouseBase):
    """backfill_plans：补数计划。plan_json 为 jsonb（SQLite 退化到 JSON）。"""

    __tablename__ = "backfill_plans"

    plan_id: Mapped[str] = mapped_column(Text, primary_key=True)
    from_date: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    to_date: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    plan_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)


class BackfillTaskRow(WarehouseBase):
    """backfill_tasks：补数任务实例。plan_id 没有强 FK 约束（infra schema 设计如此）。"""

    __tablename__ = "backfill_tasks"
    __table_args__ = (
        Index("idx_backfill_tasks_plan_status", "plan_id", "status"),
        Index("idx_backfill_tasks_dataset_dt_phase", "dataset_id", "dt", "phase"),
    )

    task_id: Mapped[str] = mapped_column(Text, primary_key=True)
    plan_id: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    recommended_command: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class SlaPolicyRow(WarehouseBase):
    """sla_policies：SLA 策略配置。"""

    __tablename__ = "sla_policies"

    policy_id: Mapped[str] = mapped_column(Text, primary_key=True)
    policy_name: Mapped[str] = mapped_column(Text, nullable=False)
    dataset_pattern: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    deadline_hour: Mapped[int | None] = mapped_column(Integer, nullable=True)
    required_outputs_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    min_qc_pass_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_etl_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_failed_workflows: Mapped[int | None] = mapped_column(Integer, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class SlaCheckRow(WarehouseBase):
    """sla_checks：单次 SLA 校验结果。"""

    __tablename__ = "sla_checks"
    __table_args__ = (
        Index("idx_sla_checks_dt_status", "dt", "status"),
        Index("idx_sla_checks_dataset_version_dt", "dataset_id", "version", "dt"),
        Index("idx_sla_checks_policy_dt", "policy_id", "dt"),
    )

    check_id: Mapped[str] = mapped_column(Text, primary_key=True)
    policy_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    qc_pass_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    etl_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    workflow_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    missing_outputs_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)
    failed_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    metrics_json: Mapped[dict | None] = mapped_column(_JsonB, nullable=True)


class DatasetPartitionReadinessRow(WarehouseBase):
    """dataset_partition_readiness：分区就绪登记。"""

    __tablename__ = "dataset_partition_readiness"

    readiness_key: Mapped[str] = mapped_column(Text, primary_key=True)
    dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    partition_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    phase: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_ready: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    quality_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    row_count: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


# ============================================================
# v1.9 AI Inference Data Plane（与 postgres/migrations/007 对齐）
# ============================================================


class ModelRegistryRow(WarehouseBase):
    """model_registry：可调用模型版本注册表。"""

    __tablename__ = "model_registry"
    __table_args__ = (
        Index("idx_model_registry_model_type", "model_type"),
        Index("idx_model_registry_backend", "backend"),
        Index("idx_model_registry_status", "status"),
    )

    model_id: Mapped[str] = mapped_column(Text, primary_key=True)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    model_type: Mapped[str] = mapped_column(Text, nullable=False)
    backend: Mapped[str] = mapped_column(Text, nullable=False)
    endpoint_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_schema_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    output_schema_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    max_batch_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    timeout_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="ACTIVE")
    tags_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class InferenceJobRow(WarehouseBase):
    """inference_jobs：批量推理任务。"""

    __tablename__ = "inference_jobs"
    __table_args__ = (
        Index("idx_inference_jobs_status_created_at", "status", "created_at"),
        Index("idx_inference_jobs_model_status", "model_id", "status"),
        Index("idx_inference_jobs_dataset_version", "dataset_id", "version"),
        Index("idx_inference_jobs_task_type_status", "task_type", "status"),
    )

    job_id: Mapped[str] = mapped_column(Text, primary_key=True)
    job_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_id: Mapped[str] = mapped_column(Text, nullable=False)
    input_uri: Mapped[str] = mapped_column(Text, nullable=False)
    output_uri: Mapped[str] = mapped_column(Text, nullable=False)
    input_format: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_format: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_family: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    batch_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_workers: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_samples: Mapped[int] = mapped_column(BigInteger, default=0)
    processed_samples: Mapped[int] = mapped_column(BigInteger, default=0)
    failed_samples: Mapped[int] = mapped_column(BigInteger, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    config_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    metrics_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class InferenceOutputRow(WarehouseBase):
    """inference_outputs：单样本推理输出。"""

    __tablename__ = "inference_outputs"
    __table_args__ = (
        Index("idx_inference_outputs_job_id", "job_id"),
        Index("idx_inference_outputs_model_created_at", "model_id", "created_at"),
        Index("idx_inference_outputs_dataset_version", "dataset_id", "version"),
        Index("idx_inference_outputs_prediction_type_status", "prediction_type", "status"),
    )

    output_id: Mapped[str] = mapped_column(Text, primary_key=True)
    job_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_id: Mapped[str] = mapped_column(Text, nullable=False)
    sample_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    episode_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    frame_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    prediction_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    prediction_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    token_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class InferenceFailureRow(WarehouseBase):
    """inference_failures：失败 / 可重试样本。"""

    __tablename__ = "inference_failures"
    __table_args__ = (
        Index("idx_inference_failures_job_id", "job_id"),
        Index("idx_inference_failures_error_type", "error_type"),
        Index("idx_inference_failures_retryable", "retryable"),
    )

    failure_id: Mapped[str] = mapped_column(Text, primary_key=True)
    job_id: Mapped[str] = mapped_column(Text, nullable=False)
    model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    sample_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retryable: Mapped[bool] = mapped_column(Boolean, default=True)
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DistillationDatasetRow(WarehouseBase):
    """distillation_datasets：teacher 推理结果蒸馏出的训练数据集。"""

    __tablename__ = "distillation_datasets"
    __table_args__ = (
        Index("idx_distillation_datasets_dataset_version", "dataset_id", "version"),
        Index("idx_distillation_datasets_teacher_model", "teacher_model_id"),
        Index("idx_distillation_datasets_status", "status"),
    )

    distill_id: Mapped[str] = mapped_column(Text, primary_key=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_inference_job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    teacher_model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    distill_format: Mapped[str] = mapped_column(Text, nullable=False)
    output_uri: Mapped[str] = mapped_column(Text, nullable=False)
    train_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    val_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    test_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_card_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    num_train: Mapped[int] = mapped_column(BigInteger, default=0)
    num_val: Mapped[int] = mapped_column(BigInteger, default=0)
    num_test: Mapped[int] = mapped_column(BigInteger, default=0)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    metrics_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class InferenceBenchmarkRunRow(WarehouseBase):
    """inference_benchmark_runs：推理后端吞吐 / 时延压测。"""

    __tablename__ = "inference_benchmark_runs"
    __table_args__ = (
        Index("idx_inference_benchmark_runs_model_created_at", "model_id", "created_at"),
        Index("idx_inference_benchmark_runs_backend_status", "backend", "status"),
        Index("idx_inference_benchmark_runs_workload", "workload_name"),
    )

    benchmark_id: Mapped[str] = mapped_column(Text, primary_key=True)
    model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    backend: Mapped[str | None] = mapped_column(Text, nullable=True)
    workload_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    concurrency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    batch_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_samples: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    succeeded_samples: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    failed_samples: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    samples_per_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    p50_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    p99_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    cost_estimate_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    metrics_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class AiTaskEventRow(WarehouseBase):
    """ai_task_events：AI 任务统一事件流（为 Kafka / Redis Streams 预留）。"""

    __tablename__ = "ai_task_events"
    __table_args__ = (
        Index("idx_ai_task_events_type_created_at", "event_type", "created_at"),
        Index("idx_ai_task_events_task_id", "task_id"),
        Index("idx_ai_task_events_job_id", "job_id"),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    task_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    dataset_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DeadLetterTaskRow(WarehouseBase):
    """dead_letter_tasks：多次重试仍失败的任务死信。"""

    __tablename__ = "dead_letter_tasks"
    __table_args__ = (
        Index("idx_dead_letter_tasks_type_created_at", "task_type", "created_at"),
        Index("idx_dead_letter_tasks_job_id", "job_id"),
    )

    dead_letter_id: Mapped[str] = mapped_column(Text, primary_key=True)
    task_type: Mapped[str] = mapped_column(Text, nullable=False)
    task_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[Any | None] = mapped_column(_JsonB, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class DwsInferenceJobDailyRow(WarehouseBase):
    """dws_inference_job_daily：推理任务按 (dt, model_id, task_type) 日度聚合。"""

    __tablename__ = "dws_inference_job_daily"
    __table_args__ = (
        PrimaryKeyConstraint("dt", "model_id", "task_type"),
        Index("idx_dws_inference_job_daily_model_dt", "model_id", "dt"),
        Index("idx_dws_inference_job_daily_backend_dt", "backend", "dt"),
    )

    dt: Mapped[datetime] = mapped_column(Date, nullable=False)
    model_id: Mapped[str] = mapped_column(Text, nullable=False)
    backend: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_type: Mapped[str] = mapped_column(Text, nullable=False)
    job_count: Mapped[int] = mapped_column(Integer, default=0)
    success_count: Mapped[int] = mapped_column(Integer, default=0)
    fail_count: Mapped[int] = mapped_column(Integer, default=0)
    success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_samples: Mapped[int] = mapped_column(BigInteger, default=0)
    samples_per_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


class AdsInferenceDashboardRow(WarehouseBase):
    """ads_inference_dashboard：推理运营看板 / 告警最终层。"""

    __tablename__ = "ads_inference_dashboard"
    __table_args__ = (
        PrimaryKeyConstraint("dt", "model_id", "task_type"),
        Index("idx_ads_inference_dashboard_alert_level_dt", "alert_level", "dt"),
        Index("idx_ads_inference_dashboard_model_dt", "model_id", "dt"),
    )

    dt: Mapped[datetime] = mapped_column(Date, nullable=False)
    model_id: Mapped[str] = mapped_column(Text, nullable=False)
    backend: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_type: Mapped[str] = mapped_column(Text, nullable=False)
    overall_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_samples: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    samples_per_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    error_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    top_error_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    alert_level: Mapped[str | None] = mapped_column(Text, nullable=True)
    alert_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, nullable=False)


WAREHOUSE_METRICS_TABLES = (
    "dim_dataset",
    "fact_etl_run",
    "fact_qc_rule_result",
    "fact_workflow_step",
    "fact_asset_profile",
    "dws_dataset_quality_daily",
    "dws_rule_failure_daily",
    "dws_workflow_ops_daily",
    "ads_quality_dashboard",
    "ads_workflow_ops_dashboard",
    "backfill_plans",
    "backfill_tasks",
    "sla_policies",
    "sla_checks",
    "dataset_partition_readiness",
)

# v1.9 推理数据平面表（独立于 v1.8 WAREHOUSE_METRICS_TABLES，避免把 v1.8 build 耦合到 v1.9 schema）。
INFERENCE_PLANE_TABLES = (
    "model_registry",
    "inference_jobs",
    "inference_outputs",
    "inference_failures",
    "distillation_datasets",
    "inference_benchmark_runs",
    "ai_task_events",
    "dead_letter_tasks",
    "dws_inference_job_daily",
    "ads_inference_dashboard",
)


def ensure_lake_tables(engine: Engine) -> None:
    """若不存在则创建 v1.4 + v1.5 + v1.6 + v1.8 元数据表；生产 Postgres 通常已有，checkfirst 保证幂等；SQLite 测试依赖此建表。"""
    WarehouseBase.metadata.create_all(engine, checkfirst=True)
