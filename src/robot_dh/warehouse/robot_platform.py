"""v1.6 warehouse 写入与读取 helper。

为避免改动 v1.4/v1.5 的 `WarehouseService` 主体，本模块以独立函数 + `PlatformWarehouse`
组合的方式提供 v1.6 表（task_heartbeats / dataset_partitions / qc_contracts /
qc_contract_runs / asset_profiles / ml_ready_datasets / workflow_runs / workflow_steps /
openlineage_events）的访问。

使用：
    svc = PlatformWarehouse(soft=True)
    svc.record_task_heartbeat(...)

DB 不可用 / 表缺失：
    soft=True（默认）时返回 None，仅 WARNING；
    soft=False 时抛 LakeMetadataUnavailableError。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import inspect, select, desc
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AssetProfileRow,
    DatasetPartitionRow,
    MlReadyDatasetRow,
    OpenLineageEventRow,
    QcContractRow,
    QcContractRunRow,
    TaskHeartbeatRow,
    WorkflowRunRow,
    WorkflowStepRow,
    ensure_lake_tables,
)
from robot_dh.warehouse.service import (
    LakeMetadataUnavailableError,
    V15SchemaMissingError,
    _is_schema_drift_error,
)

LOG = logging.getLogger(__name__)


PLATFORM_TABLES = (
    "qc_contracts",
    "qc_contract_runs",
    "workflow_runs",
    "workflow_steps",
    "asset_profiles",
    "ml_ready_datasets",
    "dataset_partitions",
    "task_heartbeats",
    "openlineage_events",
)


class PlatformSchemaMissingError(RuntimeError):
    """平台层 9 张表缺失时（严格模式）抛出，提示先执行 005_robot_platform.sql 迁移。"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PlatformWarehouse:
    """v1.6 表的访问层。与 v1.4/v1.5 `WarehouseService` 解耦，避免大改既有 service.py。"""

    def __init__(self, *, db_uri: str | None = None, soft: bool = True) -> None:
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
            self._engine = engine
        return self._engine

    def _session(self) -> Session:
        return Session(self._get_engine(), expire_on_commit=False, future=True)

    def _missing_tables(self) -> list[str]:
        engine = self._get_engine()
        existing = set(inspect(engine).get_table_names())
        return [t for t in PLATFORM_TABLES if t not in existing]

    def _ensure_or_warn(self, op: str) -> bool:
        try:
            missing = self._missing_tables()
        except SQLAlchemyError as err:
            self._handle_write_error(op, err)
            return False
        if not missing:
            return True
        msg = (
            "robot platform PostgreSQL tables missing: "
            + ", ".join(missing)
            + ". Apply schema first (postgres/migrations/005_robot_platform.sql)."
        )
        if self._soft:
            LOG.warning("%s skipped: %s", op, msg)
            return False
        raise PlatformSchemaMissingError(msg)

    def _handle_write_error(self, op: str, err: Exception) -> None:
        if _is_schema_drift_error(err):
            msg = (
                f"warehouse {op} schema mismatch: {err}. "
                "Apply v1.6 schema migration first."
            )
            if self._soft:
                LOG.error(msg)
                raise PlatformSchemaMissingError(msg) from err
            raise LakeMetadataUnavailableError(msg) from err
        if self._soft:
            LOG.warning("warehouse v1.6 %s failed (continuing in soft mode): %s", op, err)
            return
        raise LakeMetadataUnavailableError(f"warehouse v1.6 {op} failed: {err}") from err

    # ============ task_heartbeats ============

    def record_task_heartbeat(
        self,
        *,
        task_id: str,
        workflow_name: str | None = None,
        step_name: str | None = None,
        dataset_id: str | None = None,
        version: str | None = None,
        phase: str | None = None,
        progress_current: int | None = None,
        progress_total: int | None = None,
        progress_unit: str | None = None,
        message: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("record_task_heartbeat"):
                return None
            with self._session() as session:
                row = TaskHeartbeatRow(
                    task_id=task_id,
                    workflow_name=workflow_name,
                    step_name=step_name,
                    dataset_id=dataset_id,
                    version=version,
                    phase=phase,
                    progress_current=progress_current,
                    progress_total=progress_total,
                    progress_unit=progress_unit,
                    message=message,
                    metrics_json=dict(metrics) if metrics else None,
                    updated_at=_utcnow(),
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("record_task_heartbeat", err)
            return None

    def latest_heartbeat(self, *, task_id: str) -> dict[str, Any] | None:
        try:
            with self._session() as session:
                row = session.scalar(
                    select(TaskHeartbeatRow)
                    .where(TaskHeartbeatRow.task_id == task_id)
                    .order_by(desc(TaskHeartbeatRow.updated_at))
                    .limit(1)
                )
                return _heartbeat_to_dict(row) if row is not None else None
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"latest_heartbeat failed: {err}") from err

    # ============ dataset_partitions ============

    def record_dataset_partition(
        self,
        *,
        partition_id: str,
        dataset_id: str,
        version: str,
        dataset_uri: str,
        partition_type: str,
        partition_index: int,
        partition_uri: str | None = None,
        dataset_family: str | None = None,
        input_bytes: int | None = None,
        estimated_rows: int | None = None,
        status: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("record_dataset_partition"):
                return None
            with self._session() as session:
                row = session.scalar(
                    select(DatasetPartitionRow).where(DatasetPartitionRow.partition_id == partition_id)
                )
                if row is None:
                    row = DatasetPartitionRow(
                        partition_id=partition_id,
                        dataset_id=dataset_id,
                        version=version,
                        dataset_family=dataset_family,
                        dataset_uri=dataset_uri,
                        partition_type=partition_type,
                        partition_index=int(partition_index),
                        partition_uri=partition_uri,
                        input_bytes=input_bytes,
                        estimated_rows=estimated_rows,
                        status=status,
                        metrics_json=dict(metrics) if metrics else None,
                    )
                    session.add(row)
                else:
                    row.dataset_uri = dataset_uri
                    row.partition_type = partition_type
                    row.partition_index = int(partition_index)
                    if partition_uri is not None:
                        row.partition_uri = partition_uri
                    if dataset_family is not None:
                        row.dataset_family = dataset_family
                    if input_bytes is not None:
                        row.input_bytes = int(input_bytes)
                    if estimated_rows is not None:
                        row.estimated_rows = int(estimated_rows)
                    if status is not None:
                        row.status = status
                    if metrics is not None:
                        existing = dict(row.metrics_json) if row.metrics_json else {}
                        existing.update(metrics)
                        row.metrics_json = existing
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("record_dataset_partition", err)
            return None

    def list_dataset_partitions(
        self,
        *,
        dataset_id: str | None = None,
        version: str | None = None,
        partition_type: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = (
                    select(DatasetPartitionRow)
                    .order_by(DatasetPartitionRow.created_at.desc())
                    .limit(limit)
                )
                if dataset_id is not None:
                    stmt = stmt.where(DatasetPartitionRow.dataset_id == dataset_id)
                if version is not None:
                    stmt = stmt.where(DatasetPartitionRow.version == version)
                if partition_type is not None:
                    stmt = stmt.where(DatasetPartitionRow.partition_type == partition_type)
                rows = session.scalars(stmt).all()
                return [_partition_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_dataset_partitions failed: {err}") from err

    # ============ qc_contracts / qc_contract_runs ============

    def upsert_qc_contract(
        self,
        *,
        contract_id: str,
        dataset_family: str,
        version: str,
        rules: dict[str, Any],
        description: str | None = None,
        enabled: bool = True,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("upsert_qc_contract"):
                return None
            with self._session() as session:
                row = session.scalar(
                    select(QcContractRow).where(QcContractRow.contract_id == contract_id)
                )
                if row is None:
                    row = QcContractRow(
                        contract_id=contract_id,
                        dataset_family=dataset_family,
                        version=version,
                        description=description,
                        rules_json=dict(rules),
                        enabled=enabled,
                    )
                    session.add(row)
                else:
                    row.dataset_family = dataset_family
                    row.version = version
                    row.description = description
                    row.rules_json = dict(rules)
                    row.enabled = enabled
                    row.updated_at = _utcnow()
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("upsert_qc_contract", err)
            return None

    def record_qc_contract_run(
        self,
        *,
        run_id: str,
        contract_id: str,
        status: str,
        dataset_id: str | None = None,
        version: str | None = None,
        dataset_family: str | None = None,
        dataset_uri: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_sec: float | None = None,
        metrics: dict[str, Any] | None = None,
        failed_rules: list[dict[str, Any]] | None = None,
        warning_rules: list[dict[str, Any]] | None = None,
        artifacts_uri: str | None = None,
        error_message: str | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("record_qc_contract_run"):
                return None
            with self._session() as session:
                row = QcContractRunRow(
                    run_id=run_id,
                    contract_id=contract_id,
                    dataset_id=dataset_id,
                    version=version,
                    dataset_family=dataset_family,
                    dataset_uri=dataset_uri,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_sec=duration_sec,
                    metrics_json=dict(metrics) if metrics else None,
                    failed_rules_json={"items": list(failed_rules or [])},
                    warning_rules_json={"items": list(warning_rules or [])},
                    artifacts_uri=artifacts_uri,
                    error_message=error_message,
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("record_qc_contract_run", err)
            return None

    def list_qc_contracts(self, *, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                rows = session.scalars(
                    select(QcContractRow).order_by(QcContractRow.created_at.desc()).limit(limit)
                ).all()
                return [_contract_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_qc_contracts failed: {err}") from err

    def list_qc_contract_runs(
        self,
        *,
        contract_id: str | None = None,
        dataset_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = (
                    select(QcContractRunRow)
                    .order_by(QcContractRunRow.created_at.desc())
                    .limit(limit)
                )
                if contract_id is not None:
                    stmt = stmt.where(QcContractRunRow.contract_id == contract_id)
                if dataset_id is not None:
                    stmt = stmt.where(QcContractRunRow.dataset_id == dataset_id)
                if status is not None:
                    stmt = stmt.where(QcContractRunRow.status == status)
                rows = session.scalars(stmt).all()
                return [_contract_run_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_qc_contract_runs failed: {err}") from err

    def get_qc_contract_run(self, run_id: str) -> dict[str, Any] | None:
        try:
            with self._session() as session:
                row = session.scalar(
                    select(QcContractRunRow).where(QcContractRunRow.run_id == run_id)
                )
                return _contract_run_to_dict(row) if row is not None else None
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"get_qc_contract_run failed: {err}") from err

    # ============ asset_profiles ============

    def record_asset_profile(
        self,
        *,
        profile_id: str,
        asset_uri: str,
        dataset_id: str | None = None,
        version: str | None = None,
        dataset_family: str | None = None,
        asset_format: str | None = None,
        layer: str | None = None,
        bytes_: int | None = None,
        rows: int | None = None,
        files_count: int | None = None,
        episodes_count: int | None = None,
        videos_count: int | None = None,
        schema_hash: str | None = None,
        null_rate: float | None = None,
        profile: dict[str, Any] | None = None,
        status: str | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("record_asset_profile"):
                return None
            with self._session() as session:
                row = session.scalar(
                    select(AssetProfileRow).where(AssetProfileRow.profile_id == profile_id)
                )
                payload_kwargs = dict(
                    profile_id=profile_id,
                    asset_uri=asset_uri,
                    dataset_id=dataset_id,
                    version=version,
                    dataset_family=dataset_family,
                    asset_format=asset_format,
                    layer=layer,
                    bytes=bytes_,
                    rows=rows,
                    files_count=files_count,
                    episodes_count=episodes_count,
                    videos_count=videos_count,
                    schema_hash=schema_hash,
                    null_rate=null_rate,
                    profile_json=dict(profile) if profile else None,
                    status=status,
                )
                if row is None:
                    row = AssetProfileRow(**payload_kwargs)
                    session.add(row)
                else:
                    for k, v in payload_kwargs.items():
                        if v is not None:
                            setattr(row, k, v)
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("record_asset_profile", err)
            return None

    def list_asset_profiles(
        self,
        *,
        dataset_id: str | None = None,
        version: str | None = None,
        dataset_family: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = (
                    select(AssetProfileRow)
                    .order_by(AssetProfileRow.created_at.desc())
                    .limit(limit)
                )
                if dataset_id is not None:
                    stmt = stmt.where(AssetProfileRow.dataset_id == dataset_id)
                if version is not None:
                    stmt = stmt.where(AssetProfileRow.version == version)
                if dataset_family is not None:
                    stmt = stmt.where(AssetProfileRow.dataset_family == dataset_family)
                rows = session.scalars(stmt).all()
                return [_profile_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_asset_profiles failed: {err}") from err

    def get_asset_profile(self, profile_id: str) -> dict[str, Any] | None:
        try:
            with self._session() as session:
                row = session.scalar(
                    select(AssetProfileRow).where(AssetProfileRow.profile_id == profile_id)
                )
                return _profile_to_dict(row) if row is not None else None
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"get_asset_profile failed: {err}") from err

    # ============ ml_ready_datasets ============

    def record_ml_ready_dataset(
        self,
        *,
        dataset_id: str,
        version: str,
        output_uri: str,
        dataset_family: str | None = None,
        train_uri: str | None = None,
        val_uri: str | None = None,
        test_uri: str | None = None,
        dataset_card_uri: str | None = None,
        feature_schema_uri: str | None = None,
        quality_filter_uri: str | None = None,
        lineage_uri: str | None = None,
        quality_threshold: float | None = None,
        num_train: int | None = None,
        num_val: int | None = None,
        num_test: int | None = None,
        status: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("record_ml_ready_dataset"):
                return None
            with self._session() as session:
                row = session.scalar(
                    select(MlReadyDatasetRow).where(MlReadyDatasetRow.output_uri == output_uri)
                )
                payload_kwargs = dict(
                    dataset_id=dataset_id,
                    version=version,
                    dataset_family=dataset_family,
                    output_uri=output_uri,
                    train_uri=train_uri,
                    val_uri=val_uri,
                    test_uri=test_uri,
                    dataset_card_uri=dataset_card_uri,
                    feature_schema_uri=feature_schema_uri,
                    quality_filter_uri=quality_filter_uri,
                    lineage_uri=lineage_uri,
                    quality_threshold=quality_threshold,
                    num_train=num_train,
                    num_val=num_val,
                    num_test=num_test,
                    status=status,
                    metrics_json=dict(metrics) if metrics else None,
                )
                if row is None:
                    row = MlReadyDatasetRow(**payload_kwargs)
                    session.add(row)
                else:
                    for k, v in payload_kwargs.items():
                        if v is not None:
                            setattr(row, k, v)
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("record_ml_ready_dataset", err)
            return None

    def list_ml_ready_datasets(self, *, limit: int = 100) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                rows = session.scalars(
                    select(MlReadyDatasetRow).order_by(MlReadyDatasetRow.created_at.desc()).limit(limit)
                ).all()
                return [_ml_ready_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_ml_ready_datasets failed: {err}") from err

    def get_ml_ready_dataset(
        self, *, dataset_id: str, version: str
    ) -> dict[str, Any] | None:
        try:
            with self._session() as session:
                row = session.scalar(
                    select(MlReadyDatasetRow).where(
                        MlReadyDatasetRow.dataset_id == dataset_id,
                        MlReadyDatasetRow.version == version,
                    ).order_by(MlReadyDatasetRow.created_at.desc()).limit(1)
                )
                return _ml_ready_to_dict(row) if row is not None else None
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"get_ml_ready_dataset failed: {err}") from err

    # ============ workflow_runs / workflow_steps ============

    def upsert_workflow_run(
        self,
        *,
        workflow_name: str,
        workflow_namespace: str | None = None,
        workflow_uid: str | None = None,
        workflow_template: str | None = None,
        workflow_type: str | None = None,
        status: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_sec: float | None = None,
        parameters: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        workflow_doc: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("upsert_workflow_run"):
                return None
            with self._session() as session:
                stmt = select(WorkflowRunRow).where(
                    WorkflowRunRow.workflow_name == workflow_name,
                )
                if workflow_namespace is not None:
                    stmt = stmt.where(WorkflowRunRow.workflow_namespace == workflow_namespace)
                row = session.scalar(stmt)
                payload_kwargs = dict(
                    workflow_name=workflow_name,
                    workflow_uid=workflow_uid,
                    workflow_namespace=workflow_namespace,
                    workflow_template=workflow_template,
                    workflow_type=workflow_type,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_sec=duration_sec,
                    parameters_json=dict(parameters) if parameters else None,
                    metrics_json=dict(metrics) if metrics else None,
                    workflow_json=dict(workflow_doc) if workflow_doc else None,
                )
                if row is None:
                    row = WorkflowRunRow(**payload_kwargs)
                    session.add(row)
                else:
                    for k, v in payload_kwargs.items():
                        if v is not None:
                            setattr(row, k, v)
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("upsert_workflow_run", err)
            return None

    def upsert_workflow_step(
        self,
        *,
        workflow_name: str,
        step_name: str,
        workflow_namespace: str | None = None,
        template_name: str | None = None,
        pod_name: str | None = None,
        phase: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        duration_sec: float | None = None,
        dataset_id: str | None = None,
        version: str | None = None,
        dataset_family: str | None = None,
        input_uri: str | None = None,
        output_uri: str | None = None,
        metrics: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("upsert_workflow_step"):
                return None
            with self._session() as session:
                stmt = select(WorkflowStepRow).where(
                    WorkflowStepRow.workflow_name == workflow_name,
                    WorkflowStepRow.step_name == step_name,
                )
                if workflow_namespace is not None:
                    stmt = stmt.where(WorkflowStepRow.workflow_namespace == workflow_namespace)
                row = session.scalar(stmt)
                payload_kwargs = dict(
                    workflow_name=workflow_name,
                    workflow_namespace=workflow_namespace,
                    step_name=step_name,
                    template_name=template_name,
                    pod_name=pod_name,
                    phase=phase,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_sec=duration_sec,
                    dataset_id=dataset_id,
                    version=version,
                    dataset_family=dataset_family,
                    input_uri=input_uri,
                    output_uri=output_uri,
                    metrics_json=dict(metrics) if metrics else None,
                    message=message,
                )
                if row is None:
                    row = WorkflowStepRow(**payload_kwargs)
                    session.add(row)
                else:
                    for k, v in payload_kwargs.items():
                        if v is not None:
                            setattr(row, k, v)
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("upsert_workflow_step", err)
            return None

    def list_workflow_runs(
        self, *, status: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = (
                    select(WorkflowRunRow)
                    .order_by(WorkflowRunRow.created_at.desc())
                    .limit(limit)
                )
                if status is not None:
                    stmt = stmt.where(WorkflowRunRow.status == status)
                rows = session.scalars(stmt).all()
                return [_workflow_run_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_workflow_runs failed: {err}") from err

    def get_workflow_run(
        self, *, workflow_name: str, workflow_namespace: str | None = None
    ) -> dict[str, Any] | None:
        try:
            with self._session() as session:
                stmt = select(WorkflowRunRow).where(
                    WorkflowRunRow.workflow_name == workflow_name,
                )
                if workflow_namespace is not None:
                    stmt = stmt.where(WorkflowRunRow.workflow_namespace == workflow_namespace)
                row = session.scalar(stmt)
                return _workflow_run_to_dict(row) if row is not None else None
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"get_workflow_run failed: {err}") from err

    def list_workflow_steps(
        self, *, workflow_name: str, workflow_namespace: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = (
                    select(WorkflowStepRow)
                    .where(WorkflowStepRow.workflow_name == workflow_name)
                    .order_by(WorkflowStepRow.started_at.asc().nullslast())
                    .limit(limit)
                )
                if workflow_namespace is not None:
                    stmt = stmt.where(WorkflowStepRow.workflow_namespace == workflow_namespace)
                rows = session.scalars(stmt).all()
                return [_workflow_step_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_workflow_steps failed: {err}") from err

    # ============ openlineage_events ============

    def record_openlineage_event(
        self,
        *,
        event_id: str,
        event_type: str,
        event_time: datetime,
        job_namespace: str | None = None,
        job_name: str | None = None,
        run_id: str | None = None,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
        facets: dict[str, Any] | None = None,
        raw_event: dict[str, Any] | None = None,
    ) -> int | None:
        try:
            if not self._ensure_or_warn("record_openlineage_event"):
                return None
            with self._session() as session:
                row = OpenLineageEventRow(
                    event_id=event_id,
                    event_type=event_type,
                    event_time=event_time,
                    job_namespace=job_namespace,
                    job_name=job_name,
                    run_id=run_id,
                    inputs_json={"items": list(inputs or [])},
                    outputs_json={"items": list(outputs or [])},
                    facets_json=dict(facets) if facets else None,
                    raw_event_json=dict(raw_event) if raw_event else None,
                )
                session.add(row)
                session.commit()
                return row.id
        except (SQLAlchemyError, PlatformSchemaMissingError) as err:
            self._handle_write_error("record_openlineage_event", err)
            return None

    def list_openlineage_events(
        self, *, event_type: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        try:
            with self._session() as session:
                stmt = (
                    select(OpenLineageEventRow)
                    .order_by(OpenLineageEventRow.event_time.desc())
                    .limit(limit)
                )
                if event_type is not None:
                    stmt = stmt.where(OpenLineageEventRow.event_type == event_type)
                rows = session.scalars(stmt).all()
                return [_ol_event_to_dict(r) for r in rows]
        except SQLAlchemyError as err:
            raise LakeMetadataUnavailableError(f"list_openlineage_events failed: {err}") from err


# ---------- to_dict 辅助 ----------


def _heartbeat_to_dict(row: TaskHeartbeatRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "task_id": row.task_id,
        "workflow_name": row.workflow_name,
        "step_name": row.step_name,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "phase": row.phase,
        "progress_current": row.progress_current,
        "progress_total": row.progress_total,
        "progress_unit": row.progress_unit,
        "message": row.message,
        "metrics_json": row.metrics_json,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _partition_to_dict(row: DatasetPartitionRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "partition_id": row.partition_id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "dataset_family": row.dataset_family,
        "dataset_uri": row.dataset_uri,
        "partition_type": row.partition_type,
        "partition_index": row.partition_index,
        "partition_uri": row.partition_uri,
        "input_bytes": row.input_bytes,
        "estimated_rows": row.estimated_rows,
        "status": row.status,
        "metrics_json": row.metrics_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _contract_to_dict(row: QcContractRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "contract_id": row.contract_id,
        "dataset_family": row.dataset_family,
        "version": row.version,
        "description": row.description,
        "rules_json": row.rules_json,
        "enabled": row.enabled,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _contract_run_to_dict(row: QcContractRunRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "run_id": row.run_id,
        "contract_id": row.contract_id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "dataset_family": row.dataset_family,
        "dataset_uri": row.dataset_uri,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_sec": row.duration_sec,
        "metrics_json": row.metrics_json,
        "failed_rules_json": (
            row.failed_rules_json.get("items") if isinstance(row.failed_rules_json, dict) else None
        ),
        "warning_rules_json": (
            row.warning_rules_json.get("items") if isinstance(row.warning_rules_json, dict) else None
        ),
        "artifacts_uri": row.artifacts_uri,
        "error_message": row.error_message,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _profile_to_dict(row: AssetProfileRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "profile_id": row.profile_id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "dataset_family": row.dataset_family,
        "asset_uri": row.asset_uri,
        "asset_format": row.asset_format,
        "layer": row.layer,
        "bytes": row.bytes,
        "rows": row.rows,
        "files_count": row.files_count,
        "episodes_count": row.episodes_count,
        "videos_count": row.videos_count,
        "schema_hash": row.schema_hash,
        "null_rate": row.null_rate,
        "profile_json": row.profile_json,
        "status": row.status,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _ml_ready_to_dict(row: MlReadyDatasetRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "dataset_family": row.dataset_family,
        "output_uri": row.output_uri,
        "train_uri": row.train_uri,
        "val_uri": row.val_uri,
        "test_uri": row.test_uri,
        "dataset_card_uri": row.dataset_card_uri,
        "feature_schema_uri": row.feature_schema_uri,
        "quality_filter_uri": row.quality_filter_uri,
        "lineage_uri": row.lineage_uri,
        "quality_threshold": row.quality_threshold,
        "num_train": row.num_train,
        "num_val": row.num_val,
        "num_test": row.num_test,
        "status": row.status,
        "metrics_json": row.metrics_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _workflow_run_to_dict(row: WorkflowRunRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "workflow_name": row.workflow_name,
        "workflow_uid": row.workflow_uid,
        "workflow_namespace": row.workflow_namespace,
        "workflow_template": row.workflow_template,
        "workflow_type": row.workflow_type,
        "status": row.status,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_sec": row.duration_sec,
        "parameters_json": row.parameters_json,
        "metrics_json": row.metrics_json,
        "workflow_json": row.workflow_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _workflow_step_to_dict(row: WorkflowStepRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "workflow_name": row.workflow_name,
        "workflow_namespace": row.workflow_namespace,
        "step_name": row.step_name,
        "template_name": row.template_name,
        "pod_name": row.pod_name,
        "phase": row.phase,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_sec": row.duration_sec,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "dataset_family": row.dataset_family,
        "input_uri": row.input_uri,
        "output_uri": row.output_uri,
        "metrics_json": row.metrics_json,
        "message": row.message,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _ol_event_to_dict(row: OpenLineageEventRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "event_id": row.event_id,
        "event_type": row.event_type,
        "event_time": row.event_time.isoformat() if row.event_time else None,
        "job_namespace": row.job_namespace,
        "job_name": row.job_name,
        "run_id": row.run_id,
        "inputs": row.inputs_json.get("items") if isinstance(row.inputs_json, dict) else None,
        "outputs": row.outputs_json.get("items") if isinstance(row.outputs_json, dict) else None,
        "facets_json": row.facets_json,
        "raw_event_json": row.raw_event_json,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }
