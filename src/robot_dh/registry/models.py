from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _isoformat(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _loads_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    return value


@dataclass(slots=True)
class DatasetRecord:
    id: int
    dataset_id: str
    version: str
    task_type: str | None
    robot_type: str | None
    pose_format: str | None
    storage_uri: str | None
    local_path: str | None
    num_samples: int | None
    duration_sec: float | None
    fps: float | None
    created_at: str
    updated_at: str
    last_status: str | None
    last_run_id: str | None

    @classmethod
    def from_model(cls, row: Any) -> "DatasetRecord":
        return cls(
            id=row.id,
            dataset_id=row.dataset_id,
            version=row.version,
            task_type=row.task_type,
            robot_type=row.robot_type,
            pose_format=row.pose_format,
            storage_uri=row.storage_uri,
            local_path=row.local_path,
            num_samples=row.num_samples,
            duration_sec=row.duration_sec,
            fps=row.fps,
            created_at=_isoformat(row.created_at),
            updated_at=_isoformat(row.updated_at),
            last_status=row.last_status,
            last_run_id=row.last_run_id,
        )


@dataclass(slots=True)
class RunRecord:
    id: int
    run_id: str
    dataset_id: str
    dataset_version: str
    status: str
    started_at: str
    finished_at: str
    duration_sec: float
    config_path: str | None
    output_dir: str
    report_json_path: str | None
    report_html_path: str | None
    metrics_json: Any
    errors_json: Any
    warnings_json: Any

    @classmethod
    def from_model(cls, row: Any) -> "RunRecord":
        return cls(
            id=row.id,
            run_id=row.run_id,
            dataset_id=row.dataset_id,
            dataset_version=row.dataset_version,
            status=row.status,
            started_at=_isoformat(row.started_at),
            finished_at=_isoformat(row.finished_at),
            duration_sec=row.duration_sec,
            config_path=row.config_path,
            output_dir=row.output_dir,
            report_json_path=row.report_json_path,
            report_html_path=row.report_html_path,
            metrics_json=_loads_json(row.metrics_json),
            errors_json=_loads_json(row.errors_json),
            warnings_json=_loads_json(row.warnings_json),
        )


@dataclass(slots=True)
class ValidatorResultRecord:
    id: int
    run_id: str
    validator_name: str
    status: str
    duration_ms: float | None
    metrics_json: Any
    messages_json: Any
    created_at: str

    @classmethod
    def from_model(cls, row: Any) -> "ValidatorResultRecord":
        return cls(
            id=row.id,
            run_id=row.run_id,
            validator_name=row.validator_name,
            status=row.status,
            duration_ms=row.duration_ms,
            metrics_json=_loads_json(row.metrics_json),
            messages_json=_loads_json(row.messages_json),
            created_at=_isoformat(row.created_at),
        )


@dataclass(slots=True)
class GateResultRecord:
    id: int
    run_id: str
    gate_status: str
    failed_rules_json: Any
    warning_rules_json: Any
    policy_path: str | None
    created_at: str

    @classmethod
    def from_model(cls, row: Any) -> "GateResultRecord":
        return cls(
            id=row.id,
            run_id=row.run_id,
            gate_status=row.gate_status,
            failed_rules_json=_loads_json(row.failed_rules_json),
            warning_rules_json=_loads_json(row.warning_rules_json),
            policy_path=row.policy_path,
            created_at=_isoformat(row.created_at),
        )


@dataclass(slots=True)
class ArtifactRecord:
    id: int
    run_id: str
    artifact_type: str
    artifact_uri: str
    local_path: str | None
    size_bytes: int | None
    created_at: str

    @classmethod
    def from_model(cls, row: Any) -> "ArtifactRecord":
        return cls(
            id=row.id,
            run_id=row.run_id,
            artifact_type=row.artifact_type,
            artifact_uri=row.artifact_uri,
            local_path=row.local_path,
            size_bytes=row.size_bytes,
            created_at=_isoformat(row.created_at),
        )


@dataclass(slots=True)
class ScanJobRecord:
    id: int
    scan_id: str
    root_uri: str
    output_root: str
    status: str
    total: int
    succeeded: int
    failed: int
    skipped: int
    started_at: str
    finished_at: str
    summary_json: Any

    @classmethod
    def from_model(cls, row: Any) -> "ScanJobRecord":
        return cls(
            id=row.id,
            scan_id=row.scan_id,
            root_uri=row.root_uri,
            output_root=row.output_root,
            status=row.status,
            total=row.total,
            succeeded=row.succeeded,
            failed=row.failed,
            skipped=row.skipped,
            started_at=_isoformat(row.started_at),
            finished_at=_isoformat(row.finished_at),
            summary_json=_loads_json(row.summary_json),
        )


@dataclass(slots=True)
class RunDetailRecord:
    run: RunRecord
    validator_results: list[ValidatorResultRecord]
    gate_results: list[GateResultRecord]
    artifacts: list[ArtifactRecord]
