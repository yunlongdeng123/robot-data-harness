from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from robot_dh.config import load_config
from robot_dh.data.loaders import DatasetLoader
from robot_dh.registry.db import get_db_backend, get_session, init_db, resolve_db_path, resolve_db_uri
from robot_dh.registry.models import (
    ArtifactRecord,
    DatasetRecord,
    GateResultRecord,
    RunDetailRecord,
    RunRecord,
    ScanJobRecord,
    ValidatorResultRecord,
)
from robot_dh.registry.schema import ArtifactRow, DatasetRow, GateResultRow, RunRow, ScanJobRow, ValidatorResultRow
from robot_dh.reports.models import QualityReport


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso8601(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _file_size(path_value: str | None) -> int | None:
    if path_value is None:
        return None
    path = Path(path_value)
    if not path.exists() or not path.is_file():
        return None
    return path.stat().st_size


def _uri_to_local_path(artifact_uri: str) -> str | None:
    parsed = urlparse(artifact_uri)
    if parsed.scheme != "file":
        return None
    return Path(parsed.path).as_posix()


class RegistryService:
    def __init__(self, db_uri: str | None = None) -> None:
        self.db_uri = resolve_db_uri(db_uri)
        self.db_backend = get_db_backend(self.db_uri)
        self.db_path = resolve_db_path(self.db_uri) if self.db_backend == "sqlite" else None
        init_db(self.db_uri)

    def _session(self) -> Session:
        return get_session(self.db_uri)

    def register_dataset_path(
        self,
        dataset_path: Path,
        dataset_id: str,
        version: str,
        storage_uri: str,
        *,
        task_type: str | None = None,
        robot_type: str | None = None,
        pose_format: str = "eexyzxyzw",
        config_path: Path | None = None,
    ) -> DatasetRecord:
        bundle = DatasetLoader(load_config(config_path)).load(dataset_path)
        return self.upsert_dataset(
            dataset_id=dataset_id,
            version=version,
            task_type=task_type or bundle.meta.get("task_type"),
            robot_type=robot_type or bundle.meta.get("robot_type"),
            pose_format=pose_format or bundle.meta.get("pose_format", "eexyzxyzw"),
            storage_uri=storage_uri,
            local_path=str(bundle.dataset_path),
            num_samples=int(bundle.pose.shape[0]),
            duration_sec=float(bundle.video_meta.duration_sec),
            fps=float(bundle.video_meta.fps),
        )

    def upsert_dataset(
        self,
        *,
        dataset_id: str,
        version: str,
        task_type: str | None,
        robot_type: str | None,
        pose_format: str | None,
        storage_uri: str | None,
        local_path: str | None,
        num_samples: int | None,
        duration_sec: float | None,
        fps: float | None,
        last_status: str | None = None,
        last_run_id: str | None = None,
    ) -> DatasetRecord:
        now = _utc_now()
        with self._session() as session:
            row = session.scalar(
                select(DatasetRow).where(
                    DatasetRow.dataset_id == dataset_id,
                    DatasetRow.version == version,
                )
            )
            if row is None:
                row = DatasetRow(
                    dataset_id=dataset_id,
                    version=version,
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)

            row.task_type = task_type
            row.robot_type = robot_type
            row.pose_format = pose_format
            row.storage_uri = storage_uri
            row.local_path = local_path
            row.num_samples = num_samples
            row.duration_sec = duration_sec
            row.fps = fps
            row.updated_at = now
            if last_status is not None:
                row.last_status = last_status
            if last_run_id is not None:
                row.last_run_id = last_run_id
            session.commit()
            session.refresh(row)
            return DatasetRecord.from_model(row)

    def update_dataset_last_run(self, dataset_id: str, version: str, status: str, run_id: str) -> None:
        with self._session() as session:
            row = session.scalar(
                select(DatasetRow).where(
                    DatasetRow.dataset_id == dataset_id,
                    DatasetRow.version == version,
                )
            )
            if row is None:
                return
            row.last_status = status
            row.last_run_id = run_id
            row.updated_at = _utc_now()
            session.commit()

    def list_datasets(self) -> list[DatasetRecord]:
        with self._session() as session:
            rows = session.scalars(
                select(DatasetRow).order_by(DatasetRow.updated_at.desc(), DatasetRow.dataset_id.asc(), DatasetRow.version.asc())
            ).all()
        return [DatasetRecord.from_model(row) for row in rows]

    def get_dataset(self, dataset_id: str, version: str | None = None) -> DatasetRecord | None:
        with self._session() as session:
            if version is None:
                row = session.scalar(
                    select(DatasetRow)
                    .where(DatasetRow.dataset_id == dataset_id)
                    .order_by(DatasetRow.updated_at.desc())
                )
            else:
                row = session.scalar(
                    select(DatasetRow).where(
                        DatasetRow.dataset_id == dataset_id,
                        DatasetRow.version == version,
                    )
                )
        return DatasetRecord.from_model(row) if row is not None else None

    def _delete_run_related(self, session, run_id: str) -> None:  # type: ignore[no-untyped-def]
        session.execute(delete(ValidatorResultRow).where(ValidatorResultRow.run_id == run_id))
        session.execute(delete(GateResultRow).where(GateResultRow.run_id == run_id))
        session.execute(delete(ArtifactRow).where(ArtifactRow.run_id == run_id))
        existing = session.scalar(select(RunRow).where(RunRow.run_id == run_id))
        if existing is not None:
            session.delete(existing)
            session.flush()

    def _create_run_row(
        self,
        session,
        *,
        run_id: str,
        dataset_id: str,
        dataset_version: str,
        status: str,
        started_at: str,
        finished_at: str,
        duration_sec: float,
        config_path: str | None,
        output_dir: str,
        report_json_path: str | None,
        report_html_path: str | None,
        metrics: Mapping[str, Any],
        errors: list[str],
        warnings: list[str],
    ) -> RunRow:
        self._delete_run_related(session, run_id)
        row = RunRow(
            run_id=run_id,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            status=status,
            started_at=_parse_iso8601(started_at),
            finished_at=_parse_iso8601(finished_at),
            duration_sec=duration_sec,
            config_path=config_path,
            output_dir=output_dir,
            report_json_path=report_json_path,
            report_html_path=report_html_path,
            metrics_json=_json_dumps(dict(metrics)),
            errors_json=_json_dumps(errors),
            warnings_json=_json_dumps(warnings),
        )
        session.add(row)
        session.flush()
        return row

    def create_run(
        self,
        *,
        run_id: str,
        dataset_id: str,
        dataset_version: str,
        status: str,
        started_at: str,
        finished_at: str,
        duration_sec: float,
        config_path: str | None,
        output_dir: str,
        report_json_path: str | None,
        report_html_path: str | None,
        metrics: Mapping[str, Any],
        errors: list[str],
        warnings: list[str],
    ) -> int:
        with self._session() as session:
            row = self._create_run_row(
                session,
                run_id=run_id,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration_sec=duration_sec,
                config_path=config_path,
                output_dir=output_dir,
                report_json_path=report_json_path,
                report_html_path=report_html_path,
                metrics=metrics,
                errors=errors,
                warnings=warnings,
            )
            session.commit()
            session.refresh(row)
            return row.id

    def _iter_artifacts(self, report: QualityReport) -> Iterable[tuple[str, str, str | None, int | None]]:
        artifact_pairs = [
            ("report_json", report.artifacts.get("report_json_uri"), report.artifacts.get("report_json")),
            ("report_html", report.artifacts.get("report_html_uri"), report.artifacts.get("report_html")),
            ("gate_report", report.artifacts.get("gate_report_uri"), report.artifacts.get("gate_report")),
        ]
        for artifact_type, artifact_uri, local_path in artifact_pairs:
            if not artifact_uri:
                continue
            effective_local_path = local_path or _uri_to_local_path(artifact_uri)
            yield artifact_type, artifact_uri, effective_local_path, _file_size(effective_local_path)

        plots = report.artifacts.get("plots", {})
        if isinstance(plots, dict):
            for plot_name, artifact_uri in sorted(plots.items()):
                if not artifact_uri:
                    continue
                local_path = _uri_to_local_path(str(artifact_uri))
                yield f"plot:{plot_name}", str(artifact_uri), local_path, _file_size(local_path)

    def record_validation_run(
        self,
        *,
        report: QualityReport,
        dataset_version: str,
        config_path: Path | None,
        output_dir: Path,
    ) -> int:
        status = report.status
        gate_status = report.gate.get("status")
        if gate_status == "FAIL":
            status = "FAIL"
        elif gate_status == "WARN" and status != "FAIL":
            status = "WARN"

        with self._session() as session:
            run_row = self._create_run_row(
                session,
                run_id=report.run_id,
                dataset_id=report.dataset_id,
                dataset_version=dataset_version,
                status=status,
                started_at=report.started_at,
                finished_at=report.finished_at,
                duration_sec=report.duration_sec,
                config_path=str(config_path.resolve()) if config_path is not None else None,
                output_dir=str(output_dir.resolve()),
                report_json_path=report.artifacts.get("report_json"),
                report_html_path=report.artifacts.get("report_html"),
                metrics=report.metrics,
                errors=report.errors,
                warnings=report.warnings,
            )

            dataset_row = session.scalar(
                select(DatasetRow).where(
                    DatasetRow.dataset_id == report.dataset_id,
                    DatasetRow.version == dataset_version,
                )
            )
            if dataset_row is not None:
                dataset_row.last_status = status
                dataset_row.last_run_id = report.run_id
                dataset_row.updated_at = _utc_now()

            for validator in report.validators:
                session.add(
                    ValidatorResultRow(
                        run_id=report.run_id,
                        validator_name=validator.name,
                        status=validator.status.value,
                        duration_ms=validator.details.get("duration_ms") if isinstance(validator.details, dict) else None,
                        metrics_json=_json_dumps(validator.metrics),
                        messages_json=_json_dumps(validator.messages),
                    )
                )

            session.add(
                GateResultRow(
                    run_id=report.run_id,
                    gate_status=str(report.gate.get("status", "SKIP")),
                    failed_rules_json=_json_dumps(report.gate.get("failed_rules", [])),
                    warning_rules_json=_json_dumps(report.gate.get("warning_rules", [])),
                    policy_path=report.gate.get("policy_path"),
                )
            )

            for artifact_type, artifact_uri, local_path, size_bytes in self._iter_artifacts(report):
                session.add(
                    ArtifactRow(
                        run_id=report.run_id,
                        artifact_type=artifact_type,
                        artifact_uri=artifact_uri,
                        local_path=local_path,
                        size_bytes=size_bytes,
                    )
                )

            session.commit()
            session.refresh(run_row)
            return run_row.id

    def list_runs(self) -> list[RunRecord]:
        with self._session() as session:
            rows = session.scalars(select(RunRow).order_by(RunRow.started_at.desc(), RunRow.run_id.asc())).all()
        return [RunRecord.from_model(row) for row in rows]

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._session() as session:
            row = session.scalar(select(RunRow).where(RunRow.run_id == run_id))
        return RunRecord.from_model(row) if row is not None else None

    def list_run_artifacts(self, run_id: str) -> list[ArtifactRecord]:
        with self._session() as session:
            rows = session.scalars(
                select(ArtifactRow).where(ArtifactRow.run_id == run_id).order_by(ArtifactRow.id.asc())
            ).all()
        return [ArtifactRecord.from_model(row) for row in rows]

    def get_run_detail(self, run_id: str) -> RunDetailRecord | None:
        with self._session() as session:
            run_row = session.scalar(select(RunRow).where(RunRow.run_id == run_id))
            if run_row is None:
                return None
            validator_rows = session.scalars(
                select(ValidatorResultRow)
                .where(ValidatorResultRow.run_id == run_id)
                .order_by(ValidatorResultRow.id.asc())
            ).all()
            gate_rows = session.scalars(
                select(GateResultRow).where(GateResultRow.run_id == run_id).order_by(GateResultRow.id.asc())
            ).all()
            artifact_rows = session.scalars(
                select(ArtifactRow).where(ArtifactRow.run_id == run_id).order_by(ArtifactRow.id.asc())
            ).all()
        return RunDetailRecord(
            run=RunRecord.from_model(run_row),
            validator_results=[ValidatorResultRecord.from_model(row) for row in validator_rows],
            gate_results=[GateResultRecord.from_model(row) for row in gate_rows],
            artifacts=[ArtifactRecord.from_model(row) for row in artifact_rows],
        )

    def has_successful_run(self, dataset_id: str, version: str) -> bool:
        with self._session() as session:
            row = session.scalar(
                select(RunRow.id).where(
                    RunRow.dataset_id == dataset_id,
                    RunRow.dataset_version == version,
                    RunRow.status.in_(("PASS", "WARN")),
                )
            )
        return row is not None

    def record_scan_job(
        self,
        *,
        scan_id: str,
        root_uri: str,
        output_root: str,
        status: str,
        total: int,
        succeeded: int,
        failed: int,
        skipped: int,
        started_at: str,
        finished_at: str,
        summary: Mapping[str, Any],
    ) -> int:
        with self._session() as session:
            existing = session.scalar(select(ScanJobRow).where(ScanJobRow.scan_id == scan_id))
            if existing is not None:
                session.delete(existing)
                session.flush()
            row = ScanJobRow(
                scan_id=scan_id,
                root_uri=root_uri,
                output_root=output_root,
                status=status,
                total=total,
                succeeded=succeeded,
                failed=failed,
                skipped=skipped,
                started_at=_parse_iso8601(started_at),
                finished_at=_parse_iso8601(finished_at),
                summary_json=_json_dumps(dict(summary)),
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return row.id

    def get_scan_job(self, scan_id: str) -> ScanJobRecord | None:
        with self._session() as session:
            row = session.scalar(select(ScanJobRow).where(ScanJobRow.scan_id == scan_id))
        return ScanJobRecord.from_model(row) if row is not None else None
