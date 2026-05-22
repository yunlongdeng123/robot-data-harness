from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.pipeline import run_validation
from robot_dh.registry.db import _normalized_engine_uri
from robot_dh.registry import RegistryService, get_engine, init_db
from robot_dh.scan import scan_datasets


def _set_sqlite_env(monkeypatch, db_path: Path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{db_path}")


def _write_config(path: Path, expected_presses: int) -> Path:
    path.write_text(
        f"""
dataset:
  min_samples: 10
validators:
  press_event:
    press_expected_min_count: {max(expected_presses - 2, 1)}
    press_expected_max_count: {expected_presses + 2}
    press_expected_count: {expected_presses}
""".strip(),
        encoding="utf-8",
    )
    return path


def _write_policy(path: Path) -> Path:
    path.write_text(
        """
rules:
  - name: velocity_jump
    metric: max_velocity_mps
    op: "<="
    value: 2.0
    severity: fail
""".strip(),
        encoding="utf-8",
    )
    return path


def test_sqlalchemy_sqlite_create_all(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "robot_dh.db"
    _set_sqlite_env(monkeypatch, db_path)

    init_db()

    inspector = inspect(get_engine())
    assert {
        "datasets",
        "runs",
        "validator_results",
        "gate_results",
        "artifacts",
        "scan_jobs",
    }.issubset(set(inspector.get_table_names()))


def test_normalized_postgres_engine_uri_preserves_password(monkeypatch) -> None:
    uri = "postgresql+psycopg://robot_dh_app:secretpass123@127.0.0.1:15432/robot_dh"
    monkeypatch.setenv("ROBOT_DH_DB_URI", uri)

    normalized = _normalized_engine_uri()

    assert normalized == uri
    assert "***" not in normalized


def test_registry_records_run_details_and_scan_job(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "robot_dh.db"
    _set_sqlite_env(monkeypatch, db_path)
    registry = RegistryService()

    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_001",
        duration_sec=18.0,
        fps=15,
        num_buttons=5,
        num_presses=10,
    )
    dataset_record = registry.register_dataset_path(
        dataset_path=dataset_dir,
        dataset_id="button_press_001",
        version="v1",
        storage_uri=dataset_dir.resolve().as_uri(),
    )
    assert dataset_record.dataset_id == "button_press_001"

    report = run_validation(
        dataset_path=dataset_dir,
        config_path=_write_config(tmp_path / "config.yaml", expected_presses=10),
        output_dir=tmp_path / "runs" / "button_press_001",
        run_id="sqlalchemy-detail-run",
        record_to_registry=True,
        gate_policy_path=_write_policy(tmp_path / "gate.yaml"),
    )

    detail = registry.get_run_detail("sqlalchemy-detail-run")
    assert detail is not None
    assert detail.run.id == report.registry["run_db_id"]
    assert detail.run.status == "PASS"
    assert len(detail.validator_results) >= 7
    assert detail.gate_results[0].gate_status == "PASS"
    artifact_types = {artifact.artifact_type for artifact in detail.artifacts}
    assert {"report_json", "report_html", "gate_report"}.issubset(artifact_types)
    assert any(artifact_type.startswith("plot:") for artifact_type in artifact_types)
    assert report.registry["db_backend"] == "sqlite"

    scan_root = tmp_path / "scan_samples"
    generate_demo_dataset(scan_root / "dataset_a", duration_sec=18.0, fps=15, num_buttons=5, num_presses=10)
    scan_summary = scan_datasets(
        root=scan_root,
        config_path=_write_config(tmp_path / "scan_config.yaml", expected_presses=10),
        output_root=tmp_path / "runs" / "scan",
        use_registry=True,
        only_new=False,
        gate_policy_path=_write_policy(tmp_path / "scan_gate.yaml"),
    )
    scan_job = registry.get_scan_job(scan_summary["scan_id"])
    assert scan_job is not None
    assert scan_job.total == 1
    assert scan_job.succeeded == 1


def test_registry_create_run_roundtrip(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "robot_dh.db"
    _set_sqlite_env(monkeypatch, db_path)
    registry = RegistryService()
    now = datetime.now(timezone.utc).isoformat()

    registry.upsert_dataset(
        dataset_id="dataset-001",
        version="v1",
        task_type="quality",
        robot_type="arm",
        pose_format="eexyzxyzw",
        storage_uri="file:///tmp/dataset-001",
        local_path="/tmp/dataset-001",
        num_samples=128,
        duration_sec=6.0,
        fps=20.0,
    )
    run_db_id = registry.create_run(
        run_id="create-run-roundtrip",
        dataset_id="dataset-001",
        dataset_version="v1",
        status="PASS",
        started_at=now,
        finished_at=now,
        duration_sec=0.1,
        config_path=None,
        output_dir=str(tmp_path / "runs" / "dataset-001"),
        report_json_path=None,
        report_html_path=None,
        metrics={"max_velocity_mps": 0.5},
        errors=[],
        warnings=[],
    )

    run_record = registry.get_run("create-run-roundtrip")
    assert run_record is not None
    assert run_record.id == run_db_id
    assert run_record.metrics_json["max_velocity_mps"] == 0.5