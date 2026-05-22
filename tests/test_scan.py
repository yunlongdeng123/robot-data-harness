from __future__ import annotations

import json
from pathlib import Path

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.registry import RegistryService
from robot_dh.scan import scan_datasets


def write_scan_policy(path: Path) -> Path:
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


def write_scan_config(path: Path) -> Path:
        path.write_text(
                """
dataset:
    min_samples: 10
validators:
    press_event:
        press_expected_min_count: 8
        press_expected_max_count: 12
        press_expected_count: 10
""".strip(),
                encoding="utf-8",
        )
        return path


def test_scan_multiple_datasets(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path / 'robot_dh.db'}")
    root = tmp_path / "samples"
    generate_demo_dataset(root / "dataset_a", duration_sec=18.0, fps=15, num_buttons=5, num_presses=10)
    generate_demo_dataset(root / "dataset_b", duration_sec=18.0, fps=15, num_buttons=5, num_presses=10)
    config_path = write_scan_config(tmp_path / "scan_config.yaml")
    summary = scan_datasets(
        root=root,
        config_path=config_path,
        output_root=tmp_path / "runs" / "scan",
        use_registry=True,
        only_new=False,
        gate_policy_path=write_scan_policy(tmp_path / "scan_gate.yaml"),
    )

    assert summary["total"] == 2
    assert summary["succeeded"] == 2
    assert summary["failed"] == 0
    assert (tmp_path / "runs" / "scan" / "scan_summary.json").exists()
    payload = json.loads((tmp_path / "runs" / "scan" / "scan_summary.json").read_text(encoding="utf-8"))
    assert len(payload["runs"]) == 2


def test_scan_only_new_skips_successful_dataset(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path / 'robot_dh.db'}")
    root = tmp_path / "samples"
    dataset_dir = generate_demo_dataset(
        root / "dataset_a", duration_sec=8.0, fps=10, num_buttons=5, num_presses=5
    )
    registry = RegistryService()
    registry.upsert_dataset(
        dataset_id="dataset_a",
        version="v1",
        task_type=None,
        robot_type=None,
        pose_format="eexyzxyzw",
        storage_uri=dataset_dir.resolve().as_uri(),
        local_path=str(dataset_dir.resolve()),
        num_samples=80,
        duration_sec=8.0,
        fps=10.0,
        last_status="PASS",
        last_run_id="previous-run",
    )
    registry.create_run(
        run_id="previous-run",
        dataset_id="dataset_a",
        dataset_version="v1",
        status="PASS",
        started_at="2024-01-01T00:00:00+00:00",
        finished_at="2024-01-01T00:00:01+00:00",
        duration_sec=1.0,
        config_path=None,
        output_dir=str(tmp_path / "runs" / "old"),
        report_json_path=None,
        report_html_path=None,
        metrics={},
        errors=[],
        warnings=[],
    )

    config_path = write_scan_config(tmp_path / "scan_config.yaml")
    summary = scan_datasets(
        root=root,
        config_path=config_path,
        output_root=tmp_path / "runs" / "scan",
        use_registry=True,
        only_new=True,
        gate_policy_path=None,
    )
    assert summary["skipped"] == 1
    assert summary["runs"][0]["status"] == "SKIPPED"