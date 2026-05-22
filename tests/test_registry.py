from __future__ import annotations

import json
from pathlib import Path

from robot_dh.cli import main as cli_main
from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.pipeline import run_validation
from robot_dh.registry import RegistryService


def set_db_env(monkeypatch, db_path: Path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{db_path}")


def write_policy(path: Path, expected_presses: int) -> Path:
    path.write_text(
        f"""
rules:
  - name: velocity_jump
    metric: max_velocity_mps
    op: "<="
    value: 2.0
    severity: fail
  - name: press_count
    metric: detected_press_count
    op: "between"
    value: [{max(expected_presses - 2, 1)}, {expected_presses + 2}]
    severity: warn
""".strip(),
        encoding="utf-8",
    )
    return path


def write_config(path: Path, expected_presses: int) -> Path:
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


def test_registry_register_list_show(monkeypatch, tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "robot_dh.db"
    set_db_env(monkeypatch, db_path)
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_001",
        duration_sec=8.0,
        fps=10,
        num_buttons=5,
        num_presses=5,
    )

    exit_code = cli_main(
        [
            "dataset",
            "register",
            "--dataset",
            str(dataset_dir),
            "--dataset-id",
            "button_press_001",
            "--version",
            "v1",
            "--storage-uri",
            f"file://{dataset_dir}",
        ]
    )
    assert exit_code == 0

    exit_code = cli_main(["dataset", "list"])
    assert exit_code == 0
    captured = capsys.readouterr().out
    assert "button_press_001" in captured

    exit_code = cli_main(["dataset", "show", "--dataset-id", "button_press_001"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset_id"] == "button_press_001"
    assert payload["version"] == "v1"


def test_validate_records_run_to_registry(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "robot_dh.db"
    set_db_env(monkeypatch, db_path)
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_001",
        duration_sec=12.0,
        fps=12,
        num_buttons=5,
        num_presses=8,
    )
    output_dir = tmp_path / "runs" / "button_press_001"
    policy_path = write_policy(tmp_path / "gate_policy.yaml", expected_presses=8)
    config_path = write_config(tmp_path / "config.yaml", expected_presses=8)

    report = run_validation(
        dataset_path=dataset_dir,
        config_path=config_path,
        output_dir=output_dir,
        run_id="local-demo-v12-test",
        record_to_registry=True,
        gate_policy_path=policy_path,
    )

    registry = RegistryService()
    run_record = registry.get_run("local-demo-v12-test")
    dataset_record = registry.get_dataset(report.dataset_id)
    assert run_record is not None
    assert dataset_record is not None
    assert report.registry["run_db_id"] == run_record.id
    assert dataset_record.last_run_id == "local-demo-v12-test"
    assert Path(output_dir / "report.json").exists()
