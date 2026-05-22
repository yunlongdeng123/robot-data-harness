from __future__ import annotations

from pathlib import Path

from robot_dh.api.main import ValidateRequest, get_run, health, infra_health, list_datasets, list_runs, validate
from robot_dh.data.synthetic import generate_demo_dataset


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


def test_api_v13_endpoints(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path / 'robot_dh.db'}")

    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_001",
        duration_sec=18.0,
        fps=15,
        num_buttons=5,
        num_presses=10,
    )
    response = validate(
        ValidateRequest(
            dataset_path=str(dataset_dir),
            config_path=str(_write_config(tmp_path / "config.yaml", expected_presses=10)),
            output_dir=str(tmp_path / "runs" / "api-run"),
            run_id="api-run-v13-test",
            record_to_registry=True,
            gate_policy_path=str(_write_policy(tmp_path / "gate.yaml")),
        )
    )

    assert response["status"] == "PASS"
    assert health() == {"status": "ok"}

    infra = infra_health()
    assert infra["status"] == "PASS"

    datasets = list_datasets()
    assert any(item["dataset_id"] == "button_press_001" for item in datasets)

    runs = list_runs()
    assert any(item["run_id"] == "api-run-v13-test" for item in runs)

    run_detail = get_run("api-run-v13-test")
    assert run_detail["run"]["run_id"] == "api-run-v13-test"
    assert any(artifact["artifact_type"] == "report_json" for artifact in run_detail["artifacts"])