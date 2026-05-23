from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from robot_dh.benchmark.runner import run_benchmark
from robot_dh.data.synthetic import generate_demo_dataset


def _make_demo(tmp_path: Path) -> Path:
    return generate_demo_dataset(
        output_dir=tmp_path / "demo",
        duration_sec=46.0,
        fps=30,
        num_buttons=5,
        num_presses=25,
    )


def _write_suite(path: Path, dataset_path: Path) -> Path:
    payload = {
        "suite_name": "test_suite",
        "cases": [
            {
                "case_id": "clean_demo",
                "dataset": dataset_path.as_posix(),
                "expected_status": "PASS",
                "expected_failed_validators": [],
            },
            {
                "case_id": "velocity_spike",
                "source_dataset": dataset_path.as_posix(),
                "mutation": "velocity_spike",
                "expected_status": "FAIL",
                "expected_failed_validators": ["velocity_jump"],
            },
            {
                "case_id": "missing_press",
                "source_dataset": dataset_path.as_posix(),
                "mutation": "missing_press",
                "expected_status": "FAIL",
                "expected_failed_validators": ["press_event"],
            },
        ],
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def test_run_benchmark_local(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    demo = _make_demo(tmp_path)
    suite = _write_suite(tmp_path / "suite.yaml", demo)
    output = tmp_path / "bench"

    report = run_benchmark(
        suite_path=suite,
        output_dir=output,
        record_to_registry=False,
        default_config_path=Path("configs/button_press.yaml"),
    )

    assert report.total == 3
    assert (output / "benchmark_report.json").is_file()
    assert (output / "benchmark_report.html").is_file()
    payload = json.loads((output / "benchmark_report.json").read_text())
    assert payload["total"] == 3
    assert any(c["case_id"] == "velocity_spike" for c in payload["cases"])
    # clean_demo 期望 PASS；其余 case 期望 FAIL 且 validator 名匹配
    matches = {c["case_id"]: c["match"] for c in payload["cases"]}
    assert matches["clean_demo"] is True
    assert matches["velocity_spike"] is True
    assert matches["missing_press"] is True
