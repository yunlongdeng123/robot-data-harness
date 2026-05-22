from __future__ import annotations

from pathlib import Path

import yaml

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.pipeline import run_validation


def write_test_config(path: Path, expected_presses: int) -> Path:
    config = {
        "validators": {
            "press_event": {
                "press_expected_min_count": max(expected_presses - 2, 5),
                "press_expected_max_count": expected_presses + 2,
                "press_expected_count": expected_presses,
            }
        }
    }
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, sort_keys=False)
    return path


def test_synthetic_pipeline_generates_passing_report(tmp_path: Path) -> None:
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_001",
        duration_sec=18.0,
        fps=15,
        num_buttons=5,
        num_presses=10,
    )
    config_path = write_test_config(tmp_path / "config.yaml", expected_presses=10)
    output_dir = tmp_path / "runs" / "button_press_001"

    report = run_validation(
        dataset_path=dataset_dir,
        config_path=config_path,
        output_dir=output_dir,
        run_id="synthetic-pass",
    )

    assert report.status == "PASS"
    assert (output_dir / "report.json").exists()
    assert (output_dir / "report.html").exists()
    assert (output_dir / "plots" / "z_press_events.png").exists()
    assert (output_dir / "plots" / "xy_clusters.png").exists()
    assert (output_dir / "plots" / "euler_angles.png").exists()
    assert (output_dir / "plots" / "velocity_profile.png").exists()


def test_pipeline_honors_report_output_flags(tmp_path: Path) -> None:
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_flags",
        duration_sec=12.0,
        fps=15,
        num_buttons=5,
        num_presses=10,
    )
    config_path = tmp_path / "report_flags.yaml"
    with config_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "validators": {
                    "press_event": {
                        "press_expected_min_count": 8,
                        "press_expected_max_count": 12,
                        "press_expected_count": 10,
                    }
                },
                "reports": {
                    "save_plots": False,
                    "html": False,
                    "json": True,
                },
            },
            handle,
            sort_keys=False,
        )

    output_dir = tmp_path / "runs" / "button_press_flags"
    report = run_validation(
        dataset_path=dataset_dir,
        config_path=config_path,
        output_dir=output_dir,
        run_id="synthetic-flags",
    )

    assert report.status == "PASS"
    assert (output_dir / "report.json").exists()
    assert not (output_dir / "report.html").exists()
    assert not (output_dir / "plots").exists()
