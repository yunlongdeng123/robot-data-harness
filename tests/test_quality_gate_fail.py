from __future__ import annotations

from pathlib import Path

import torch
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


def test_pipeline_fails_for_velocity_spike(tmp_path: Path) -> None:
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "samples" / "button_press_bad",
        duration_sec=16.0,
        fps=15,
        num_buttons=5,
        num_presses=10,
    )
    pose = torch.load(dataset_dir / "endpose.pt", map_location="cpu", weights_only=False)
    pose[80, 0] += 8.0
    torch.save(pose, dataset_dir / "endpose.pt")

    config_path = write_test_config(tmp_path / "bad_config.yaml", expected_presses=10)
    output_dir = tmp_path / "runs" / "button_press_bad"
    report = run_validation(
        dataset_path=dataset_dir,
        config_path=config_path,
        output_dir=output_dir,
        run_id="synthetic-fail",
    )

    assert report.status == "FAIL"