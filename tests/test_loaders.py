from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import torch

from robot_dh.config import load_config
from robot_dh.data.loaders import DatasetLoader, load_endpose


def write_pose(path: Path, pose: np.ndarray) -> None:
    torch.save(torch.tensor(pose, dtype=torch.float32), path)


def test_load_endpose_accepts_n_by_7(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    pose = np.tile(np.array([[0.1, 0.2, 0.3, 0.0, 0.0, 0.0, 1.0]]), (16, 1))
    write_pose(dataset_dir / "endpose.pt", pose)

    loaded, warnings = load_endpose(dataset_dir / "endpose.pt")

    assert loaded.shape == (16, 7)
    assert warnings == []


def test_loader_transposes_7_by_n_and_records_warning(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    pose = np.tile(np.array([[0.1], [0.2], [0.3], [0.0], [0.0], [0.0], [1.0]]), (1, 24))
    write_pose(dataset_dir / "endpose.pt", pose)

    loader = DatasetLoader(load_config())
    bundle = loader.load(dataset_dir)

    assert bundle.pose.shape == (24, 7)
    assert any("transposed" in warning.lower() for warning in bundle.warnings)


def test_loader_rejects_nan_values(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    pose = np.tile(np.array([[0.1, 0.2, 0.3, 0.0, 0.0, 0.0, 1.0]]), (8, 1))
    pose[3, 2] = np.nan
    write_pose(dataset_dir / "endpose.pt", pose)

    with pytest.raises(ValueError, match="NaN or Inf"):
        DatasetLoader(load_config()).load(dataset_dir)


def test_loader_uses_config_duration_when_video_is_missing(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    pose = np.tile(np.array([[0.1, 0.2, 0.3, 0.0, 0.0, 0.0, 1.0]]), (16, 1))
    write_pose(dataset_dir / "endpose.pt", pose)

    loader = DatasetLoader(
        {
            "dataset": {
                "duration_sec": 8.0,
                "fps": 120.0,
                "default_fps": 30.0,
            }
        }
    )
    bundle = loader.load(dataset_dir)

    assert bundle.video_meta.source == "duration"
    assert bundle.video_meta.duration_sec == pytest.approx(8.0)
    assert bundle.timestamps[-1] == pytest.approx(8.0)