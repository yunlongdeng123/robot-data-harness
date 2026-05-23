from __future__ import annotations

from pathlib import Path

import numpy as np
import torch
import yaml

from robot_dh.benchmark.mutations import apply_mutation, list_supported_mutations
from robot_dh.data.synthetic import generate_demo_dataset


def _make_demo(tmp_path: Path) -> Path:
    return generate_demo_dataset(
        output_dir=tmp_path / "src",
        duration_sec=4.0,
        fps=30,
        num_buttons=4,
        num_presses=10,
    )


def test_mutation_list_contains_expected(tmp_path: Path) -> None:
    names = list_supported_mutations()
    assert "velocity_spike" in names
    assert "quat_drift" in names
    assert "missing_press" in names
    assert "video_missing" in names
    assert "schema_missing_column" in names


def test_velocity_spike_changes_pose(tmp_path: Path) -> None:
    src = _make_demo(tmp_path)
    out = apply_mutation(source_dataset=src, output_dataset=tmp_path / "vel", mutation="velocity_spike")
    src_arr = torch.load(src / "endpose.pt", map_location="cpu", weights_only=False).numpy()
    new_arr = torch.load(out / "endpose.pt", map_location="cpu", weights_only=False).numpy()
    diff = np.max(np.abs(src_arr - new_arr))
    assert diff > 0.5
    meta = yaml.safe_load((out / "meta.yaml").read_text())
    assert meta["mutation_type"] == "velocity_spike"


def test_video_missing_removes_video(tmp_path: Path) -> None:
    src = _make_demo(tmp_path)
    out = apply_mutation(source_dataset=src, output_dataset=tmp_path / "novid", mutation="video_missing")
    assert not (out / "video.mp4").exists()


def test_schema_missing_column_shortens_samples(tmp_path: Path) -> None:
    """schema_missing_column 通过把样本数砍到 <min_samples 触发 SchemaValidator FAIL；不改列数避免 loader 早爆。"""
    src = _make_demo(tmp_path)
    out = apply_mutation(source_dataset=src, output_dataset=tmp_path / "schema", mutation="schema_missing_column")
    arr = torch.load(out / "endpose.pt", map_location="cpu", weights_only=False).numpy()
    assert arr.shape[1] == 7
    assert arr.shape[0] <= 5


def test_nan_injection_inserts_nan(tmp_path: Path) -> None:
    src = _make_demo(tmp_path)
    out = apply_mutation(source_dataset=src, output_dataset=tmp_path / "nan", mutation="nan_injection")
    arr = torch.load(out / "endpose.pt", map_location="cpu", weights_only=False).numpy()
    assert not np.isfinite(arr).all()


def test_output_does_not_overwrite_source(tmp_path: Path) -> None:
    src = _make_demo(tmp_path)
    src_arr_before = torch.load(src / "endpose.pt", map_location="cpu", weights_only=False).numpy().copy()
    apply_mutation(source_dataset=src, output_dataset=tmp_path / "muta", mutation="quat_drift")
    src_arr_after = torch.load(src / "endpose.pt", map_location="cpu", weights_only=False).numpy()
    np.testing.assert_allclose(src_arr_before, src_arr_after)
