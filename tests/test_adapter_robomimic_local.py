"""v1.7：RobomimicAdapter 本地路径 probe / list_episodes / 并发 fail-fast。"""

from __future__ import annotations

from pathlib import Path

import h5py
import pytest

from robot_dh.adapters import get_adapter
from robot_dh.lake.uri import to_file_uri


def _write_low_dim(path: Path, demos: int = 3, length: int = 5) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as f:
        for i in range(demos):
            g = f.create_group(f"data/demo_{i}")
            g.create_dataset("actions", data=[[0.0] * 7 for _ in range(length)])
            g.create_dataset("rewards", data=[1.0] * length)


def test_robomimic_adapter_detects_local_hdf5(tmp_path: Path) -> None:
    p = tmp_path / "low_dim_v141.hdf5"
    _write_low_dim(p)
    adapter = get_adapter("robomimic")
    det = adapter.detect(to_file_uri(tmp_path))
    assert det.family == "robomimic"
    assert det.confidence >= 0.6
    assert any("low_dim_v141.hdf5" in m for m in det.matched_markers)


def test_robomimic_adapter_probe_local_counts_demos(tmp_path: Path) -> None:
    _write_low_dim(tmp_path / "low_dim_a.hdf5", demos=3, length=10)
    _write_low_dim(tmp_path / "low_dim_b.hdf5", demos=2, length=20)
    adapter = get_adapter("robomimic")
    result = adapter.probe(to_file_uri(tmp_path), sample_limit=4)
    assert result.status == "OK"
    assert result.hdf5_files == 2
    assert result.episodes_count == 5


def test_robomimic_adapter_list_episodes_returns_each_hdf5(tmp_path: Path) -> None:
    for i in range(3):
        _write_low_dim(tmp_path / f"low_dim_{i}.hdf5", demos=1, length=1)
    adapter = get_adapter("robomimic")
    eps = adapter.list_episodes(to_file_uri(tmp_path))
    assert len(eps) == 3
    for e in eps:
        assert e.rel_path.endswith(".hdf5")
        assert e.file_uri.startswith("file://")


def test_robomimic_adapter_probe_fail_fast_on_bad_file(tmp_path: Path) -> None:
    _write_low_dim(tmp_path / "good.hdf5", demos=2, length=5)
    (tmp_path / "broken.hdf5").write_bytes(b"not an hdf5 file")
    adapter = get_adapter("robomimic")
    result = adapter.probe(
        to_file_uri(tmp_path),
        sample_limit=4,
        options={"max_workers": 1, "fail_fast": True},
    )
    # fail_fast 触发 FAIL；至少一个错误
    assert result.status == "FAIL"
    assert any(e.get("file", "").endswith("broken.hdf5") for e in result.errors)
