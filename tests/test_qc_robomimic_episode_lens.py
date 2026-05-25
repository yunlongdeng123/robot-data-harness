"""robomimic episode_len 修复：覆盖 probe_hdf5 + robomimic_metrics 聚合。"""

from __future__ import annotations

from pathlib import Path

import h5py
import pytest

from robot_dh.qc.contracts import run_contract
from robot_dh.qc.hdf5_probe import probe_hdf5
from robot_dh.qc.robomimic import _percentile, robomimic_metrics
from robot_dh.qc.base import AssetProfile


def _write_robomimic_with_lengths(path: Path, lengths: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as f:
        for i, length in enumerate(lengths):
            g = f.create_group(f"data/demo_{i}")
            g.create_dataset("actions", data=[[0.0] * 7 for _ in range(length)])
            g.create_dataset("rewards", data=[1.0] * length)
            g.create_dataset("dones", data=[0] * length)


def test_probe_hdf5_writes_episode_lens(tmp_path: Path) -> None:
    out = tmp_path / "low_dim.hdf5"
    _write_robomimic_with_lengths(out, [10, 20, 30, 40])
    probe = probe_hdf5(out)
    assert probe["readable"] is True
    assert probe["demo_count"] == 4
    assert probe["episode_lens"] == [10, 20, 30, 40]
    assert probe["actions_shape"] == [10, 7]


def test_percentile_handles_empty_and_single() -> None:
    assert _percentile([], 50) == 0
    assert _percentile([100], 50) == 100
    assert _percentile([100], 95) == 100


def test_percentile_interpolation() -> None:
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    assert _percentile(values, 50) == 55
    assert _percentile(values, 95) == 96


def test_robomimic_metrics_aggregates_episode_lens() -> None:
    profile = AssetProfile(
        profile_id="p1",
        asset_uri="local://x",
        profile={
            "hdf5": [
                {"readable": True, "demo_count": 3, "episode_lens": [10, 50, 100], "has_actions": True,
                 "has_obs": False, "has_next_obs": False, "has_rewards": True, "has_dones": True},
                {"readable": True, "demo_count": 2, "episode_lens": [200, 300], "has_actions": True,
                 "has_obs": False, "has_next_obs": False, "has_rewards": True, "has_dones": True},
            ],
        },
    )
    metrics = robomimic_metrics(profile)
    assert metrics["demo_count"] == 5
    assert metrics["episode_count_total"] == 5
    assert metrics["episode_len_min"] == 10
    assert metrics["episode_len_max"] == 300
    # 5 个值 [10,50,100,200,300]，p50=100，p95=280
    assert metrics["episode_len_p50"] == 100
    assert metrics["episode_len_p95"] == 280


def test_robomimic_contract_p50_p95_non_zero(tmp_path: Path) -> None:
    """run_contract end-to-end：episode_len_p50/p95 不再为 0。"""
    root = tmp_path / "robomimic/v1"
    _write_robomimic_with_lengths(root / "ph.hdf5", [40, 60, 80, 100])
    _write_robomimic_with_lengths(root / "mh.hdf5", [120, 140, 160, 180, 200])
    report, _ = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="robomimic",
        dataset_id="robomimic_scale30",
        version="v1",
    )
    assert report.metrics["episode_len_p50"] > 0
    assert report.metrics["episode_len_p95"] > 0
    assert report.metrics["episode_count_total"] == 9
    # 新增 episode_len_p50_min 规则严格度 warn；threshold=5，9 个值都 > 5 应 PASS
    rules = {r.rule_id: r for r in report.rules}
    assert "episode_len_p50_min" in rules
    assert rules["episode_len_p50_min"].status == "PASS"


def test_robomimic_handles_missing_episode_lens_gracefully() -> None:
    """老格式 probe（没 episode_lens 字段）不能崩，p50/p95 退化为 0。"""
    profile = AssetProfile(
        profile_id="p2",
        asset_uri="local://x",
        profile={
            "hdf5": [
                {"readable": True, "demo_count": 3, "has_actions": True,
                 "has_obs": False, "has_next_obs": False, "has_rewards": True, "has_dones": True},
            ],
        },
    )
    metrics = robomimic_metrics(profile)
    assert metrics["episode_len_p50"] == 0
    assert metrics["episode_len_p95"] == 0
    assert metrics["episode_count_total"] == 0
