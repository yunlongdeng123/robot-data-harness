"""BridgeData V2 显式 adapter：state -> 7D pose 转换，多 episode 分组。"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.etl.normalize import normalize_dataset
from robot_dh.lake.hf_adapter import load_huggingface_dataset
from robot_dh.lake.hf_adapters import (
    AdapterContext,
    adapt_bridgedata_v2,
    euler_to_quaternion,
    list_registered_adapters,
)


def _write_bridge_shard(
    path: Path,
    *,
    n_per_ep: int = 20,
    n_episodes: int = 2,
    use_struct: bool = False,
) -> None:
    """构造 BridgeData V2 风格 parquet：state=(x,y,z,roll,pitch,yaw,gripper)。

    use_struct=True 时把 state / action 写成 Struct<axis_0..axis_6>，模拟早期 LeRobot
    转换格式；False 时写成 FixedSizeList[7]（现行 LeRobot v3.0 默认）。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for ep in range(n_episodes):
        for i in range(n_per_ep):
            t = i / 5.0  # fps=5
            state = [0.4 + 0.001 * i, 0.05 * ep, 0.2 - 0.001 * i,
                     0.0, 0.0, 0.01 * i, 1.0]
            action = [0.001, 0.0, -0.001, 0.0, 0.0, 0.01, 0.0]
            row = {
                "episode_index": ep,
                "frame_index": i,
                "index": ep * n_per_ep + i,
                "task_index": 0,
                "timestamp": t,
                "language_instruction": "pick up object",
                "date": "2026-05-24",
            }
            if use_struct:
                row["observation.state"] = {f"axis_{j}": state[j] for j in range(7)}
                row["action"] = {f"axis_{j}": action[j] for j in range(7)}
            else:
                row["observation.state"] = state
                row["action"] = action
            rows.append(row)
    df = pd.DataFrame(rows)
    if not use_struct:
        # 强制 FixedSizeList[float32, 7] 这种现行 LeRobot 编码
        state_arr = pa.array(
            df["observation.state"].tolist(),
            type=pa.list_(pa.float32(), 7),
        )
        action_arr = pa.array(df["action"].tolist(), type=pa.list_(pa.float32(), 7))
        table = pa.Table.from_pandas(df.drop(columns=["observation.state", "action"]), preserve_index=False)
        table = table.append_column("observation.state", state_arr)
        table = table.append_column("action", action_arr)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)


def test_bridgedata_v2_registered() -> None:
    names = {entry["name"] for entry in list_registered_adapters()}
    assert "bridgedata_v2" in names


def test_euler_to_quaternion_identity() -> None:
    quat = euler_to_quaternion(
        np.array([0.0]), np.array([0.0]), np.array([0.0])
    )
    assert quat.shape == (1, 4)
    np.testing.assert_allclose(quat[0], [0.0, 0.0, 0.0, 1.0], atol=1e-9)


def test_bridge_adapter_extracts_multi_episode_from_list_column(tmp_path: Path) -> None:
    root = tmp_path / "raw" / "bridgedata_v2_scale30" / "v1"
    _write_bridge_shard(root / "data" / "shard_0-00000-of-00001.parquet",
                        n_per_ep=10, n_episodes=3, use_struct=False)
    ctx = AdapterContext(
        dataset_dir=root,
        dataset_id="bridgedata_v2_scale30",
        version="v1",
        base_meta={},
    )
    episodes = adapt_bridgedata_v2(ctx)
    assert len(episodes) == 3
    for ep in episodes:
        assert ep.pose.shape[1] == 7
        # quaternion 部分应当接近单位
        qnorms = np.linalg.norm(ep.pose[:, 3:7], axis=1)
        np.testing.assert_allclose(qnorms, 1.0, atol=1e-6)


def test_bridge_adapter_extracts_from_struct_column(tmp_path: Path) -> None:
    root = tmp_path / "raw" / "bridgedata_v2" / "v1"
    _write_bridge_shard(root / "data" / "shard_0-00000-of-00001.parquet",
                        n_per_ep=12, n_episodes=2, use_struct=True)
    ctx = AdapterContext(
        dataset_dir=root,
        dataset_id="bridgedata_v2",
        version="v1",
        base_meta={},
    )
    episodes = adapt_bridgedata_v2(ctx)
    assert len(episodes) == 2
    for ep in episodes:
        assert ep.pose.shape == (12, 7)


def test_load_huggingface_dataset_uses_registry_for_bridge(tmp_path: Path) -> None:
    root = tmp_path / "raw" / "bridgedata_v2_scale30" / "v1"
    _write_bridge_shard(root / "data" / "shard_0-00000-of-00001.parquet",
                        n_per_ep=8, n_episodes=2)
    episodes = load_huggingface_dataset(
        root,
        dataset_id="bridgedata_v2_scale30",
        version="v1",
    )
    assert len(episodes) == 2
    assert all(ep.pose.shape[1] == 7 for ep in episodes)


def test_normalize_dataset_works_for_bridgedata_v2(tmp_path: Path, monkeypatch) -> None:
    """端到端：bridgedata_v2 dataset -> ods/{pose,episode_meta,video_meta}.parquet + manifest。"""
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = tmp_path / "raw" / "bridgedata_v2_scale30" / "v1"
    _write_bridge_shard(raw_dir / "data" / "shard_0-00000-of-00001.parquet",
                        n_per_ep=15, n_episodes=2)
    out_dir = tmp_path / "lake" / "ods" / "bridgedata_v2_scale30" / "v1"
    result = normalize_dataset(
        dataset_uri=raw_dir.as_posix(),
        output_uri=out_dir.as_posix(),
        dataset_id="bridgedata_v2_scale30",
        version="v1",
    )
    assert result.num_samples == 30
    pose_path = out_dir / "pose.parquet"
    assert pose_path.is_file()
    pose_df = pq.read_table(pose_path).to_pandas()
    # episode_id 列：两个 episode 各 15 帧
    assert pose_df.groupby("episode_id").size().tolist() == [15, 15]
    qnorm = np.sqrt(
        pose_df["qx"] ** 2 + pose_df["qy"] ** 2 + pose_df["qz"] ** 2 + pose_df["qw"] ** 2
    )
    np.testing.assert_allclose(qnorm.to_numpy(), 1.0, atol=1e-5)


def test_unknown_dataset_falls_back_to_heuristic(tmp_path: Path) -> None:
    """没在 registry 里注册的 dataset_id 走启发式 + dry-run schema 诊断。"""
    root = tmp_path / "raw" / "unknown" / "v1"
    root.mkdir(parents=True)
    # 故意写一个完全不带 pose 列的 parquet
    df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), root / "data.parquet")
    with pytest.raises(ValueError, match="Observed parquet column samples"):
        load_huggingface_dataset(root, dataset_id="unknown_v1", version="v1")
