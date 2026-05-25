"""droid_lerobot_v2 normalize adapter（v1.6.8 fvx5z §6.2.4）。

覆盖五条核心断言：

1. ``observation.cartesian_position`` (6 维 xyz+rpy) → euler→quat → (N, 7) pose；
2. ``observation.cartesian_pose`` (7 维 xyz+quat) → 直接用，pose_source 字段写对；
3. ``observation.state`` (8 维) fallback：必须带 warning，pose_source 标 joint-angle；
4. action 列拍到 bundle.action 且行序与 pose 同步；
5. 跨 shard 同 episode_index 必须合并 → 一个 DatasetBundle，frame_index 排序正确；
6. 完全无 pose 列 → raise ValueError，错误消息列出 schema sniff 结果（防止"静默 0 episodes"）。
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.lake.hf_adapter import load_huggingface_dataset
from robot_dh.lake.hf_adapters import (
    AdapterContext,
    adapt_droid_lerobot_v2,
)


def _write_parquet(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _ctx(dataset_dir: Path, *, dataset_id: str = "droid_lerobot_scale30") -> AdapterContext:
    return AdapterContext(
        dataset_dir=dataset_dir,
        dataset_id=dataset_id,
        version="v1",
        base_meta={"fps": 15.0, "source_format": "huggingface"},
    )


def _make_cartesian_position_table(
    *,
    n: int = 4,
    episode_indices: list[int] | None = None,
    frame_offset: int = 0,
) -> pa.Table:
    eps = episode_indices if episode_indices is not None else [0] * n
    frames = list(range(frame_offset, frame_offset + n))
    xyz = [[float(i), float(i) + 0.1, float(i) + 0.2] for i in range(n)]
    rpy = [[0.0, 0.0, 0.0] for _ in range(n)]  # 全 0 → quat = (0, 0, 0, 1)
    cart = [[*xyz[i], *rpy[i]] for i in range(n)]
    actions = [[0.01 * (i + 1)] * 7 for i in range(n)]
    return pa.table(
        {
            "episode_index": eps,
            "frame_index": frames,
            "timestamp": [float(i) / 15.0 for i in range(n)],
            "observation.cartesian_position": cart,
            "action": actions,
            "observation.images.wrist": [b"\x00" * 16] * n,
        }
    )


def test_droid_adapter_cartesian_position_rpy_to_quat(tmp_path: Path) -> None:
    """6 维 xyz+rpy 走 euler→quat 转换；rpy=0 → quat=(0,0,0,1)。"""
    table = _make_cartesian_position_table(n=4)
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table)

    bundles = adapt_droid_lerobot_v2(_ctx(tmp_path))
    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.pose.shape == (4, 7)
    # rpy 全 0 → quat = (0, 0, 0, 1)
    np.testing.assert_allclose(bundle.pose[:, 3], 0.0, atol=1e-12)
    np.testing.assert_allclose(bundle.pose[:, 4], 0.0, atol=1e-12)
    np.testing.assert_allclose(bundle.pose[:, 5], 0.0, atol=1e-12)
    np.testing.assert_allclose(bundle.pose[:, 6], 1.0, atol=1e-12)
    assert bundle.meta["pose_source"] == "observation.cartesian_position(rpy->quat)"
    # 无 fallback warning
    assert not any("joint-angle" in w for w in bundle.warnings)


def test_droid_adapter_cartesian_pose_seven_dim_passthrough(tmp_path: Path) -> None:
    """7 维 (xyz, qx, qy, qz, qw) 直读，不走 euler 转换。"""
    n = 5
    pose7 = [[0.1 * i, 0.2 * i, 0.3 * i, 0.0, 0.0, 0.0, 1.0] for i in range(n)]
    table = pa.table(
        {
            "episode_index": [0] * n,
            "frame_index": list(range(n)),
            "timestamp": [float(i) / 15.0 for i in range(n)],
            "observation.cartesian_pose": pose7,
            "action": [[0.0] * 7 for _ in range(n)],
        }
    )
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table)

    bundle = adapt_droid_lerobot_v2(_ctx(tmp_path))[0]
    assert bundle.meta["pose_source"] == "observation.cartesian_pose"
    np.testing.assert_allclose(bundle.pose[0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0])
    np.testing.assert_allclose(bundle.pose[-1], [0.4, 0.8, 1.2, 0.0, 0.0, 0.0, 1.0])


def test_droid_adapter_state_fallback_emits_warning(tmp_path: Path) -> None:
    """无 cartesian_position / cartesian_pose 时，observation.state[:7] 兜底 + warning。"""
    n = 4
    state8 = [[float(j) + 0.01 * i for j in range(8)] for i in range(n)]
    table = pa.table(
        {
            "episode_index": [0] * n,
            "frame_index": list(range(n)),
            "observation.state": state8,
            "action": [[0.0] * 7 for _ in range(n)],
        }
    )
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table)

    bundle = adapt_droid_lerobot_v2(_ctx(tmp_path))[0]
    assert bundle.meta["pose_source"] == "observation.state[:7]"
    assert any(
        "joint-angle" in w and "forward kinematics" in w for w in bundle.warnings
    ), f"missing fallback warning, got warnings={bundle.warnings}"
    # state[:7] 第 0 行 = (0, 1, 2, 3, 4, 5, 6)；quat 部分会被 normalize_quaternions 覆盖，
    # adapter 自己只保证原值传出去，不做归一化。
    np.testing.assert_allclose(bundle.pose[0], [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0])


def test_droid_adapter_action_attached_with_correct_row_order(tmp_path: Path) -> None:
    """action 列必须写到 bundle.action 且按 frame_index 排序。"""
    # 故意把 frame 写成乱序（[2, 0, 1, 3]），adapter 必须排回 [0, 1, 2, 3]
    n = 4
    cart = [[float(i), 0.0, 0.0, 0.0, 0.0, 0.0] for i in range(n)]
    # frame_index = [2, 0, 1, 3] → 期待 action 排序后是 [a1, a2, a0, a3]
    actions = [
        [10.0] * 7,  # frame=2
        [20.0] * 7,  # frame=0
        [30.0] * 7,  # frame=1
        [40.0] * 7,  # frame=3
    ]
    table = pa.table(
        {
            "episode_index": [0] * n,
            "frame_index": [2, 0, 1, 3],
            "observation.cartesian_position": cart,
            "action": actions,
        }
    )
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table)

    bundle = adapt_droid_lerobot_v2(_ctx(tmp_path))[0]
    assert bundle.action is not None
    assert bundle.action.shape == (4, 7)
    # action 行序应该和 frame_index 升序对齐：frame 0 → 20.0, 1 → 30.0, 2 → 10.0, 3 → 40.0
    np.testing.assert_allclose(bundle.action[:, 0], [20.0, 30.0, 10.0, 40.0])
    assert bundle.meta["action_layout"] == "x_y_z_roll_pitch_yaw_grasp"


def test_droid_adapter_groups_episodes_across_shards(tmp_path: Path) -> None:
    """同一 episode_index 的 frame 跨 shard 时必须合并成一个 bundle。"""
    # shard A: episode=0 frames=[0,1,2]，episode=1 frames=[0]
    # shard B: episode=0 frames=[3,4]，          episode=1 frames=[1,2]
    # 期望：episode 0 总长 5，episode 1 总长 3，frame 全部排序正确
    table_a = pa.table(
        {
            "episode_index": [0, 0, 0, 1],
            "frame_index": [0, 1, 2, 0],
            "observation.cartesian_position": [
                [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.1, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.2, 0.0, 0.0, 0.0, 0.0, 0.0],
                [9.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ],
        }
    )
    table_b = pa.table(
        {
            "episode_index": [0, 0, 1, 1],
            "frame_index": [3, 4, 1, 2],
            "observation.cartesian_position": [
                [0.3, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.4, 0.0, 0.0, 0.0, 0.0, 0.0],
                [9.1, 0.0, 0.0, 0.0, 0.0, 0.0],
                [9.2, 0.0, 0.0, 0.0, 0.0, 0.0],
            ],
        }
    )
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table_a)
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-001.parquet", table_b)

    bundles = adapt_droid_lerobot_v2(_ctx(tmp_path))
    assert len(bundles) == 2
    by_ep = {b.meta["droid_episode_index"]: b for b in bundles}
    # episode 0：x = 0.0, 0.1, 0.2, 0.3, 0.4
    np.testing.assert_allclose(by_ep[0].pose[:, 0], [0.0, 0.1, 0.2, 0.3, 0.4])
    # episode 1：x = 9.0, 9.1, 9.2
    np.testing.assert_allclose(by_ep[1].pose[:, 0], [9.0, 9.1, 9.2])


def test_droid_adapter_raises_when_no_pose_column(tmp_path: Path) -> None:
    """完全没有可用 pose 列 → raise ValueError + 列出 schema sniff 结果。"""
    table = pa.table(
        {
            "episode_index": [0, 0, 0],
            "frame_index": [0, 1, 2],
            # 故意只放无关列：language + reward + done，不含任何 cartesian / state
            "language_instruction": ["pick"] * 3,
            "reward": [0.0, 0.0, 1.0],
            "done": [False, False, True],
        }
    )
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table)

    with pytest.raises(ValueError) as excinfo:
        adapt_droid_lerobot_v2(_ctx(tmp_path))
    msg = str(excinfo.value)
    assert "cartesian_position" in msg and "observation.state" in msg
    assert "Sample schema" in msg


def test_droid_adapter_routed_via_load_huggingface_dataset(tmp_path: Path) -> None:
    """主入口 load_huggingface_dataset 必须通过 registry 命中 droid adapter，
    不能再 fall back 到通用启发式（否则 8 维 state 会被错当 7 维 pose）。"""
    n = 3
    state8 = [[float(j) for j in range(8)] for _ in range(n)]
    table = pa.table(
        {
            "episode_index": [0] * n,
            "frame_index": list(range(n)),
            "observation.state": state8,
            "action": [[0.0] * 7 for _ in range(n)],
        }
    )
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table)

    bundles = load_huggingface_dataset(
        tmp_path, dataset_id="droid_lerobot_scale30", version="v1"
    )
    assert len(bundles) == 1
    # registry 命中后必须带 fallback warning + pose_source label，证明走的是 droid adapter
    # 而不是通用 fallback（通用 fallback 不会写 pose_source / fallback warning）。
    assert bundles[0].meta["pose_source"] == "observation.state[:7]"
    assert any("joint-angle" in w for w in bundles[0].warnings)


def test_droid_adapter_prefix_match_routes_lerobot_droid_id(tmp_path: Path) -> None:
    """前缀 ``lerobot/droid`` 也要命中（hub 上 dataset_id = lerobot/droid_100 等）。"""
    table = _make_cartesian_position_table(n=2)
    _write_parquet(tmp_path / "data" / "chunk-000" / "file-000.parquet", table)
    ctx = _ctx(tmp_path, dataset_id="lerobot/droid_100")
    bundles = adapt_droid_lerobot_v2(ctx)
    assert len(bundles) == 1
    # 而且通过主入口走 registry 也能命中前缀
    bundles_via_loader = load_huggingface_dataset(
        tmp_path, dataset_id="lerobot/droid_100", version="v1"
    )
    assert len(bundles_via_loader) == 1
    assert bundles_via_loader[0].meta["pose_source"] == (
        "observation.cartesian_position(rpy->quat)"
    )
