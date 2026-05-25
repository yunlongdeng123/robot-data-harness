"""基于 robot-dh-infra 提供的真实 fixture 跑 v1.6 bridgedata_v2_scale30 全链路修复验收。

fixture：``tests/fixtures/bridgedata_v2_scale30/shard_0-sample.parquet``
- 40 行 / 2 episode（``episode_idx ∈ {0,1}``，每个 episode 20 步）
- 完整保留 mbodiai/oxe_bridge_v2 嵌套 schema：
  - ``action: struct<pose: struct<x,y,z,roll,pitch,yaw>, grasp>``
  - ``state: struct<end_effector_pose: struct<x,y,z,r,p,y>, is_first/last/terminal, language_embedding: list<double>>``

覆盖 ``robot-dh-fhkvr-bundle/v1_6_bridgedata_v2_normalize_adapter_request.md`` §7 验收：

- 1: adapter 走嵌套 schema yield 2 个 episode、pose (20, 7)。
- 3: qc-contract `traj_len_p50 ≈ 20`、`language_missing_rate < 1.0`。
- 5: partition planner `estimated_rows` 误差 < 5%（这里用真实 num_rows=40）。
"""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "bridgedata_v2_scale30" / "shard_0-sample.parquet"


pytestmark = pytest.mark.skipif(
    not FIXTURE.is_file(),
    reason="bridgedata_v2_scale30 sample fixture missing; copy from robot-dh-fhkvr-bundle/",
)


def _layout_fixture(tmp_path: Path) -> Path:
    """复制 fixture 到一个 dataset 风格目录 ``raw/<dataset_id>/<version>/data/<shard>``。"""
    root = tmp_path / "raw" / "bridgedata_v2_scale30" / "v1"
    (root / "data").mkdir(parents=True)
    shutil.copy(FIXTURE, root / "data" / "shard_0-00000-of-00001.parquet")
    return root


def test_adapter_handles_nested_oxe_bridge_v2_schema(tmp_path: Path) -> None:
    from robot_dh.lake.hf_adapters import AdapterContext, adapt_bridgedata_v2

    root = _layout_fixture(tmp_path)
    ctx = AdapterContext(
        dataset_dir=root,
        dataset_id="bridgedata_v2_scale30",
        version="v1",
        base_meta={},
    )
    episodes = adapt_bridgedata_v2(ctx)
    assert len(episodes) == 2, "expected 2 episodes split by episode_idx"
    for ep in episodes:
        assert ep.pose.shape == (20, 7), f"pose shape wrong: {ep.pose.shape}"
        qnorms = np.linalg.norm(ep.pose[:, 3:7], axis=1)
        np.testing.assert_allclose(qnorms, 1.0, atol=1e-6)
    # language_embedding 在嵌套里、且非空 -> missing rate = 0
    assert all(ep.meta.get("language_missing_rate") == 0.0 for ep in episodes)
    assert all(ep.meta.get("language_embedding_dim") == 512 for ep in episodes)
    # task 文本从 observation.task 字段抓取
    tasks = [ep.meta.get("task") for ep in episodes]
    assert all(isinstance(t, str) and len(t) > 0 for t in tasks)


def test_adapter_preserves_action_with_grasp(tmp_path: Path) -> None:
    """嵌套 action / absolute_action 必须按 (N, 7) = (x,y,z,r,p,y,grasp) 落到 bundle。

    覆盖 review 缺口 1：grasp 是 manipulation policy 的控制信号，必须保留。
    """
    import pyarrow.parquet as pq2

    from robot_dh.lake.hf_adapters import AdapterContext, adapt_bridgedata_v2

    root = _layout_fixture(tmp_path)
    ctx = AdapterContext(
        dataset_dir=root,
        dataset_id="bridgedata_v2_scale30",
        version="v1",
        base_meta={},
    )
    episodes = adapt_bridgedata_v2(ctx)
    for ep in episodes:
        assert ep.action is not None, "action with grasp must be preserved"
        assert ep.absolute_action is not None
        assert ep.action.shape == (20, 7)
        assert ep.absolute_action.shape == (20, 7)
        assert ep.meta.get("action_layout") == "x_y_z_roll_pitch_yaw_grasp"

    # 与 raw parquet 第 1 行的 action.grasp / action.pose.x 对账，断 episode 0 (idx [0..19]) 的首行
    table = pq2.read_table(root / "data" / "shard_0-00000-of-00001.parquet")
    a = table.column("action").combine_chunks()
    grasp_first = float(a.field("grasp")[0].as_py())
    pose_x_first = float(a.field("pose").field("x")[0].as_py())
    ep0 = next(e for e in episodes if e.meta.get("bridge_episode_index") == 0)
    np.testing.assert_allclose(ep0.action[0, 0], pose_x_first, atol=1e-12)
    np.testing.assert_allclose(ep0.action[0, 6], grasp_first, atol=1e-12)


def test_load_huggingface_dataset_uses_registry_for_nested_bridgedata_v2(tmp_path: Path) -> None:
    from robot_dh.lake.hf_adapter import load_huggingface_dataset

    root = _layout_fixture(tmp_path)
    episodes = load_huggingface_dataset(
        root,
        dataset_id="bridgedata_v2_scale30",
        version="v1",
    )
    assert len(episodes) == 2
    assert all(ep.pose.shape == (20, 7) for ep in episodes)


def test_normalize_end_to_end_on_nested_bridge_v2(tmp_path: Path, monkeypatch) -> None:
    """端到端：嵌套 schema fixture -> ods/{pose,episode_meta}.parquet。"""
    from robot_dh.etl.normalize import normalize_dataset

    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw = _layout_fixture(tmp_path)
    out = tmp_path / "lake" / "ods" / "bridgedata_v2_scale30" / "v1"
    res = normalize_dataset(
        dataset_uri=raw.as_posix(),
        output_uri=out.as_posix(),
        dataset_id="bridgedata_v2_scale30",
        version="v1",
    )
    assert res.status == "OK"
    assert res.num_samples == 40
    pose_df = pq.read_table(out / "pose.parquet").to_pandas()
    # 两个 episode 各 20 帧
    assert pose_df.groupby("episode_id").size().tolist() == [20, 20]
    qnorm = np.sqrt(
        pose_df["qx"] ** 2 + pose_df["qy"] ** 2 + pose_df["qz"] ** 2 + pose_df["qw"] ** 2
    )
    np.testing.assert_allclose(qnorm.to_numpy(), 1.0, atol=1e-5)


def test_qc_bridge_contract_uses_per_episode_lengths(tmp_path: Path) -> None:
    """qc-contract 应该按 episode_idx 切 trajectory 而不是把整文件当一条。"""
    from robot_dh.qc.contracts import run_contract

    raw = _layout_fixture(tmp_path)
    report, profile = run_contract(
        dataset_uri=(raw / "data").as_posix(),
        dataset_family="bridge",
        dataset_id="bridgedata_v2_scale30",
        version="v1",
    )
    metrics = report.metrics
    # 修复前：traj_len_p50/p95 = 40（整文件当一条）
    # 修复后：fixture 两条 episode 各 20 步 -> p50=p95=20
    assert metrics["traj_len_p50"] == 20, metrics
    assert metrics["traj_len_p95"] == 20, metrics
    # state.language_embedding 是非空 list<double>，missing rate 应该是 0.0
    assert metrics["language_missing_rate"] == 0.0, metrics
    # action 列也应被嵌套路径识别到
    assert metrics["action_column_coverage"] == 1.0
    # 由于 fixture 是有效 parquet，整体 status 不应是 FAIL
    assert report.status in ("PASS", "WARN")


def test_partition_planner_uses_real_num_rows_from_footer(tmp_path: Path) -> None:
    """partition planner 不应再走 bytes/256 启发式（v1.6 报告 §3.D）。

    fixture 是 32 KiB / 40 行；修复前 estimated_rows ≈ 128（误差 220%）；
    修复后必须读 footer 拿到真实 40。
    """
    from robot_dh.partition import plan_dataset_partitions

    raw = _layout_fixture(tmp_path)
    plan = plan_dataset_partitions(
        dataset_uri=raw.as_posix(),
        dataset_id="bridgedata_v2_scale30",
        version="v1",
        family_hint="bridge",
    )
    assert plan.dataset_family == "bridge"
    assert plan.partition_type == "parquet_file"
    assert len(plan.partitions) == 1
    estimated = plan.partitions[0].estimated_rows
    assert estimated == 40, f"expected 40 from parquet footer, got {estimated}"
