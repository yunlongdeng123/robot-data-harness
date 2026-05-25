"""BridgeData V2 contract：parquet shard。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.qc.contracts import run_contract


def _write_bridge_parquet(
    path: Path,
    n: int,
    *,
    with_language: bool = True,
    num_episodes: int = 1,
) -> None:
    """写一份 bridge 风格 parquet：``episode_idx`` + ``step_idx`` 两列做 trajectory 切分。

    v1.6.7：bridge contract 现在要求 ``episode_idx`` 切出 >=1 episode 才 PASS
    （否则 ``episode_count_min`` rule FAIL，复刻 ddbfb traj=314 失真已经被门掉）。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    rows_per_ep = max(1, n // num_episodes)
    episode_idx: list[int] = []
    step_idx: list[int] = []
    for ep in range(num_episodes):
        length = n - ep * rows_per_ep if ep == num_episodes - 1 else rows_per_ep
        episode_idx.extend([ep] * length)
        step_idx.extend(range(length))
    total = len(episode_idx)
    cols = {
        "episode_idx": episode_idx,
        "step_idx": step_idx,
        "action": [[0.0] * 7 for _ in range(total)],
        "primary_image": ["s3://images/x.jpg"] * total,
        "environment": ["lab_kitchen"] * total,
        "skill": ["pick"] * total,
    }
    if with_language:
        cols["language_instruction"] = ["pick up the apple"] * total
    df = pd.DataFrame(cols)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)


def test_bridge_pass(tmp_path: Path) -> None:
    root = tmp_path / "bridge/v1"
    _write_bridge_parquet(root / "shard_001.parquet", 100, num_episodes=2)
    _write_bridge_parquet(root / "shard_002.parquet", 50, num_episodes=1)
    report, _ = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="bridge",
        dataset_id="bridge",
        version="v1",
    )
    rules = {r.rule_id: r for r in report.rules}
    assert rules["action_column_coverage"].status == "PASS"
    assert rules["parquet_valid_rate"].status == "PASS"
    assert rules["num_parquet_files_min"].status == "PASS"
    # v1.6.7：episode_count_min 是 fail rule，必须按 episode_idx 切出 episode
    assert rules["episode_count_min"].status == "PASS"
    # core metric：traj_p50 ≈ rows_per_ep（50 / 50 / 50），不是 row_count
    metrics = report.metrics
    assert metrics["episode_count"] >= 3
    assert 0 < metrics["traj_len_p50"] < 100, (
        f"traj_p50={metrics['traj_len_p50']} should be per-episode length, not row_count"
    )


def test_bridge_warn_when_language_missing(tmp_path: Path) -> None:
    root = tmp_path / "bridge/v1"
    _write_bridge_parquet(root / "shard_001.parquet", 30, with_language=False, num_episodes=1)
    report, _ = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="bridge",
        dataset_id="bridge",
        version="v1",
    )
    assert report.status == "WARN"
    assert any(r["rule_id"] == "language_missing_rate" for r in report.warning_rules)


def test_bridge_fails_when_no_episode_column(tmp_path: Path) -> None:
    """v1.6.7：parquet 不含 ``episode_idx`` 类列（enrich 失败）→ episode_count=0 → FAIL。

    ddbfb 凌晨 status=PASS metric=314 那次失真路径上，aggregator 静默回退到
    ``row_count`` 作 traj_len 而 ``episode_count`` 仍写 0；本测试确认这种回退已经
    被 ``episode_count_min`` fail rule 关掉，不再悄悄 PASS。
    """
    path = tmp_path / "bridge/v1/shard.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            # 故意不带任何 episode/trajectory 列
            "row_idx": list(range(20)),
            "action": [[0.0] * 7 for _ in range(20)],
            "language_instruction": ["pick"] * 20,
            "primary_image": ["s3://x.jpg"] * 20,
            "environment": ["lab"] * 20,
            "skill": ["s"] * 20,
        }
    )
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)

    report, _ = run_contract(
        dataset_uri=path.parent.as_posix(),
        dataset_family="bridge",
        dataset_id="bridge",
        version="v1",
    )
    rules = {r.rule_id: r for r in report.rules}
    assert rules["episode_count_min"].status == "FAIL"
    assert report.metrics["episode_count"] == 0
    # core metric 不再回退到 row_count 失真，traj_p50 必须为 0
    assert report.metrics["traj_len_p50"] == 0
    assert report.metrics["traj_len_p95"] == 0
    assert report.status == "FAIL"
