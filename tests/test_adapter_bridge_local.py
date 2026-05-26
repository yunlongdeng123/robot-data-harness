"""v1.7：BridgeDataAdapter 本地 parquet probe + s3 路径硬 timeout / cause。"""

from __future__ import annotations

from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from robot_dh.adapters import get_adapter
from robot_dh.adapters.bridgedata import BridgeDataAdapter
from robot_dh.lake.uri import to_file_uri


def _make_bridge_local(root: Path, shards: int = 2) -> None:
    (root / "data").mkdir(parents=True, exist_ok=True)
    for i in range(shards):
        table = pa.table({
            "episode_idx": [i, i],
            "step_idx": [0, 1],
        })
        pq.write_table(table, root / "data" / f"shard_{i:05d}.parquet")
    (root / "README").write_text("bridge data v2 dev")


def test_bridge_detects_data_parquet(tmp_path: Path) -> None:
    _make_bridge_local(tmp_path)
    res = get_adapter("bridge").detect(to_file_uri(tmp_path))
    assert res.family == "bridge"
    assert res.confidence >= 0.6


def test_bridge_probe_local_counts_shards(tmp_path: Path) -> None:
    _make_bridge_local(tmp_path, shards=3)
    result = get_adapter("bridge").probe(to_file_uri(tmp_path))
    assert result.status in ("OK", "WARN")
    assert result.parquet_files == 3


def test_bridge_probe_s3_timeout_surfaces_cause_type() -> None:
    """模拟 s3 路径 probe 跑超时，验证 cause_type=REMOTE_PARQUET_TIMEOUT。"""
    adapter = BridgeDataAdapter()

    with (
        mock.patch("robot_dh.adapters.bridgedata.ThreadPoolExecutor") as MockExec,
        mock.patch(
            "robot_dh.qc.profile._list_files",
            return_value=[("s3://b/k/data/shard_0.parquet", 1024)],
        ),
    ):
        ctx = mock.MagicMock()
        future = mock.MagicMock()
        future.result.side_effect = FutureTimeout()
        ctx.submit.return_value = future
        ctx.__enter__.return_value = ctx
        ctx.__exit__.return_value = False
        MockExec.return_value = ctx
        result = adapter.probe(
            "s3://b/k/",
            options={"probe_timeout_sec": 0.01, "max_retries": 1},
        )
    assert result.status == "WARN"
    assert result.errors
    assert result.errors[0]["cause_type"] == "REMOTE_PARQUET_TIMEOUT"


def test_bridge_disable_remote_lazy_returns_fail() -> None:
    adapter = BridgeDataAdapter()
    result = adapter.probe("s3://b/k/", options={"disable_remote_lazy": True})
    assert result.status == "FAIL"
    assert result.errors[0]["error_type"] == "RemoteLazyDisabled"
