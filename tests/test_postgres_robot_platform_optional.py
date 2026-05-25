"""可选 PostgreSQL：v1.6 表的写入是否能在远端 PG 上跑通。

只有设置 `ROBOT_DH_TEST_POSTGRES_URI` 时才执行；其余情况下 skip。
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from robot_dh.warehouse.robot_platform import PlatformSchemaMissingError, PlatformWarehouse


pytestmark = pytest.mark.skipif(
    not os.environ.get("ROBOT_DH_TEST_POSTGRES_URI"),
    reason="ROBOT_DH_TEST_POSTGRES_URI not set; skipping v1.6 PG integration test",
)


@pytest.fixture
def wh_plat(monkeypatch) -> PlatformWarehouse:
    monkeypatch.setenv("ROBOT_DH_DB_URI", os.environ["ROBOT_DH_TEST_POSTGRES_URI"])
    return PlatformWarehouse(soft=False)


def test_record_task_heartbeat_round_trip(wh_plat: PlatformWarehouse) -> None:
    task = f"t-{uuid.uuid4().hex[:8]}"
    rid = wh_plat.record_task_heartbeat(
        task_id=task, phase="normalize",
        progress_current=1, progress_total=10, progress_unit="bundles",
        message="ok",
    )
    assert rid is not None
    latest = wh_plat.latest_heartbeat(task_id=task)
    assert latest is not None and latest["task_id"] == task


def test_record_dataset_partition_round_trip(wh_plat: PlatformWarehouse) -> None:
    pid = f"part-{uuid.uuid4().hex[:8]}-p000"
    rid = wh_plat.record_dataset_partition(
        partition_id=pid,
        dataset_id="demo",
        version="v1",
        dataset_uri="s3://bucket/raw/demo/v1",
        partition_type="parquet_file",
        partition_index=0,
        input_bytes=1024,
        estimated_rows=10,
        status="PLANNED",
    )
    assert rid is not None
    rows = wh_plat.list_dataset_partitions(dataset_id="demo", version="v1", limit=5)
    assert any(r["partition_id"] == pid for r in rows)
