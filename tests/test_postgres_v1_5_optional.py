"""可选 v1.5 PostgreSQL 集成测试；走 ROBOT_DH_TEST_POSTGRES_URI 开关。

未配置时整文件 skip。配置时假设远端已经执行过 v1.5 schema。
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

from robot_dh.perf.profiler import PerfRecord
from robot_dh.runtime.events import RuntimeEvent
from robot_dh.warehouse.service import V15SchemaMissingError, WarehouseService


_PG_URI = os.environ.get("ROBOT_DH_TEST_POSTGRES_URI")
pytestmark = pytest.mark.skipif(not _PG_URI, reason="ROBOT_DH_TEST_POSTGRES_URI not set")


@pytest.fixture(scope="module")
def warehouse() -> WarehouseService:
    return WarehouseService(soft=False, db_uri=_PG_URI)


def test_v1_5_perf_record_insert(warehouse: WarehouseService) -> None:
    record = PerfRecord(
        job_id=f"perf-{uuid.uuid4().hex[:8]}",
        run_id="run-test",
        dataset_id="demo",
        version="v1",
        phase="normalize",
        input_bytes=1234,
        output_bytes=2345,
        duration_sec=1.5,
        status="OK",
    )
    try:
        rid = warehouse.record_etl_perf_run(record)
    except V15SchemaMissingError as err:
        pytest.skip(str(err))
    assert rid is None or isinstance(rid, int)
    rows = warehouse.list_etl_perf_runs(dataset_id="demo", limit=10)
    assert any(r["job_id"] == record.job_id for r in rows)


def test_v1_5_runtime_event_insert(warehouse: WarehouseService) -> None:
    event = RuntimeEvent(event_type="etl_plan_created", payload={"k": "v"}, run_id="run-evt-test")
    try:
        warehouse.record_runtime_event(event)
    except V15SchemaMissingError as err:
        pytest.skip(str(err))
    rows = warehouse.list_runtime_events(run_id="run-evt-test", limit=10)
    assert any(r["event_id"] == event.event_id for r in rows)
