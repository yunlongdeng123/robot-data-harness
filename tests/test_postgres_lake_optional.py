"""可选 PostgreSQL 数据湖元数据集成测试。

仅当设置 `ROBOT_DH_TEST_POSTGRES_URI`（例如
`postgresql+psycopg://robot_dh_app:...@host:5432/robot_dh`）时启用。验证
`WarehouseService` 能访问云端 schema（5 张 v1.4 表）；写入使用唯一 run_id，结束后清理。
"""

from __future__ import annotations

import os
import uuid

import pytest

POSTGRES_URI = os.environ.get("ROBOT_DH_TEST_POSTGRES_URI")
pytestmark = pytest.mark.skipif(
    not POSTGRES_URI, reason="Postgres lake integration env not configured"
)


@pytest.fixture
def warehouse(monkeypatch):
    monkeypatch.setenv("ROBOT_DH_DB_URI", POSTGRES_URI)
    from robot_dh.warehouse.service import WarehouseService

    return WarehouseService(soft=False)


def test_postgres_lake_tables_present(warehouse) -> None:
    presence = warehouse.tables_present()
    expected = {"lake_assets", "etl_jobs", "lineage_edges", "dataset_versions", "quality_snapshots"}
    assert expected <= set(presence)
    assert all(presence[t] for t in expected), f"missing: {[t for t, v in presence.items() if not v]}"


def test_postgres_lake_asset_round_trip(warehouse) -> None:
    uniq = f"s3://robot-lake/tmp/__pytest__/{uuid.uuid4().hex}/pose.parquet"
    aid = warehouse.record_lake_asset(
        dataset_id="__pytest__",
        version="v1",
        layer="tmp",
        asset_type="pose_parquet",
        uri=uniq,
        format="parquet",
        size_bytes=1,
        row_count=1,
        checksum="0" * 64,
    )
    assert aid is not None
    rows = warehouse.list_lake_assets(layer="tmp", dataset_id="__pytest__")
    matching = [r for r in rows if r["uri"] == uniq]
    assert matching
    assert matching[0]["asset_type"] == "pose_parquet"


def test_postgres_etl_job_lifecycle(warehouse) -> None:
    jid = f"__pytest__-{uuid.uuid4().hex}"
    warehouse.record_etl_job_start(
        job_id=jid,
        job_type="normalize",
        input_uri="s3://robot-lake/tmp/in",
        output_uri="s3://robot-lake/tmp/out",
        metrics={"phase": "start"},
    )
    warehouse.record_etl_job_finish(
        job_id=jid,
        status="OK",
        metrics={"rows_in": 100, "rows_out": 100},
    )
    detail = warehouse.get_etl_job(jid)
    assert detail is not None
    assert detail["status"] == "OK"
    assert detail["duration_sec"] is not None
    metrics = detail["metrics_json"] or {}
    assert metrics.get("rows_in") == 100
