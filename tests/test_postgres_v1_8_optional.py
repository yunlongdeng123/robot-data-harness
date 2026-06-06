"""v1.8 PostgreSQL 集成测试。

仅在设置了 ROBOT_DH_TEST_POSTGRES_URI 时执行；未设置时整文件 skip。
不要把真实生产 URI 当 ROBOT_DH_TEST_POSTGRES_URI 用——本测试会真实写库（含 rollback 兜底）。
"""

from __future__ import annotations

import os

import pytest

PG_URI = os.environ.get("ROBOT_DH_TEST_POSTGRES_URI")

pytestmark = pytest.mark.skipif(
    PG_URI is None,
    reason="set ROBOT_DH_TEST_POSTGRES_URI to enable v1.8 PostgreSQL integration tests",
)


def test_v1_8_tables_exist() -> None:
    import psycopg
    raw = PG_URI.replace("postgresql+psycopg://", "postgresql://") if PG_URI.startswith("postgresql+psycopg://") else PG_URI
    expected = {
        "dim_dataset",
        "fact_etl_run", "fact_qc_rule_result", "fact_workflow_step", "fact_asset_profile",
        "dws_dataset_quality_daily", "dws_rule_failure_daily", "dws_workflow_ops_daily",
        "ads_quality_dashboard", "ads_workflow_ops_dashboard",
        "backfill_plans", "backfill_tasks",
        "sla_policies", "sla_checks", "dataset_partition_readiness",
    }
    with psycopg.connect(raw) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public' "
                "AND table_name = ANY(%s) ORDER BY table_name",
                ([t for t in expected],),
            )
            actual = {r[0] for r in cur.fetchall()}
            missing = expected - actual
            assert not missing, f"v1.8 表缺失：{missing}；请先在 infra 项目执行 ./scripts/39_pg_apply_v1_8_schema.sh"


def test_v1_8_smoke_upsert_dim_dataset_rolled_back() -> None:
    import psycopg
    raw = PG_URI.replace("postgresql+psycopg://", "postgresql://") if PG_URI.startswith("postgresql+psycopg://") else PG_URI
    with psycopg.connect(raw) as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute(
                    """
                    INSERT INTO dim_dataset (dataset_key, dataset_id, version, dataset_family)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (dataset_key) DO UPDATE SET updated_at = now()
                    """,
                    ("dataset:test_smoke_v1_8:1", "test_smoke_v1_8", "1", "smoke"),
                )
                cur.execute(
                    "SELECT count(*) FROM dim_dataset WHERE dataset_key = %s",
                    ("dataset:test_smoke_v1_8:1",),
                )
                cnt = cur.fetchone()[0]
                assert cnt == 1
                raise psycopg.Rollback()


def test_v1_8_warehouse_query_service_against_pg() -> None:
    """通过 WarehouseQueryService 直接打 PG。"""
    os.environ["ROBOT_DH_DB_URI"] = PG_URI
    from robot_dh.warehouse_metrics import WarehouseQueryService, load_warehouse_metrics_config
    from robot_dh.warehouse_metrics.query import QueryRequest

    cfg = load_warehouse_metrics_config()
    svc = WarehouseQueryService(config=cfg)
    rows = svc.query(QueryRequest(table="dim_dataset", limit=1))
    assert isinstance(rows, list)
