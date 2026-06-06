"""v1.9 PostgreSQL 集成测试。

仅在设置 ROBOT_DH_TEST_POSTGRES_URI 时执行；未设置时整文件 skip。
本测试会真实写库（含 rollback 兜底），不要用生产 URI。
"""

from __future__ import annotations

import os

import pytest

PG_URI = os.environ.get("ROBOT_DH_TEST_POSTGRES_URI")

pytestmark = pytest.mark.skipif(
    PG_URI is None,
    reason="set ROBOT_DH_TEST_POSTGRES_URI to enable v1.9 PostgreSQL integration tests",
)

_EXPECTED = {
    "model_registry",
    "inference_jobs",
    "inference_outputs",
    "inference_failures",
    "distillation_datasets",
    "inference_benchmark_runs",
    "ai_task_events",
    "dead_letter_tasks",
    "dws_inference_job_daily",
    "ads_inference_dashboard",
}


def _raw_uri() -> str:
    assert PG_URI is not None
    return PG_URI.replace("postgresql+psycopg://", "postgresql://") if PG_URI.startswith("postgresql+psycopg://") else PG_URI


def test_v1_9_tables_exist() -> None:
    import psycopg

    with psycopg.connect(_raw_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public' "
                "AND table_name = ANY(%s) ORDER BY table_name",
                ([t for t in _EXPECTED],),
            )
            actual = {r[0] for r in cur.fetchall()}
    missing = _EXPECTED - actual
    assert not missing, f"v1.9 表缺失：{missing}；请先执行 ./scripts/45_pg_apply_inference_schema.sh"


def test_v1_9_smoke_upsert_model_registry_rolled_back() -> None:
    import psycopg

    with psycopg.connect(_raw_uri()) as conn:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute(
                    """
                    INSERT INTO model_registry (model_id, model_name, model_type, backend, status)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (model_id) DO UPDATE SET updated_at = now()
                    """,
                    ("__smoke_v1_9__", "smoke", "mock", "mock", "ACTIVE"),
                )
                cur.execute("SELECT count(*) FROM model_registry WHERE model_id = %s", ("__smoke_v1_9__",))
                assert cur.fetchone()[0] == 1
                raise psycopg.Rollback()


def test_v1_9_warehouse_inference_layer_build_against_pg() -> None:
    """通过 WarehouseBuilder 在 PG 上 build inference 层（dry-run 校验表存在）。"""
    os.environ["ROBOT_DH_DB_URI"] = PG_URI
    from robot_dh.warehouse_metrics import WarehouseBuilder, load_warehouse_metrics_config
    from robot_dh.warehouse_metrics.dates import parse_date_range

    cfg = load_warehouse_metrics_config()
    builder = WarehouseBuilder(config=cfg)
    window = parse_date_range(date_="2026-06-01")
    report = builder.build(window=window, layers=["inference"], dry_run=True)
    assert report.backend == "postgresql"
    assert any(r.layer == "inference" for r in report.results)
