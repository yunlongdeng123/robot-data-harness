"""F3 schema contract 守门：``EtlPerfRunRow`` 模型字段集必须显式声明并与 infra 端 PG schema 对齐。

背景：``docs/v1_6_etl_perf_runs_schema_align_request.md`` §4.3。

这套测试本身不连远端 PG，只做两件事：

1. ORM 字段集合 == 显式期望集合（``EXPECTED_COLUMNS``）。任何对 ``EtlPerfRunRow`` 的列增删
   都必须同步更新 ``EXPECTED_COLUMNS`` + 通知 infra 维护方加 migration（不然这个 case 会先挂）。
2. 用 SQLite 把 ``ensure_lake_tables`` 跑一遍，反射出来的物理列等于 ORM 声明的列；保证 ORM
   与 ``create_all`` 真的同步（避免 ``Mapped`` 写了字段但没被 metadata 收录的低级错误）。

可选第 3 步：``ROBOT_DH_TEST_POSTGRES_URI`` 设了之后会去远端 PG 拉 ``information_schema``
做差集断言；CI 没配 PG 时自动 skip。
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, inspect

from robot_dh.warehouse.models import EtlPerfRunRow, ensure_lake_tables

# 与 docs/v1_6_etl_perf_runs_schema_align_request.md §5 「远端对齐后字段约定」逐字段对齐
# 改动须三步走：(1) 更新本集合；(2) 通知 robot-dh-infra 加 migration；(3) PR 描述记录新增列
EXPECTED_COLUMNS: frozenset[str] = frozenset(
    {
        "id",
        "job_id",
        "run_id",
        "dataset_id",
        "version",
        "phase",
        "input_uri",
        "output_uri",
        "input_bytes",
        "output_bytes",
        "input_rows",
        "output_rows",
        "duration_sec",
        "download_duration_sec",
        "upload_duration_sec",
        "compute_duration_sec",
        "peak_memory_mb",
        "worker_id",
        "status",
        "error_message",
        "started_at",
        "finished_at",
        "metrics_json",
        "created_at",
    }
)


def test_etl_perf_runs_orm_columns_match_expected_set() -> None:
    orm_columns = {c.key for c in EtlPerfRunRow.__table__.columns}
    missing = EXPECTED_COLUMNS - orm_columns
    extra = orm_columns - EXPECTED_COLUMNS
    assert not missing and not extra, (
        "EtlPerfRunRow 与期望字段集不一致；"
        f"missing={sorted(missing)} extra={sorted(extra)}；"
        "改动需同步 (1) 本测试 EXPECTED_COLUMNS；(2) infra 端 migration；"
        "(3) PR 描述列出新增列。参见 docs/v1_6_etl_perf_runs_schema_align_request.md §4.3。"
    )


def test_etl_perf_runs_sqlite_create_all_matches_orm() -> None:
    engine = create_engine("sqlite:///:memory:")
    ensure_lake_tables(engine)
    inspector = inspect(engine)
    actual = {col["name"] for col in inspector.get_columns("etl_perf_runs")}
    expected = {c.key for c in EtlPerfRunRow.__table__.columns}
    assert actual == expected, (
        f"create_all 反射列与 ORM 不一致：sqlite={sorted(actual)} orm={sorted(expected)}"
    )


@pytest.mark.skipif(
    not os.environ.get("ROBOT_DH_TEST_POSTGRES_URI"),
    reason="ROBOT_DH_TEST_POSTGRES_URI not set; skip live PG drift assertion",
)
def test_etl_perf_runs_remote_pg_columns_aligned() -> None:
    """可选：连远端 PG 校验真实 schema 与 ORM 字段集一致。

    远端缺列时会暴露具体差集，提示 infra 跑对应 migration；远端多列（兼容字段）允许，
    但要 ORM 声明的列在远端都存在。
    """
    pg_uri = os.environ["ROBOT_DH_TEST_POSTGRES_URI"]
    engine = create_engine(pg_uri)
    inspector = inspect(engine)
    if "etl_perf_runs" not in inspector.get_table_names():
        pytest.skip("remote PG does not have etl_perf_runs; apply v1.5 schema first")
    actual = {col["name"] for col in inspector.get_columns("etl_perf_runs")}
    expected = {c.key for c in EtlPerfRunRow.__table__.columns}
    missing_on_remote = expected - actual
    assert not missing_on_remote, (
        f"远端 PG etl_perf_runs 缺列：{sorted(missing_on_remote)}；"
        "请让 infra 维护方 apply 对应 migration（参考 006_v1_6_etl_perf_runs_align.sql）。"
    )
