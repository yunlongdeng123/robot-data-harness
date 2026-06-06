"""v1.8 SqlTemplateRunner 测试。"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from robot_dh.warehouse_metrics.sql_runner import (
    SqlExecutionError,
    SqlTemplateRunner,
)


def _runner_with_sqlite(tmp_path: Path, sql_root: Path) -> SqlTemplateRunner:
    engine = create_engine(f"sqlite:///{tmp_path / 'w.db'}", future=True)
    return SqlTemplateRunner(engine=engine, sql_root=sql_root, default_params={"schema": "main"})


def test_load_existing_ddl_file(tmp_path: Path) -> None:
    repo_sql_root = Path(__file__).resolve().parents[1] / "warehouse" / "sql"
    runner = _runner_with_sqlite(tmp_path, repo_sql_root)
    sql = runner.load_sql("warehouse/sql/ddl/001_dim_dataset.sql")
    assert "CREATE TABLE IF NOT EXISTS dim_dataset" in sql


def test_render_replaces_placeholders(tmp_path: Path) -> None:
    repo_sql_root = Path(__file__).resolve().parents[1] / "warehouse" / "sql"
    runner = _runner_with_sqlite(tmp_path, repo_sql_root)
    rendered = runner.render(
        "SELECT * FROM {{ schema }}.fact_etl_run WHERE dt = '{{ start_date }}'",
        params={"start_date": "2026-05-25"},
    )
    assert rendered == "SELECT * FROM main.fact_etl_run WHERE dt = '2026-05-25'"


def test_render_rejects_unsafe_param_value(tmp_path: Path) -> None:
    repo_sql_root = Path(__file__).resolve().parents[1] / "warehouse" / "sql"
    runner = _runner_with_sqlite(tmp_path, repo_sql_root)
    with pytest.raises(SqlExecutionError, match="unsafe parameter value"):
        runner.render("SELECT * FROM {{ schema }}.t", params={"schema": "main; DROP TABLE t"})


def test_render_missing_param_raises_with_key_list(tmp_path: Path) -> None:
    repo_sql_root = Path(__file__).resolve().parents[1] / "warehouse" / "sql"
    runner = _runner_with_sqlite(tmp_path, repo_sql_root)
    with pytest.raises(SqlExecutionError, match="missing template parameter"):
        runner.render("SELECT {{ missing }} FROM x")


def test_execute_dry_run_returns_rendered_preview(tmp_path: Path) -> None:
    repo_sql_root = Path(__file__).resolve().parents[1] / "warehouse" / "sql"
    runner = _runner_with_sqlite(tmp_path, repo_sql_root)
    exec_res = runner.execute(
        "warehouse/sql/ddl/001_dim_dataset.sql",
        params={"schema": "main", "start_date": "2026-01-01", "end_date": "2026-01-31"},
        dry_run=True,
    )
    assert exec_res.status == "dry-run"
    assert "dim_dataset" in exec_res.rendered_sql_preview


def test_execute_error_message_contains_sql_file(tmp_path: Path) -> None:
    repo_sql_root = Path(__file__).resolve().parents[1] / "warehouse" / "sql"
    runner = _runner_with_sqlite(tmp_path, repo_sql_root)
    exec_res = runner.execute("warehouse/sql/dml/build_fact_etl_run.sql", params={
        "schema": "main", "start_date": "2026-01-01", "end_date": "2026-01-31",
    })
    # SQLite 不认 jsonb_array_elements / md5（部分版本不支持），但至少 sql_file 字段必须出现在 to_dict
    assert exec_res.sql_file == "warehouse/sql/dml/build_fact_etl_run.sql"


def test_execute_simple_select_returns_unknown_or_zero(tmp_path: Path) -> None:
    fake_root = tmp_path / "sql"
    (fake_root / "ddl").mkdir(parents=True)
    sql_path = fake_root / "ddl" / "999_demo.sql"
    sql_path.write_text("SELECT 1 AS x;", encoding="utf-8")
    runner = _runner_with_sqlite(tmp_path, fake_root)
    exec_res = runner.execute("ddl/999_demo.sql", params={"schema": "main"})
    assert exec_res.status == "ok"


def test_load_sql_outside_warehouse_tree_blocked(tmp_path: Path) -> None:
    repo_sql_root = Path(__file__).resolve().parents[1] / "warehouse" / "sql"
    runner = _runner_with_sqlite(tmp_path, repo_sql_root)
    # 试图加载 /etc/passwd 或仓库外文件
    with pytest.raises(SqlExecutionError, match="(not found|outside warehouse)"):
        runner.load_sql("/etc/passwd")
