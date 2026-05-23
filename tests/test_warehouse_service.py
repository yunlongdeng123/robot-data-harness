from __future__ import annotations

import pytest
from sqlalchemy import select
from psycopg import errors as psycopg_errors
from sqlalchemy.exc import OperationalError, ProgrammingError

from robot_dh.warehouse.models import BenchmarkCaseRow
from robot_dh.warehouse.service import V15SchemaMissingError, WarehouseService


def test_soft_mode_raises_on_schema_drift_error() -> None:
    service = WarehouseService(soft=True)
    err = ProgrammingError(
        statement="SELECT missing_column FROM etl_shards",
        params={},
        orig=psycopg_errors.UndefinedColumn("column etl_shards.shard_index does not exist"),
    )

    with pytest.raises(V15SchemaMissingError, match="schema mismatch"):
        service._handle_write_error("record_etl_shard", err)


def test_soft_mode_still_swallows_transient_write_error() -> None:
    service = WarehouseService(soft=True)
    err = OperationalError(statement="SELECT 1", params={}, orig=ConnectionError("connection reset"))

    assert service._handle_write_error("record_etl_shard", err) is None


def test_record_benchmark_case_writes_aligned_columns(tmp_path) -> None:
    service = WarehouseService(db_uri=f"sqlite:///{tmp_path}/registry.db", soft=False)

    row_id = service.record_benchmark_case(
        benchmark_id="bench-1",
        case_id="case-1",
        mutation="velocity_spike",
        expected_status="FAIL",
        actual_status="FAIL",
        expected_failed_validators=["velocity_jump"],
        actual_failed_validators=["velocity_jump"],
        match=True,
        duration_sec=1.25,
        error_message=None,
        metrics={"quality_score": 0.9},
        dataset_uri="/tmp/datasets/case-1",
        artifacts_uri="/tmp/reports/case-1/report.json",
    )

    assert row_id is not None
    with service._session() as session:
        row = session.scalar(select(BenchmarkCaseRow).where(BenchmarkCaseRow.id == row_id))

    assert row is not None
    assert row.dataset_uri == "/tmp/datasets/case-1"
    assert row.artifacts_uri == "/tmp/reports/case-1/report.json"
    assert row.mutation == "velocity_spike"
    assert row.mutation_type is None
    assert row.passed is None
    assert row.match is True
