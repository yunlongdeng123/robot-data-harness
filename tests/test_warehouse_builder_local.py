"""v1.8 WarehouseBuilder SQLite 路径测试。

覆盖 promptB 第十一节："使用临时 SQLite / build dim/fact/dws/ads 不报错 / 空数据也能生成 report"。
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    AdsQualityDashboardRow,
    AssetProfileRow,
    DatasetVersionRow,
    DwsDatasetQualityDailyRow,
    EtlPerfRunRow,
    MlReadyDatasetRow,
    QcContractRunRow,
    QualitySnapshotRow,
    WorkflowRunRow,
    WorkflowStepRow,
    ensure_lake_tables,
)
from robot_dh.warehouse_metrics import (
    WarehouseBuilder,
    WarehouseMetricsConfig,
    parse_date_range,
)


@pytest.fixture()
def sqlite_db_uri(monkeypatch, tmp_path):
    db_path = tmp_path / "warehouse.db"
    uri = f"sqlite:///{db_path}"
    monkeypatch.setenv("ROBOT_DH_DB_URI", uri)
    resolved = resolve_db_uri(uri)
    engine = get_engine(resolved)
    init_db(resolved)
    ensure_lake_tables(engine)
    yield uri


def _config_for_local() -> WarehouseMetricsConfig:
    repo_root = Path(__file__).resolve().parents[1]
    return WarehouseMetricsConfig(
        schema="main",
        output_root=f"file://{(repo_root / 'runs' / 'warehouse-test').as_posix()}",
        sql_root=repo_root / "warehouse" / "sql",
    )


def test_build_empty_db_does_not_fail(sqlite_db_uri: str) -> None:
    builder = WarehouseBuilder(config=_config_for_local())
    window = parse_date_range(date_="2026-05-25")
    report = builder.build(window=window, dry_run=False)
    assert report.status in ("ok", "warn")
    assert report.backend == "sqlite"
    # 所有 layer 都有结果记录
    layers = {r.layer for r in report.results}
    assert layers == {"dim", "fact", "dws", "ads"}


def test_init_check_reports_existing_tables(sqlite_db_uri: str) -> None:
    builder = WarehouseBuilder(config=_config_for_local())
    report = builder.init_check()
    assert report.backend == "sqlite"
    assert set(report.existing_tables) >= {
        "dim_dataset", "fact_etl_run", "ads_quality_dashboard", "backfill_plans", "sla_checks",
    }
    assert report.missing_tables == []


def test_dry_run_build_produces_all_layer_results(sqlite_db_uri: str) -> None:
    builder = WarehouseBuilder(config=_config_for_local())
    window = parse_date_range(date_="2026-05-25")
    report = builder.build(window=window, dry_run=True)
    assert report.dry_run is True
    assert all(r.status in ("dry-run", "ok") for r in report.results)


def test_build_with_fake_data_produces_dws_and_ads(sqlite_db_uri: str) -> None:
    """端到端：插入 fake dataset_version + etl_perf_run + qc_contract_run + workflow_step + ads → ADS 行已生成。"""
    builder = WarehouseBuilder(config=_config_for_local())
    engine = builder.get_engine()
    dt = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    with Session(engine, expire_on_commit=False, future=True) as session:
        session.add_all([
            DatasetVersionRow(
                dataset_id="droid_lerobot_dev1g", version="v1",
                raw_uri="file:///raw/droid", ods_uri="file:///ods/droid", dwd_uri="file:///dwd/droid",
                status="OK", created_at=dt,
            ),
            EtlPerfRunRow(
                job_id="j-1", run_id="r-1", dataset_id="droid_lerobot_dev1g", version="v1",
                phase="normalize", input_bytes=10, output_bytes=20, input_rows=5, output_rows=5,
                duration_sec=12.0, status="OK", started_at=dt, finished_at=dt,
            ),
            EtlPerfRunRow(
                job_id="j-2", run_id="r-2", dataset_id="droid_lerobot_dev1g", version="v1",
                phase="build_features", input_bytes=20, output_bytes=40, input_rows=5, output_rows=5,
                duration_sec=22.0, status="OK", started_at=dt, finished_at=dt,
            ),
            QcContractRunRow(
                run_id="qrun-1", contract_id="droid_v1", dataset_id="droid_lerobot_dev1g", version="v1",
                dataset_family="droid", status="PASS", started_at=dt, finished_at=dt,
                duration_sec=1.0, failed_rules_json=[], warning_rules_json=[],
            ),
            QcContractRunRow(
                run_id="qrun-2", contract_id="droid_v1", dataset_id="droid_lerobot_dev1g", version="v1",
                dataset_family="droid", status="WARN", started_at=dt, finished_at=dt,
                duration_sec=1.0,
                failed_rules_json=[],
                warning_rules_json=[{"rule_id": "row_count_min", "severity": "warn", "metric": "row_count", "op": ">=", "threshold": 100, "actual": 90}],
            ),
            WorkflowRunRow(
                workflow_name="wf-1", workflow_namespace="robot-dh", workflow_type="normalize",
                status="Succeeded", started_at=dt, finished_at=dt, duration_sec=10.0,
            ),
            WorkflowStepRow(
                workflow_name="wf-1", workflow_namespace="robot-dh", step_name="step-1",
                pod_name="pod-1", phase="Succeeded",
                dataset_id="droid_lerobot_dev1g", version="v1", dataset_family="droid",
                started_at=dt, finished_at=dt, duration_sec=10.0,
                metrics_json={"exit_code": 0},
            ),
            MlReadyDatasetRow(
                dataset_id="droid_lerobot_dev1g", version="v1", dataset_family="droid",
                output_uri="file:///ml-ready/droid", num_train=80, num_val=10, num_test=10,
                status="OK", created_at=dt,
            ),
            QualitySnapshotRow(
                dataset_id="droid_lerobot_dev1g", version="v1",
                quality_status="OK", quality_score=0.92,
                created_at=dt,
            ),
            AssetProfileRow(
                profile_id="prof-1", dataset_id="droid_lerobot_dev1g", version="v1",
                dataset_family="droid", asset_uri="file:///dwd/droid",
                asset_format="parquet", layer="dwd", bytes=200, rows=5,
                files_count=1, episodes_count=2, status="OK", created_at=dt,
            ),
        ])
        session.commit()

    window = parse_date_range(date_="2026-05-25")
    report = builder.build(window=window)
    assert report.status in ("ok", "warn")

    with Session(engine, expire_on_commit=False, future=True) as session:
        dws = session.query(DwsDatasetQualityDailyRow).all()
        assert len(dws) == 1
        d = dws[0]
        assert d.dataset_id == "droid_lerobot_dev1g"
        assert d.etl_run_count == 2
        assert d.qc_run_count == 2
        assert d.qc_pass_count == 1
        assert d.qc_warn_count == 1
        assert d.workflow_success_count == 1

        ads_rows = session.query(AdsQualityDashboardRow).all()
        assert len(ads_rows) == 1
        ads = ads_rows[0]
        assert ads.overall_status in ("PASS", "WARN", "FAIL")
        # quality_score = 100 * qc * 0.5 + 100 * etl * 0.3 + 100 * wf * 0.2，QC PASS+WARN 各 1 → 0.5
        assert ads.quality_score is not None
        assert ads.dwd_bytes == 200
