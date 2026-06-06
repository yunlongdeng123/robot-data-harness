"""v1.8 etl_success_rate 口径 + perf_records_from_etl_run 单点写库 回归守门。

背景见 `runs/v1_8_e2e/quality_report/quality_summary.html` 排查：

- Bug A：``normalize.py::_emit_normalize_perf`` 之前用 ``emit_perf_records`` 既写 JSON
  又写 PG，调用点又在 ``with EtlProfiler`` 块**内部**，导致 PG 多出一条
  ``status='RUNNING'`` 的 normalize 孤儿；cli 上层 ``perf_records_from_etl_run``
  又写一条 OK，PG 同次 normalize 留下 RUNNING + OK 双行，``etl_success_rate``
  被误算为 N/(N+1)。修复后：normalize 内部只写 JSON；``perf_records_from_etl_run``
  改成把 ``NormalizeResult.status`` 透传 + 合并 ``NormalizeResult.metrics``。

- Bug B：``build_dws_dataset_quality_daily.sql`` 之前只把 ``OK / SUCCESS / SUCCEEDED``
  计入 ``etl_success_count``，把 ``WARN`` 排除在外。但 ``cli.py`` 里 WARN 是
  ``return 0`` 的 "带警告的成功"。features 单步 WARN 会把 etl_success_rate 拖到
  67%/80%，触发假阳性 CRITICAL alert。修复后 DML 把 WARN 也算 success；同时把
  ``RUNNING / PENDING / STARTED`` 这种非终态排除在分母外，兜底未来再有 early-write。

本测试不连 PG，全部用 sqlite + WarehouseBuilder（与 ``test_warehouse_builder_local.py``
共享 fixture 方式）。
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from robot_dh.etl.normalize import NormalizeResult
from robot_dh.etl.features import FeatureResult
from robot_dh.etl.ads import AdsResult
from robot_dh.etl.runner import EtlRunResult
from robot_dh.perf.writer import perf_records_from_etl_run
from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    DwsDatasetQualityDailyRow,
    EtlPerfRunRow,
    ensure_lake_tables,
)
from robot_dh.warehouse_metrics import (
    WarehouseBuilder,
    WarehouseMetricsConfig,
    parse_date_range,
)


# =========================================================================
# Bug A 守门：perf_records_from_etl_run 必须透传 NormalizeResult.status + metrics
# =========================================================================


def _make_normalize_result(status: str = "OK", metrics: dict | None = None) -> NormalizeResult:
    return NormalizeResult(
        dataset_id="demo_warn",
        version="v1",
        episode_id="ep-0",
        output_uri="file:///ods/demo_warn",
        manifest_uri="file:///ods/demo_warn/_manifest.json",
        pose_uri="file:///ods/demo_warn/pose.parquet",
        video_meta_uri=None,
        episode_meta_uri="file:///ods/demo_warn/episode_meta.parquet",
        num_samples=10,
        duration_sec=1.2,
        source_uris=["file:///raw/demo_warn"],
        job_id="job-norm",
        duration_job_sec=1.2,
        status=status,
        metrics=dict(metrics or {}),
    )


def _make_feature_result(status: str = "OK") -> FeatureResult:
    return FeatureResult(
        dataset_id="demo_warn",
        version="v1",
        output_uri="file:///dwd/demo_warn",
        manifest_uri="file:///dwd/demo_warn/_manifest.json",
        job_id="job-feat",
        duration_job_sec=0.8,
        job_status=status,
        num_press_events=42,
        cluster_silhouette=None,
    )


def _make_ads_result() -> AdsResult:
    return AdsResult(
        output_uri="file:///ads/demo_warn",
        manifest_uri="file:///ads/demo_warn/_manifest.json",
        job_id="job-ads",
        duration_job_sec=0.4,
        num_episodes=1,
        num_datasets=1,
    )


def _make_etl_run_result(
    norm_status: str = "OK",
    norm_metrics: dict | None = None,
    feat_status: str = "OK",
) -> EtlRunResult:
    return EtlRunResult(
        dataset_id="demo_warn",
        version="v1",
        raw_uri="file:///raw/demo_warn",
        ods_uri="file:///ods/demo_warn",
        dwd_uri="file:///dwd/demo_warn",
        ads_uri="file:///ads/demo_warn",
        job_id="etl-run-demo_warn",
        status="OK",
        duration_sec=2.5,
        normalize=_make_normalize_result(norm_status, norm_metrics),
        features=_make_feature_result(feat_status),
        ads=_make_ads_result(),
    )


def test_perf_record_normalize_status_passthrough() -> None:
    """normalize PerfRecord 必须用 NormalizeResult.status，不再 hardcoded "OK"。"""
    res = _make_etl_run_result(norm_status="WARN")
    records = perf_records_from_etl_run(etl_result=res)
    norm_recs = [r for r in records if r.phase == "normalize"]
    assert len(norm_recs) == 1, "perf_records 必须只产出 1 条 normalize"
    assert norm_recs[0].status == "WARN", (
        "回归保护：normalize PerfRecord.status 必须透传 NormalizeResult.status，"
        "不能再硬编码 OK；否则一旦 normalize WARN 会被 dashboard 误显示成 OK。"
    )


def test_perf_record_normalize_carries_substage_metrics() -> None:
    """NormalizeResult.metrics 必须合并进 PerfRecord.metrics（防止 sub-stage 诊断丢失）。"""
    sub = {
        "materialize_input_duration_sec": 0.15,
        "bundles_loaded": 8,
        "s3_upload_bytes": 21031,
        "manifest_duration_sec": 0.02,
    }
    res = _make_etl_run_result(norm_status="OK", norm_metrics=sub)
    records = perf_records_from_etl_run(etl_result=res)
    norm_recs = [r for r in records if r.phase == "normalize"]
    assert len(norm_recs) == 1
    assert norm_recs[0].metrics == sub, (
        "回归保护：NormalizeResult.metrics 必须透传到 PerfRecord.metrics，"
        "否则 PG 写库后 metrics_json 永远空，丢失 sub-stage profile 诊断价值。"
    )


def test_perf_records_no_running_orphan_when_status_explicit() -> None:
    """perf_records_from_etl_run 必须产出 3 条 final-state PerfRecord，没有 RUNNING。"""
    res = _make_etl_run_result(norm_status="OK", feat_status="WARN")
    records = perf_records_from_etl_run(etl_result=res)
    assert len(records) == 3
    phases = sorted(r.phase for r in records)
    assert phases == ["build_ads", "build_features", "normalize"]
    statuses = {r.phase: r.status for r in records}
    assert statuses == {"normalize": "OK", "build_features": "WARN", "build_ads": "OK"}
    assert "RUNNING" not in statuses.values(), (
        "回归保护：perf_records_from_etl_run 不允许产生 RUNNING——它只在 EtlProfiler "
        "未退出时存在，正常路径走不到。"
    )


# =========================================================================
# Bug B 守门：DWS DML 把 WARN 算 success、RUNNING 不进分母
# =========================================================================


@pytest.fixture()
def sqlite_warehouse(monkeypatch, tmp_path):
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
        output_root=f"file://{(repo_root / 'runs' / 'warehouse-test-warn-running').as_posix()}",
        sql_root=repo_root / "warehouse" / "sql",
    )


def _insert_perf_runs(engine, dt: datetime, rows: list[dict]) -> None:
    """rows = [{phase, status, duration_sec, job_id?}, ...]"""
    with Session(engine, expire_on_commit=False, future=True) as session:
        for i, r in enumerate(rows):
            session.add(EtlPerfRunRow(
                job_id=r.get("job_id", f"j-{i}"),
                run_id=r.get("run_id", "r-1"),
                dataset_id="demo_warn",
                version="v1",
                phase=r["phase"],
                input_bytes=10, output_bytes=20, input_rows=5, output_rows=5,
                duration_sec=float(r.get("duration_sec", 1.0)),
                status=r["status"],
                started_at=dt, finished_at=dt,
            ))
        session.commit()


def test_dws_etl_success_rate_warn_counts_as_success(sqlite_warehouse: str) -> None:
    """WARN 必须计入 etl_success_count；与 runner.py / cli.py 语义对齐。"""
    builder = WarehouseBuilder(config=_config_for_local())
    engine = builder.get_engine()
    dt = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    _insert_perf_runs(engine, dt, [
        {"phase": "normalize",      "status": "OK"},
        {"phase": "build_features", "status": "WARN"},
        {"phase": "build_ads",      "status": "OK"},
    ])

    report = builder.build(window=parse_date_range(date_="2026-05-25"))
    assert report.status in ("ok", "warn")

    with Session(engine, expire_on_commit=False, future=True) as session:
        dws = session.query(DwsDatasetQualityDailyRow).filter_by(dataset_id="demo_warn").one()
        assert dws.etl_run_count == 3
        assert dws.etl_success_count == 3, (
            "WARN 必须算 success，否则 features 一旦 WARN 就会触发"
            " etl_success_rate=67% < 0.8 假阳性 CRITICAL 告警。"
        )
        assert dws.etl_success_rate == pytest.approx(1.0)


def test_dws_etl_running_orphan_does_not_count_in_denominator(sqlite_warehouse: str) -> None:
    """RUNNING / PENDING / STARTED 等非终态不进分母，避免历史 bug 残留拖低成功率。"""
    builder = WarehouseBuilder(config=_config_for_local())
    engine = builder.get_engine()
    dt = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    # 3 条 final + 1 条 RUNNING 孤儿（模拟 normalize.py 修复前的旧数据残留）
    _insert_perf_runs(engine, dt, [
        {"phase": "normalize",      "status": "OK"},
        {"phase": "normalize",      "status": "RUNNING", "job_id": "orphan-normalize"},
        {"phase": "build_features", "status": "OK"},
        {"phase": "build_ads",      "status": "OK"},
    ])

    report = builder.build(window=parse_date_range(date_="2026-05-25"))
    assert report.status in ("ok", "warn")

    with Session(engine, expire_on_commit=False, future=True) as session:
        dws = session.query(DwsDatasetQualityDailyRow).filter_by(dataset_id="demo_warn").one()
        assert dws.etl_run_count == 3, (
            "RUNNING 孤儿必须从分母剔除，否则历史 bug 残留会永久把 success_rate "
            "拖到 N/(N+1)，新跑 ETL 也救不回来。"
        )
        assert dws.etl_success_count == 3
        assert dws.etl_success_rate == pytest.approx(1.0)


def test_dws_etl_real_fail_still_drives_success_rate_down(sqlite_warehouse: str) -> None:
    """真 FAIL 仍要让 etl_success_rate < 1.0，避免改 DML 时把口径放得太宽。"""
    builder = WarehouseBuilder(config=_config_for_local())
    engine = builder.get_engine()
    dt = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    _insert_perf_runs(engine, dt, [
        {"phase": "normalize",      "status": "OK"},
        {"phase": "build_features", "status": "FAIL"},
        {"phase": "build_ads",      "status": "OK"},
    ])

    report = builder.build(window=parse_date_range(date_="2026-05-25"))
    assert report.status in ("ok", "warn")

    with Session(engine, expire_on_commit=False, future=True) as session:
        dws = session.query(DwsDatasetQualityDailyRow).filter_by(dataset_id="demo_warn").one()
        assert dws.etl_run_count == 3
        assert dws.etl_success_count == 2
        assert dws.etl_fail_count == 1
        assert dws.etl_success_rate == pytest.approx(2 / 3)
