from __future__ import annotations

from pathlib import Path

from robot_dh.api.main import (
    etl_job_detail,
    etl_jobs,
    lake_assets,
    lake_lineage,
    quality_summary,
)
from robot_dh.warehouse.service import WarehouseService


def test_api_v14_lake_read_endpoints(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path / 'robot_dh.db'}")
    wh = WarehouseService()
    wh.record_etl_job_start(
        job_id="job-v14",
        job_type="normalize",
        input_uri="raw/demo/v1",
        output_uri="lake/ods/demo/v1",
    )
    wh.record_etl_job_finish(job_id="job-v14", status="OK", metrics={"rows_out": 10})
    wh.record_lake_asset(
        dataset_id="demo",
        version="v1",
        layer="ods",
        asset_type="pose_parquet",
        uri="lake/ods/demo/v1/pose.parquet",
        format="parquet",
        size_bytes=123,
        row_count=10,
        checksum="a" * 64,
    )
    wh.record_lineage_edge(
        source_uri="raw/demo/v1",
        target_uri="lake/ods/demo/v1/pose.parquet",
        job_id="job-v14",
        job_type="normalize",
    )
    wh.record_quality_snapshot(
        dataset_id="demo",
        version="v1",
        run_id="job-v14",
        quality_status="PASS",
        quality_score=98.0,
        metrics={"gate_passed": True, "failed_checks": []},
    )

    assets = lake_assets(layer="ods", dataset_id="demo", version="v1")
    assert assets and assets[0]["uri"].endswith("pose.parquet")

    lineage = lake_lineage(uri="raw/demo/v1")
    assert lineage["outbound"][0]["target_uri"].endswith("pose.parquet")

    jobs = etl_jobs()
    assert any(job["job_id"] == "job-v14" for job in jobs)
    assert etl_job_detail("job-v14")["status"] == "OK"

    summary = quality_summary()
    assert summary[0]["dataset_id"] == "demo"
    assert summary[0]["quality_score"] == 98.0
