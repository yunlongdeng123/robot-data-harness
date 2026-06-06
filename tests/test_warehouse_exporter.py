"""v1.8 WarehouseExporter 测试。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from robot_dh.warehouse_metrics.exporter import WarehouseExporter


@pytest.fixture()
def sample_rows() -> list[dict]:
    return [
        {"dt": "2026-05-25", "dataset_id": "a", "version": "v1", "qc_pass_rate": 0.95},
        {"dt": "2026-05-25", "dataset_id": "b", "version": "v1", "qc_pass_rate": 0.80},
    ]


def test_export_csv_writes_data_and_manifest(tmp_path: Path, sample_rows) -> None:
    exporter = WarehouseExporter()
    target_dir = tmp_path / "ads" / "dt=2026-05-25"
    output_uri = f"file://{target_dir.as_posix()}/"
    manifest = exporter.export(
        rows=sample_rows,
        table="ads_quality_dashboard",
        dt="2026-05-25",
        output_uri=output_uri,
        format="csv",
    )
    csv_file = target_dir / "ads_quality_dashboard.csv"
    assert csv_file.exists()
    content = csv_file.read_text(encoding="utf-8").splitlines()
    assert content[0].startswith("dt,dataset_id,version")
    assert manifest.row_count == 2

    manifest_file = target_dir / "_manifest.json"
    assert manifest_file.exists()
    m = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert m["table"] == "ads_quality_dashboard"
    assert m["format"] == "csv"
    assert m["checksum_sha256"] is not None


def test_export_json_writes_pretty_array(tmp_path: Path, sample_rows) -> None:
    exporter = WarehouseExporter()
    target = tmp_path / "out.json"
    manifest = exporter.export(
        rows=sample_rows, table="ads_quality_dashboard", dt="2026-05-25",
        output_uri=str(target), format="json",
    )
    data = json.loads(target.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert manifest.row_count == 2


def test_export_parquet_or_falls_back(tmp_path: Path, sample_rows) -> None:
    exporter = WarehouseExporter()
    target_dir = tmp_path / "p"
    manifest = exporter.export(
        rows=sample_rows, table="ads_quality_dashboard", dt="2026-05-25",
        output_uri=f"file://{target_dir.as_posix()}/", format="parquet",
    )
    if manifest.format == "parquet":
        target_file = target_dir / "ads_quality_dashboard.parquet"
        assert target_file.exists()
    else:
        # fallback 到 csv 时必须带 warning 说明
        assert manifest.format == "csv"
        assert any("pyarrow" in w for w in manifest.warnings)


def test_export_empty_rows_does_not_fail(tmp_path: Path) -> None:
    exporter = WarehouseExporter()
    manifest = exporter.export(
        rows=[], table="ads_quality_dashboard", dt="2026-05-25",
        output_uri=f"file://{tmp_path.as_posix()}/", format="csv",
    )
    assert manifest.row_count == 0
