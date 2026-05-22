from __future__ import annotations

import json
from pathlib import Path

import pytest

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.etl.runner import etl_run, etl_scan
from robot_dh.lake.manifest import MANIFEST_FILENAME
from robot_dh.warehouse.service import WarehouseService


@pytest.fixture
def lake_root(tmp_path: Path) -> Path:
    return tmp_path / "lake"


def _make_raw_root(tmp_path: Path, datasets: list[tuple[str, str]]) -> Path:
    """在 tmp_path/raw/<dataset>/<version>/ 下构造本地 raw 布局。"""
    root = tmp_path
    (root / "raw").mkdir(parents=True, exist_ok=True)
    for ds, ver in datasets:
        target = root / "raw" / ds / ver
        # generate_demo_dataset 将扁平布局直接写入目标目录
        generate_demo_dataset(
            output_dir=target,
            duration_sec=3.0,
            fps=30,
            num_buttons=2,
            num_presses=4,
        )
    return root


def test_etl_run_normalize_then_features_local(tmp_path: Path, lake_root: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo",
        duration_sec=3.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )
    result = etl_run(
        dataset_uri=raw_dir.as_posix(),
        dataset_id="demo",
        version="v1",
        lake_root_uri=lake_root.as_posix(),
        build_ads_layer=False,
        summary_dir=tmp_path,
    )
    assert result.status in {"OK", "WARN"}
    assert result.normalize is not None
    assert result.features is not None
    assert (lake_root / "ods" / "demo" / "v1" / MANIFEST_FILENAME).is_file()
    assert (lake_root / "dwd" / "demo" / "v1" / MANIFEST_FILENAME).is_file()
    summary_path = tmp_path / "etl_summary.json"
    assert summary_path.is_file()
    payload = json.loads(summary_path.read_text())
    assert payload["dataset_id"] == "demo"


def test_etl_run_with_build_ads(tmp_path: Path, lake_root: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo",
        duration_sec=3.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )
    result = etl_run(
        dataset_uri=raw_dir.as_posix(),
        dataset_id="demo",
        version="v1",
        lake_root_uri=lake_root.as_posix(),
        build_ads_layer=True,
    )
    assert result.ads is not None
    assert (lake_root / "ads" / "quality" / MANIFEST_FILENAME).is_file()


def test_etl_run_infers_identity_from_path(tmp_path: Path, lake_root: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = generate_demo_dataset(
        output_dir=tmp_path / "raw" / "infer_ds" / "v1",
        duration_sec=3.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )
    result = etl_run(
        dataset_uri=raw_dir.as_posix(),
        dataset_id=None,
        version=None,
        lake_root_uri=lake_root.as_posix(),
    )
    assert result.dataset_id == "infer_ds"
    assert result.version == "v1"


def test_etl_scan_discovers_and_runs_local(tmp_path: Path, lake_root: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    _make_raw_root(tmp_path, [("alpha", "v1"), ("beta", "v1")])

    result = etl_scan(
        root_uri=tmp_path.as_posix(),
        lake_root_uri=lake_root.as_posix(),
        limit=None,
        force=False,
        summary_dir=tmp_path,
    )
    assert result.total == 2
    assert result.failed == 0
    assert result.succeeded == 2
    summary_path = tmp_path / "etl_scan_summary.json"
    assert summary_path.is_file()
    payload = json.loads(summary_path.read_text())
    assert payload["total"] == 2

    # 第二次 scan 应跳过（ods+dwd manifest 已存在）
    result2 = etl_scan(
        root_uri=tmp_path.as_posix(),
        lake_root_uri=lake_root.as_posix(),
        limit=None,
        force=False,
    )
    assert result2.skipped == 2
    assert result2.succeeded == 0


def test_etl_scan_respects_limit(tmp_path: Path, lake_root: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    _make_raw_root(tmp_path, [("ds_a", "v1"), ("ds_b", "v1"), ("ds_c", "v1")])
    result = etl_scan(
        root_uri=tmp_path.as_posix(),
        lake_root_uri=lake_root.as_posix(),
        limit=2,
    )
    assert result.total == 2
    assert result.succeeded == 2


def test_etl_runner_records_lineage_to_warehouse(tmp_path: Path, lake_root: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo",
        duration_sec=3.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )
    etl_run(
        dataset_uri=raw_dir.as_posix(),
        dataset_id="demo",
        version="v1",
        lake_root_uri=lake_root.as_posix(),
    )
    wh = WarehouseService()
    lineage = wh.list_lineage(uri=raw_dir.as_posix())
    assert any(edge["job_type"] == "etl_run" for edge in lineage["outbound"])
    assert any(edge["job_type"] == "normalize" for edge in lineage["outbound"])
