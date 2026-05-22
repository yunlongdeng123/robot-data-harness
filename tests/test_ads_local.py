from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.etl.ads import build_ads
from robot_dh.etl.features import build_features
from robot_dh.etl.normalize import normalize_dataset
from robot_dh.lake.manifest import MANIFEST_FILENAME
from robot_dh.lake.schema import (
    DATASET_QUALITY_SUMMARY_SCHEMA,
    EPISODE_QUALITY_SCORE_SCHEMA,
    VALIDATOR_FAILURE_STATS_SCHEMA,
)
from robot_dh.warehouse.service import WarehouseService


@pytest.fixture
def dwd_root(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    lake_root = tmp_path / "lake"
    for ds in ("alpha", "beta"):
        raw_dir = generate_demo_dataset(
            output_dir=tmp_path / "raw" / ds,
            duration_sec=4.0,
            fps=30,
            num_buttons=3,
            num_presses=9,
        )
        ods_dir = lake_root / "ods" / ds / "v1"
        dwd_dir = lake_root / "dwd" / ds / "v1"
        normalize_dataset(
            dataset_uri=raw_dir.as_posix(),
            output_uri=ods_dir.as_posix(),
            dataset_id=ds,
            version="v1",
        )
        build_features(input_uri=ods_dir.as_posix(), output_uri=dwd_dir.as_posix())
    return lake_root / "dwd"


def test_build_ads_writes_three_parquets_and_manifest(dwd_root: Path, tmp_path: Path) -> None:
    ads_dir = tmp_path / "lake/ads/quality"
    result = build_ads(input_root_uri=dwd_root.as_posix(), output_uri=ads_dir.as_posix())
    assert result.num_datasets == 2
    for name in ("dataset_quality_summary.parquet", "validator_failure_stats.parquet", "episode_quality_score.parquet"):
        assert (ads_dir / name).is_file(), f"missing {name}"
    assert (ads_dir / MANIFEST_FILENAME).is_file()


def test_build_ads_schemas(dwd_root: Path, tmp_path: Path) -> None:
    ads_dir = tmp_path / "lake/ads/quality"
    build_ads(input_root_uri=dwd_root.as_posix(), output_uri=ads_dir.as_posix())
    assert pq.read_table(ads_dir / "dataset_quality_summary.parquet").schema.equals(
        DATASET_QUALITY_SUMMARY_SCHEMA, check_metadata=False
    )
    assert pq.read_table(ads_dir / "validator_failure_stats.parquet").schema.equals(
        VALIDATOR_FAILURE_STATS_SCHEMA, check_metadata=False
    )
    assert pq.read_table(ads_dir / "episode_quality_score.parquet").schema.equals(
        EPISODE_QUALITY_SCORE_SCHEMA, check_metadata=False
    )


def test_build_ads_summary_metrics(dwd_root: Path, tmp_path: Path) -> None:
    ads_dir = tmp_path / "lake/ads/quality"
    build_ads(input_root_uri=dwd_root.as_posix(), output_uri=ads_dir.as_posix())
    summary = pq.read_table(ads_dir / "dataset_quality_summary.parquet").to_pandas()
    assert sorted(summary["dataset_id"].tolist()) == ["alpha", "beta"]
    assert (summary["num_episodes"] == 1).all()
    assert (summary["avg_quality_score"] >= 0).all()
    assert (summary["avg_quality_score"] <= 100).all()
    assert (summary["pass_rate"] >= 0).all()
    assert (summary["pass_rate"] <= 1).all()

    scores = pq.read_table(ads_dir / "episode_quality_score.parquet").to_pandas()
    assert len(scores) == 2
    assert set(scores["quality_status"].unique()) <= {"PASS", "WARN", "FAIL"}


def test_build_ads_records_quality_snapshots(dwd_root: Path, tmp_path: Path) -> None:
    ads_dir = tmp_path / "lake/ads/quality"
    build_ads(input_root_uri=dwd_root.as_posix(), output_uri=ads_dir.as_posix())
    wh = WarehouseService()
    snaps = wh.latest_quality_summary(limit=10)
    ids = sorted([(s["dataset_id"], s["version"]) for s in snaps])
    assert ids == [("alpha", "v1"), ("beta", "v1")]
    for s in snaps:
        assert s["quality_score"] is not None
        assert s["quality_status"] in {"PASS", "WARN", "FAIL"}
