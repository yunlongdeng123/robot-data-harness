from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.etl.features import build_features
from robot_dh.etl.normalize import normalize_dataset
from robot_dh.lake.manifest import MANIFEST_FILENAME
from robot_dh.lake.schema import (
    EPISODE_FEATURE_SCHEMA,
    POSE_FEATURE_SCHEMA,
    PRESS_EVENT_SCHEMA,
    TRAJECTORY_SEGMENT_SCHEMA,
)
from robot_dh.warehouse.service import WarehouseService


@pytest.fixture
def ods_slice(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo",
        duration_sec=5.0,
        fps=30,
        num_buttons=3,
        num_presses=9,
    )
    ods_dir = tmp_path / "lake/ods/demo/v1"
    normalize_dataset(
        dataset_uri=raw_dir.as_posix(),
        output_uri=ods_dir.as_posix(),
        dataset_id="demo",
        version="v1",
    )
    return ods_dir, tmp_path


def test_build_features_writes_four_parquets(ods_slice) -> None:
    ods_dir, root = ods_slice
    dwd_dir = root / "lake/dwd/demo/v1"
    result = build_features(input_uri=ods_dir.as_posix(), output_uri=dwd_dir.as_posix())
    assert result.dataset_id == "demo"
    for name in ("pose_feature.parquet", "press_event.parquet", "trajectory_segment.parquet", "episode_feature.parquet"):
        assert (dwd_dir / name).is_file(), f"missing {name}"
    assert (dwd_dir / MANIFEST_FILENAME).is_file()


def test_build_features_schemas(ods_slice) -> None:
    ods_dir, root = ods_slice
    dwd_dir = root / "lake/dwd/demo/v1"
    build_features(input_uri=ods_dir.as_posix(), output_uri=dwd_dir.as_posix())
    assert pq.read_table(dwd_dir / "pose_feature.parquet").schema.equals(POSE_FEATURE_SCHEMA, check_metadata=False)
    assert pq.read_table(dwd_dir / "press_event.parquet").schema.equals(PRESS_EVENT_SCHEMA, check_metadata=False)
    assert pq.read_table(dwd_dir / "trajectory_segment.parquet").schema.equals(TRAJECTORY_SEGMENT_SCHEMA, check_metadata=False)
    assert pq.read_table(dwd_dir / "episode_feature.parquet").schema.equals(EPISODE_FEATURE_SCHEMA, check_metadata=False)


def test_build_features_detects_press_events(ods_slice) -> None:
    ods_dir, root = ods_slice
    dwd_dir = root / "lake/dwd/demo/v1"
    result = build_features(input_uri=ods_dir.as_posix(), output_uri=dwd_dir.as_posix())
    assert result.num_press_events >= 1
    df = pq.read_table(dwd_dir / "press_event.parquet").to_pandas()
    assert len(df) == result.num_press_events
    assert (df["dataset_id"] == "demo").all()


def test_build_features_episode_metrics_sane(ods_slice) -> None:
    ods_dir, root = ods_slice
    dwd_dir = root / "lake/dwd/demo/v1"
    build_features(input_uri=ods_dir.as_posix(), output_uri=dwd_dir.as_posix())
    ep = pq.read_table(dwd_dir / "episode_feature.parquet").to_pandas()
    assert len(ep) == 1
    row = ep.iloc[0]
    assert row["num_samples"] > 0
    assert row["duration_sec"] > 0
    assert row["mean_velocity_mps"] >= 0
    assert row["max_velocity_mps"] >= row["mean_velocity_mps"]
    assert row["quat_max_norm_error"] >= 0


def test_build_features_registers_lake_assets(ods_slice) -> None:
    ods_dir, root = ods_slice
    dwd_dir = root / "lake/dwd/demo/v1"
    build_features(input_uri=ods_dir.as_posix(), output_uri=dwd_dir.as_posix())
    wh = WarehouseService()
    dwd_assets = wh.list_lake_assets(layer="dwd", dataset_id="demo")
    names = sorted([a["uri"].rsplit("/", 1)[-1] for a in dwd_assets])
    assert names == [
        "episode_feature.parquet",
        "pose_feature.parquet",
        "press_event.parquet",
        "trajectory_segment.parquet",
    ]
