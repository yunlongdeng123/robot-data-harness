from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.etl.normalize import normalize_dataset
from robot_dh.lake.manifest import MANIFEST_FILENAME
from robot_dh.lake.schema import EPISODE_META_SCHEMA, POSE_SCHEMA, VIDEO_META_SCHEMA
from robot_dh.warehouse.service import WarehouseService


@pytest.fixture
def synthetic_dataset(tmp_path: Path) -> Path:
    return generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo",
        duration_sec=4.0,
        fps=30,
        num_buttons=3,
        num_presses=9,
    )


def test_normalize_local_writes_three_parquets_and_manifest(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out_dir = tmp_path / "lake/ods/demo/v1"
    result = normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out_dir.as_posix(),
        dataset_id="demo",
        version="v1",
    )
    assert result.dataset_id == "demo"
    assert result.version == "v1"
    assert result.num_samples > 0
    assert (out_dir / "pose.parquet").is_file()
    assert (out_dir / "video_meta.parquet").is_file()
    assert (out_dir / "episode_meta.parquet").is_file()
    assert (out_dir / MANIFEST_FILENAME).is_file()


def test_normalize_pose_schema_matches(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out_dir = tmp_path / "lake/ods/demo/v1"
    normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out_dir.as_posix(),
        dataset_id="demo",
        version="v1",
    )
    table = pq.read_table(out_dir / "pose.parquet")
    assert table.schema.equals(POSE_SCHEMA, check_metadata=False)
    df = table.to_pandas()
    assert (df["dataset_id"] == "demo").all()
    assert (df["version"] == "v1").all()
    assert df["frame_idx"].tolist() == list(range(len(df)))
    assert (df["quat_norm"] > 0.99).all()


def test_normalize_video_meta_and_episode_meta_schema(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out_dir = tmp_path / "lake/ods/demo/v1"
    normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out_dir.as_posix(),
        dataset_id="demo",
        version="v1",
    )
    vm = pq.read_table(out_dir / "video_meta.parquet")
    assert vm.schema.equals(VIDEO_META_SCHEMA, check_metadata=False)
    assert vm.num_rows == 1

    em = pq.read_table(out_dir / "episode_meta.parquet")
    assert em.schema.equals(EPISODE_META_SCHEMA, check_metadata=False)
    em_df = em.to_pandas()
    assert em_df.iloc[0]["dataset_id"] == "demo"
    assert em_df.iloc[0]["num_samples"] > 0
    assert em_df.iloc[0]["duration_sec"] >= 0


def test_normalize_records_to_warehouse(synthetic_dataset: Path, tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    out_dir = tmp_path / "lake/ods/demo/v1"
    normalize_dataset(
        dataset_uri=synthetic_dataset.as_posix(),
        output_uri=out_dir.as_posix(),
        dataset_id="demo",
        version="v1",
    )
    wh = WarehouseService()
    assets = wh.list_lake_assets(layer="ods", dataset_id="demo")
    paths = sorted([a["uri"].rsplit("/", 1)[-1] for a in assets])
    assert paths == ["episode_meta.parquet", "pose.parquet", "video_meta.parquet"]
    for a in assets:
        assert a["asset_type"].endswith("_parquet")
        assert a["row_count"] is not None
        assert a["checksum"] and len(a["checksum"]) == 64

    jobs = wh.list_etl_jobs(limit=10)
    norm = [j for j in jobs if j["job_type"] == "normalize"]
    assert norm and norm[0]["status"] == "OK"


def test_normalize_video_missing_still_emits_ods(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    dataset_dir = generate_demo_dataset(
        output_dir=tmp_path / "raw" / "demo_novideo",
        duration_sec=3.0,
        fps=30,
        num_buttons=2,
        num_presses=4,
    )
    video = dataset_dir / "video.mp4"
    if video.is_file():
        video.unlink()
    out_dir = tmp_path / "lake/ods/demo_novideo/v1"
    result = normalize_dataset(
        dataset_uri=dataset_dir.as_posix(),
        output_uri=out_dir.as_posix(),
        dataset_id="demo_novideo",
        version="v1",
    )
    assert result.num_samples > 0
    assert (out_dir / "pose.parquet").is_file()
    assert (out_dir / "episode_meta.parquet").is_file()
    vm_df = pq.read_table(out_dir / "video_meta.parquet").to_pandas()
    assert vm_df.iloc[0]["video_uri"] == ""
