from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from robot_dh.etl.features import build_features
from robot_dh.etl.normalize import normalize_dataset
from robot_dh.etl.runner import etl_scan
from robot_dh.lake.manifest import MANIFEST_FILENAME


def _write_parquet_dataset(root: Path) -> Path:
    data_dir = root / "data" / "chunk-000"
    data_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        [
            {
                "episode_index": episode,
                "frame_index": frame,
                "timestamp": frame / 30.0,
                "endpose": [
                    frame * 0.01,
                    episode * 0.1,
                    0.2 - frame * 0.005,
                    0.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            }
            for episode in range(2)
            for frame in range(6)
        ]
    )
    df.to_parquet(data_dir / "file-000.parquet", index=False)
    return root


def test_normalize_hf_parquet_dataset_writes_multi_episode_ods(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = _write_parquet_dataset(tmp_path / "raw" / "hf_demo" / "sample")
    out_dir = tmp_path / "lake" / "ods" / "hf_demo" / "sample"

    result = normalize_dataset(
        dataset_uri=raw_dir.as_posix(),
        output_uri=out_dir.as_posix(),
        dataset_id="hf_demo",
        version="sample",
    )

    assert result.num_samples == 12
    pose = pq.read_table(out_dir / "pose.parquet").to_pandas()
    assert sorted(pose["episode_id"].unique().tolist()) == ["0", "1"]
    assert pose.groupby("episode_id").size().to_dict() == {"0": 6, "1": 6}
    assert (out_dir / MANIFEST_FILENAME).is_file()


def test_build_features_handles_multi_episode_ods(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    raw_dir = _write_parquet_dataset(tmp_path / "raw" / "hf_demo" / "sample")
    ods_dir = tmp_path / "lake" / "ods" / "hf_demo" / "sample"
    dwd_dir = tmp_path / "lake" / "dwd" / "hf_demo" / "sample"

    normalize_dataset(
        dataset_uri=raw_dir.as_posix(),
        output_uri=ods_dir.as_posix(),
        dataset_id="hf_demo",
        version="sample",
    )
    result = build_features(input_uri=ods_dir.as_posix(), output_uri=dwd_dir.as_posix())

    assert result.dataset_id == "hf_demo"
    episode_features = pq.read_table(dwd_dir / "episode_feature.parquet").to_pandas()
    assert sorted(episode_features["episode_id"].tolist()) == ["0", "1"]


def test_etl_scan_discovers_local_hf_raw_slice(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("ROBOT_DH_DB_URI", f"sqlite:///{tmp_path}/registry.db")
    _write_parquet_dataset(tmp_path / "raw" / "hf_demo" / "sample")
    lake_root = tmp_path / "lake"

    result = etl_scan(
        root_uri=tmp_path.as_posix(),
        lake_root_uri=lake_root.as_posix(),
        limit=None,
    )

    assert result.total == 1
    assert result.succeeded == 1
    assert (lake_root / "ods" / "hf_demo" / "sample" / MANIFEST_FILENAME).is_file()
