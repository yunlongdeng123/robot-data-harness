"""ml-ready export：本地 fake dwd + ads 跑通 train/val/test + dataset_card。"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.ml_ready import export_ml_ready


def _make_dwd_episode_feature(root: Path, dataset_id: str, n: int = 30) -> Path:
    out = root / "dwd" / dataset_id / "v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(n):
        rows.append(
            {
                "episode_id": f"ep_{i}",
                "dataset_id": dataset_id,
                "version": "v1",
                "num_samples": 60 + i,
                "duration_sec": 1.0 + 0.05 * i,
                "max_velocity_mps": 1.0,
                "quat_max_norm_error": 1e-4,
                "detected_press_count": 5 + (i % 3),
                "cluster_silhouette": 0.8,
            }
        )
    df = pd.DataFrame(rows)
    target = out / "episode_feature.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), target)
    return target


def _make_quality(root: Path, dataset_id: str) -> Path:
    out = root / "ads" / "quality"
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(30):
        rows.append(
            {
                "episode_id": f"ep_{i}",
                "dataset_id": dataset_id,
                "version": "v1",
                "quality_score": 95.0 if i < 25 else 50.0,
                "quality_status": "PASS" if i < 25 else "FAIL",
                "max_velocity_mps": 1.0,
                "quat_max_norm_error": 1e-4,
                "detected_press_count": 5,
                "cluster_silhouette": 0.8,
            }
        )
    df = pd.DataFrame(rows)
    target = out / "episode_quality_score.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), target)
    return target


def test_export_local(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    _make_dwd_episode_feature(root, "demo_button_press")
    _make_quality(root, "demo_button_press")
    out_dir = tmp_path / "ml_ready" / "demo" / "v1"

    result = export_ml_ready(
        input_root=(root / "dwd").as_posix(),
        output_uri=out_dir.as_posix(),
        quality_root=(root / "ads" / "quality").as_posix(),
        qc_root=None,
        dataset_id="demo",
        version="v1",
        quality_threshold=80.0,
        split=(0.8, 0.1, 0.1),
    )
    assert result.num_train + result.num_val + result.num_test == 25  # FAIL 的 5 行被过滤
    assert (out_dir / "train.parquet").is_file()
    assert (out_dir / "val.parquet").is_file()
    assert (out_dir / "test.parquet").is_file()
    assert (out_dir / "dataset_card.json").is_file()
    assert (out_dir / "feature_schema.json").is_file()
    assert (out_dir / "lineage.json").is_file()
    assert (out_dir / "_manifest.json").is_file()

    # split 比例校验（粗）
    assert result.num_train > result.num_val
    assert result.num_train > result.num_test


def test_export_no_features_raises(tmp_path: Path) -> None:
    out_dir = tmp_path / "ml_ready"
    import pytest

    with pytest.raises(ValueError, match="no episode_feature"):
        export_ml_ready(
            input_root=(tmp_path / "empty_dwd").as_posix(),
            output_uri=out_dir.as_posix(),
            dataset_id="x",
            version="v1",
        )
