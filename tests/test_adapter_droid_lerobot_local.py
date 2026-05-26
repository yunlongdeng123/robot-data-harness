"""v1.7：DroidLeRobotAdapter 本地 LeRobot v2 layout 探针。"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.adapters import get_adapter
from robot_dh.lake.uri import to_file_uri


def _make_lerobot_v2(root: Path, episodes: int = 4) -> None:
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "info.json").write_text(json.dumps({
        "robot_type": "droid",
        "total_episodes": episodes,
        "total_frames": episodes * 5,
        "fps": 15,
    }))
    chunk = root / "data" / "chunk-000"
    chunk.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "episode_index": list(range(episodes)),
        "frame_index": [0] * episodes,
    })
    pq.write_table(table, chunk / "file-000.parquet")
    # 加一个 mp4 占位（adapter 不读 video，但要数）
    (root / "videos" / "observation.images.exterior_1_left" / "chunk-000").mkdir(
        parents=True, exist_ok=True,
    )
    (root / "videos" / "observation.images.exterior_1_left" / "chunk-000" / "file-000.mp4").write_bytes(b"\x00" * 64)


def test_droid_detects_meta_info(tmp_path: Path) -> None:
    _make_lerobot_v2(tmp_path, episodes=3)
    res = get_adapter("droid").detect(to_file_uri(tmp_path), dataset_id="droid_lerobot_dev1g")
    assert res.family == "droid"
    assert res.confidence >= 0.6


def test_droid_probe_local_reads_info_json(tmp_path: Path) -> None:
    _make_lerobot_v2(tmp_path, episodes=2)
    result = get_adapter("droid").probe(to_file_uri(tmp_path), sample_limit=4)
    assert result.status in ("OK", "WARN")
    assert result.parquet_files == 1
    assert result.video_files == 1
    assert result.episodes_count == 2
    assert result.schema_summary["info_json"]["fps"] == 15


def test_droid_list_episodes_filters_parquet_under_data(tmp_path: Path) -> None:
    _make_lerobot_v2(tmp_path, episodes=2)
    eps = get_adapter("droid").list_episodes(to_file_uri(tmp_path))
    assert len(eps) == 1
    assert eps[0].rel_path.startswith("data/")


def test_droid_normalize_options_skips_videos() -> None:
    opts = get_adapter("droid").normalize_options("file:///not/used")
    assert opts["skip_videos"] is True
    assert "data/" in opts["include_prefixes"]
    assert "meta/" in opts["include_prefixes"]
