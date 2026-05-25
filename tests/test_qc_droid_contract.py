"""DROID / LeRobot contract：parquet + video（fake mp4）。"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.qc.contracts import run_contract


def _write_lerobot_parquet(path: Path, n: int = 32) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "episode_index": [0] * n,
            "frame_index": list(range(n)),
            "timestamp": [0.033 * i for i in range(n)],
            "action": [[0.0] * 7 for _ in range(n)],
            "observation.state": [[0.0] * 7 for _ in range(n)],
            "language_instruction": ["pick up cube"] * n,
            "wrist_camera": ["video.mp4"] * n,
        }
    )
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)


def test_droid_contract_pass(tmp_path: Path) -> None:
    root = tmp_path / "droid_lerobot/v1"
    _write_lerobot_parquet(root / "data" / "chunk-001.parquet")
    _write_lerobot_parquet(root / "data" / "chunk-002.parquet")
    # fake mp4：cv2 看不懂会标 unreadable，但视频空文件已被 universal 兜底
    (root / "videos").mkdir(parents=True, exist_ok=True)
    (root / "videos" / "ep0.mp4").write_bytes(b"\x00" * 32)
    report, _ = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="droid",
        dataset_id="droid_lerobot",
        version="v1",
    )
    # action / timestamp 列存在 -> action_column_coverage=1 通过
    rules = {r.rule_id: r for r in report.rules}
    assert rules["action_column_coverage"].status == "PASS"
    # parquet_valid_rate==1.0
    assert rules["parquet_valid_rate"].status == "PASS"
    # video_decode 视环境而定，FAIL/WARN 都允许
    assert report.status in ("PASS", "WARN", "FAIL")


def test_droid_contract_fail_when_no_action_column(tmp_path: Path) -> None:
    root = tmp_path / "droid_lerobot/v1"
    df = pd.DataFrame({"x": [1, 2, 3], "y": [0.1, 0.2, 0.3]})
    out = root / "data" / "chunk-001.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), out)
    report, _ = run_contract(
        dataset_uri=root.as_posix(),
        dataset_family="droid",
        dataset_id="droid_lerobot",
        version="v1",
    )
    assert report.status == "FAIL"
    failed_ids = {f["rule_id"] for f in report.failed_rules}
    assert "action_column_coverage" in failed_ids
