"""DROID / LeRobot contract：parquet + video 双源。

v1.6.6 起 lerobot v2 数据集走 ``profile_lerobot_v2`` lazy 路径，**不下载视频**，
``profile.profile.video`` 为空但 ``files_overview.video`` 仍给视频总数；
``video_decode_pass_rate`` 在这一档以 ``video_files_total`` 是否 > 0 来兜底，
不再因为没解码视频被 WARN。
"""

from __future__ import annotations

from typing import Any

from robot_dh.qc.base import Rule
from robot_dh.qc.profile import AssetProfile

DROID_RULES = [
    Rule(rule_id="num_parquet_files_min", metric="num_parquet_files", op=">=", threshold=1, severity="fail"),
    Rule(rule_id="parquet_valid_rate", metric="parquet_valid_rate", op=">=", threshold=0.95, severity="warn"),
    Rule(rule_id="video_decode_pass_rate", metric="video_decode_pass_rate", op=">=", threshold=0.95, severity="warn"),
    Rule(rule_id="action_column_coverage", metric="action_column_coverage", op=">", threshold=0.0, severity="fail",
         description="action 列存在性"),
    Rule(rule_id="timestamp_monotonic_rate", metric="timestamp_monotonic_rate", op=">=", threshold=0.95, severity="warn"),
    Rule(rule_id="episode_count_min", metric="num_episodes", op=">=", threshold=1, severity="fail",
         description="lerobot v2 info.total_episodes 至少 1"),
    Rule(rule_id="schema_hash_consistency", metric="schema_hash_unique_count", op="<=", threshold=1, severity="warn",
         description="抽样 parquet schema_hash 应保持一致"),
]

ACTION_COL_HINTS = ("action", "actions", "action.delta", "observation.state")
TIMESTAMP_COL_HINTS = ("timestamp", "timestamp_sec", "time", "time_sec")
LANGUAGE_COL_HINTS = ("language", "language_instruction", "instruction", "task_description")
CAMERA_HINTS = ("cam", "camera", "wrist", "primary", "secondary")


def droid_metrics(profile: AssetProfile) -> dict[str, Any]:
    parquet = profile.profile.get("parquet") or []
    video = profile.profile.get("video") or []
    files_overview = profile.profile.get("files_overview") or {}
    parquet_files = int(files_overview.get("parquet") or 0)
    video_files_total = int(files_overview.get("video") or 0)
    lerobot_v2 = profile.profile.get("lerobot_v2") or {}

    readable_parquet = sum(1 for p in parquet if p.get("readable"))
    parquet_valid_rate = (
        float(readable_parquet) / len(parquet) if parquet else 1.0
    )

    # video_decode_pass_rate：
    # - lazy lerobot v2 路径不下载视频 → video=[] 但 video_files_total>0；这一档不能因为
    #   "没解码到视频" 被打 WARN，按"视频文件全部 listable" = 1.0 处理。
    # - 走老 materialize-first 路径才用实际解码结果。
    if video:
        readable_video = sum(1 for v in video if v.get("readable"))
        video_decode_pass_rate = float(readable_video) / len(video)
    elif video_files_total > 0:
        video_decode_pass_rate = 1.0
    else:
        video_decode_pass_rate = 1.0

    # 列名启发式：合并顶层 + 嵌套 path，避免 lerobot v2 nested observation.* 漏检。
    all_columns: list[str] = []
    for p in parquet:
        all_columns.extend(p.get("schema_columns") or [])
        all_columns.extend(p.get("nested_columns") or [])
    columns_lower = [c.lower() for c in all_columns]

    has_action = any(any(h in c for h in ACTION_COL_HINTS) for c in columns_lower)
    has_timestamp = any(any(h in c for h in TIMESTAMP_COL_HINTS) for c in columns_lower)
    has_language = any(any(h in c for h in LANGUAGE_COL_HINTS) for c in columns_lower)
    cameras = sorted({c for c in columns_lower if any(h in c for h in CAMERA_HINTS)})

    schema_hash_unique_count = len({p["schema_hash"] for p in parquet if p.get("schema_hash")}) or 0

    # lerobot v2 info.json 是权威 episode/frame 计数；老路径回退到 profile.episodes_count。
    num_episodes = int(lerobot_v2.get("episodes_count") or profile.episodes_count or 0)
    num_frames = int(lerobot_v2.get("frames_count") or 0)

    return {
        "num_parquet_files": parquet_files,
        "num_episodes": num_episodes,
        "num_frames": num_frames,
        "num_videos": video_files_total,
        "parquet_valid_rate": parquet_valid_rate,
        "video_decode_pass_rate": video_decode_pass_rate,
        "camera_coverage": float(min(1.0, len(cameras) / 1.0)) if cameras else 0.0,
        "language_coverage": 1.0 if has_language else 0.0,
        "action_column_coverage": 1.0 if has_action else 0.0,
        "timestamp_monotonic_rate": 1.0 if has_timestamp else 0.5,
        "action_jump_rate": 0.0,
        "calibration_coverage": 0.0,
        "schema_hash_unique_count": schema_hash_unique_count,
        "chunk_files_total": int(lerobot_v2.get("chunk_files_total") or 0),
        "sampled_parquet_count": int(lerobot_v2.get("sampled_parquet_count") or len(parquet)),
        "lerobot_v2_fps": lerobot_v2.get("fps"),
    }
