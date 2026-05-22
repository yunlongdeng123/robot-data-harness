"""v1.4 数据湖 parquet 的 Pyarrow schema。

集中定义列序与类型，供 normalize / build-features / build-ads 及测试共用。
"""

from __future__ import annotations

import pyarrow as pa

POSE_SCHEMA: pa.Schema = pa.schema(
    [
        ("episode_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("frame_idx", pa.int64()),
        ("timestamp_sec", pa.float64()),
        ("x", pa.float64()),
        ("y", pa.float64()),
        ("z", pa.float64()),
        ("qx", pa.float64()),
        ("qy", pa.float64()),
        ("qz", pa.float64()),
        ("qw", pa.float64()),
        ("quat_norm", pa.float64()),
    ]
)

VIDEO_META_SCHEMA: pa.Schema = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("video_uri", pa.string()),
        ("fps", pa.float64()),
        ("frame_count", pa.int64()),
        ("duration_sec", pa.float64()),
        ("width", pa.int64()),
        ("height", pa.int64()),
    ]
)

EPISODE_META_SCHEMA: pa.Schema = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("episode_id", pa.string()),
        ("num_samples", pa.int64()),
        ("duration_sec", pa.float64()),
        ("source_uri", pa.string()),
        ("created_at", pa.string()),
        ("meta_json", pa.string()),
    ]
)

POSE_FEATURE_SCHEMA: pa.Schema = pa.schema(
    [
        ("episode_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("frame_idx", pa.int64()),
        ("timestamp_sec", pa.float64()),
        ("x", pa.float64()),
        ("y", pa.float64()),
        ("z", pa.float64()),
        ("roll", pa.float64()),
        ("pitch", pa.float64()),
        ("yaw", pa.float64()),
        ("velocity_mps", pa.float64()),
        ("delta_d", pa.float64()),
        ("quat_norm", pa.float64()),
        ("is_velocity_jump", pa.bool_()),
        ("is_press_candidate", pa.bool_()),
    ]
)

PRESS_EVENT_SCHEMA: pa.Schema = pa.schema(
    [
        ("event_id", pa.string()),
        ("episode_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("frame_idx", pa.int64()),
        ("timestamp_sec", pa.float64()),
        ("x", pa.float64()),
        ("y", pa.float64()),
        ("z", pa.float64()),
        ("cluster_id", pa.int64()),
        ("cluster_center_x", pa.float64()),
        ("cluster_center_y", pa.float64()),
        ("z_prominence", pa.float64()),
    ]
)

TRAJECTORY_SEGMENT_SCHEMA: pa.Schema = pa.schema(
    [
        ("segment_id", pa.string()),
        ("episode_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("start_frame", pa.int64()),
        ("end_frame", pa.int64()),
        ("start_time_sec", pa.float64()),
        ("end_time_sec", pa.float64()),
        ("segment_type", pa.string()),
        ("duration_sec", pa.float64()),
        ("distance", pa.float64()),
    ]
)

EPISODE_FEATURE_SCHEMA: pa.Schema = pa.schema(
    [
        ("episode_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("num_samples", pa.int64()),
        ("duration_sec", pa.float64()),
        ("z_min", pa.float64()),
        ("z_max", pa.float64()),
        ("max_velocity_mps", pa.float64()),
        ("mean_velocity_mps", pa.float64()),
        ("p95_velocity_mps", pa.float64()),
        ("quat_max_norm_error", pa.float64()),
        ("roll_var", pa.float64()),
        ("pitch_var", pa.float64()),
        ("yaw_var", pa.float64()),
        ("detected_press_count", pa.int64()),
        ("cluster_silhouette", pa.float64()),
    ]
)

DATASET_QUALITY_SUMMARY_SCHEMA: pa.Schema = pa.schema(
    [
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("num_episodes", pa.int64()),
        ("avg_quality_score", pa.float64()),
        ("pass_rate", pa.float64()),
        ("avg_max_velocity_mps", pa.float64()),
        ("avg_cluster_silhouette", pa.float64()),
        ("total_press_count", pa.int64()),
        ("updated_at", pa.string()),
    ]
)

VALIDATOR_FAILURE_STATS_SCHEMA: pa.Schema = pa.schema(
    [
        ("validator_name", pa.string()),
        ("total_runs", pa.int64()),
        ("fail_count", pa.int64()),
        ("warn_count", pa.int64()),
        ("failure_rate", pa.float64()),
        ("updated_at", pa.string()),
    ]
)

EPISODE_QUALITY_SCORE_SCHEMA: pa.Schema = pa.schema(
    [
        ("episode_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("quality_score", pa.float64()),
        ("quality_status", pa.string()),
        ("max_velocity_mps", pa.float64()),
        ("quat_max_norm_error", pa.float64()),
        ("detected_press_count", pa.int64()),
        ("cluster_silhouette", pa.float64()),
    ]
)

ODS_SCHEMAS: dict[str, pa.Schema] = {
    "pose.parquet": POSE_SCHEMA,
    "video_meta.parquet": VIDEO_META_SCHEMA,
    "episode_meta.parquet": EPISODE_META_SCHEMA,
}

DWD_SCHEMAS: dict[str, pa.Schema] = {
    "pose_feature.parquet": POSE_FEATURE_SCHEMA,
    "press_event.parquet": PRESS_EVENT_SCHEMA,
    "trajectory_segment.parquet": TRAJECTORY_SEGMENT_SCHEMA,
    "episode_feature.parquet": EPISODE_FEATURE_SCHEMA,
}

ADS_SCHEMAS: dict[str, pa.Schema] = {
    "dataset_quality_summary.parquet": DATASET_QUALITY_SUMMARY_SCHEMA,
    "validator_failure_stats.parquet": VALIDATOR_FAILURE_STATS_SCHEMA,
    "episode_quality_score.parquet": EPISODE_QUALITY_SCORE_SCHEMA,
}
