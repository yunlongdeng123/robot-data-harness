"""build-features：ods -> dwd。

读取 ods/pose.parquet（及可选 video_meta、episode_meta），执行与 v1.3 validator 等价的特征提取
（Euler、速度、按压检测、xy 聚类、轨迹分段），产出 pose_feature/press_event/trajectory_segment/
episode_feature.parquet 与 _manifest.json。
"""

from __future__ import annotations

import logging
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from scipy.signal import find_peaks, savgol_filter
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from robot_dh.etl.lineage import LineageEvent, write_lineage_events
from robot_dh.lake.manifest import (
    CodeInfo,
    JobInfo,
    ManifestBuilder,
    collect_file_stats,
    utcnow_iso,
    write_manifest,
)
from robot_dh.lake.schema import (
    EPISODE_FEATURE_SCHEMA,
    POSE_FEATURE_SCHEMA,
    PRESS_EVENT_SCHEMA,
    TRAJECTORY_SEGMENT_SCHEMA,
)
from robot_dh.lake.store import LakeStore, create_lake_store
from robot_dh.lake.uri import join_uri, parse_uri
from robot_dh.validators.euler_stability import quaternion_to_euler
from robot_dh.validators.quaternion import (
    make_quaternions_continuous,
    normalize_quaternions,
)
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)

DEFAULT_FEATURES_CFG: dict[str, Any] = {
    "velocity_jump_threshold_mps": 2.5,
    "press_z_prominence": None,
    "press_min_distance_frames": 5,
    "expected_num_buttons": 5,
    "kmeans_n_init": 20,
    "kmeans_random_state": 0,
    "segment_velocity_quiet_threshold_mps": 0.05,
}


def _load_features_config(config_path: Path | None) -> dict[str, Any]:
    cfg = dict(DEFAULT_FEATURES_CFG)
    if config_path is None:
        return cfg
    if not config_path.is_file():
        raise FileNotFoundError(f"feature config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    section = (raw.get("etl") or {}).get("features") or {}
    cfg.update({k: v for k, v in section.items() if k in cfg})
    return cfg


@dataclass(slots=True)
class FeatureResult:
    dataset_id: str
    version: str
    output_uri: str
    manifest_uri: str
    job_id: str
    duration_job_sec: float
    job_status: str
    num_press_events: int
    cluster_silhouette: float | None
    files: list[dict[str, Any]] = field(default_factory=list)


def _read_pose_table(local_dir: Path) -> pd.DataFrame:
    table = pq.read_table(local_dir / "pose.parquet")
    df = table.to_pandas()
    df.sort_values(["episode_id", "frame_idx"], inplace=True, ignore_index=True)
    return df


def _read_episode_meta(local_dir: Path) -> dict[str, Any] | None:
    p = local_dir / "episode_meta.parquet"
    if not p.is_file():
        return None
    df = pq.read_table(p).to_pandas()
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def _compute_pose_features(pose_df: pd.DataFrame, cfg: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    n = len(pose_df)
    xyz = pose_df[["x", "y", "z"]].to_numpy()
    quats = pose_df[["qx", "qy", "qz", "qw"]].to_numpy()
    normalized = make_quaternions_continuous(normalize_quaternions(quats))
    euler = quaternion_to_euler(quats)
    timestamps = pose_df["timestamp_sec"].to_numpy()
    quat_norm = np.linalg.norm(quats, axis=1)

    delta_d = np.zeros(n, dtype=np.float64)
    velocity = np.zeros(n, dtype=np.float64)
    if n >= 2:
        diff_xyz = np.diff(xyz, axis=0)
        delta_d[1:] = np.linalg.norm(diff_xyz, axis=1)
        delta_t = np.diff(timestamps)
        safe_dt = np.where(delta_t > 1e-12, delta_t, np.nan)
        v = delta_d[1:] / safe_dt
        v = np.where(np.isnan(v), 0.0, v)
        velocity[1:] = v

    threshold = float(cfg["velocity_jump_threshold_mps"])
    is_velocity_jump = velocity > threshold

    # 按压候选：z 低于 60 分位且参与峰值检测
    z_smooth = xyz[:, 2].copy()
    if n >= 7:
        window = min(31, n if n % 2 == 1 else n - 1)
        window = max(window, 5)
        if window % 2 == 0:
            window -= 1
        poly = min(3, window - 1)
        try:
            z_smooth = savgol_filter(xyz[:, 2], window_length=window, polyorder=poly, mode="nearest")
        except Exception as err:  # noqa: BLE001
            LOG.warning("savgol smoothing failed (%s); using raw z", err)
            z_smooth = xyz[:, 2].copy()

    z_range = float(np.max(z_smooth) - np.min(z_smooth)) if n > 0 else 0.0
    prominence = cfg["press_z_prominence"]
    if prominence is None:
        prominence = max(z_range * 0.05, 1.0e-6)
    min_distance = max(1, int(cfg["press_min_distance_frames"]))
    press_indices, properties = find_peaks(-z_smooth, prominence=prominence, distance=min_distance)
    cutoff = float(np.quantile(z_smooth, 0.6)) if n > 0 else np.inf
    selected = [int(i) for i in press_indices if z_smooth[int(i)] <= cutoff]
    selected_prominences: dict[int, float] = {}
    if len(press_indices) > 0:
        peaks_arr = np.asarray(press_indices)
        prom_arr = np.asarray(properties.get("prominences", np.zeros_like(peaks_arr, dtype=np.float64)))
        for idx, prom in zip(peaks_arr.tolist(), prom_arr.tolist()):
            if int(idx) in selected:
                selected_prominences[int(idx)] = float(prom)

    is_press_candidate = np.zeros(n, dtype=bool)
    if selected:
        is_press_candidate[np.asarray(selected, dtype=int)] = True

    features = pd.DataFrame(
        {
            "episode_id": pose_df["episode_id"].astype(object),
            "dataset_id": pose_df["dataset_id"].astype(object),
            "version": pose_df["version"].astype(object),
            "frame_idx": pose_df["frame_idx"].astype(np.int64),
            "timestamp_sec": timestamps.astype(np.float64),
            "x": xyz[:, 0].astype(np.float64),
            "y": xyz[:, 1].astype(np.float64),
            "z": xyz[:, 2].astype(np.float64),
            "roll": euler[:, 0].astype(np.float64),
            "pitch": euler[:, 1].astype(np.float64),
            "yaw": euler[:, 2].astype(np.float64),
            "velocity_mps": velocity.astype(np.float64),
            "delta_d": delta_d.astype(np.float64),
            "quat_norm": quat_norm.astype(np.float64),
            "is_velocity_jump": is_velocity_jump.astype(bool),
            "is_press_candidate": is_press_candidate.astype(bool),
        }
    )

    extras = {
        "press_indices": selected,
        "press_prominences": selected_prominences,
        "z_smooth": z_smooth,
        "euler": euler,
        "velocity": velocity,
        "quat_norm": quat_norm,
    }
    return features, extras


def _compute_press_events(
    pose_df: pd.DataFrame,
    extras: dict[str, Any],
    cfg: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    press_indices: list[int] = extras["press_indices"]
    proms: dict[int, float] = extras["press_prominences"]
    if not press_indices:
        return pd.DataFrame(
            {col.name: pd.Series(dtype=col.type.to_pandas_dtype() if col.type != pa.bool_() else bool) for col in PRESS_EVENT_SCHEMA}
        ), {"cluster_silhouette": None, "cluster_centers": []}

    press_xyz = pose_df[["x", "y", "z"]].to_numpy()[press_indices]
    xy = press_xyz[:, :2]
    expected = max(1, int(cfg["expected_num_buttons"]))
    cluster_ids: list[int | None] = []
    centers: list[tuple[float, float]] = []
    silhouette: float | None = None
    if len(xy) >= max(2, expected):
        k = min(expected, len(xy))
        try:
            model = KMeans(
                n_clusters=k,
                n_init=int(cfg["kmeans_n_init"]),
                random_state=int(cfg["kmeans_random_state"]),
            )
            labels = model.fit_predict(xy)
            cluster_ids = [int(l) for l in labels.tolist()]
            centers = [(float(c[0]), float(c[1])) for c in model.cluster_centers_.tolist()]
            if k >= 2 and len(set(labels)) >= 2:
                try:
                    silhouette = float(silhouette_score(xy, labels))
                except Exception:
                    silhouette = None
        except Exception as err:  # noqa: BLE001
            LOG.warning("kmeans failed (%s); leaving cluster_id null", err)
    else:
        cluster_ids = [None] * len(press_indices)

    rows: list[dict[str, Any]] = []
    timestamps = pose_df["timestamp_sec"].to_numpy()
    dataset_id = str(pose_df["dataset_id"].iloc[0]) if not pose_df.empty else ""
    version = str(pose_df["version"].iloc[0]) if not pose_df.empty else ""
    episode_id = str(pose_df["episode_id"].iloc[0]) if not pose_df.empty else ""
    for i, idx in enumerate(press_indices):
        cid = cluster_ids[i] if cluster_ids else None
        cx: float | None = None
        cy: float | None = None
        if cid is not None and centers:
            cx, cy = centers[cid]
        rows.append(
            {
                "event_id": f"{episode_id}-press-{i:04d}",
                "episode_id": episode_id,
                "dataset_id": dataset_id,
                "version": version,
                "frame_idx": int(idx),
                "timestamp_sec": float(timestamps[int(idx)]),
                "x": float(press_xyz[i, 0]),
                "y": float(press_xyz[i, 1]),
                "z": float(press_xyz[i, 2]),
                "cluster_id": int(cid) if cid is not None else None,
                "cluster_center_x": cx,
                "cluster_center_y": cy,
                "z_prominence": float(proms.get(int(idx), 0.0)),
            }
        )
    df = pd.DataFrame(rows, columns=[f.name for f in PRESS_EVENT_SCHEMA])
    return df, {"cluster_silhouette": silhouette, "cluster_centers": centers}


def _compute_segments(features: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    n = len(features)
    if n == 0:
        return pd.DataFrame(columns=[f.name for f in TRAJECTORY_SEGMENT_SCHEMA])

    velocity = features["velocity_mps"].to_numpy()
    timestamps = features["timestamp_sec"].to_numpy()
    quiet_threshold = float(cfg["segment_velocity_quiet_threshold_mps"])
    is_press = features["is_press_candidate"].to_numpy()

    states = np.full(n, "MOVE", dtype=object)
    states[velocity <= quiet_threshold] = "QUIET"
    states[is_press] = "PRESS"

    segments: list[dict[str, Any]] = []
    if n == 1:
        segments.append(
            {
                "segment_id": f"{features['episode_id'].iloc[0]}-seg-0000",
                "episode_id": features["episode_id"].iloc[0],
                "dataset_id": features["dataset_id"].iloc[0],
                "version": features["version"].iloc[0],
                "start_frame": 0,
                "end_frame": 0,
                "start_time_sec": float(timestamps[0]),
                "end_time_sec": float(timestamps[0]),
                "segment_type": str(states[0]),
                "duration_sec": 0.0,
                "distance": 0.0,
            }
        )
        return pd.DataFrame(segments, columns=[f.name for f in TRAJECTORY_SEGMENT_SCHEMA])

    delta_d = features["delta_d"].to_numpy()
    xyz = features[["x", "y", "z"]].to_numpy()
    start_idx = 0
    current_state = states[0]
    seg_count = 0
    for i in range(1, n):
        if states[i] != current_state:
            end_idx = i - 1
            distance = float(np.sum(delta_d[start_idx + 1 : end_idx + 1])) if end_idx > start_idx else 0.0
            segments.append(
                {
                    "segment_id": f"{features['episode_id'].iloc[0]}-seg-{seg_count:04d}",
                    "episode_id": features["episode_id"].iloc[0],
                    "dataset_id": features["dataset_id"].iloc[0],
                    "version": features["version"].iloc[0],
                    "start_frame": int(start_idx),
                    "end_frame": int(end_idx),
                    "start_time_sec": float(timestamps[start_idx]),
                    "end_time_sec": float(timestamps[end_idx]),
                    "segment_type": str(current_state),
                    "duration_sec": float(timestamps[end_idx] - timestamps[start_idx]),
                    "distance": distance,
                }
            )
            seg_count += 1
            start_idx = i
            current_state = states[i]

    end_idx = n - 1
    distance = float(np.sum(delta_d[start_idx + 1 : end_idx + 1])) if end_idx > start_idx else 0.0
    segments.append(
        {
            "segment_id": f"{features['episode_id'].iloc[0]}-seg-{seg_count:04d}",
            "episode_id": features["episode_id"].iloc[0],
            "dataset_id": features["dataset_id"].iloc[0],
            "version": features["version"].iloc[0],
            "start_frame": int(start_idx),
            "end_frame": int(end_idx),
            "start_time_sec": float(timestamps[start_idx]),
            "end_time_sec": float(timestamps[end_idx]),
            "segment_type": str(current_state),
            "duration_sec": float(timestamps[end_idx] - timestamps[start_idx]),
            "distance": distance,
        }
    )
    return pd.DataFrame(segments, columns=[f.name for f in TRAJECTORY_SEGMENT_SCHEMA])


def _compute_episode_features(
    features: pd.DataFrame,
    press_events: pd.DataFrame,
    extras: dict[str, Any],
    cluster_info: dict[str, Any],
) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame(columns=[f.name for f in EPISODE_FEATURE_SCHEMA])

    velocity = features["velocity_mps"].to_numpy()
    euler = extras["euler"]
    quat_norm = extras["quat_norm"]
    timestamps = features["timestamp_sec"].to_numpy()

    p95 = float(np.percentile(velocity, 95)) if velocity.size else 0.0
    row = {
        "episode_id": features["episode_id"].iloc[0],
        "dataset_id": features["dataset_id"].iloc[0],
        "version": features["version"].iloc[0],
        "num_samples": int(len(features)),
        "duration_sec": float(timestamps[-1] - timestamps[0]) if len(timestamps) > 1 else 0.0,
        "z_min": float(np.min(features["z"].to_numpy())),
        "z_max": float(np.max(features["z"].to_numpy())),
        "max_velocity_mps": float(np.max(velocity)) if velocity.size else 0.0,
        "mean_velocity_mps": float(np.mean(velocity)) if velocity.size else 0.0,
        "p95_velocity_mps": p95,
        "quat_max_norm_error": float(np.max(np.abs(quat_norm - 1.0))) if quat_norm.size else 0.0,
        "roll_var": float(np.var(euler[:, 0])) if euler.size else 0.0,
        "pitch_var": float(np.var(euler[:, 1])) if euler.size else 0.0,
        "yaw_var": float(np.var(euler[:, 2])) if euler.size else 0.0,
        "detected_press_count": int(len(press_events)),
        "cluster_silhouette": cluster_info.get("cluster_silhouette"),
    }
    return pd.DataFrame([row], columns=[f.name for f in EPISODE_FEATURE_SCHEMA])


def _write_dwd_parquets(
    local_dir: Path,
    features: pd.DataFrame,
    press_events: pd.DataFrame,
    segments: pd.DataFrame,
    episode_features: pd.DataFrame,
) -> None:
    pq.write_table(pa.Table.from_pandas(features, schema=POSE_FEATURE_SCHEMA, preserve_index=False), local_dir / "pose_feature.parquet")
    pq.write_table(pa.Table.from_pandas(press_events, schema=PRESS_EVENT_SCHEMA, preserve_index=False), local_dir / "press_event.parquet")
    pq.write_table(pa.Table.from_pandas(segments, schema=TRAJECTORY_SEGMENT_SCHEMA, preserve_index=False), local_dir / "trajectory_segment.parquet")
    pq.write_table(pa.Table.from_pandas(episode_features, schema=EPISODE_FEATURE_SCHEMA, preserve_index=False), local_dir / "episode_feature.parquet")


def _package_version() -> str:
    try:
        from robot_dh import __version__

        return str(__version__)
    except Exception:
        return "unknown"


def build_features(
    *,
    input_uri: str,
    output_uri: str,
    config_path: Path | None = None,
    job_id: str | None = None,
    warehouse: WarehouseService | None = None,
    lake_root_uri: str | None = None,
) -> FeatureResult:
    """ods slice -> dwd slice。"""
    if warehouse is None:
        warehouse = WarehouseService(soft=True)
    cfg = _load_features_config(config_path)

    job_id = job_id or f"build-features-{uuid.uuid4().hex[:12]}"
    started = time.time()
    started_iso = utcnow_iso()
    LOG.info("build-features: job_id=%s input_uri=%s output_uri=%s", job_id, input_uri, output_uri)

    warehouse.record_etl_job_start(
        job_id=job_id,
        job_type="build_features",
        input_uri=input_uri,
        output_uri=output_uri,
    )

    in_store = create_lake_store(input_uri)
    out_store = create_lake_store(output_uri)

    try:
        with tempfile.TemporaryDirectory(prefix="robot-dh-features-") as tmp_str:
            tmp = Path(tmp_str)
            input_local = tmp / "ods"
            input_local.mkdir(parents=True, exist_ok=True)
            in_store.download_dir(input_uri, input_local)

            pose_df = _read_pose_table(input_local)
            feature_parts: list[pd.DataFrame] = []
            press_parts: list[pd.DataFrame] = []
            segment_parts: list[pd.DataFrame] = []
            episode_feature_parts: list[pd.DataFrame] = []
            silhouettes: list[float] = []

            for _episode_id, episode_pose_df in pose_df.groupby("episode_id", sort=True):
                episode_pose_df = episode_pose_df.sort_values("frame_idx", ignore_index=True)
                episode_features_df, extras = _compute_pose_features(episode_pose_df, cfg)
                episode_press_events, cluster_info = _compute_press_events(episode_pose_df, extras, cfg)
                episode_segments = _compute_segments(episode_features_df, cfg)
                episode_summary = _compute_episode_features(
                    episode_features_df,
                    episode_press_events,
                    extras,
                    cluster_info,
                )
                feature_parts.append(episode_features_df)
                press_parts.append(episode_press_events)
                segment_parts.append(episode_segments)
                episode_feature_parts.append(episode_summary)
                if cluster_info.get("cluster_silhouette") is not None:
                    silhouettes.append(float(cluster_info["cluster_silhouette"]))

            features = pd.concat(feature_parts, ignore_index=True) if feature_parts else pd.DataFrame(columns=[f.name for f in POSE_FEATURE_SCHEMA])
            press_events = pd.concat(press_parts, ignore_index=True) if press_parts else pd.DataFrame(columns=[f.name for f in PRESS_EVENT_SCHEMA])
            segments = pd.concat(segment_parts, ignore_index=True) if segment_parts else pd.DataFrame(columns=[f.name for f in TRAJECTORY_SEGMENT_SCHEMA])
            episode_features = pd.concat(episode_feature_parts, ignore_index=True) if episode_feature_parts else pd.DataFrame(columns=[f.name for f in EPISODE_FEATURE_SCHEMA])
            cluster_info = {
                "cluster_silhouette": float(np.mean(silhouettes)) if silhouettes else None
            }

            staging = tmp / "dwd"
            staging.mkdir(parents=True, exist_ok=True)
            _write_dwd_parquets(staging, features, press_events, segments, episode_features)

            uploaded = out_store.upload_dir(staging, output_uri)
            files = collect_file_stats(
                staging,
                output_uri,
                files=[
                    "pose_feature.parquet",
                    "press_event.parquet",
                    "trajectory_segment.parquet",
                    "episode_feature.parquet",
                ],
            )

            dataset_id = str(pose_df["dataset_id"].iloc[0]) if not pose_df.empty else ""
            version = str(pose_df["version"].iloc[0]) if not pose_df.empty else ""

            num_press_events = int(len(press_events))
            expected = int(cfg["expected_num_buttons"])
            episode_count = max(1, int(pose_df["episode_id"].nunique()))
            job_status = "OK"
            if num_press_events < max(1, (expected * episode_count) // 2):
                job_status = "WARN"

            for info in files:
                warehouse.record_lake_asset(
                    dataset_id=dataset_id,
                    version=version,
                    layer="dwd",
                    asset_type=info["path"].replace(".parquet", "_parquet"),
                    uri=info["uri"],
                    format=info["format"],
                    size_bytes=info["size_bytes"],
                    row_count=info["row_count"],
                    checksum=info["checksum_sha256"],
                )

            warehouse.record_lineage_edge(
                source_uri=input_uri,
                target_uri=output_uri,
                job_id=job_id,
                job_type="build_features",
            )
            for info in files:
                warehouse.record_lineage_edge(
                    source_uri=input_uri,
                    target_uri=info["uri"],
                    job_id=job_id,
                    job_type="build_features",
                )

            warehouse.upsert_dataset_version(
                dataset_id=dataset_id,
                version=version,
                dwd_uri=output_uri,
                status="featurized",
            )

            elapsed = time.time() - started
            metrics = {
                "rows_in": int(len(pose_df)),
                "rows_out": int(len(features)),
                "num_episodes": episode_count,
                "num_press_events": num_press_events,
                "cluster_silhouette": cluster_info.get("cluster_silhouette"),
                "duration_ms": int(elapsed * 1000),
            }
            warehouse.record_etl_job_finish(job_id=job_id, status=job_status, metrics=metrics)

            if lake_root_uri:
                events = [
                    LineageEvent(
                        job_id=job_id,
                        job_type="build_features",
                        source_uri=input_uri,
                        target_uri=info["uri"],
                    )
                    for info in files
                ]
                lineage_store = create_lake_store(lake_root_uri)
                write_lineage_events(lineage_store, lake_root_uri, events)

            finished_iso = utcnow_iso()
            builder = ManifestBuilder(
                dataset_id=dataset_id,
                version=version,
                layer="dwd",
                output_uri=output_uri,
                source_uris=[input_uri],
                files=files,
                metrics=metrics,
                job=JobInfo(
                    job_id=job_id,
                    job_type="build_features",
                    started_at=started_iso,
                    finished_at=finished_iso,
                    duration_sec=elapsed,
                ),
                code=CodeInfo(package_version=_package_version()),
            )
            manifest_uri = write_manifest(out_store, builder)

            return FeatureResult(
                dataset_id=dataset_id,
                version=version,
                output_uri=output_uri,
                manifest_uri=manifest_uri,
                job_id=job_id,
                duration_job_sec=elapsed,
                job_status=job_status,
                num_press_events=num_press_events,
                cluster_silhouette=cluster_info.get("cluster_silhouette"),
                files=files,
            )
    except Exception as err:
        warehouse.record_etl_job_finish(job_id=job_id, status="FAIL", error_message=str(err))
        raise
