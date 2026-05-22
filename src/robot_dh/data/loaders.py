from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import numpy as np
import torch
import yaml

from robot_dh.data.dataset import DatasetBundle, VideoMetadata

PREFERRED_POSE_KEYS = (
    "eexyzxyzw",
    "endpose",
    "pose",
    "poses",
    "data",
    "tensor",
)


def extract_pose_candidate(payload: Any) -> np.ndarray | None:
    if isinstance(payload, torch.Tensor):
        return payload.detach().cpu().numpy()
    if isinstance(payload, np.ndarray):
        return payload
    if isinstance(payload, (list, tuple)):
        return np.asarray(payload)
    if isinstance(payload, dict):
        for key in PREFERRED_POSE_KEYS:
            if key in payload:
                candidate = extract_pose_candidate(payload[key])
                if candidate is not None:
                    return candidate
        for value in payload.values():
            candidate = extract_pose_candidate(value)
            if candidate is not None:
                return candidate
    return None


def coerce_pose_array(array_like: Any) -> tuple[np.ndarray, list[str]]:
    array = np.asarray(array_like, dtype=np.float64)
    warnings: list[str] = []

    if array.ndim == 1:
        if array.size != 7:
            raise ValueError(f"Expected a 7D pose vector, got shape {array.shape}")
        array = array.reshape(1, 7)
    elif array.ndim == 2:
        if array.shape[1] == 7:
            pass
        elif array.shape[0] == 7:
            array = array.T
            warnings.append("Pose array was transposed from [7, N] to [N, 7]")
        else:
            raise ValueError(f"Expected pose matrix shape [N, 7] or [7, N], got {array.shape}")
    else:
        raise ValueError(f"Unsupported pose array shape {array.shape}; expected a 1D or 2D array")

    if not np.isfinite(array).all():
        raise ValueError("Pose array contains NaN or Inf values")
    return array, warnings


def load_endpose(path: Path) -> tuple[np.ndarray, list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"endpose.pt not found: {path}")
    try:
        payload = torch.load(path, map_location="cpu", weights_only=False)
    except TypeError:
        payload = torch.load(path, map_location="cpu")
    candidate = extract_pose_candidate(payload)
    if candidate is None:
        raise ValueError(f"Unable to locate pose data in {path}")
    return coerce_pose_array(candidate)


def read_video_metadata(video_path: Path) -> VideoMetadata:
    import cv2

    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise RuntimeError(f"Failed to open video: {video_path}")
    fps = float(capture.get(cv2.CAP_PROP_FPS))
    frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT))
    capture.release()

    if fps <= 0.0 or frame_count <= 0:
        raise RuntimeError(
            f"Invalid video metadata from {video_path}: fps={fps}, frame_count={frame_count}"
        )
    return VideoMetadata(
        fps=fps,
        frame_count=frame_count,
        duration_sec=frame_count / fps,
        source="video",
    )


def load_meta(meta_path: Path | None) -> dict[str, Any]:
    if meta_path is None or not meta_path.exists():
        return {}
    with meta_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"meta.yaml must contain a YAML mapping: {meta_path}")
    return payload


def build_timestamps(
    num_samples: int,
    duration_sec: float | None,
    fps: float | None,
) -> tuple[np.ndarray, float, VideoMetadata]:
    if num_samples <= 0:
        raise ValueError("num_samples must be positive")
    if duration_sec is not None and duration_sec > 0:
        if num_samples == 1:
            timestamps = np.array([0.0], dtype=np.float64)
            dt = duration_sec
        else:
            timestamps = np.linspace(0.0, float(duration_sec), num_samples, dtype=np.float64)
            dt = float(duration_sec) / float(num_samples - 1)
        resolved_fps = 1.0 / dt if dt > 0 else float(fps or 0.0)
        meta = VideoMetadata(
            fps=resolved_fps,
            frame_count=num_samples,
            duration_sec=float(duration_sec),
            source="duration",
        )
        return timestamps, dt, meta

    resolved_fps = float(fps or 30.0)
    if resolved_fps <= 0:
        raise ValueError(f"fps must be positive, got {resolved_fps}")
    timestamps = np.arange(num_samples, dtype=np.float64) / resolved_fps
    dt = 1.0 / resolved_fps
    duration = 0.0 if num_samples == 1 else float(timestamps[-1])
    meta = VideoMetadata(
        fps=resolved_fps,
        frame_count=num_samples,
        duration_sec=duration,
        source="fps",
    )
    return timestamps, dt, meta


class DatasetLoader:
    def __init__(self, config: Mapping[str, Any] | None = None) -> None:
        self.config = dict(config or {})

    def load(self, dataset_path: Path) -> DatasetBundle:
        dataset_path = dataset_path.expanduser().resolve()
        if not dataset_path.exists() or not dataset_path.is_dir():
            raise FileNotFoundError(f"Dataset directory not found: {dataset_path}")
        endpose_path = dataset_path / "endpose.pt"
        video_path = dataset_path / "video.mp4"
        meta_path = dataset_path / "meta.yaml"

        pose, load_warnings = load_endpose(endpose_path)
        meta = load_meta(meta_path)

        if video_path.exists():
            video_meta = read_video_metadata(video_path)
            timestamps, dt, _ = build_timestamps(
                num_samples=pose.shape[0],
                duration_sec=video_meta.duration_sec,
                fps=video_meta.fps,
            )
            video_meta = VideoMetadata(
                fps=video_meta.fps,
                frame_count=video_meta.frame_count,
                duration_sec=video_meta.duration_sec,
                source=video_meta.source,
            )
        else:
            dataset_cfg = self.config.get("dataset", {})
            duration_sec = meta.get("duration_sec", dataset_cfg.get("duration_sec"))
            fps = meta.get("fps", dataset_cfg.get("fps", dataset_cfg.get("default_fps", 30)))
            timestamps, dt, video_meta = build_timestamps(
                num_samples=pose.shape[0],
                duration_sec=float(duration_sec) if duration_sec is not None else None,
                fps=float(fps) if fps is not None else None,
            )

        return DatasetBundle(
            dataset_id=str(meta.get("dataset_id", dataset_path.name)),
            dataset_path=dataset_path,
            endpose_path=endpose_path,
            pose=pose,
            timestamps=timestamps,
            dt=dt,
            video_meta=video_meta,
            meta=meta,
            video_path=video_path if video_path.exists() else None,
            meta_path=meta_path if meta_path.exists() else None,
            warnings=load_warnings,
        )

