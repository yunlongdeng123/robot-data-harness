"""HuggingFace 风格机器人数据集适配器。

v1.3 demo 为含 ``endpose.pt`` 的扁平目录；v1.4 raw 桶另有 HF/LeRobot 布局（``data/`` 下 parquet、robomimic 式 HDF5）。
本模块在不引入完整 datasets 栈的前提下，将 ODS 所需子集转为 ``DatasetBundle`` episode。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from robot_dh.data.dataset import DatasetBundle, VideoMetadata
from robot_dh.data.loaders import build_timestamps, coerce_pose_array

LOG = logging.getLogger(__name__)

PARQUET_SUFFIX = ".parquet"
HDF5_SUFFIXES = (".hdf5", ".h5")

POSE_VECTOR_COLUMN_HINTS = (
    "endpose",
    "ee_pose",
    "eef_pose",
    "tcp_pose",
    "cartesian_pose",
    "pose",
    "observation.state",
    "state",
    "action",
    "actions",
)
EPISODE_COLUMNS = ("episode_id", "episode_index", "episode.idx", "episode")
FRAME_COLUMNS = ("frame_idx", "frame_index", "frame", "index")
TIMESTAMP_COLUMNS = ("timestamp_sec", "timestamp", "time_sec", "time")


def is_huggingface_dataset_dir(dataset_dir: Path) -> bool:
    """目录具备 HF/LeRobot/robomimic 快照特征时返回 True。"""

    dataset_dir = dataset_dir.expanduser().resolve()
    if (dataset_dir / "endpose.pt").is_file():
        return False
    return any(_iter_data_files(dataset_dir))


def load_huggingface_dataset(
    dataset_dir: Path,
    *,
    dataset_id: str | None = None,
    version: str | None = None,
) -> list[DatasetBundle]:
    """从 HuggingFace 风格快照加载一个或多个 pose episode。

    策略偏保守：优先显式 7D pose 列，再回退 ``observation.state``、``action`` 等常见向量列；
    仍无法识别时抛错，提示需专用 mapper。
    """

    dataset_dir = dataset_dir.expanduser().resolve()
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"HuggingFace dataset directory not found: {dataset_dir}")

    meta = _load_dataset_meta(dataset_dir)
    resolved_dataset_id = dataset_id or str(meta.get("dataset_id") or dataset_dir.name)
    resolved_version = version or str(meta.get("version") or meta.get("dataset_version") or "v1")

    episodes: list[DatasetBundle] = []
    for path in _iter_data_files(dataset_dir):
        if path.suffix.lower() == PARQUET_SUFFIX:
            episodes.extend(
                _load_parquet_episodes(
                    path,
                    dataset_dir=dataset_dir,
                    dataset_id=resolved_dataset_id,
                    version=resolved_version,
                    base_meta=meta,
                )
            )
        elif path.suffix.lower() in HDF5_SUFFIXES:
            episodes.extend(
                _load_hdf5_episodes(
                    path,
                    dataset_dir=dataset_dir,
                    dataset_id=resolved_dataset_id,
                    version=resolved_version,
                    base_meta=meta,
                )
            )

    if not episodes:
        candidates = ", ".join(p.relative_to(dataset_dir).as_posix() for p in _iter_data_files(dataset_dir))
        raise ValueError(
            "Unable to extract pose episodes from HuggingFace-style dataset "
            f"{dataset_dir}. Checked files: {candidates or '<none>'}. "
            "Add an explicit adapter mapping for this dataset schema."
        )
    return episodes


def _iter_data_files(dataset_dir: Path) -> Iterable[Path]:
    for path in sorted(dataset_dir.rglob("*")):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix == PARQUET_SUFFIX or suffix in HDF5_SUFFIXES:
            if "/.cache/" in path.as_posix():
                continue
            yield path


def _load_dataset_meta(dataset_dir: Path) -> dict[str, Any]:
    meta: dict[str, Any] = {"source_format": "huggingface"}
    info_path = dataset_dir / "meta" / "info.json"
    if info_path.is_file():
        try:
            payload = json.loads(info_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                meta.update(payload)
        except Exception as err:  # noqa: BLE001
            LOG.warning("failed to parse %s: %s", info_path, err)
    return meta


def _load_parquet_episodes(
    path: Path,
    *,
    dataset_dir: Path,
    dataset_id: str,
    version: str,
    base_meta: dict[str, Any],
) -> list[DatasetBundle]:
    try:
        df = pq.read_table(path).to_pandas()
    except Exception as err:  # noqa: BLE001
        LOG.warning("skipping unreadable parquet %s: %s", path, err)
        return []
    if df.empty:
        return []

    pose = _extract_pose_from_dataframe(df)
    if pose is None:
        LOG.warning("skipping parquet without pose-like columns: %s", path)
        return []

    episode_values = _series_or_default(df, EPISODE_COLUMNS, default=path.stem)
    frame_values = _optional_series(df, FRAME_COLUMNS)
    timestamp_values = _optional_series(df, TIMESTAMP_COLUMNS)

    rows = pd.DataFrame(
        {
            "_row": np.arange(len(df), dtype=np.int64),
            "_episode": episode_values.astype(str),
            "_frame": frame_values if frame_values is not None else np.arange(len(df), dtype=np.int64),
        }
    )
    episodes: list[DatasetBundle] = []
    for episode_id, group in rows.groupby("_episode", sort=True):
        order = group.sort_values("_frame")["_row"].to_numpy(dtype=np.int64)
        episode_pose = pose[order]
        timestamps = None if timestamp_values is None else np.asarray(timestamp_values)[order]
        episodes.append(
            _make_bundle(
                dataset_dir=dataset_dir,
                source_path=path,
                dataset_id=dataset_id,
                version=version,
                episode_id=str(episode_id),
                pose=episode_pose,
                timestamps=timestamps,
                base_meta=base_meta,
            )
        )
    return episodes


def _extract_pose_from_dataframe(df: pd.DataFrame) -> np.ndarray | None:
    scalar = _extract_scalar_pose_columns(df)
    if scalar is not None:
        return scalar

    for column in _candidate_vector_columns(df.columns):
        values = _series_to_pose_matrix(df[column])
        if values is not None:
            return values
    return None


def _extract_scalar_pose_columns(df: pd.DataFrame) -> np.ndarray | None:
    lowered = {str(c).lower(): c for c in df.columns}
    groups = (
        ("x", "y", "z", "qx", "qy", "qz", "qw"),
        ("ee_x", "ee_y", "ee_z", "ee_qx", "ee_qy", "ee_qz", "ee_qw"),
        ("eef_x", "eef_y", "eef_z", "eef_qx", "eef_qy", "eef_qz", "eef_qw"),
    )
    for group in groups:
        if all(name in lowered for name in group):
            arr = df[[lowered[name] for name in group]].to_numpy(dtype=np.float64)
            return _coerce_pose_or_none(arr)
    return None


def _candidate_vector_columns(columns: Iterable[Any]) -> list[Any]:
    def score(column: Any) -> tuple[int, str]:
        name = str(column).lower()
        for i, hint in enumerate(POSE_VECTOR_COLUMN_HINTS):
            if hint in name:
                return i, name
        return len(POSE_VECTOR_COLUMN_HINTS), name

    return sorted(columns, key=score)


def _series_to_pose_matrix(series: pd.Series) -> np.ndarray | None:
    rows: list[np.ndarray] = []
    for value in series.tolist():
        row = _coerce_row_vector(value)
        if row is None:
            return None
        rows.append(row)
    if not rows:
        return None
    return _coerce_pose_or_none(np.vstack(rows))


def _coerce_row_vector(value: Any) -> np.ndarray | None:
    if isinstance(value, dict):
        keys = ("x", "y", "z", "qx", "qy", "qz", "qw")
        lowered = {str(k).lower(): k for k in value}
        if all(k in lowered for k in keys):
            return np.asarray([value[lowered[k]] for k in keys], dtype=np.float64)
        return None
    arr = np.asarray(value)
    if arr.ndim == 0:
        return None
    flat = arr.astype(np.float64, copy=False).reshape(-1)
    if flat.size < 7:
        return None
    return flat[:7]


def _coerce_pose_or_none(values: Any) -> np.ndarray | None:
    try:
        pose, _ = coerce_pose_array(values)
    except Exception:
        return None
    return pose


def _series_or_default(df: pd.DataFrame, names: tuple[str, ...], default: Any) -> np.ndarray:
    found = _optional_series(df, names)
    if found is not None:
        return found
    return np.full(len(df), default, dtype=object)


def _optional_series(df: pd.DataFrame, names: tuple[str, ...]) -> np.ndarray | None:
    lowered = {str(c).lower(): c for c in df.columns}
    for name in names:
        if name in lowered:
            return df[lowered[name]].to_numpy()
    return None


def _load_hdf5_episodes(
    path: Path,
    *,
    dataset_dir: Path,
    dataset_id: str,
    version: str,
    base_meta: dict[str, Any],
) -> list[DatasetBundle]:
    try:
        import h5py
    except ImportError as err:
        raise ImportError(
            "HDF5 raw datasets require h5py. Install robot-data-harness with the "
            "v1.4 requirements before running normalize on robomimic-style assets."
        ) from err

    episodes: list[DatasetBundle] = []
    with h5py.File(path, "r") as handle:
        data = handle.get("data")
        if data is not None:
            for name, group in data.items():
                pose = _extract_pose_from_hdf5_group(group)
                if pose is None:
                    continue
                timestamps = _extract_hdf5_dataset(group, ("timestamps", "timestamp", "time"))
                episodes.append(
                    _make_bundle(
                        dataset_dir=dataset_dir,
                        source_path=path,
                        dataset_id=dataset_id,
                        version=version,
                        episode_id=str(name),
                        pose=pose,
                        timestamps=timestamps,
                        base_meta=base_meta,
                    )
                )
        if not episodes:
            pose = _extract_pose_from_hdf5_group(handle)
            if pose is not None:
                timestamps = _extract_hdf5_dataset(handle, ("timestamps", "timestamp", "time"))
                episodes.append(
                    _make_bundle(
                        dataset_dir=dataset_dir,
                        source_path=path,
                        dataset_id=dataset_id,
                        version=version,
                        episode_id=path.stem,
                        pose=pose,
                        timestamps=timestamps,
                        base_meta=base_meta,
                    )
                )
    return episodes


def _extract_pose_from_hdf5_group(group: Any) -> np.ndarray | None:
    obs = group.get("obs") if hasattr(group, "get") else None
    search_roots = [obs, group] if obs is not None else [group]
    for root in search_roots:
        pos = _find_hdf5_dataset(root, ("eef_pos", "ee_pos", "end_effector_pos", "tcp_pos"))
        quat = _find_hdf5_dataset(root, ("eef_quat", "ee_quat", "end_effector_quat", "tcp_quat"))
        if pos is not None and quat is not None and pos.ndim == 2 and quat.ndim == 2:
            if pos.shape[0] == quat.shape[0] and pos.shape[1] >= 3 and quat.shape[1] >= 4:
                return _coerce_pose_or_none(np.hstack([pos[:, :3], quat[:, :4]]))

    for root in search_roots:
        vector = _find_hdf5_dataset(root, POSE_VECTOR_COLUMN_HINTS)
        if vector is not None and vector.ndim == 2 and vector.shape[1] >= 7:
            return _coerce_pose_or_none(vector[:, :7])
    return None


def _find_hdf5_dataset(group: Any, hints: tuple[str, ...]) -> np.ndarray | None:
    if group is None:
        return None
    found: np.ndarray | None = None

    def visit(name: str, obj: Any) -> None:
        nonlocal found
        if found is not None:
            return
        if not hasattr(obj, "shape"):
            return
        lower = name.lower()
        if any(hint in lower for hint in hints):
            arr = np.asarray(obj)
            if arr.ndim >= 1:
                found = arr

    group.visititems(visit)
    return found


def _extract_hdf5_dataset(group: Any, hints: tuple[str, ...]) -> np.ndarray | None:
    arr = _find_hdf5_dataset(group, hints)
    if arr is None:
        return None
    return np.asarray(arr).reshape(-1)


def _make_bundle(
    *,
    dataset_dir: Path,
    source_path: Path,
    dataset_id: str,
    version: str,
    episode_id: str,
    pose: Any,
    timestamps: Any | None,
    base_meta: dict[str, Any],
) -> DatasetBundle:
    pose_array, warnings = coerce_pose_array(pose)
    n = int(pose_array.shape[0])
    fps = float(base_meta.get("fps") or base_meta.get("video_fps") or 30.0)

    if timestamps is None:
        ts, dt, video_meta = build_timestamps(num_samples=n, duration_sec=None, fps=fps)
    else:
        ts = np.asarray(timestamps, dtype=np.float64).reshape(-1)
        if ts.shape[0] != n:
            raise ValueError(
                f"timestamp length mismatch in {source_path}: pose rows={n}, timestamps={ts.shape[0]}"
            )
        if n > 1:
            diffs = np.diff(ts)
            positive = diffs[diffs > 1.0e-12]
            dt = float(np.median(positive)) if positive.size else 1.0 / fps
            duration_sec = float(ts[-1] - ts[0])
            resolved_fps = 1.0 / dt if dt > 0 else fps
        else:
            dt = 1.0 / fps
            duration_sec = 0.0
            resolved_fps = fps
        video_meta = VideoMetadata(
            fps=float(resolved_fps),
            frame_count=n,
            duration_sec=duration_sec,
            source="timestamp",
        )

    meta = dict(base_meta)
    meta.update(
        {
            "dataset_id": dataset_id,
            "version": version,
            "episode_id": episode_id,
            "source_format": "huggingface",
            "source_file": source_path.relative_to(dataset_dir).as_posix(),
        }
    )
    return DatasetBundle(
        dataset_id=dataset_id,
        dataset_path=dataset_dir,
        endpose_path=source_path,
        pose=pose_array,
        timestamps=ts,
        dt=dt,
        video_meta=video_meta,
        meta=meta,
        video_path=None,
        meta_path=None,
        warnings=warnings,
    )
