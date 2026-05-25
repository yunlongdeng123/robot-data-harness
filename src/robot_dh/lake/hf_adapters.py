"""HuggingFace / OXE 风格数据集的显式 adapter 注册表。

通用启发式（``hf_adapter.py``）覆盖大多数 LeRobot/robomimic 样式数据集；但 OXE 子集
（如 BridgeData V2、其他 Open-X-Embodiment 转换格式）的列布局多样，把它们硬塞进
"任意 7 维向量 = pose" 的启发式会出现 schema 命中失败、或语义错位（把 joint 角度
错当成 quaternion）。这一层做两件事：

1. 维护一个 ``dataset_id -> adapter`` 字典；遇到已注册 dataset 直接用，跳过启发式；
2. 提供一个 ``adapt_via_registry()`` 入口，被 ``hf_adapter.load_huggingface_dataset``
   调用，命中时直接返回 ``list[DatasetBundle]``，未命中返回 None 走 fallback。

新增 adapter 时**只新增本文件**：不要在 ``hf_adapter.py`` 散加 ``if dataset_id == ...``
分支。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from robot_dh.data.dataset import DatasetBundle, VideoMetadata

LOG = logging.getLogger(__name__)


@dataclass(slots=True)
class AdapterContext:
    """传给 adapter 的最小上下文。"""

    dataset_dir: Path
    dataset_id: str
    version: str
    base_meta: dict[str, Any]


AdapterFn = Callable[[AdapterContext], list[DatasetBundle]]

# 按 dataset_id 精确匹配；按前缀匹配走 _PREFIX_REGISTRY。
_EXACT_REGISTRY: dict[str, AdapterFn] = {}
_PREFIX_REGISTRY: list[tuple[str, AdapterFn]] = []


def register_dataset_adapter(name: str, fn: AdapterFn, *, match: str = "exact") -> None:
    """注册一个 adapter。

    Args:
        name: dataset_id 或前缀；按 ``match`` 决定语义。
        fn: 输入 AdapterContext，返回 list[DatasetBundle]。
        match: ``"exact"`` 走全字匹配；``"prefix"`` 走 startswith。
    """
    if match == "exact":
        _EXACT_REGISTRY[name] = fn
    elif match == "prefix":
        _PREFIX_REGISTRY.append((name, fn))
    else:
        raise ValueError(f"unsupported match mode {match}; use 'exact' or 'prefix'")


def adapt_via_registry(ctx: AdapterContext) -> list[DatasetBundle] | None:
    """命中则返回 episode 列表；未命中返回 None，让上层走通用 fallback。"""
    fn = _EXACT_REGISTRY.get(ctx.dataset_id)
    if fn is not None:
        LOG.info("hf_adapter[%s]: using registered exact adapter", ctx.dataset_id)
        return fn(ctx)
    for prefix, fn in _PREFIX_REGISTRY:
        if ctx.dataset_id.startswith(prefix):
            LOG.info(
                "hf_adapter[%s]: matched prefix '%s', using registered adapter",
                ctx.dataset_id, prefix,
            )
            return fn(ctx)
    return None


def list_registered_adapters() -> list[dict[str, str]]:
    """testing / CLI 诊断用。"""
    return (
        [{"name": k, "match": "exact"} for k in sorted(_EXACT_REGISTRY)]
        + [{"name": k, "match": "prefix"} for k, _ in _PREFIX_REGISTRY]
    )


# ----------------------------------------------------------------------------
# 通用工具
# ----------------------------------------------------------------------------


def iter_parquet_shards(dataset_dir: Path) -> Iterable[Path]:
    """递归找 dataset_dir 下所有 parquet shard，跳过 .cache 与 meta 目录。"""
    for path in sorted(dataset_dir.rglob("*.parquet")):
        posix = path.as_posix()
        if "/.cache/" in posix or "/meta/" in posix:
            continue
        yield path


def coerce_row_vector(value: Any, *, dim: int) -> np.ndarray | None:
    """把单行 cell（list / ndarray / dict）转 ``shape=(dim,)`` 的 float64 向量。"""
    if value is None:
        return None
    if isinstance(value, dict):
        # LeRobot 部分老版本 Struct 编码：按键名升序取数值字段
        nums: list[float] = []
        for key in sorted(value):
            v = value[key]
            if isinstance(v, (int, float)):
                nums.append(float(v))
        if len(nums) >= dim:
            return np.asarray(nums[:dim], dtype=np.float64)
        return None
    arr = np.asarray(value)
    if arr.ndim == 0:
        return None
    flat = arr.astype(np.float64, copy=False).reshape(-1)
    if flat.size < dim:
        return None
    return flat[:dim]


def series_to_matrix(series: pd.Series, *, dim: int) -> np.ndarray | None:
    """把整列转为 ``shape=(N, dim)`` 的 ndarray；任意 row 转换失败即返回 None。"""
    rows: list[np.ndarray] = []
    for value in series.tolist():
        row = coerce_row_vector(value, dim=dim)
        if row is None:
            return None
        rows.append(row)
    if not rows:
        return None
    return np.vstack(rows)


def euler_to_quaternion(roll: np.ndarray, pitch: np.ndarray, yaw: np.ndarray) -> np.ndarray:
    """ZYX (yaw-pitch-roll) intrinsic 顺序 -> (qx, qy, qz, qw)。

    BridgeData V2 / WidowX 的 EE 姿态是 ``rpy`` (XYZ-Euler) 系。返回 N×4 数组。
    与 ``robot_dh.validators.quaternion.normalize_quaternions`` 兼容。
    """
    half_r = 0.5 * roll
    half_p = 0.5 * pitch
    half_y = 0.5 * yaw
    cr = np.cos(half_r)
    sr = np.sin(half_r)
    cp = np.cos(half_p)
    sp = np.sin(half_p)
    cy = np.cos(half_y)
    sy = np.sin(half_y)
    qx = sr * cp * cy - cr * sp * sy
    qy = cr * sp * cy + sr * cp * sy
    qz = cr * cp * sy - sr * sp * cy
    qw = cr * cp * cy + sr * sp * sy
    return np.stack([qx, qy, qz, qw], axis=1)


def make_bundle_for_episode(
    *,
    ctx: AdapterContext,
    source_path: Path,
    episode_id: str,
    pose: np.ndarray,
    timestamps: np.ndarray | None,
    fps_hint: float | None = None,
    extra_meta: dict[str, Any] | None = None,
    action: np.ndarray | None = None,
    absolute_action: np.ndarray | None = None,
) -> DatasetBundle:
    """通用 bundle 工厂；只校验 pose 形状，其它字段补默认值。

    ``action`` / ``absolute_action`` 为可选控制信号（bridge / OXE 系会传 grasp 进来），
    约定 shape ``(N, 7) = (x, y, z, roll, pitch, yaw, grasp)``；行数与 ``pose`` 必须一致。
    """
    if pose.ndim != 2 or pose.shape[1] != 7:
        raise ValueError(
            f"adapter returned pose with shape {pose.shape}; expected [N, 7]"
        )
    n = int(pose.shape[0])
    if not np.isfinite(pose).all():
        # 用 0 兜底（quaternion 部分会被 downstream normalize_quaternions 重新单位化）
        pose = np.nan_to_num(pose, nan=0.0, posinf=0.0, neginf=0.0)
        # 兜底后 qw 部分依旧可能为 0，至少给一个 identity quaternion 避免除零
        zero_quat_norms = np.linalg.norm(pose[:, 3:7], axis=1) < 1e-9
        if np.any(zero_quat_norms):
            pose[zero_quat_norms, 3:7] = np.array([0.0, 0.0, 0.0, 1.0])

    fps = float(
        fps_hint
        if fps_hint is not None and fps_hint > 0
        else ctx.base_meta.get("fps") or ctx.base_meta.get("video_fps") or 30.0
    )
    if timestamps is None or len(timestamps) != n:
        ts = np.arange(n, dtype=np.float64) / fps
        dt = 1.0 / fps
        duration_sec = float(ts[-1] - ts[0]) if n > 1 else 0.0
    else:
        ts = np.asarray(timestamps, dtype=np.float64).reshape(-1)
        if n > 1:
            diffs = np.diff(ts)
            positive = diffs[diffs > 1e-12]
            dt = float(np.median(positive)) if positive.size else 1.0 / fps
            duration_sec = float(ts[-1] - ts[0])
            if dt > 0:
                fps = 1.0 / dt
        else:
            dt = 1.0 / fps
            duration_sec = 0.0

    video_meta = VideoMetadata(
        fps=float(fps),
        frame_count=n,
        duration_sec=float(duration_sec),
        source="timestamp",
    )
    meta = dict(ctx.base_meta)
    meta.update(
        {
            "dataset_id": ctx.dataset_id,
            "version": ctx.version,
            "episode_id": episode_id,
            "source_format": "huggingface",
            "source_file": source_path.relative_to(ctx.dataset_dir).as_posix(),
        }
    )
    if extra_meta:
        meta.update(extra_meta)
    action_arr = _coerce_action_array(action, expected_rows=n, label="action")
    absolute_action_arr = _coerce_action_array(
        absolute_action, expected_rows=n, label="absolute_action"
    )

    return DatasetBundle(
        dataset_id=ctx.dataset_id,
        dataset_path=ctx.dataset_dir,
        endpose_path=source_path,
        pose=pose.astype(np.float64, copy=False),
        timestamps=ts,
        dt=float(dt),
        video_meta=video_meta,
        meta=meta,
        video_path=None,
        meta_path=None,
        warnings=[],
        action=action_arr,
        absolute_action=absolute_action_arr,
    )


def _coerce_action_array(
    arr: np.ndarray | None,
    *,
    expected_rows: int,
    label: str,
) -> np.ndarray | None:
    """校验可选控制信号矩阵；行数对不上时给 warning 并丢弃，避免污染下游。"""
    if arr is None:
        return None
    if arr.ndim != 2:
        LOG.warning("%s: expected 2D array, got shape=%s; dropping", label, arr.shape)
        return None
    if int(arr.shape[0]) != expected_rows:
        LOG.warning(
            "%s: row count mismatch (%d vs pose %d); dropping",
            label, int(arr.shape[0]), expected_rows,
        )
        return None
    return arr.astype(np.float64, copy=False)


# ----------------------------------------------------------------------------
# BridgeData V2 adapter
# ----------------------------------------------------------------------------


# BridgeData V2 在 LeRobot v3.0 转换里把 `observation.state` 编为 7 维 EE 状态：
# 通常是 (x, y, z, roll, pitch, yaw, gripper)。早期 HF 镜像把它误标成 joint 角度，
# 实际数值看是 EE Cartesian 系；我们按 EE 系处理：xyz 直接拿，rpy → quaternion。
_BRIDGE_STATE_COL = "observation.state"
_BRIDGE_ACTION_COL = "action"
_BRIDGE_TIMESTAMP_COL = "timestamp"
# v1 LeRobot 用 episode_index / frame_index；mbodiai/oxe_bridge_v2 改用 episode_idx / step_idx。
_BRIDGE_EPISODE_COLS = ("episode_index", "episode_idx", "episode")
_BRIDGE_FRAME_COLS = ("frame_index", "step_idx", "frame_idx", "frame", "index")
# mbodiai/oxe_bridge_v2：`state` 是 struct，包含 end_effector_pose / is_first / is_last / language_embedding。
_BRIDGE_NESTED_STATE_FIELD = "end_effector_pose"
_BRIDGE_NESTED_LANG_FIELD = "language_embedding"
_BRIDGE_NESTED_ACTION_POSE_FIELD = "pose"
_BRIDGE_NESTED_ACTION_GRASP_FIELD = "grasp"
_RPY_FIELDS = ("x", "y", "z", "roll", "pitch", "yaw")


def _bridge_pick_state_column(df: pd.DataFrame) -> str | None:
    """优先 ``observation.state``，其次任何 `state*`/`proprio*` 名字的 7-维列。"""
    lowered = {str(c).lower(): c for c in df.columns}
    if _BRIDGE_STATE_COL in lowered:
        return lowered[_BRIDGE_STATE_COL]
    for key, col in lowered.items():
        if "state" in key or "proprio" in key:
            sample = df[col].dropna().head(1)
            if sample.empty:
                continue
            row = coerce_row_vector(sample.iloc[0], dim=7)
            if row is not None:
                return col
    return None


def _bridge_pick_action_column(df: pd.DataFrame) -> str | None:
    lowered = {str(c).lower(): c for c in df.columns}
    for name in (_BRIDGE_ACTION_COL, "actions", "delta_action"):
        if name in lowered:
            return lowered[name]
    return None


def _bridge_optional_int_series(df: pd.DataFrame, name: str) -> np.ndarray | None:
    lowered = {str(c).lower(): c for c in df.columns}
    if name in lowered:
        return df[lowered[name]].to_numpy()
    return None


def _bridge_pick_first_present_col(df: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    """从一组候选列名里返回第一个存在的（大小写不敏感）。"""
    lowered = {str(c).lower(): c for c in df.columns}
    for name in names:
        if name in lowered:
            return lowered[name]
    return None


def _is_nested_bridge_state_struct(field_type: Any) -> bool:
    """mbodiai/oxe_bridge_v2 的 `state` struct 必含 end_effector_pose<x,y,z,roll,pitch,yaw>。"""
    if not pa.types.is_struct(field_type):
        return False
    try:
        ee_field = field_type.field(_BRIDGE_NESTED_STATE_FIELD)
    except (KeyError, IndexError, ValueError):
        return False
    if not pa.types.is_struct(ee_field.type):
        return False
    ee_names = {ee_field.type.field(i).name for i in range(ee_field.type.num_fields)}
    return all(k in ee_names for k in _RPY_FIELDS)


def _is_nested_bridge_action_struct(field_type: Any) -> bool:
    """mbodiai/oxe_bridge_v2 的 `action` struct = pose<x,y,z,r,p,y> + grasp。"""
    if not pa.types.is_struct(field_type):
        return False
    try:
        pose = field_type.field(_BRIDGE_NESTED_ACTION_POSE_FIELD)
    except (KeyError, IndexError, ValueError):
        return False
    if not pa.types.is_struct(pose.type):
        return False
    pose_names = {pose.type.field(i).name for i in range(pose.type.num_fields)}
    return all(k in pose_names for k in _RPY_FIELDS)


def _flatten_pose_struct(struct_array: pa.Array) -> np.ndarray:
    """struct<x,y,z,roll,pitch,yaw> -> ndarray shape=(N, 6) float64。"""
    chunks = struct_array.combine_chunks() if isinstance(struct_array, pa.ChunkedArray) else struct_array
    cols = [np.asarray(chunks.field(name).to_numpy(zero_copy_only=False), dtype=np.float64) for name in _RPY_FIELDS]
    return np.stack(cols, axis=1)


def _flatten_pose_grasp_struct(column: pa.Array) -> np.ndarray | None:
    """`action` / `absolute_action`: struct<pose: struct<...>, grasp> -> (N, 7) ndarray。

    grasp 字段保留在第 7 列；没有 grasp 子字段或 schema 不匹配时返回 None，
    让上层决定是否丢弃这条控制信号。
    """
    if not isinstance(column, (pa.Array, pa.ChunkedArray)):
        return None
    if not _is_nested_bridge_action_struct(column.type):
        return None
    arr = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    pose = arr.field(_BRIDGE_NESTED_ACTION_POSE_FIELD)
    xyz_rpy = _flatten_pose_struct(pose)
    try:
        grasp = np.asarray(
            arr.field(_BRIDGE_NESTED_ACTION_GRASP_FIELD).to_numpy(zero_copy_only=False),
            dtype=np.float64,
        )
    except (KeyError, IndexError, ValueError):
        return None
    return np.hstack([xyz_rpy, grasp.reshape(-1, 1)])


def _try_adapt_bridge_nested(
    ctx: AdapterContext,
    shard: Path,
    table: pa.Table,
) -> list[DatasetBundle] | None:
    """mbodiai/oxe_bridge_v2 嵌套 schema 走 pyarrow struct 直读路径。

    不命中时返回 None，让上层回退到 pandas + heuristics 的旧路径，保持向后兼容。
    """
    names = set(table.schema.names)
    if "state" not in names:
        return None
    state_type = table.schema.field("state").type
    if not _is_nested_bridge_state_struct(state_type):
        return None

    # 拿到 6-dim (x,y,z,roll,pitch,yaw)
    state_col = table.column("state").combine_chunks()
    ee_struct = state_col.field(_BRIDGE_NESTED_STATE_FIELD)
    xyz_rpy = _flatten_pose_struct(ee_struct)
    pose7 = np.hstack(
        [
            xyz_rpy[:, :3],
            euler_to_quaternion(xyz_rpy[:, 3], xyz_rpy[:, 4], xyz_rpy[:, 5]),
        ]
    )

    # episode / step / timestamp 列名按 mbodiai 习惯优先 episode_idx / step_idx。
    episode_col = _first_col_in_schema(names, _BRIDGE_EPISODE_COLS)
    step_col = _first_col_in_schema(names, _BRIDGE_FRAME_COLS)
    ts_col = "timestamp" if "timestamp" in names else None

    episodes_arr: np.ndarray | None = None
    if episode_col is not None:
        episodes_arr = np.asarray(table.column(episode_col).to_pylist())
    steps_arr: np.ndarray | None = None
    if step_col is not None:
        steps_arr = np.asarray(table.column(step_col).to_pylist())
    ts_arr: np.ndarray | None = None
    if ts_col is not None:
        ts_arr = np.asarray(table.column(ts_col).to_pylist(), dtype=np.float64)

    # language_embedding：list<double>。空 list 视为 missing，写到 meta 用作 v1.6.5 QC 穿透。
    lang_lists: list[list[float]] | None = None
    state_struct_type = state_type
    state_field_names = {state_struct_type.field(i).name for i in range(state_struct_type.num_fields)}
    if _BRIDGE_NESTED_LANG_FIELD in state_field_names:
        lang_lists = state_col.field(_BRIDGE_NESTED_LANG_FIELD).to_pylist()

    task_col_values: list[str] | None = None
    if "observation" in names:
        obs_type = table.schema.field("observation").type
        if pa.types.is_struct(obs_type):
            obs_field_names = {obs_type.field(i).name for i in range(obs_type.num_fields)}
            if "task" in obs_field_names:
                task_col_values = table.column("observation").combine_chunks().field("task").to_pylist()

    # 控制信号：action.pose + action.grasp -> (N, 7) (x, y, z, roll, pitch, yaw, grasp)；
    # absolute_action 同形状。Pose 走 quaternion 在 state 已经做完，这两路保留 rpy 原值，
    # 避免对控制量做 lossy 旋转表示转换。
    action_full: np.ndarray | None = None
    absolute_action_full: np.ndarray | None = None
    if "action" in names:
        action_full = _flatten_pose_grasp_struct(table.column("action"))
    if "absolute_action" in names:
        absolute_action_full = _flatten_pose_grasp_struct(table.column("absolute_action"))

    return _emit_bridge_nested_episodes(
        ctx=ctx,
        shard=shard,
        pose7=pose7,
        episodes_arr=episodes_arr,
        steps_arr=steps_arr,
        ts_arr=ts_arr,
        lang_lists=lang_lists,
        task_values=task_col_values,
        action_full=action_full,
        absolute_action_full=absolute_action_full,
    )


def _first_col_in_schema(names: set[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {n.lower(): n for n in names}
    for cand in candidates:
        if cand in lowered:
            return lowered[cand]
    return None


def _emit_bridge_nested_episodes(
    *,
    ctx: AdapterContext,
    shard: Path,
    pose7: np.ndarray,
    episodes_arr: np.ndarray | None,
    steps_arr: np.ndarray | None,
    ts_arr: np.ndarray | None,
    lang_lists: list[list[float]] | None,
    task_values: list[str] | None,
    action_full: np.ndarray | None,
    absolute_action_full: np.ndarray | None,
) -> list[DatasetBundle]:
    n = int(pose7.shape[0])
    if episodes_arr is None:
        ep_values: np.ndarray = np.zeros(n, dtype=np.int64)
        ep_id_strs = [shard.stem]
    else:
        ep_values = episodes_arr
        ep_id_strs = None  # 走下面 group 处理

    out: list[DatasetBundle] = []
    unique_eps = pd.unique(ep_values)
    for ep_value in unique_eps:
        mask = ep_values == ep_value
        idx = np.flatnonzero(mask)
        if idx.size == 0:
            continue
        if steps_arr is not None:
            order = idx[np.argsort(steps_arr[idx], kind="stable")]
        else:
            order = idx
        ts_sub: np.ndarray | None = None if ts_arr is None else ts_arr[order]
        meta_extra: dict[str, Any] = {"bridge_episode_index": int(ep_value)}
        if lang_lists is not None:
            # 取该 episode 内第一条非空 language_embedding，写进 meta 备查
            for j in order.tolist():
                emb = lang_lists[j]
                if emb:
                    meta_extra["language_embedding_dim"] = len(emb)
                    break
            empty = sum(1 for j in order.tolist() if not lang_lists[j])
            meta_extra["language_missing_rate"] = float(empty) / float(order.size)
        if task_values is not None:
            tasks = [task_values[j] for j in order.tolist() if task_values[j]]
            if tasks:
                meta_extra["task"] = tasks[0]
        # 控制信号沿 episode 切片，保持与 pose 行序一致
        action_sub = action_full[order] if action_full is not None else None
        absolute_action_sub = (
            absolute_action_full[order] if absolute_action_full is not None else None
        )
        if action_sub is not None or absolute_action_sub is not None:
            meta_extra["action_layout"] = "x_y_z_roll_pitch_yaw_grasp"
        out.append(
            make_bundle_for_episode(
                ctx=ctx,
                source_path=shard,
                episode_id=(
                    f"{ctx.dataset_id}_ep{int(ep_value):05d}"
                    if ep_id_strs is None
                    else ep_id_strs[0]
                ),
                pose=pose7[order],
                timestamps=ts_sub,
                fps_hint=_bridge_infer_fps(ts_sub),
                extra_meta=meta_extra,
                action=action_sub,
                absolute_action=absolute_action_sub,
            )
        )
    return out


def adapt_bridgedata_v2(ctx: AdapterContext) -> list[DatasetBundle]:
    """BridgeData V2 (LeRobot v3 转换 + OXE 风格) -> DatasetBundle 列表。

    策略：
    1. 优先用 ``observation.state`` 的 (x, y, z, roll, pitch, yaw, gripper) 7 维，
       (roll, pitch, yaw) 走 ZYX Euler -> quaternion；gripper 保留在 meta 不进 pose。
    2. 没有 state 时回退到 ``action`` 的 (dx, dy, dz, droll, dpitch, dyaw, gripper)
       前 6 维做累积积分，得到伪 absolute pose；这条路径 features step 可用，但
       不要拿来训 IK / 控制。日志会打 warning。
    3. ``episode_index`` 作为天然分组；缺失时一个 shard 视为一个 episode。
    """
    shards = list(iter_parquet_shards(ctx.dataset_dir))
    if not shards:
        return []

    episodes: list[DatasetBundle] = []
    for shard in shards:
        try:
            table = pq.read_table(shard)
        except Exception as err:  # noqa: BLE001
            LOG.warning("bridgedata_v2 adapter: unreadable shard %s: %s", shard, err)
            continue
        if table.num_rows == 0:
            continue
        # 优先走 mbodiai/oxe_bridge_v2 嵌套 schema 直读；命中即跳过 pandas 转换。
        nested = _try_adapt_bridge_nested(ctx, shard, table)
        if nested is not None:
            LOG.info(
                "bridgedata_v2 adapter[%s]: matched nested oxe_bridge_v2 schema "
                "(state.end_effector_pose), %d episodes",
                ctx.dataset_id, len(nested),
            )
            episodes.extend(nested)
            continue
        df = table.to_pandas()
        if df.empty:
            continue
        episodes.extend(_adapt_bridge_shard(ctx, shard, df))

    if not episodes:
        raise ValueError(
            f"bridgedata_v2 adapter could not extract any pose episode from "
            f"{ctx.dataset_dir}. Found {len(shards)} parquet shards. "
            "Schema may be unknown; sample columns: "
            f"{sorted(set().union(*([set(map(str, pq.read_schema(s).names)) for s in shards[:1]])))}."
        )
    return episodes


def _adapt_bridge_shard(
    ctx: AdapterContext,
    shard: Path,
    df: pd.DataFrame,
) -> list[DatasetBundle]:
    state_col = _bridge_pick_state_column(df)
    state_matrix: np.ndarray | None = None
    if state_col is not None:
        state_matrix = series_to_matrix(df[state_col], dim=7)
    if state_matrix is None:
        action_col = _bridge_pick_action_column(df)
        if action_col is None:
            LOG.warning(
                "bridgedata_v2 adapter: no usable state / action column in %s (cols=%s)",
                shard, list(df.columns)[:20],
            )
            return []
        action_matrix = series_to_matrix(df[action_col], dim=7)
        if action_matrix is None:
            LOG.warning(
                "bridgedata_v2 adapter: action column %s in %s not coercible to 7-dim",
                action_col, shard,
            )
            return []
        LOG.warning(
            "bridgedata_v2 adapter: %s missing observation.state, "
            "integrating %s as pseudo pose (do not use for IK / control)",
            shard, action_col,
        )
        state_matrix = _integrate_action_to_pose(action_matrix)

    pose = _bridge_state_to_pose(state_matrix)
    ep_col_name = _bridge_pick_first_present_col(df, _BRIDGE_EPISODE_COLS)
    frame_col_name = _bridge_pick_first_present_col(df, _BRIDGE_FRAME_COLS)
    episodes = df[ep_col_name].to_numpy() if ep_col_name else None
    frames = df[frame_col_name].to_numpy() if frame_col_name else None
    timestamps = _bridge_optional_int_series(df, _BRIDGE_TIMESTAMP_COL)

    if episodes is None:
        # 单 shard 视为一个 episode，episode_id 用 shard 文件名（去后缀）。
        ep_id = shard.stem
        if frames is None:
            frames = np.arange(len(df), dtype=np.int64)
        order = np.argsort(frames, kind="stable")
        ts = None if timestamps is None else np.asarray(timestamps, dtype=np.float64)[order]
        return [
            make_bundle_for_episode(
                ctx=ctx,
                source_path=shard,
                episode_id=ep_id,
                pose=pose[order],
                timestamps=ts,
                fps_hint=_bridge_infer_fps(timestamps),
            )
        ]

    episodes_arr = np.asarray(episodes)
    out: list[DatasetBundle] = []
    for ep_value in pd.unique(episodes_arr):
        mask = episodes_arr == ep_value
        idx = np.flatnonzero(mask)
        if idx.size == 0:
            continue
        order = (
            idx[np.argsort(frames[idx], kind="stable")]
            if frames is not None
            else idx
        )
        ts = None if timestamps is None else np.asarray(timestamps, dtype=np.float64)[order]
        out.append(
            make_bundle_for_episode(
                ctx=ctx,
                source_path=shard,
                episode_id=f"ep_{int(ep_value):06d}",
                pose=pose[order],
                timestamps=ts,
                fps_hint=_bridge_infer_fps(timestamps),
                extra_meta={"bridge_episode_index": int(ep_value)},
            )
        )
    return out


def _bridge_state_to_pose(state: np.ndarray) -> np.ndarray:
    """(x, y, z, roll, pitch, yaw, gripper) -> (x, y, z, qx, qy, qz, qw)。"""
    if state.ndim != 2 or state.shape[1] < 6:
        raise ValueError(f"bridge state must be (N, >=6); got {state.shape}")
    xyz = state[:, :3].astype(np.float64, copy=False)
    if state.shape[1] >= 7:
        rpy = state[:, 3:6].astype(np.float64, copy=False)
    else:
        rpy = state[:, 3:6].astype(np.float64, copy=False)
    quat = euler_to_quaternion(rpy[:, 0], rpy[:, 1], rpy[:, 2])
    return np.hstack([xyz, quat])


def _integrate_action_to_pose(action: np.ndarray) -> np.ndarray:
    """积分 delta-EE action（前 6 维）得到伪 absolute (x, y, z, roll, pitch, yaw)。"""
    deltas = action[:, :6].astype(np.float64, copy=False)
    cumulative = np.cumsum(deltas, axis=0)
    # 拼接最后一列为零，保持下游 (N, 7) 形状（gripper 槽位填 0）
    gripper = np.zeros((cumulative.shape[0], 1), dtype=np.float64)
    return np.hstack([cumulative, gripper])


def _bridge_infer_fps(timestamps: np.ndarray | None) -> float | None:
    if timestamps is None or len(timestamps) < 2:
        return None
    ts = np.asarray(timestamps, dtype=np.float64).reshape(-1)
    diffs = np.diff(np.sort(ts))
    positive = diffs[diffs > 1e-12]
    if not positive.size:
        return None
    median_dt = float(np.median(positive))
    return 1.0 / median_dt if median_dt > 0 else None


# 注册：精确 + 前缀，覆盖 bridgedata_v2 / bridgedata_v2_scale30 / bridgedata_v2_full 等。
register_dataset_adapter("bridgedata_v2", adapt_bridgedata_v2, match="exact")
register_dataset_adapter("bridgedata_v2", adapt_bridgedata_v2, match="prefix")


# ----------------------------------------------------------------------------
# DROID / LeRobot v2 adapter
# ----------------------------------------------------------------------------
#
# v1.6.8（fvx5z §6.2.4）：droid_lerobot_scale30 不能再走通用 fallback——通用路径会把
# 8 维 ``observation.state``（joint 角度 7 + gripper 1）的前 7 维当成 (x, y, z, qx, qy,
# qz, qw)，下游 normalize_quaternions 强行重单位化得到完全错乱的姿态。
#
# 列嗅探优先级（高→低，越靠前语义越准确）：
#   1. ``observation.cartesian_position`` (6 维 xyz + rpy)            → euler→quat → (N, 7)
#   2. ``observation.cartesian_pose`` / ``observation.tcp_pose``      → 7 维 (xyz + quat) 直读
#      / ``observation.ee_pose``
#   3. ``x``/``y``/``z``/``qx``/``qy``/``qz``/``qw`` 扁平 7 列（少见） → 直接组装
#   4. fallback：``observation.state`` 前 7 维 → warning 标 "joint angles, do not train IK"
#   5. 都不行 → raise ValueError，让 normalize 阶段 fail-fast，不要静默错位
#
# action 列：
#   - 7 维 list[double]：原值写到 ``bundle.action``（layout = "x_y_z_roll_pitch_yaw_grasp"）；
#   - 8 维：取前 7 维 + warning；
#   - struct：拍平到 list[double] 再走上述路径；
#   - 其它形状：保留 None，下游 features / training 自行决定是否消费。
#
# 不打开任何 mp4：``video_path=None``、``video_meta`` 走 info.json 的 fps + 行数；
# normalize 阶段 ``_materialize_input`` 已经 ``exclude_prefixes=("videos/",)``，本 adapter
# 也不再尝试找视频文件。
#
# 多 shard：droid_lerobot_scale30 是 156 个 ``data/chunk-***/file-***.parquet`` shard，
# 同一 episode 的 frame 可能跨 shard。本 adapter 会先把所有 shard 的 (episode_idx, frame,
# pose, action) 拍到同一张内存表，再按 episode_index group + frame_index sort，避免
# split-by-shard 把一个 episode 切成几段。

_DROID_EPISODE_COLS = ("episode_index", "episode_idx", "episode")
_DROID_FRAME_COLS = ("frame_index", "frame_idx", "frame", "step_idx", "index")
_DROID_TIMESTAMP_COLS = ("timestamp", "timestamp_sec", "time", "time_sec")
_DROID_CARTESIAN_POS_COL = "observation.cartesian_position"
_DROID_CARTESIAN_POSE_COLS = (
    "observation.cartesian_pose",
    "observation.tcp_pose",
    "observation.ee_pose",
    "observation.endpose",
)
_DROID_FLAT_POSE_GROUP = ("x", "y", "z", "qx", "qy", "qz", "qw")
_DROID_STATE_COL = "observation.state"
_DROID_ACTION_COLS = ("action", "actions")

# v1.6.8 注释：joint→cartesian forward kinematics 不在 normalize 阶段做。如果数据集只有
# joint state，pose 字段是 placeholder（非 EE 真值），下游训练前必须自己跑 FK 或
# 改用 cartesian dataset。Bundle.warnings 会带这条提示。
_DROID_FALLBACK_WARNING = (
    "droid_lerobot_v2 adapter: pose comes from joint-angle observation.state[:7]; "
    "this is NOT a Cartesian end-effector pose. Do not use for IK / control without "
    "running forward kinematics in a downstream stage."
)


def _droid_first_col(names: set[str], candidates: tuple[str, ...]) -> str | None:
    """大小写敏感版的优先列名查找；LeRobot v2 列名按 dotted path，统一原样匹配。"""
    for cand in candidates:
        if cand in names:
            return cand
    return None


def _droid_extract_vector_column(column: pa.ChunkedArray | pa.Array, *, dim: int) -> np.ndarray | None:
    """list<double>[d] / fixed_size_list / Struct → ``shape=(N, dim)`` ndarray；不匹配返 None。

    LeRobot v2 因为 codebase_version 不同，这同一逻辑列在不同 shard 里可能编成
    list<float64>、fixed_size_list<float64, dim>，甚至 struct<axis_0..axis_{dim-1}>。
    全部统一拍成 (N, dim)；shape 兜底失败返回 None 让上层 fallback。
    """
    if isinstance(column, pa.ChunkedArray):
        column = column.combine_chunks()
    arr_type = column.type
    if pa.types.is_struct(arr_type):
        # struct<axis_0..axis_{dim-1}> 路径
        try:
            field_names = [arr_type.field(i).name for i in range(arr_type.num_fields)]
        except (KeyError, IndexError, ValueError):
            return None
        if len(field_names) < dim:
            return None
        cols = []
        for i in range(dim):
            try:
                vec = column.field(field_names[i]).to_numpy(zero_copy_only=False)
            except (KeyError, IndexError, ValueError):
                return None
            cols.append(np.asarray(vec, dtype=np.float64))
        return np.stack(cols, axis=1)
    # list<double> / fixed_size_list<double, dim>：to_pylist 后逐行 coerce
    try:
        py = column.to_pylist()
    except Exception:  # noqa: BLE001
        return None
    rows: list[np.ndarray] = []
    for value in py:
        if value is None:
            return None
        flat = np.asarray(value, dtype=np.float64).reshape(-1)
        if flat.size < dim:
            return None
        rows.append(flat[:dim])
    if not rows:
        return None
    return np.vstack(rows)


def _droid_pose_from_table(table: pa.Table) -> tuple[np.ndarray, str, list[str]] | None:
    """按优先级返回 ``(pose7, source_label, warnings)``；都不命中返回 None。

    source_label 写到 ``bundle.meta["pose_source"]``，QC / lineage 可据此区分语义。
    """
    names = set(table.schema.names)

    # 优先级 1：cartesian_position (xyz + rpy)
    if _DROID_CARTESIAN_POS_COL in names:
        xyzrpy = _droid_extract_vector_column(
            table.column(_DROID_CARTESIAN_POS_COL), dim=6
        )
        if xyzrpy is not None:
            quat = euler_to_quaternion(xyzrpy[:, 3], xyzrpy[:, 4], xyzrpy[:, 5])
            pose7 = np.hstack([xyzrpy[:, :3], quat])
            return pose7, "observation.cartesian_position(rpy->quat)", []

    # 优先级 2：cartesian_pose / tcp_pose / ee_pose (xyz + quat)
    pose_col = _droid_first_col(names, _DROID_CARTESIAN_POSE_COLS)
    if pose_col is not None:
        pose7 = _droid_extract_vector_column(table.column(pose_col), dim=7)
        if pose7 is not None:
            return pose7, pose_col, []

    # 优先级 3：扁平 7 列 x/y/z/qx/qy/qz/qw（部分 lerobot v3 转换）
    if all(c in names for c in _DROID_FLAT_POSE_GROUP):
        cols = [
            np.asarray(
                table.column(c).to_numpy(zero_copy_only=False), dtype=np.float64
            )
            for c in _DROID_FLAT_POSE_GROUP
        ]
        return np.stack(cols, axis=1), "flat:x_y_z_qxqyqzqw", []

    # 优先级 4：observation.state fallback。joint angles 当 (xyz, quat) 是错的，
    # 但保留路径 + warning，避免数据集只有 joint 时 normalize 完全无法启动；
    # 真正的修复是让 raw 同时带 cartesian_position（数据采集端）或 ml-ready 阶段做 FK。
    if _DROID_STATE_COL in names:
        state = _droid_extract_vector_column(table.column(_DROID_STATE_COL), dim=7)
        if state is not None:
            return state, "observation.state[:7]", [_DROID_FALLBACK_WARNING]

    return None


def _droid_extract_action(table: pa.Table) -> tuple[np.ndarray, str] | None:
    """``action`` / ``actions`` → (N, 7)；找不到 / 形状不符返回 None。"""
    names = set(table.schema.names)
    col = _droid_first_col(names, _DROID_ACTION_COLS)
    if col is None:
        return None
    column = table.column(col)
    arr7 = _droid_extract_vector_column(column, dim=7)
    if arr7 is not None:
        return arr7, col
    # 8 维兜底（droid 部分 codebase 的 action 是 cartesian_velocity(6) + gripper(1) + pad(1)）
    arr8 = _droid_extract_vector_column(column, dim=8)
    if arr8 is not None:
        LOG.warning(
            "droid_lerobot_v2 adapter: action column %s has dim>=8, taking first 7 "
            "(layout assumed: cartesian_velocity(6) + gripper(1))",
            col,
        )
        return arr8[:, :7], col
    return None


def _droid_pick_int_column(table: pa.Table, candidates: tuple[str, ...]) -> str | None:
    names = set(table.schema.names)
    return _droid_first_col(names, candidates)


def _droid_pick_float_column(table: pa.Table, candidates: tuple[str, ...]) -> str | None:
    names = set(table.schema.names)
    return _droid_first_col(names, candidates)


def _droid_load_shard(shard: Path) -> pa.Table | None:
    try:
        return pq.read_table(shard)
    except Exception as err:  # noqa: BLE001
        LOG.warning("droid_lerobot_v2 adapter: unreadable shard %s: %s", shard, err)
        return None


def _droid_collect_shard_records(
    shard: Path, table: pa.Table
) -> tuple[
    np.ndarray,  # episodes
    np.ndarray,  # frames
    np.ndarray,  # timestamps (NaN if absent)
    np.ndarray,  # pose7
    np.ndarray | None,  # action7
    str,  # pose_source label
    list[str],  # warnings
] | None:
    """把单个 shard 拍成"列形式"的小段记录；缺关键列返 None。

    返回结构按 row 对齐，调用方负责跨 shard concat 后再 group by episode。
    """
    if table.num_rows == 0:
        return None
    pose_result = _droid_pose_from_table(table)
    if pose_result is None:
        LOG.warning(
            "droid_lerobot_v2 adapter: shard %s has no recognizable pose column "
            "(checked: cartesian_position / cartesian_pose / tcp_pose / ee_pose / "
            "x_y_z_qxqyqzqw / observation.state); skipping",
            shard,
        )
        return None
    pose7, pose_source, warnings = pose_result
    n = int(pose7.shape[0])

    ep_col = _droid_pick_int_column(table, _DROID_EPISODE_COLS)
    if ep_col is None:
        # 没有 episode_index 列：把整个 shard 当一个 episode（episode_id 用 shard.stem）
        episodes = np.zeros(n, dtype=np.int64)
    else:
        try:
            episodes = np.asarray(
                table.column(ep_col).to_numpy(zero_copy_only=False)
            ).astype(np.int64, copy=False)
        except Exception:  # noqa: BLE001
            episodes = np.asarray(table.column(ep_col).to_pylist())

    fr_col = _droid_pick_int_column(table, _DROID_FRAME_COLS)
    if fr_col is None:
        frames = np.arange(n, dtype=np.int64)
    else:
        try:
            frames = np.asarray(
                table.column(fr_col).to_numpy(zero_copy_only=False)
            ).astype(np.int64, copy=False)
        except Exception:  # noqa: BLE001
            frames = np.asarray(table.column(fr_col).to_pylist())

    ts_col = _droid_pick_float_column(table, _DROID_TIMESTAMP_COLS)
    if ts_col is None:
        timestamps = np.full(n, np.nan, dtype=np.float64)
    else:
        try:
            timestamps = np.asarray(
                table.column(ts_col).to_numpy(zero_copy_only=False), dtype=np.float64
            )
        except Exception:  # noqa: BLE001
            timestamps = np.asarray(table.column(ts_col).to_pylist(), dtype=np.float64)

    action_result = _droid_extract_action(table)
    action7 = action_result[0] if action_result is not None else None

    return episodes, frames, timestamps, pose7, action7, pose_source, warnings


def _droid_concat_shard_records(
    records: list[
        tuple[
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray | None,
            str,
            list[str],
        ]
    ],
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray | None,
    str,
    list[str],
]:
    eps = np.concatenate([r[0] for r in records])
    frs = np.concatenate([r[1] for r in records])
    tss = np.concatenate([r[2] for r in records])
    pose = np.vstack([r[3] for r in records])

    actions: np.ndarray | None
    if all(r[4] is not None for r in records):
        actions = np.vstack([r[4] for r in records])  # type: ignore[arg-type]
    else:
        actions = None

    sources = sorted({r[5] for r in records})
    pose_source = sources[0] if len(sources) == 1 else ",".join(sources)
    warnings: list[str] = []
    seen: set[str] = set()
    for r in records:
        for w in r[6]:
            if w not in seen:
                warnings.append(w)
                seen.add(w)
    return eps, frs, tss, pose, actions, pose_source, warnings


def _droid_emit_episodes(
    *,
    ctx: AdapterContext,
    representative_shard: Path,
    episodes_arr: np.ndarray,
    frames_arr: np.ndarray,
    timestamps_arr: np.ndarray,
    pose7: np.ndarray,
    action7: np.ndarray | None,
    pose_source: str,
    warnings: list[str],
    fps_hint: float | None,
) -> list[DatasetBundle]:
    out: list[DatasetBundle] = []
    unique_eps = pd.unique(episodes_arr)
    for ep_value in unique_eps:
        mask = episodes_arr == ep_value
        idx = np.flatnonzero(mask)
        if idx.size == 0:
            continue
        order = idx[np.argsort(frames_arr[idx], kind="stable")]
        ep_pose = pose7[order]
        # timestamp NaN 整列时降级为 None，由 make_bundle_for_episode 用 fps 重建
        ep_ts_raw = timestamps_arr[order]
        ts_for_bundle: np.ndarray | None
        if np.all(np.isnan(ep_ts_raw)):
            ts_for_bundle = None
        else:
            ts_for_bundle = np.where(np.isnan(ep_ts_raw), 0.0, ep_ts_raw)
        ep_action = action7[order] if action7 is not None else None
        meta_extra: dict[str, Any] = {
            "droid_episode_index": int(ep_value),
            "pose_source": pose_source,
        }
        if ep_action is not None:
            meta_extra["action_layout"] = "x_y_z_roll_pitch_yaw_grasp"
        bundle = make_bundle_for_episode(
            ctx=ctx,
            source_path=representative_shard,
            episode_id=f"{ctx.dataset_id}_ep{int(ep_value):06d}",
            pose=ep_pose,
            timestamps=ts_for_bundle,
            fps_hint=fps_hint,
            extra_meta=meta_extra,
            action=ep_action,
        )
        if warnings:
            bundle.warnings.extend(warnings)
        out.append(bundle)
    return out


def adapt_droid_lerobot_v2(ctx: AdapterContext) -> list[DatasetBundle]:
    """DROID / LeRobot v2 数据集 → DatasetBundle 列表。

    入参 ``ctx.dataset_dir`` 可能是：

    1. 整个 raw root（含 ``meta/info.json`` + ``data/chunk-***/file-***.parquet``）；
    2. partition planner 切出来的子集（只有几个 ``file-***.parquet``）；

    两种情况都按 "递归找 parquet shard、跨 shard concat、group by episode_index" 处理。
    视频 (``videos/...``) 在本阶段**不打开也不计数**（normalize 已经 exclude videos/），
    要 video metadata 请走 ml-ready 阶段。
    """
    shards = list(iter_parquet_shards(ctx.dataset_dir))
    if not shards:
        return []

    fps_hint = _droid_fps_from_meta(ctx.base_meta)

    records: list[
        tuple[
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray,
            np.ndarray | None,
            str,
            list[str],
        ]
    ] = []
    for shard in shards:
        table = _droid_load_shard(shard)
        if table is None:
            continue
        rec = _droid_collect_shard_records(shard, table)
        if rec is not None:
            records.append(rec)

    if not records:
        sample_schemas: list[list[str]] = []
        for shard in shards[:1]:
            try:
                sample_schemas.append(list(map(str, pq.read_schema(shard).names)))
            except Exception:  # noqa: BLE001
                pass
        raise ValueError(
            f"droid_lerobot_v2 adapter could not extract pose from any of "
            f"{len(shards)} parquet shards under {ctx.dataset_dir}. "
            f"Expected one of: observation.cartesian_position (6-dim xyz+rpy), "
            f"observation.cartesian_pose / tcp_pose / ee_pose (7-dim xyz+quat), "
            f"flat x/y/z/qx/qy/qz/qw, or observation.state (>=7-dim, joint-angle "
            f"fallback). Sample schema: {sample_schemas}."
        )

    eps, frs, tss, pose7, action7, pose_source, warnings = (
        _droid_concat_shard_records(records)
    )
    LOG.info(
        "droid_lerobot_v2 adapter[%s]: %d shards, %d total rows, "
        "pose_source=%s, action_present=%s",
        ctx.dataset_id,
        len(records),
        int(pose7.shape[0]),
        pose_source,
        action7 is not None,
    )
    return _droid_emit_episodes(
        ctx=ctx,
        representative_shard=shards[0],
        episodes_arr=eps,
        frames_arr=frs,
        timestamps_arr=tss,
        pose7=pose7,
        action7=action7,
        pose_source=pose_source,
        warnings=warnings,
        fps_hint=fps_hint,
    )


def _droid_fps_from_meta(base_meta: dict[str, Any]) -> float | None:
    """``meta/info.json`` 在 hf_adapter 已经合并到 base_meta；优先 ``fps``，回退 ``video_fps``。"""
    for key in ("fps", "video_fps"):
        v = base_meta.get(key)
        if isinstance(v, (int, float)) and float(v) > 0:
            return float(v)
    return None


# 注册：精确 + 前缀，覆盖 droid_lerobot_scale30 / droid_lerobot_full / droid_lerobot_v2 等。
register_dataset_adapter("droid_lerobot_scale30", adapt_droid_lerobot_v2, match="exact")
register_dataset_adapter("droid_lerobot", adapt_droid_lerobot_v2, match="prefix")
register_dataset_adapter("lerobot/droid", adapt_droid_lerobot_v2, match="prefix")
