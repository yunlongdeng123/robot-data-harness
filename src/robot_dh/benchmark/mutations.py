"""mutation：对本地 demo dataset 进行注入，产生预期会失败的副本。

支持：
  velocity_spike            - 在中段插入若干超大速度脉冲。
  quat_drift                - 把后半段四元数归一化破坏。
  missing_press             - 抹平所有 z 方向下凹，使 PressEvent 检测不到按压。
  xy_cluster_collapse       - 将 xy 平面所有点压缩到一个聚类中心。
  timestamp_jitter          - 给 timestamps（间接通过 fps 调整）注入抖动信号，体现在 meta + video。
  schema_missing_column     - 删除/截断 endpose.pt 使列数 != 7。
  nan_injection             - 在 pose 中插入 NaN。
  video_missing             - 删除 video.mp4。

实现要点：
  - 不修改输入；输出到一个新目录。
  - meta.yaml 中标注 mutation_type。
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Callable

import numpy as np
import torch
import yaml

LOG = logging.getLogger(__name__)


class MutationError(RuntimeError):
    """mutation 不能在该 dataset 上下文中执行。"""


def _load_pose_tensor(path: Path) -> tuple[torch.Tensor, np.ndarray]:
    payload = torch.load(path, map_location="cpu", weights_only=False)
    if isinstance(payload, torch.Tensor):
        tensor = payload
    elif isinstance(payload, dict):
        # 简单回退：取第一个 Tensor
        tensor = next((v for v in payload.values() if isinstance(v, torch.Tensor)), None)
        if tensor is None:
            raise MutationError(f"endpose.pt missing tensor in {path}")
    else:
        raise MutationError(f"unsupported endpose payload type {type(payload)} at {path}")
    arr = tensor.detach().cpu().numpy()
    return tensor, arr


def _save_pose(out_path: Path, arr: np.ndarray) -> None:
    tensor = torch.tensor(arr, dtype=torch.float32)
    torch.save(tensor, out_path)


def _copy_tree(src: Path, dst: Path) -> None:
    dst = dst.expanduser().resolve()
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _update_meta(meta_path: Path, mutation_type: str, extra: dict | None = None) -> None:
    raw: dict = {}
    if meta_path.exists():
        try:
            raw = yaml.safe_load(meta_path.read_text()) or {}
        except Exception:
            raw = {}
    raw["mutation_type"] = mutation_type
    if extra:
        raw.setdefault("mutation_meta", {}).update(extra)
    with meta_path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(raw, fh, sort_keys=False, allow_unicode=True)


# 具体 mutation 实现 ---------------------------------------------------------


def _velocity_spike(pose: np.ndarray) -> np.ndarray:
    n = pose.shape[0]
    out = pose.copy()
    if n < 6:
        # 数据太短：在最后一帧之前移动 1 米
        if n >= 2:
            out[-1, 0] = out[-2, 0] + 5.0
            out[-1, 1] = out[-2, 1] + 5.0
        return out
    # 在中段加一个 0.5m 大跳，明显超过常用 2.5 m/s 阈值
    mid = n // 2
    out[mid, 0] = out[mid - 1, 0] + 5.0
    out[mid, 1] = out[mid - 1, 1] + 5.0
    out[mid, 2] = out[mid - 1, 2] + 2.0
    # 第二次跳保证 max_velocity 显著
    if n >= 12:
        q = n // 4
        out[q, 0] = out[q - 1, 0] + 4.0
    return out


def _quat_drift(pose: np.ndarray) -> np.ndarray:
    out = pose.copy()
    n = out.shape[0]
    half = max(1, n // 2)
    # 给四元数加 0.5 偏移并 *不* 归一化，破坏 quat_norm 检验
    out[half:, 3:7] = out[half:, 3:7] + 0.5
    return out


def _missing_press(pose: np.ndarray) -> np.ndarray:
    out = pose.copy()
    # 把 z 通道压成几乎平直；保留极小噪声避免 0 方差
    base = float(np.mean(out[:, 2]))
    out[:, 2] = base + 1e-6 * np.arange(out.shape[0])
    return out


def _xy_cluster_collapse(pose: np.ndarray) -> np.ndarray:
    out = pose.copy()
    out[:, 0] = 0.0
    out[:, 1] = 0.0
    return out


def _timestamp_jitter(pose: np.ndarray) -> np.ndarray:
    # 通过把 pose 的 dt 不再单调，注入位置抖动；同时 meta 会被打上标识
    out = pose.copy()
    if out.shape[0] >= 5:
        out[2, :3] = out[2, :3] + np.array([0.5, -0.5, 0.3])
    return out


def _schema_missing_column(pose: np.ndarray) -> np.ndarray:
    """触发 SchemaValidator FAIL：保留 7 列以便 loader 不爆，但样本数砍到 <min_samples。

    note：loader 早期对列数 != 7 直接抛 ValueError，会绕过 validator。
    """
    if pose.shape[0] <= 5:
        return pose.copy()
    return pose[:5].copy()


def _nan_injection(pose: np.ndarray) -> np.ndarray:
    out = pose.copy().astype(np.float64)
    n = out.shape[0]
    if n >= 3:
        out[n // 2, 0] = float("nan")
    if n >= 5:
        out[n // 3, 1] = float("nan")
    return out


def _video_missing_only(_pose: np.ndarray) -> np.ndarray:
    return _pose


def _post_video_missing(output_dir: Path) -> None:
    vid = output_dir / "video.mp4"
    if vid.exists():
        vid.unlink()


MutationFn = Callable[[np.ndarray], np.ndarray]


MUTATIONS: dict[str, MutationFn] = {
    "velocity_spike": _velocity_spike,
    "quat_drift": _quat_drift,
    "missing_press": _missing_press,
    "xy_cluster_collapse": _xy_cluster_collapse,
    "timestamp_jitter": _timestamp_jitter,
    "schema_missing_column": _schema_missing_column,
    "nan_injection": _nan_injection,
    "video_missing": _video_missing_only,
}


def list_supported_mutations() -> list[str]:
    return sorted(MUTATIONS.keys())


def apply_mutation(
    *,
    source_dataset: Path,
    output_dataset: Path,
    mutation: str,
) -> Path:
    """对 source_dataset 应用 mutation，写入 output_dataset 并返回路径。"""
    source_dataset = source_dataset.expanduser().resolve()
    output_dataset = output_dataset.expanduser().resolve()
    if mutation not in MUTATIONS:
        raise MutationError(f"unsupported mutation: {mutation}; valid: {list_supported_mutations()}")
    if not source_dataset.is_dir():
        raise MutationError(f"source dataset not found: {source_dataset}")
    if output_dataset == source_dataset:
        raise MutationError("output_dataset must differ from source_dataset")

    _copy_tree(source_dataset, output_dataset)

    endpose = output_dataset / "endpose.pt"
    if not endpose.exists():
        raise MutationError(f"endpose.pt not found at {endpose}; mutation requires v1.3 layout")
    _, arr = _load_pose_tensor(endpose)
    new_arr = MUTATIONS[mutation](arr)
    _save_pose(endpose, new_arr)

    if mutation == "video_missing":
        _post_video_missing(output_dataset)

    _update_meta(
        output_dataset / "meta.yaml",
        mutation_type=mutation,
        extra={"source_dataset": source_dataset.as_posix()},
    )

    LOG.info("mutation %s applied: %s -> %s", mutation, source_dataset, output_dataset)
    return output_dataset
