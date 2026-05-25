from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np


@dataclass(slots=True)
class VideoMetadata:
    fps: float
    frame_count: int
    duration_sec: float
    source: str


@dataclass(slots=True)
class DatasetBundle:
    dataset_id: str
    dataset_path: Path
    endpose_path: Path
    pose: np.ndarray
    timestamps: np.ndarray
    dt: float
    video_meta: VideoMetadata
    meta: dict[str, Any] = field(default_factory=dict)
    video_path: Path | None = None
    meta_path: Path | None = None
    warnings: list[str] = field(default_factory=list)
    # v1.6.6：bridge / OXE 系携带控制信号时把动作单独存出来，pose 是 state、action 是控制。
    # 约定 shape：(N, 7) = (x, y, z, roll, pitch, yaw, grasp)；不存在则保持 None，
    # 下游 features / training 自行决定是否消费。`absolute_action` 是 mbodiai 提供的
    # 同维度绝对目标，可选；缺失时只填 action。
    action: np.ndarray | None = None
    absolute_action: np.ndarray | None = None

    @property
    def xyz(self) -> np.ndarray:
        return self.pose[:, :3]

    @property
    def quaternions(self) -> np.ndarray:
        return self.pose[:, 3:7]
