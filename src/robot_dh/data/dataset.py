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

    @property
    def xyz(self) -> np.ndarray:
        return self.pose[:, :3]

    @property
    def quaternions(self) -> np.ndarray:
        return self.pose[:, 3:7]
