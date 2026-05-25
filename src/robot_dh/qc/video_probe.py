"""mp4 video 探针：fps / duration / frame_count，可选 cv2，否则跳过。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)


def probe_video(path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {
        "uri": path.as_posix(),
        "size_bytes": int(path.stat().st_size),
        "readable": False,
        "fps": 0.0,
        "frame_count": 0,
        "duration_sec": 0.0,
        "width": 0,
        "height": 0,
    }
    if path.stat().st_size == 0:
        return out
    try:
        import cv2

        cap = cv2.VideoCapture(str(path))
        if cap.isOpened():
            out["readable"] = True
            out["fps"] = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
            out["frame_count"] = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
            out["width"] = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
            out["height"] = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
            if out["fps"] > 0:
                out["duration_sec"] = out["frame_count"] / out["fps"]
        cap.release()
    except Exception as err:  # noqa: BLE001
        out["error"] = f"{type(err).__name__}: {err}"
    return out
