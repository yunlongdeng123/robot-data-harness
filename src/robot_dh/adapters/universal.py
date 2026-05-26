"""UniversalAdapter：未识别 family 时的兜底。

只做基本的 file listing + size 统计；不假定 schema。用于 ``robot-dh adapter detect``
没匹配到 DROID/robomimic/bridge 时返回，避免 CLI 直接报错。
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from robot_dh.adapters.base import (
    DetectionResult,
    ProbeResult,
    RobotDatasetAdapter,
)
from robot_dh.lake.uri import is_local_uri, parse_uri


class UniversalAdapter(RobotDatasetAdapter):
    family = "universal"

    def detect(
        self,
        dataset_uri: str,
        *,
        dataset_id: str | None = None,
        listings: list[str] | None = None,
    ) -> DetectionResult:
        # universal 永远命中，但 confidence 低，仅作兜底。
        return DetectionResult(
            family=self.family,
            confidence=0.1,
            notes=["fallback adapter; no family-specific markers detected"],
        )

    def probe(
        self,
        dataset_uri: str,
        *,
        sample_limit: int = 32,
        options: dict[str, Any] | None = None,
    ) -> ProbeResult:
        started = time.time()
        if not is_local_uri(dataset_uri):
            return ProbeResult(
                family=self.family,
                dataset_uri=dataset_uri,
                status="WARN",
                errors=[{
                    "error_type": "UnsupportedURI",
                    "msg": "universal adapter probe only supports local URIs",
                }],
                duration_sec=time.time() - started,
                options=options or {},
            )
        root = Path(parse_uri(dataset_uri).local_path)
        files = list(root.rglob("*")) if root.exists() else []
        bytes_total = 0
        parquet = hdf5 = video = 0
        for f in files:
            if not f.is_file():
                continue
            try:
                bytes_total += f.stat().st_size
            except OSError:
                continue
            name = f.name.lower()
            if name.endswith(".parquet"):
                parquet += 1
            elif name.endswith((".hdf5", ".h5")):
                hdf5 += 1
            elif name.endswith((".mp4", ".mkv", ".webm")):
                video += 1
        return ProbeResult(
            family=self.family,
            dataset_uri=dataset_uri,
            status="OK" if root.exists() else "FAIL",
            files_count=sum(1 for f in files if f.is_file()),
            bytes_total=int(bytes_total),
            parquet_files=parquet,
            hdf5_files=hdf5,
            video_files=video,
            episodes_count=0,
            schema_summary={"root_exists": root.exists()},
            errors=[],
            duration_sec=time.time() - started,
            options=options or {},
        )
