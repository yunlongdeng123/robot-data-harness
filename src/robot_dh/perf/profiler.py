"""EtlProfiler：阶段性性能采集 + 本地 JSON 写入 + 可选 DB 写入。

设计要点：
- context manager 用法，进入即开始计时与峰值内存采样；退出时落盘 perf record。
- 通过 with prof.measure_download() / measure_upload() 计时 IO；其余视为 compute。
- 由调用方填充 input_bytes / output_bytes / input_rows / output_rows / metrics。
- 写本地 `_perf.json` 到指定 work_dir；可选写 PostgreSQL `etl_perf_runs`。

PerfRecord 字段与远端 `etl_perf_runs` schema 对齐。
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterator

from robot_dh.perf.memory import track_peak_memory

LOG = logging.getLogger(__name__)


@dataclass
class PerfRecord:
    """单阶段性能记录。字段命名与远端 etl_perf_runs 表保持一致。"""

    job_id: str
    run_id: str
    dataset_id: str
    version: str
    phase: str
    input_uri: str | None = None
    output_uri: str | None = None
    input_bytes: int = 0
    output_bytes: int = 0
    input_rows: int = 0
    output_rows: int = 0
    duration_sec: float = 0.0
    download_duration_sec: float = 0.0
    upload_duration_sec: float = 0.0
    compute_duration_sec: float = 0.0
    peak_memory_mb: float = 0.0
    worker_id: str = ""
    status: str = "RUNNING"
    error_message: str | None = None
    started_at: str = ""
    finished_at: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _utc_iso(ts: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class EtlProfiler:
    """阶段性能 profiler；用法：

    with EtlProfiler(job_id=..., dataset_id=..., version=..., phase="normalize") as prof:
        with prof.measure_download():
            ...
        with prof.measure_upload():
            ...
        prof.set_io(input_bytes=..., output_bytes=..., input_rows=..., output_rows=...)
        prof.add_metric("key", value)
    """

    def __init__(
        self,
        *,
        job_id: str,
        dataset_id: str,
        version: str,
        phase: str,
        run_id: str | None = None,
        input_uri: str | None = None,
        output_uri: str | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.record = PerfRecord(
            job_id=job_id,
            run_id=run_id or job_id,
            dataset_id=dataset_id,
            version=version,
            phase=phase,
            input_uri=input_uri,
            output_uri=output_uri,
            worker_id=worker_id or _default_worker_id(),
        )
        self._started: float = 0.0
        self._finished: float = 0.0
        self._mem_ctx = None
        self._mem_sampler = None
        self._download_acc: float = 0.0
        self._upload_acc: float = 0.0

    def __enter__(self) -> "EtlProfiler":
        self._started = time.time()
        self.record.started_at = _utc_iso(self._started)
        self._mem_ctx = track_peak_memory()
        self._mem_sampler = self._mem_ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._finished = time.time()
        self.record.finished_at = _utc_iso(self._finished)
        if self._mem_ctx is not None:
            self._mem_ctx.__exit__(exc_type, exc, tb)
            if self._mem_sampler is not None:
                self.record.peak_memory_mb = float(self._mem_sampler.peak_mb)
        self.record.duration_sec = max(0.0, self._finished - self._started)
        self.record.download_duration_sec = self._download_acc
        self.record.upload_duration_sec = self._upload_acc
        self.record.compute_duration_sec = max(
            0.0, self.record.duration_sec - self._download_acc - self._upload_acc
        )
        if exc is not None:
            self.record.status = "FAIL"
            self.record.error_message = f"{type(exc).__name__}: {exc}"
        else:
            if self.record.status == "RUNNING":
                self.record.status = "OK"

    @contextmanager
    def measure_download(self) -> Iterator[None]:
        t0 = time.time()
        try:
            yield
        finally:
            self._download_acc += max(0.0, time.time() - t0)

    @contextmanager
    def measure_upload(self) -> Iterator[None]:
        t0 = time.time()
        try:
            yield
        finally:
            self._upload_acc += max(0.0, time.time() - t0)

    def set_io(
        self,
        *,
        input_bytes: int | None = None,
        output_bytes: int | None = None,
        input_rows: int | None = None,
        output_rows: int | None = None,
    ) -> None:
        if input_bytes is not None:
            self.record.input_bytes = int(input_bytes)
        if output_bytes is not None:
            self.record.output_bytes = int(output_bytes)
        if input_rows is not None:
            self.record.input_rows = int(input_rows)
        if output_rows is not None:
            self.record.output_rows = int(output_rows)

    def set_status(self, status: str, *, error_message: str | None = None) -> None:
        self.record.status = status
        if error_message is not None:
            self.record.error_message = error_message

    def add_metric(self, key: str, value: Any) -> None:
        self.record.metrics[key] = value

    def update_metrics(self, payload: dict[str, Any]) -> None:
        self.record.metrics.update(payload)


def _default_worker_id() -> str:
    pod = os.environ.get("HOSTNAME") or os.environ.get("WORKER_ID")
    if pod:
        return str(pod)
    try:
        return socket.gethostname()
    except Exception:
        return f"pid-{os.getpid()}"


def perf_filename(record: PerfRecord) -> str:
    """统一 perf 文件名：<phase>_perf.json。"""
    safe_phase = record.phase.replace("/", "_")
    return f"{safe_phase}_perf.json"


def write_perf_record_to_dir(record: PerfRecord, work_dir: Path) -> Path:
    work_dir = work_dir.expanduser().resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    path = work_dir / perf_filename(record)
    path.write_text(json.dumps(record.to_dict(), indent=2, ensure_ascii=False))
    return path
