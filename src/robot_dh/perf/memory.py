"""peak memory 监控辅助：优先 psutil，缺失时退化到 resource。"""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from typing import Iterator


try:
    import psutil  # type: ignore

    _HAS_PSUTIL = True
except Exception:
    _HAS_PSUTIL = False


def _resource_peak_mb() -> float:
    # 在 macOS 上 ru_maxrss 单位是 bytes，Linux 上是 KB；这里按 Linux 处理，macOS 偏差可接受。
    try:
        import resource  # type: ignore

        usage = resource.getrusage(resource.RUSAGE_SELF)
        return float(usage.ru_maxrss) / 1024.0
    except Exception:
        return 0.0


class _PeakSampler:
    """后台线程轮询 RSS，记录峰值（MB）；仅在 psutil 可用时实际工作。"""

    def __init__(self, *, interval_sec: float = 0.2) -> None:
        self._interval = interval_sec
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._peak_mb: float = 0.0
        self._proc = psutil.Process(os.getpid()) if _HAS_PSUTIL else None

    def _loop(self) -> None:
        assert self._proc is not None
        while not self._stop.is_set():
            try:
                rss = self._proc.memory_info().rss / (1024.0 * 1024.0)
                if rss > self._peak_mb:
                    self._peak_mb = rss
            except Exception:
                # 进程可能瞬时不可读，下一轮再说
                pass
            self._stop.wait(self._interval)

    def start(self) -> None:
        if self._proc is None:
            return
        try:
            self._peak_mb = self._proc.memory_info().rss / (1024.0 * 1024.0)
        except Exception:
            self._peak_mb = 0.0
        self._thread = threading.Thread(target=self._loop, name="perf-mem", daemon=True)
        self._thread.start()

    def stop(self) -> float:
        if self._thread is None:
            return self._peak_mb
        self._stop.set()
        self._thread.join(timeout=2.0)
        return self._peak_mb

    @property
    def peak_mb(self) -> float:
        return self._peak_mb


@contextmanager
def track_peak_memory() -> Iterator[_PeakSampler]:
    """启动后台峰值采样；退出时若 psutil 不可用，则回退使用 resource.getrusage 估算。"""
    sampler = _PeakSampler()
    sampler.start()
    try:
        yield sampler
    finally:
        sampler.stop()
        if not _HAS_PSUTIL:
            sampler._peak_mb = max(sampler._peak_mb, _resource_peak_mb())
