"""ProgressLogger：长循环周期性进度日志（structured log）。

与 HeartbeatReporter 解耦：HeartbeatReporter 关注的是「外部 watcher 看到这个任务还活着」，
ProgressLogger 关注的是「人/grep 在 stdout 里看到当前进度」。

二者通常一同使用；ProgressLogger 是廉价 stdout 输出，HeartbeatReporter 还要写文件 / DB。
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

LOG = logging.getLogger(__name__)


class ProgressLogger:
    """长循环周期性进度日志。

    用法：
        pl = ProgressLogger(name="normalize.load_bundles", total=10_000, interval_sec=30)
        for i, b in enumerate(bundles):
            ...
            pl.tick(i + 1, extra={"bundle": b.dataset_id})
        pl.done()
    """

    def __init__(
        self,
        *,
        name: str,
        total: int | None = None,
        interval_sec: float = 30.0,
        unit: str = "items",
        logger: logging.Logger | None = None,
    ) -> None:
        self._name = name
        self._total = total
        self._interval_sec = max(0.0, float(interval_sec))
        self._unit = unit
        self._log = logger or LOG
        self._started = time.time()
        self._last_tick = self._started

    def tick(self, current: int, *, extra: dict[str, Any] | None = None) -> bool:
        now = time.time()
        if self._interval_sec > 0 and now - self._last_tick < self._interval_sec:
            return False
        elapsed = now - self._started
        rate = current / elapsed if elapsed > 0 else 0.0
        eta_sec: float | None = None
        if self._total and self._total > 0 and rate > 0:
            remaining = max(0, self._total - current)
            eta_sec = remaining / rate if rate else None
        payload = {
            "phase": self._name,
            "progress_current": int(current),
            "progress_total": int(self._total) if self._total is not None else None,
            "progress_unit": self._unit,
            "rate_per_sec": float(rate),
            "elapsed_sec": float(elapsed),
            "eta_sec": float(eta_sec) if eta_sec is not None else None,
        }
        if extra:
            payload.update({"extra": extra})
        self._log.info(
            "progress %s current=%d/%s rate=%.2f/s elapsed=%.1fs eta=%s",
            self._name,
            int(current),
            str(self._total) if self._total is not None else "?",
            float(rate),
            float(elapsed),
            f"{eta_sec:.1f}s" if eta_sec is not None else "?",
        )
        self._last_tick = now
        return True

    def done(self, *, current: int | None = None) -> None:
        now = time.time()
        elapsed = now - self._started
        self._log.info(
            "progress %s DONE current=%s/%s elapsed=%.1fs",
            self._name,
            current if current is not None else "?",
            str(self._total) if self._total is not None else "?",
            float(elapsed),
        )

    def started_at_iso(self) -> str:
        return datetime.fromtimestamp(self._started, tz=timezone.utc).isoformat()
