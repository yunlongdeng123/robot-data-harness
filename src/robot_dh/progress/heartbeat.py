"""HeartbeatReporter：周期性写本地 JSONL + 结构化日志 + 可选 Postgres。

设计要点：
- 通过 emit() 主动写一拍；通过 with HeartbeatReporter(...) as hb: hb.tick() 周期性写。
- 周期性写不开后台线程，由 caller 在循环里手动 maybe_emit() 打点；避免子线程泄漏。
- DB 不可用时仅 warning，不阻断主任务。
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_FALLBACK_EVENTS_DIR = Path("/tmp/robot-dh/events")


def default_heartbeats_dir() -> Path:
    """heartbeat jsonl 落盘根目录。

    优先级：
    1. `ROBOT_DH_EVENTS_DIR` 环境变量（Argo step template 推荐显式注入 emptyDir 挂载点）；
    2. `runs/events`（开发机默认）；
    3. `/tmp/robot-dh/events`（兜底；容器内 `/app/runs` 不可写时使用，写到 ephemeral 卷至少能把
       心跳 jsonl 留到容器退出，便于事后归档）。
    """
    base = os.environ.get("ROBOT_DH_EVENTS_DIR")
    if base:
        return Path(base).expanduser().resolve()
    return Path("runs") / "events"


def _ensure_writable_dir(preferred: Path) -> Path:
    """优先创建 preferred；失败则降级到 `/tmp/robot-dh/events`。

    返回真正可写的目录（已 mkdir）。容器内 `/app/runs/events` 经常因为
    runAsUser=1000 无权限创建，回退到 `/tmp` 比直接抛错更稳。
    """
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        # mkdir 成功不代表可写：再做一次 touch-and-remove 探测。
        probe = preferred / ".heartbeat_writable_probe"
        probe.touch()
        probe.unlink(missing_ok=True)
        return preferred
    except OSError as err:
        LOG.warning(
            "heartbeat events dir %s not writable (%s); falling back to %s",
            preferred, err, _FALLBACK_EVENTS_DIR,
        )
        _FALLBACK_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
        return _FALLBACK_EVENTS_DIR


def _daily_jsonl_path(events_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return events_dir / f"heartbeats_{stamp}.jsonl"


@dataclass
class HeartbeatPayload:
    """单次 heartbeat 字段；与远端 task_heartbeats 表对齐。"""

    task_id: str
    workflow_name: str | None = None
    step_name: str | None = None
    dataset_id: str | None = None
    version: str | None = None
    phase: str | None = None
    progress_current: int | None = None
    progress_total: int | None = None
    progress_unit: str | None = None
    message: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    updated_at: str = field(default_factory=_utcnow_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class HeartbeatReporter:
    """长任务心跳上报器。

    用法：
        with HeartbeatReporter(task_id=..., interval_sec=30) as hb:
            for i in iterable:
                ...
                hb.maybe_emit(progress_current=i, progress_total=total, message="processing")
    """

    def __init__(
        self,
        *,
        task_id: str,
        workflow_name: str | None = None,
        step_name: str | None = None,
        dataset_id: str | None = None,
        version: str | None = None,
        phase: str | None = None,
        progress_unit: str | None = None,
        interval_sec: float = 30.0,
        events_dir: Path | None = None,
        warehouse_v16: Any | None = None,
    ) -> None:
        self._template = HeartbeatPayload(
            task_id=task_id,
            workflow_name=workflow_name,
            step_name=step_name,
            dataset_id=dataset_id,
            version=version,
            phase=phase,
            progress_unit=progress_unit,
        )
        self._interval_sec = max(0.0, float(interval_sec))
        preferred = (events_dir or default_heartbeats_dir()).expanduser().resolve()
        self._events_dir = _ensure_writable_dir(preferred)
        self._wh = warehouse_v16
        self._last_emit: float = 0.0
        # 兜底 stderr：jsonl 持续写失败时，至少把心跳输出到 stderr，
        # 让 argo-logs/ 归档能事后捞回来。`_jsonl_write_errors` 是连续失败计数。
        self._jsonl_write_errors: int = 0
        self._max_jsonl_warn: int = 3

    def __enter__(self) -> "HeartbeatReporter":
        # 入口立即打一拍，便于 watcher 捕获 phase 起点。
        self.emit(message="phase_start")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        msg = "phase_finish" if exc is None else f"phase_failed: {type(exc).__name__}"
        try:
            self.emit(message=msg)
        except Exception as err:
            LOG.warning("heartbeat phase_finish emit failed: %s", err)

    def maybe_emit(
        self,
        *,
        progress_current: int | None = None,
        progress_total: int | None = None,
        message: str | None = None,
        metrics: dict[str, Any] | None = None,
        force: bool = False,
    ) -> bool:
        """间隔时间到了才写；返回是否实际写入。"""
        now = time.time()
        if not force and self._interval_sec > 0 and now - self._last_emit < self._interval_sec:
            return False
        self.emit(
            progress_current=progress_current,
            progress_total=progress_total,
            message=message,
            metrics=metrics,
        )
        return True

    def emit(
        self,
        *,
        progress_current: int | None = None,
        progress_total: int | None = None,
        message: str | None = None,
        metrics: dict[str, Any] | None = None,
        phase: str | None = None,
    ) -> HeartbeatPayload:
        payload = HeartbeatPayload(
            task_id=self._template.task_id,
            workflow_name=self._template.workflow_name,
            step_name=self._template.step_name,
            dataset_id=self._template.dataset_id,
            version=self._template.version,
            phase=phase or self._template.phase,
            progress_current=progress_current,
            progress_total=progress_total,
            progress_unit=self._template.progress_unit,
            message=message,
            metrics=dict(metrics) if metrics else {},
        )
        self._write_jsonl(payload)
        LOG.info(
            "heartbeat task=%s phase=%s progress=%s/%s msg=%s",
            payload.task_id,
            payload.phase,
            payload.progress_current,
            payload.progress_total,
            payload.message,
        )
        self._write_db(payload)
        self._last_emit = time.time()
        return payload

    def _write_jsonl(self, payload: HeartbeatPayload) -> None:
        try:
            path = _daily_jsonl_path(self._events_dir)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload.to_dict(), ensure_ascii=False))
                fh.write("\n")
            self._jsonl_write_errors = 0
        except Exception as err:
            self._jsonl_write_errors += 1
            # 头几次保持 WARN 级别；持续失败不再刷屏，但 stderr fallback 一直生效。
            if self._jsonl_write_errors <= self._max_jsonl_warn:
                LOG.warning(
                    "heartbeat jsonl write failed at %s: %s (fallback stderr; will suppress after %d)",
                    self._events_dir, err, self._max_jsonl_warn,
                )
            self._fallback_stderr(payload)

    def _fallback_stderr(self, payload: HeartbeatPayload) -> None:
        """jsonl 写不进时的兜底通道：直接 stderr，让 Argo `archiveLogs` 也能归档心跳。"""
        try:
            import sys

            sys.stderr.write(
                "HEARTBEAT_FALLBACK " + json.dumps(payload.to_dict(), ensure_ascii=False) + "\n"
            )
            sys.stderr.flush()
        except Exception:
            # 真发生 stderr 关闭这类极端情况就直接放弃，绝不影响主流程。
            return

    def _write_db(self, payload: HeartbeatPayload) -> None:
        if self._wh is None:
            return
        try:
            self._wh.record_task_heartbeat(
                task_id=payload.task_id,
                workflow_name=payload.workflow_name,
                step_name=payload.step_name,
                dataset_id=payload.dataset_id,
                version=payload.version,
                phase=payload.phase,
                progress_current=payload.progress_current,
                progress_total=payload.progress_total,
                progress_unit=payload.progress_unit,
                message=payload.message,
                metrics=payload.metrics,
            )
        except Exception as err:
            LOG.warning("heartbeat db write failed: %s", err)

    def update_phase(self, phase: str) -> None:
        """切阶段时调用；下一拍 heartbeat 自动带新 phase。"""
        self._template = HeartbeatPayload(
            task_id=self._template.task_id,
            workflow_name=self._template.workflow_name,
            step_name=self._template.step_name,
            dataset_id=self._template.dataset_id,
            version=self._template.version,
            phase=phase,
            progress_unit=self._template.progress_unit,
        )
