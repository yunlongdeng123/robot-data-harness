"""runtime_events：统一记录平台关键动作。

支持本地 JSONL（runs/events/runtime_events_YYYYmmdd.jsonl）+ 可选 PostgreSQL `runtime_events`。
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)

EVENT_TYPES = (
    "etl_plan_created",
    "etl_shard_started",
    "etl_shard_finished",
    "dataset_etl_started",
    "dataset_etl_finished",
    "benchmark_started",
    "benchmark_case_finished",
    "benchmark_finished",
    "argo_workflow_submitted",
    "argo_workflow_finished",
    "argo_step_started",
    "argo_step_finished",
    "mutation_applied",
    "merge_summary_written",
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def new_event_id() -> str:
    return f"evt-{uuid.uuid4().hex[:16]}"


@dataclass
class RuntimeEvent:
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    event_id: str = field(default_factory=new_event_id)
    created_at: str = field(default_factory=utcnow_iso)
    job_id: str | None = None
    run_id: str | None = None
    dataset_id: str | None = None
    version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def default_events_dir() -> Path:
    base = os.environ.get("ROBOT_DH_EVENTS_DIR")
    if base:
        return Path(base).expanduser().resolve()
    return Path("runs") / "events"


def _daily_jsonl_path(events_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return events_dir / f"runtime_events_{stamp}.jsonl"


class RuntimeEventLogger:
    """记录 runtime events；本地 JSONL 总是写入，DB 写入 soft 失败。"""

    def __init__(
        self,
        *,
        events_dir: Path | None = None,
        warehouse: Any | None = None,
    ) -> None:
        self.events_dir = events_dir or default_events_dir()
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self._warehouse = warehouse

    def emit(
        self,
        event_type: str,
        *,
        payload: dict[str, Any] | None = None,
        job_id: str | None = None,
        run_id: str | None = None,
        dataset_id: str | None = None,
        version: str | None = None,
    ) -> RuntimeEvent:
        event = RuntimeEvent(
            event_type=event_type,
            payload=dict(payload or {}),
            job_id=job_id,
            run_id=run_id,
            dataset_id=dataset_id,
            version=version,
        )
        try:
            path = _daily_jsonl_path(self.events_dir)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event.to_dict(), ensure_ascii=False))
                fh.write("\n")
        except Exception as err:
            LOG.warning("runtime event jsonl write failed: %s", err)
        if self._warehouse is not None:
            try:
                self._warehouse.record_runtime_event(event)
            except Exception as err:
                LOG.warning("runtime event db write failed: %s", err)
        return event


def get_default_logger(*, warehouse: Any | None = None) -> RuntimeEventLogger:
    return RuntimeEventLogger(warehouse=warehouse)
