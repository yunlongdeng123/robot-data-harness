"""AI 任务事件定义。

事件先落本地 JSONL（runs/events/ai_task_events_YYYYmmdd.jsonl），DB 可用时同时写
ai_task_events 表。为后续把事件流接到 Redis Streams / Kafka 预留统一结构。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# 事件类型（见 v1_9_promptB 第十节）。
EVENT_MODEL_REGISTERED = "model_registered"
EVENT_INFERENCE_JOB_CREATED = "inference_job_created"
EVENT_INFERENCE_JOB_STARTED = "inference_job_started"
EVENT_INFERENCE_BATCH_FINISHED = "inference_batch_finished"
EVENT_INFERENCE_JOB_FINISHED = "inference_job_finished"
EVENT_INFERENCE_JOB_FAILED = "inference_job_failed"
EVENT_DISTILL_BUILD_STARTED = "distill_build_started"
EVENT_DISTILL_BUILD_FINISHED = "distill_build_finished"
EVENT_BENCHMARK_STARTED = "benchmark_started"
EVENT_BENCHMARK_FINISHED = "benchmark_finished"

EVENT_TYPES: tuple[str, ...] = (
    EVENT_MODEL_REGISTERED,
    EVENT_INFERENCE_JOB_CREATED,
    EVENT_INFERENCE_JOB_STARTED,
    EVENT_INFERENCE_BATCH_FINISHED,
    EVENT_INFERENCE_JOB_FINISHED,
    EVENT_INFERENCE_JOB_FAILED,
    EVENT_DISTILL_BUILD_STARTED,
    EVENT_DISTILL_BUILD_FINISHED,
    EVENT_BENCHMARK_STARTED,
    EVENT_BENCHMARK_FINISHED,
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class AiTaskEvent:
    """统一 AI 任务事件。"""

    event_type: str
    event_id: str = field(default_factory=lambda: f"evt-{uuid.uuid4().hex[:16]}")
    task_id: str | None = None
    job_id: str | None = None
    model_id: str | None = None
    dataset_id: str | None = None
    version: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_utcnow_iso)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "task_id": self.task_id,
            "job_id": self.job_id,
            "model_id": self.model_id,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "payload": dict(self.payload),
            "created_at": self.created_at,
        }
