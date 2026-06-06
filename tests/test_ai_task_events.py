"""v1.9 AiTaskStore 事件 JSONL 写入测试。"""

from __future__ import annotations

import glob
import json

from robot_dh.ai_tasks import AiTaskEvent, AiTaskStore
from robot_dh.ai_tasks.events import EVENT_INFERENCE_JOB_CREATED


def test_emit_writes_jsonl(tmp_path) -> None:
    store = AiTaskStore(events_dir=tmp_path / "events", local_only=True)
    ev = store.emit(AiTaskEvent(
        event_type=EVENT_INFERENCE_JOB_CREATED,
        job_id="job-1", model_id="mock-captioner-v1",
        dataset_id="demo", version="v1", payload={"k": "v"},
    ))
    files = glob.glob(str(tmp_path / "events" / "ai_task_events_*.jsonl"))
    assert len(files) == 1
    lines = [json.loads(l) for l in open(files[0], encoding="utf-8")]
    assert len(lines) == 1
    row = lines[0]
    for key in ("event_id", "event_type", "job_id", "model_id", "dataset_id", "version", "payload", "created_at"):
        assert key in row
    assert row["event_type"] == EVENT_INFERENCE_JOB_CREATED
    assert row["job_id"] == "job-1"
    assert row["payload"] == {"k": "v"}
    assert ev.event_id == row["event_id"]


def test_emit_appends(tmp_path) -> None:
    store = AiTaskStore(events_dir=tmp_path / "events", local_only=True)
    store.emit(AiTaskEvent(event_type="model_registered", model_id="a"))
    store.emit(AiTaskEvent(event_type="model_registered", model_id="b"))
    files = glob.glob(str(tmp_path / "events" / "ai_task_events_*.jsonl"))
    lines = open(files[0], encoding="utf-8").read().splitlines()
    assert len(lines) == 2


def test_dead_letter_without_db_returns_id(tmp_path) -> None:
    store = AiTaskStore(events_dir=tmp_path / "events", local_only=True)
    dlid = store.record_dead_letter(
        task_type="inference_job", task_id=None, job_id="job-1", reason="boom",
    )
    assert dlid.startswith("dlt-")
