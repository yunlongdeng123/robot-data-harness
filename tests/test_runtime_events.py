from __future__ import annotations

import json
from pathlib import Path

from robot_dh.runtime.events import RuntimeEventLogger
from robot_dh.runtime.jsonlog import emit_step_log


def test_runtime_event_writes_jsonl(tmp_path: Path) -> None:
    logger = RuntimeEventLogger(events_dir=tmp_path)
    logger.emit("etl_plan_created", payload={"plan_id": "plan-x", "shards": 3}, run_id="plan-x")
    logger.emit("etl_shard_started", payload={"shard_id": "plan-x::shard-000"}, run_id="plan-x")

    files = list(tmp_path.glob("runtime_events_*.jsonl"))
    assert files, "expect at least one JSONL event file"
    payloads = [json.loads(line) for line in files[0].read_text().splitlines() if line.strip()]
    assert len(payloads) == 2
    types = {p["event_type"] for p in payloads}
    assert types == {"etl_plan_created", "etl_shard_started"}
    assert all("event_id" in p and "created_at" in p for p in payloads)


def test_emit_step_log_prints_json(capsys) -> None:
    emit_step_log(event="argo_step_started", step="run-shard-0", status="RUNNING", payload={"k": 1})
    captured = capsys.readouterr()
    line = captured.out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "argo_step_started"
    assert payload["step"] == "run-shard-0"
    assert payload["payload"]["k"] == 1
