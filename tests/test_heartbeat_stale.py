"""v1.7：heartbeat stale check 在 jsonl / 多 phase / fail_on 阈值下的行为。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from robot_dh.progress.stale import check_stale_heartbeats


def _emit_heartbeat(
    events_dir: Path,
    *,
    workflow_name: str,
    phase: str,
    step_name: str,
    seconds_ago: float,
) -> None:
    events_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    jsonl = events_dir / f"heartbeats_{ts.strftime('%Y%m%d')}.jsonl"
    rec = {
        "workflow_name": workflow_name,
        "phase": phase,
        "step_name": step_name,
        "updated_at": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    with jsonl.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")


def test_check_stale_returns_ok_when_recent(tmp_path: Path) -> None:
    events = tmp_path / "events"
    _emit_heartbeat(events, workflow_name="wf-1", phase="qc", step_name="run", seconds_ago=5)
    report = check_stale_heartbeats(
        workflow_name="wf-1",
        warn_after_sec=60,
        stale_after_sec=300,
        events_dir=events,
    )
    assert report.status == "ok"
    assert len(report.rows) == 1
    assert report.rows[0].status == "ok"


def test_check_stale_flags_warn_and_stale(tmp_path: Path) -> None:
    events = tmp_path / "events"
    _emit_heartbeat(events, workflow_name="wf-2", phase="qc", step_name="probe", seconds_ago=180)
    _emit_heartbeat(events, workflow_name="wf-2", phase="etl", step_name="normalize", seconds_ago=600)
    report = check_stale_heartbeats(
        workflow_name="wf-2",
        warn_after_sec=120,
        stale_after_sec=300,
        events_dir=events,
    )
    statuses = {(r.phase, r.step_name): r.status for r in report.rows}
    assert statuses[("qc", "probe")] == "warn"
    assert statuses[("etl", "normalize")] == "stale"
    assert report.status == "stale"


def test_check_stale_filters_by_workflow_name(tmp_path: Path) -> None:
    events = tmp_path / "events"
    _emit_heartbeat(events, workflow_name="wf-a", phase="qc", step_name="s1", seconds_ago=10)
    _emit_heartbeat(events, workflow_name="wf-b", phase="etl", step_name="s2", seconds_ago=900)
    report = check_stale_heartbeats(
        workflow_name="wf-a",
        warn_after_sec=60,
        stale_after_sec=300,
        events_dir=events,
    )
    assert all(r.workflow_name == "wf-a" for r in report.rows)
    assert report.status == "ok"


def test_check_stale_takes_latest_per_step(tmp_path: Path) -> None:
    events = tmp_path / "events"
    # 同 step 两条心跳，最新的应胜出。
    _emit_heartbeat(events, workflow_name="wf-x", phase="qc", step_name="probe", seconds_ago=600)
    _emit_heartbeat(events, workflow_name="wf-x", phase="qc", step_name="probe", seconds_ago=5)
    report = check_stale_heartbeats(
        workflow_name="wf-x",
        warn_after_sec=60,
        stale_after_sec=300,
        events_dir=events,
    )
    assert len(report.rows) == 1
    assert report.rows[0].status == "ok"
