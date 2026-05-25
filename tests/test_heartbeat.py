"""HeartbeatReporter：本地 JSONL 写入 + soft DB 失败不阻塞。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from robot_dh.progress.heartbeat import HeartbeatReporter


def test_heartbeat_writes_jsonl_each_emit(tmp_path: Path) -> None:
    events_dir = tmp_path / "events"
    with HeartbeatReporter(
        task_id="t-001",
        workflow_name="wf-x",
        step_name="normalize",
        dataset_id="demo",
        version="v1",
        phase="normalize",
        interval_sec=0.0,
        events_dir=events_dir,
    ) as hb:
        hb.emit(progress_current=10, progress_total=100, message="loading")
        hb.emit(progress_current=50, progress_total=100, message="halfway")

    files = list(events_dir.glob("heartbeats_*.jsonl"))
    assert len(files) == 1
    payloads = [json.loads(line) for line in files[0].read_text().splitlines()]
    # phase_start + 2 emits + phase_finish == 4
    assert len(payloads) >= 4
    assert any(p.get("message") == "phase_start" for p in payloads)
    assert any(p.get("message") == "phase_finish" for p in payloads)
    progress_msgs = [p for p in payloads if p.get("message") in {"loading", "halfway"}]
    assert {p["message"] for p in progress_msgs} == {"loading", "halfway"}


def test_heartbeat_maybe_emit_respects_interval(tmp_path: Path) -> None:
    events_dir = tmp_path / "events"
    hb = HeartbeatReporter(
        task_id="t-002",
        interval_sec=10000.0,
        events_dir=events_dir,
    )
    # 直接 maybe_emit：第一次 force=True 时写一次；之后 interval 没到不再写
    assert hb.maybe_emit(progress_current=1, force=True) is True
    assert hb.maybe_emit(progress_current=2) is False
    files = list(events_dir.glob("heartbeats_*.jsonl"))
    assert len(files) == 1
    payloads = files[0].read_text().splitlines()
    assert len(payloads) == 1


def test_heartbeat_db_failure_is_soft(tmp_path: Path) -> None:
    """warehouse_v16.record_task_heartbeat 抛错时不应中断主流程。"""
    events_dir = tmp_path / "events"

    class _ExplodingWh:
        def record_task_heartbeat(self, **kwargs):
            raise RuntimeError("simulated db down")

    hb = HeartbeatReporter(
        task_id="t-003",
        interval_sec=0.0,
        events_dir=events_dir,
        warehouse_v16=_ExplodingWh(),
    )
    hb.emit(message="x")  # 不应抛
    files = list(events_dir.glob("heartbeats_*.jsonl"))
    assert len(files) == 1


def test_heartbeat_falls_back_to_tmp_when_events_dir_readonly(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """events_dir 不可写时（容器内 /app/runs/events 经典坑），降级到 /tmp 不抛错。"""
    from robot_dh.progress import heartbeat as hb_mod

    readonly = tmp_path / "readonly_events"
    readonly.mkdir()
    readonly.chmod(0o555)
    fallback = tmp_path / "fallback_events"
    monkeypatch.setattr(hb_mod, "_FALLBACK_EVENTS_DIR", fallback)
    try:
        hb = HeartbeatReporter(task_id="t-fallback", interval_sec=0.0, events_dir=readonly)
        hb.emit(message="x")
    finally:
        # 还原权限便于 tmp 清理
        readonly.chmod(0o755)
    assert fallback.is_dir()
    files = list(fallback.glob("heartbeats_*.jsonl"))
    assert len(files) >= 1


def test_heartbeat_stderr_fallback_when_disk_write_fails(
    tmp_path: Path, capsys
) -> None:
    """jsonl 写失败时仍打 stderr，Argo archiveLogs 能事后捞回。"""
    events_dir = tmp_path / "events"
    hb = HeartbeatReporter(task_id="t-stderr", interval_sec=0.0, events_dir=events_dir)
    # 通过覆盖 _events_dir 模拟运行中目录被删除场景
    events_dir.mkdir(parents=True, exist_ok=True)
    import os
    os.chmod(events_dir, 0o555)
    try:
        hb.emit(message="post-failure")
    finally:
        os.chmod(events_dir, 0o755)
    captured = capsys.readouterr()
    assert "HEARTBEAT_FALLBACK" in captured.err
