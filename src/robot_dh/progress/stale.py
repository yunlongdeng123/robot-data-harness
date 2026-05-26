"""heartbeat stale detection（v1.7）：扫描本地 JSONL 心跳，
聚合按 ``(workflow_name, phase)`` 算最新一拍距今的秒数，超过阈值即 WARN / FAIL。

数据源优先级：
  1. PG 表 ``task_heartbeats`` —— 当 ``warehouse_v16`` 可注入且连通时优先；
  2. 本地 JSONL（``ROBOT_DH_EVENTS_DIR`` 或 ``runs/events/heartbeats_YYYYMMDD.jsonl``）。

调用方一般在 Argo 末尾节点 / 独立 watcher 跑 ``check_stale_heartbeats``，
然后根据 ``status`` 决定是否退出 1。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

LOG = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _events_dir() -> Path:
    raw = os.environ.get("ROBOT_DH_EVENTS_DIR")
    if raw:
        return Path(raw).expanduser()
    return Path("runs") / "events"


def _iter_local_records(
    events_dir: Path,
    *,
    workflow_name: str | None,
) -> Iterable[dict[str, Any]]:
    if not events_dir.exists():
        return
    for jsonl in sorted(events_dir.glob("heartbeats_*.jsonl")):
        try:
            with jsonl.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if workflow_name and rec.get("workflow_name") != workflow_name:
                        continue
                    yield rec
        except OSError as err:
            LOG.warning("failed to read %s: %s", jsonl, err)


@dataclass(slots=True)
class StaleCheckRow:
    workflow_name: str | None
    phase: str | None
    step_name: str | None
    last_updated_at: str | None
    seconds_since_last: float | None
    status: str   # ok / warn / stale
    source: str   # pg / jsonl

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StaleCheckReport:
    generated_at: str
    workflow_name: str | None
    stale_after_sec: float
    warn_after_sec: float
    rows: list[StaleCheckRow] = field(default_factory=list)
    status: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "workflow_name": self.workflow_name,
            "stale_after_sec": self.stale_after_sec,
            "warn_after_sec": self.warn_after_sec,
            "rows": [r.to_dict() for r in self.rows],
            "status": self.status,
        }


def check_stale_heartbeats(
    *,
    workflow_name: str | None = None,
    stale_after_sec: float = 300.0,
    warn_after_sec: float = 120.0,
    events_dir: Path | None = None,
    warehouse: Any | None = None,
    now: datetime | None = None,
) -> StaleCheckReport:
    """主入口。

    ``warehouse`` 是 :class:`robot_dh.warehouse.robot_platform.PlatformWarehouse`
    实例（或鸭子类型），若提供且暴露 ``list_task_heartbeats`` 方法则优先用 PG。
    """
    now_ts = (now or datetime.now(timezone.utc)).timestamp()
    latest: dict[tuple[str, str, str], dict[str, Any]] = {}
    source = "jsonl"

    pg_records: list[dict[str, Any]] = []
    if warehouse is not None and hasattr(warehouse, "list_task_heartbeats"):
        try:
            pg_records = list(
                warehouse.list_task_heartbeats(workflow_name=workflow_name)  # type: ignore[call-arg]
            )
            source = "pg"
        except Exception as err:  # noqa: BLE001
            LOG.warning("PG heartbeat read failed (fallback to jsonl): %s", err)
            pg_records = []
            source = "jsonl"

    records: Iterable[dict[str, Any]]
    if pg_records:
        records = pg_records
    else:
        records = _iter_local_records(events_dir or _events_dir(), workflow_name=workflow_name)

    for rec in records:
        wf = rec.get("workflow_name") or "-"
        phase = rec.get("phase") or "-"
        step = rec.get("step_name") or "-"
        key = (str(wf), str(phase), str(step))
        ts = _parse_iso(rec.get("updated_at"))
        if ts is None:
            continue
        existing = latest.get(key)
        if existing is None or ts > existing["_ts"]:
            latest[key] = {**rec, "_ts": ts, "_source": source}

    rows: list[StaleCheckRow] = []
    any_warn = False
    any_stale = False
    for (wf, phase, step), rec in sorted(latest.items()):
        ts: datetime = rec["_ts"]
        delta = max(0.0, now_ts - ts.timestamp())
        if delta >= stale_after_sec:
            status = "stale"
            any_stale = True
        elif delta >= warn_after_sec:
            status = "warn"
            any_warn = True
        else:
            status = "ok"
        rows.append(
            StaleCheckRow(
                workflow_name=None if wf == "-" else wf,
                phase=None if phase == "-" else phase,
                step_name=None if step == "-" else step,
                last_updated_at=ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                seconds_since_last=delta,
                status=status,
                source=str(rec.get("_source", source)),
            )
        )

    overall = "stale" if any_stale else ("warn" if any_warn else "ok")
    return StaleCheckReport(
        generated_at=_now_iso(),
        workflow_name=workflow_name,
        stale_after_sec=float(stale_after_sec),
        warn_after_sec=float(warn_after_sec),
        rows=rows,
        status=overall,
    )
