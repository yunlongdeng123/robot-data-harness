"""血缘事件记录。

双写持久化：
  1. PostgreSQL `lineage_edges` 表（可查询、有索引）
  2. `s3://<lake>/lineage/events/YYYY/MM/DD/*.jsonl`（追加式、可回放，OpenLineage 子集格式）
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from robot_dh.lake.store import LakeStore
from robot_dh.lake.uri import join_uri


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(slots=True)
class LineageEvent:
    job_id: str
    job_type: str
    source_uri: str
    target_uri: str
    run_id: str | None = None
    event_time: str = field(default_factory=_utcnow_iso)
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    producer: str = "robot-dh/v1.4"

    def to_dict(self) -> dict:
        return asdict(self)


def write_lineage_events(
    store: LakeStore,
    lake_root_uri: str,
    events: Iterable[LineageEvent],
    *,
    now: datetime | None = None,
) -> str:
    """追加 JSONL 到 `<lake>/lineage/events/yyyy/mm/dd/<run_id>-<uuid>.jsonl`；无事件时 no-op 并返回空串。"""
    materialized = list(events)
    if not materialized:
        return ""
    stamp = now or datetime.now(timezone.utc)
    yyyy = stamp.strftime("%Y")
    mm = stamp.strftime("%m")
    dd = stamp.strftime("%d")
    name_run = materialized[0].run_id or materialized[0].job_id
    file_name = f"{name_run}-{uuid.uuid4().hex[:8]}.jsonl"
    target_uri = join_uri(lake_root_uri, "lineage", "events", yyyy, mm, dd, file_name)
    body = "\n".join(json.dumps(e.to_dict(), ensure_ascii=False) for e in materialized) + "\n"
    store.write_text(target_uri, body)
    return target_uri


def write_lineage_events_local(local_dir: Path, events: Iterable[LineageEvent]) -> Path | None:
    """测试便利：直接写入 local_dir/ 下 JSONL。"""
    materialized = list(events)
    if not materialized:
        return None
    local_dir.mkdir(parents=True, exist_ok=True)
    target = local_dir / f"{materialized[0].job_id}-{uuid.uuid4().hex[:8]}.jsonl"
    target.write_text(
        "\n".join(json.dumps(e.to_dict(), ensure_ascii=False) for e in materialized) + "\n",
        encoding="utf-8",
    )
    return target
