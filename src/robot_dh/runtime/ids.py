"""v1.5 统一 ID 生成。"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


def _ts_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def new_plan_id(prefix: str = "plan") -> str:
    return f"{prefix}-{_ts_compact()}-{uuid.uuid4().hex[:8]}"


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}-{_ts_compact()}-{uuid.uuid4().hex[:8]}"


def new_shard_id(plan_id: str, shard_index: int) -> str:
    return f"{plan_id}::shard-{int(shard_index):03d}"
