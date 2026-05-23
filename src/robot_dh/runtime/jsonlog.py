"""结构化 step-level JSON 日志：CLI / Argo step 入口与出口都可调用。"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

LOG = logging.getLogger(__name__)


def _iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def emit_step_log(
    *,
    event: str,
    step: str | None = None,
    status: str | None = None,
    payload: dict[str, Any] | None = None,
    stream: Any | None = None,
) -> None:
    """打印一行 JSON 步骤日志，便于 Argo / kubectl logs 中 grep 关键事件。"""
    record: dict[str, Any] = {"timestamp": _iso(), "event": event}
    if step:
        record["step"] = step
    if status:
        record["status"] = status
    if payload:
        record["payload"] = dict(payload)
    line = json.dumps(record, ensure_ascii=False, default=str)
    target = stream or sys.stdout
    try:
        target.write(line + "\n")
        target.flush()
    except Exception as err:
        LOG.warning("emit_step_log failed: %s", err)
