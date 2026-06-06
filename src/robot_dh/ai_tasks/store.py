"""AI 任务事件 / 死信的持久化：本地 JSONL + 可选 PostgreSQL。

DB 不可用时仍写本地 JSONL，绝不让事件记录阻断主流程（best-effort）。
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.ai_tasks.events import AiTaskEvent
from robot_dh.registry import get_engine, resolve_db_uri
from robot_dh.warehouse.models import AiTaskEventRow, DeadLetterTaskRow, ensure_lake_tables

LOG = logging.getLogger(__name__)

DEFAULT_EVENTS_DIR = "runs/events"


def resolve_optional_engine(db_uri: str | None = None, *, local_only: bool = False) -> Engine | None:
    """返回可用 engine；本地模式或连接失败返回 None。所有 v1.9 PG 写入共用此探测。"""
    if local_only or os.environ.get("ROBOT_DH_INFERENCE_LOCAL") == "1":
        return None
    try:
        resolved = resolve_db_uri(db_uri)
        engine = get_engine(resolved)
        if engine.dialect.name == "sqlite":
            ensure_lake_tables(engine)
        else:
            with engine.connect():
                pass
        return engine
    except SQLAlchemyError as err:
        LOG.warning("v1.9: DB 不可用，仅走本地路径：%s", err)
        return None
    except Exception as err:  # 驱动/连接异常一律降级
        LOG.warning("v1.9: DB 探测异常，仅走本地路径：%s", err)
        return None


class AiTaskStore:
    """事件 / 死信记录入口。"""

    def __init__(
        self,
        *,
        db_uri: str | None = None,
        events_dir: Path | str | None = None,
        local_only: bool = False,
    ) -> None:
        self._db_uri = db_uri
        self._local_only = local_only
        env_dir = os.environ.get("ROBOT_DH_AI_EVENTS_DIR")
        self._events_dir = Path(events_dir or env_dir or DEFAULT_EVENTS_DIR)

    def _engine(self) -> Engine | None:
        return resolve_optional_engine(self._db_uri, local_only=self._local_only)

    def _jsonl_path(self) -> Path:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        return self._events_dir / f"ai_task_events_{day}.jsonl"

    def emit(self, event: AiTaskEvent) -> AiTaskEvent:
        """记录一个事件：先 JSONL，再 best-effort 写 PG。"""
        self._append_jsonl(event)
        engine = self._engine()
        if engine is not None:
            self._write_pg(engine, event)
        return event

    def _append_jsonl(self, event: AiTaskEvent) -> None:
        try:
            self._events_dir.mkdir(parents=True, exist_ok=True)
            with self._jsonl_path().open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")
        except OSError as err:
            # 事件落盘失败不应中断主流程，退到 stderr 兜底。
            LOG.warning("ai_task_events JSONL 写入失败：%s；event=%s", err, event.event_type)

    def _write_pg(self, engine: Engine, event: AiTaskEvent) -> None:
        try:
            with Session(engine, expire_on_commit=False, future=True) as session:
                session.add(
                    AiTaskEventRow(
                        event_id=event.event_id,
                        event_type=event.event_type,
                        task_id=event.task_id,
                        job_id=event.job_id,
                        model_id=event.model_id,
                        dataset_id=event.dataset_id,
                        version=event.version,
                        payload_json=dict(event.payload),
                    )
                )
                session.commit()
        except SQLAlchemyError as err:
            LOG.warning("ai_task_events PG 写入失败（已落 JSONL）：%s", err)

    def record_dead_letter(
        self,
        *,
        task_type: str,
        task_id: str | None,
        job_id: str | None,
        reason: str,
        payload: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> str:
        """登记死信任务，返回 dead_letter_id（DB 不可用时仅返回 id 不落库）。"""
        dead_letter_id = f"dlt-{uuid.uuid4().hex[:16]}"
        engine = self._engine()
        if engine is None:
            LOG.warning("dead_letter_tasks: DB 不可用，跳过落库 task_type=%s job=%s", task_type, job_id)
            return dead_letter_id
        try:
            with Session(engine, expire_on_commit=False, future=True) as session:
                session.add(
                    DeadLetterTaskRow(
                        dead_letter_id=dead_letter_id,
                        task_type=task_type,
                        task_id=task_id,
                        job_id=job_id,
                        reason=reason,
                        payload_json=dict(payload or {}),
                        retry_count=retry_count,
                    )
                )
                session.commit()
        except SQLAlchemyError as err:
            LOG.warning("dead_letter_tasks PG 写入失败：%s", err)
        return dead_letter_id
