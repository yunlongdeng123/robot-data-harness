"""推理任务对象 + inference_jobs 持久化（PG 可用时）。"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.ai_tasks.state import JOB_CREATED
from robot_dh.warehouse.models import InferenceJobRow

LOG = logging.getLogger(__name__)


def new_job_id() -> str:
    return f"infer-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"


@dataclass
class InferenceJob:
    """一次批量推理任务的内存表示，对应 inference_jobs 一行。"""

    job_id: str
    model_id: str
    input_uri: str
    output_uri: str
    task_type: str
    status: str = JOB_CREATED
    job_name: str | None = None
    input_format: str | None = None
    output_format: str | None = "parquet"
    dataset_id: str | None = None
    version: str | None = None
    dataset_family: str | None = None
    priority: int = 0
    batch_size: int | None = None
    max_workers: int | None = None
    total_samples: int = 0
    processed_samples: int = 0
    failed_samples: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_sec: float | None = None
    error_message: str | None = None
    config: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_row_kwargs(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "model_id": self.model_id,
            "input_uri": self.input_uri,
            "output_uri": self.output_uri,
            "input_format": self.input_format,
            "output_format": self.output_format,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "dataset_family": self.dataset_family,
            "task_type": self.task_type,
            "status": self.status,
            "priority": self.priority,
            "batch_size": self.batch_size,
            "max_workers": self.max_workers,
            "total_samples": self.total_samples,
            "processed_samples": self.processed_samples,
            "failed_samples": self.failed_samples,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_sec": self.duration_sec,
            "error_message": self.error_message,
            "config_json": dict(self.config),
            "metrics_json": dict(self.metrics),
        }

    def to_dict(self) -> dict[str, Any]:
        d = self.to_row_kwargs()
        d["config_json"] = dict(self.config)
        d["metrics_json"] = dict(self.metrics)
        for k in ("started_at", "finished_at"):
            v = d.get(k)
            d[k] = v.isoformat() if isinstance(v, datetime) else v
        return d


def write_job_pg(engine: Engine, job: InferenceJob) -> bool:
    """UPSERT inference_jobs；best-effort，返回是否写入成功。"""
    now = datetime.now(timezone.utc)
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            row = session.get(InferenceJobRow, job.job_id)
            kwargs = job.to_row_kwargs()
            if row is None:
                session.add(InferenceJobRow(created_at=now, updated_at=now, **kwargs))
            else:
                for k, v in kwargs.items():
                    setattr(row, k, v)
                row.updated_at = now
            session.commit()
        return True
    except SQLAlchemyError as err:
        LOG.warning("inference_jobs PG 写入失败：%s", err)
        return False


def row_to_dict(row: InferenceJobRow) -> dict[str, Any]:
    return {
        "job_id": row.job_id,
        "job_name": row.job_name,
        "model_id": row.model_id,
        "input_uri": row.input_uri,
        "output_uri": row.output_uri,
        "input_format": row.input_format,
        "output_format": row.output_format,
        "dataset_id": row.dataset_id,
        "version": row.version,
        "dataset_family": row.dataset_family,
        "task_type": row.task_type,
        "status": row.status,
        "priority": row.priority,
        "batch_size": row.batch_size,
        "max_workers": row.max_workers,
        "total_samples": row.total_samples,
        "processed_samples": row.processed_samples,
        "failed_samples": row.failed_samples,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "duration_sec": row.duration_sec,
        "error_message": row.error_message,
        "config_json": dict(row.config_json or {}),
        "metrics_json": dict(row.metrics_json or {}),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def list_jobs(engine: Engine, *, limit: int = 50) -> list[dict[str, Any]]:
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            rows = session.execute(
                select(InferenceJobRow).order_by(InferenceJobRow.created_at.desc()).limit(limit)
            ).scalars().all()
            return [row_to_dict(r) for r in rows]
    except SQLAlchemyError as err:
        LOG.warning("inference_jobs 列举失败：%s", err)
        return []


def get_job(engine: Engine, job_id: str) -> dict[str, Any] | None:
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            row = session.get(InferenceJobRow, job_id)
            return row_to_dict(row) if row else None
    except SQLAlchemyError as err:
        LOG.warning("inference_jobs 查询失败：%s", err)
        return None
