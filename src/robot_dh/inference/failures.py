"""推理失败样本：分类 error_type、写 failed_samples.parquet、写 PG inference_failures。"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.inference.outputs import (
    FAILED_SAMPLES_FILENAME,
    FAILED_SAMPLES_SCHEMA,
    read_table_from_uri,
    write_table_to_uri,
    _rows_to_table,
)
from robot_dh.models.schemas import InferencePrediction, InferenceSample
from robot_dh.lake.uri import join_uri
from robot_dh.warehouse.models import InferenceFailureRow

LOG = logging.getLogger(__name__)

# openai_compatible backend 的失败前缀，分类时优先识别。
_KNOWN_ERROR_PREFIXES = (
    "OPENAI_ENDPOINT_UNAVAILABLE",
    "OPENAI_TIMEOUT",
    "OPENAI_BAD_RESPONSE",
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def classify_error(error_message: str | None) -> str:
    """从 error_message 提取 error_type；无法识别时归类 PREDICTION_ERROR。"""
    if not error_message:
        return "UNKNOWN"
    head = error_message.split(":", 1)[0].strip()
    if head in _KNOWN_ERROR_PREFIXES:
        return head
    # 形如 'ValueError' / 'KeyError' 的异常名也保留。
    if head and head.replace("_", "").isalnum():
        return head
    return "PREDICTION_ERROR"


def is_retryable(error_type: str) -> bool:
    """超时 / 端点不可达视为可重试；契约类错误（BAD_RESPONSE）默认不可重试。"""
    return error_type in ("OPENAI_TIMEOUT", "OPENAI_ENDPOINT_UNAVAILABLE", "UNKNOWN")


@dataclass
class FailedSampleRecord:
    """failed_samples.parquet 一行 + inference_failures 一行。"""

    job_id: str
    sample_id: str
    model_id: str | None = None
    dataset_id: str | None = None
    version: str | None = None
    episode_id: str | None = None
    frame_id: str | None = None
    input_uri: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    attempt: int = 1
    retryable: bool = True
    failure_id: str = field(default_factory=lambda: f"fail-{uuid.uuid4().hex[:16]}")
    created_at: str = field(default_factory=_utcnow_iso)

    def to_parquet_row(self) -> dict[str, Any]:
        return {
            "failure_id": self.failure_id,
            "job_id": self.job_id,
            "model_id": self.model_id,
            "sample_id": self.sample_id,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "episode_id": self.episode_id,
            "frame_id": self.frame_id,
            "input_uri": self.input_uri,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "attempt": self.attempt,
            "created_at": self.created_at,
        }

    def to_row_kwargs(self) -> dict[str, Any]:
        return {
            "failure_id": self.failure_id,
            "job_id": self.job_id,
            "model_id": self.model_id,
            "sample_id": self.sample_id,
            "input_uri": self.input_uri,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "retryable": self.retryable,
            "attempt": self.attempt,
        }


def failure_from(
    *,
    job_id: str,
    model_id: str,
    sample: InferenceSample,
    prediction: InferencePrediction,
    attempt: int = 1,
) -> FailedSampleRecord:
    """从失败的 prediction 构造失败记录。"""
    error_type = classify_error(prediction.error_message)
    return FailedSampleRecord(
        job_id=job_id,
        sample_id=sample.sample_id,
        model_id=model_id,
        dataset_id=sample.dataset_id,
        version=sample.version,
        episode_id=sample.episode_id,
        frame_id=sample.frame_id,
        input_uri=sample.input_uri,
        error_type=error_type,
        error_message=prediction.error_message,
        attempt=attempt,
        retryable=is_retryable(error_type),
    )


def write_failed_samples(output_uri: str, records: list[FailedSampleRecord]) -> str:
    """写 output_uri/failed_samples.parquet（空也写，含 schema），返回文件 URI。"""
    target = join_uri(output_uri, FAILED_SAMPLES_FILENAME)
    rows = [r.to_parquet_row() for r in records]
    table = _rows_to_table(rows, FAILED_SAMPLES_SCHEMA)
    return write_table_to_uri(target, table)


def read_failed_samples(uri: str) -> list[dict[str, Any]]:
    """读 failed_samples.parquet（接受目录或文件 URI）。"""
    file_uri = uri if uri.endswith(".parquet") else join_uri(uri, FAILED_SAMPLES_FILENAME)
    table = read_table_from_uri(file_uri)
    return table.to_pylist()


def write_failures_pg(engine: Engine, records: list[FailedSampleRecord]) -> int:
    """批量写 inference_failures；best-effort。"""
    if not records:
        return 0
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            for rec in records:
                session.add(InferenceFailureRow(**rec.to_row_kwargs()))
            session.commit()
        return len(records)
    except SQLAlchemyError as err:
        LOG.warning("inference_failures PG 写入失败：%s", err)
        return 0
