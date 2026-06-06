"""推理输出：predictions.parquet / failed_samples.parquet 读写 + PG 写入。

parquet 读写统一支持 file:// 与 s3://：
- 本地：直接 pyarrow 读写。
- s3：写走临时文件 + LakeStore.upload_file；读走 s3fs lazy open。

prediction_json 在 parquet 里是 JSON 字符串列；写 PG inference_outputs 时还原成 dict（jsonb）。
"""

from __future__ import annotations

import json
import logging
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import join_uri, parse_uri
from robot_dh.models.schemas import InferencePrediction, InferenceSample
from robot_dh.warehouse.models import InferenceOutputRow

LOG = logging.getLogger(__name__)

PREDICTIONS_FILENAME = "predictions.parquet"
FAILED_SAMPLES_FILENAME = "failed_samples.parquet"

PREDICTIONS_SCHEMA = pa.schema(
    [
        ("output_id", pa.string()),
        ("job_id", pa.string()),
        ("model_id", pa.string()),
        ("sample_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("episode_id", pa.string()),
        ("frame_id", pa.string()),
        ("input_uri", pa.string()),
        ("prediction_type", pa.string()),
        ("prediction_json", pa.string()),
        ("confidence", pa.float64()),
        ("latency_ms", pa.float64()),
        ("token_count", pa.int64()),
        ("status", pa.string()),
        ("error_message", pa.string()),
        ("created_at", pa.string()),
    ]
)

FAILED_SAMPLES_SCHEMA = pa.schema(
    [
        ("failure_id", pa.string()),
        ("job_id", pa.string()),
        ("model_id", pa.string()),
        ("sample_id", pa.string()),
        ("dataset_id", pa.string()),
        ("version", pa.string()),
        ("episode_id", pa.string()),
        ("frame_id", pa.string()),
        ("input_uri", pa.string()),
        ("error_type", pa.string()),
        ("error_message", pa.string()),
        ("attempt", pa.int64()),
        ("created_at", pa.string()),
    ]
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------- 通用 parquet 读写（file:// / s3://） ----------


def write_table_to_uri(uri: str, table: pa.Table) -> str:
    """把 pa.Table 写到 uri；本地直写，s3 走临时文件 + upload。返回规范化 URI。"""
    parsed = parse_uri(uri)
    if parsed.is_local:
        path = Path(parsed.local_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path.as_posix())
        return parsed.uri
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "out.parquet"
        pq.write_table(table, local.as_posix())
        store = create_lake_store(uri)
        return store.upload_file(local, uri)


def read_table_from_uri(uri: str) -> pa.Table:
    """读取单个 parquet 文件为 pa.Table（file:// 直读，s3 走 s3fs lazy open）。"""
    parsed = parse_uri(uri)
    if parsed.is_local:
        return pq.read_table(Path(parsed.local_path).as_posix())
    from robot_dh.lake.s3_fs import get_s3fs

    fs = get_s3fs()
    with fs.open(uri.replace("s3://", ""), "rb") as fobj:
        return pq.read_table(fobj)


# ---------- 输出记录 ----------


@dataclass
class InferenceOutputRecord:
    """predictions.parquet 一行；prediction 以 dict 持有，落 parquet 时序列化为字符串。"""

    job_id: str
    model_id: str
    sample_id: str
    prediction_type: str
    prediction: dict[str, Any] = field(default_factory=dict)
    dataset_id: str | None = None
    version: str | None = None
    episode_id: str | None = None
    frame_id: str | None = None
    input_uri: str | None = None
    output_uri: str | None = None
    confidence: float | None = None
    latency_ms: float | None = None
    token_count: int | None = None
    status: str = "OK"
    error_message: str | None = None
    output_id: str = field(default_factory=lambda: f"out-{uuid.uuid4().hex[:16]}")
    created_at: str = field(default_factory=_utcnow_iso)

    def to_parquet_row(self) -> dict[str, Any]:
        return {
            "output_id": self.output_id,
            "job_id": self.job_id,
            "model_id": self.model_id,
            "sample_id": self.sample_id,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "episode_id": self.episode_id,
            "frame_id": self.frame_id,
            "input_uri": self.input_uri,
            "prediction_type": self.prediction_type,
            "prediction_json": json.dumps(self.prediction, ensure_ascii=False),
            "confidence": self.confidence,
            "latency_ms": self.latency_ms,
            "token_count": self.token_count,
            "status": self.status,
            "error_message": self.error_message,
            "created_at": self.created_at,
        }

    def to_row_kwargs(self) -> dict[str, Any]:
        """转成 InferenceOutputRow（PG）构造参数；prediction_json 还原成 dict。"""
        return {
            "output_id": self.output_id,
            "job_id": self.job_id,
            "model_id": self.model_id,
            "sample_id": self.sample_id,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "episode_id": self.episode_id,
            "frame_id": self.frame_id,
            "input_uri": self.input_uri,
            "output_uri": self.output_uri,
            "prediction_type": self.prediction_type,
            "prediction_json": dict(self.prediction),
            "confidence": self.confidence,
            "latency_ms": self.latency_ms,
            "token_count": self.token_count,
            "status": self.status,
            "error_message": self.error_message,
        }


def record_from_prediction(
    *,
    job_id: str,
    model_id: str,
    sample: InferenceSample,
    prediction: InferencePrediction,
    output_uri: str | None = None,
) -> InferenceOutputRecord:
    """合并 sample + prediction 成一条输出记录。"""
    return InferenceOutputRecord(
        job_id=job_id,
        model_id=model_id,
        sample_id=sample.sample_id,
        prediction_type=prediction.prediction_type,
        prediction=dict(prediction.prediction_json),
        dataset_id=sample.dataset_id,
        version=sample.version,
        episode_id=sample.episode_id,
        frame_id=sample.frame_id,
        input_uri=sample.input_uri,
        output_uri=output_uri,
        confidence=prediction.confidence,
        latency_ms=prediction.latency_ms,
        token_count=prediction.token_count,
        status=prediction.status,
        error_message=prediction.error_message,
    )


def write_predictions(output_uri: str, records: list[InferenceOutputRecord]) -> str:
    """把输出记录写成 output_uri/predictions.parquet，返回该文件 URI。"""
    target = join_uri(output_uri, PREDICTIONS_FILENAME)
    rows = [r.to_parquet_row() for r in records]
    table = _rows_to_table(rows, PREDICTIONS_SCHEMA)
    return write_table_to_uri(target, table)


def read_predictions(uri: str) -> list[dict[str, Any]]:
    """读 predictions.parquet（接受目录或文件 URI），返回 dict 列表。"""
    file_uri = uri if uri.endswith(".parquet") else join_uri(uri, PREDICTIONS_FILENAME)
    table = read_table_from_uri(file_uri)
    return table.to_pylist()


def write_outputs_pg(engine: Engine, records: list[InferenceOutputRecord]) -> int:
    """批量写 inference_outputs；best-effort，失败仅 warning。返回写入条数。"""
    if not records:
        return 0
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            for rec in records:
                session.add(InferenceOutputRow(**rec.to_row_kwargs()))
            session.commit()
        return len(records)
    except SQLAlchemyError as err:
        LOG.warning("inference_outputs PG 写入失败：%s", err)
        return 0


def _rows_to_table(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    """按 schema 列序构造 pa.Table，空行也产出带 schema 的空表。"""
    columns: dict[str, list[Any]] = {name: [] for name in schema.names}
    for row in rows:
        for name in schema.names:
            columns[name].append(row.get(name))
    arrays = [pa.array(columns[field.name], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)
