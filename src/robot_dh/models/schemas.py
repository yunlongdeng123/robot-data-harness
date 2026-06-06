"""模型注册 / 推理的核心数据类与取值约定。

这些 dataclass 是 models / inference / distill 三个子系统的公共契约：
- ModelSpec        对应 model_registry 一行
- InferenceSample  单条推理输入
- InferencePrediction 单条推理输出
- BackendHealth    backend 健康检查结果

刻意不引入 pydantic（与 warehouse_metrics 一致），用 dataclass + 显式 to_dict/from_dict。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# model_registry.model_type 取值（见 v1_9_promptB 第三节）。
MODEL_TYPES: tuple[str, ...] = (
    "caption",
    "embedding",
    "anomaly_scorer",
    "vlm",
    "llm",
    "mock",
)

# model_registry.backend 取值。autodl_worker 仅预留，主项目不实现真实 worker（见 workers/）。
BACKENDS: tuple[str, ...] = (
    "mock",
    "local_cpu",
    "openai_compatible",
    "autodl_worker",
    "http_json",
)

# 模型状态。ACTIVE 可用；DISABLED / INACTIVE / DEPRECATED 不参与新任务。
MODEL_STATUSES: tuple[str, ...] = ("ACTIVE", "DISABLED", "INACTIVE", "DEPRECATED")

# infer run --task-type 取值；与 model_type 的对应见 task_type_for_model()。
TASK_TYPES: tuple[str, ...] = ("caption", "embedding", "anomaly_score")

# 单样本预测状态。
PREDICTION_OK = "OK"
PREDICTION_FAILED = "FAILED"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def task_type_for_model(model_type: str) -> str:
    """把 model_type 映射到默认 task_type（caption/embedding/anomaly_score）。"""
    mt = (model_type or "").lower()
    if mt == "embedding":
        return "embedding"
    if mt in ("anomaly_scorer", "anomaly"):
        return "anomaly_score"
    # caption / vlm / llm / mock 默认走 caption（文本生成）。
    return "caption"


def prediction_type_for_task(task_type: str) -> str:
    """把 task_type 映射到 inference_outputs.prediction_type。"""
    tt = (task_type or "").lower()
    if tt in ("embedding",):
        return "embedding"
    if tt in ("anomaly_score", "anomaly"):
        return "anomaly_score"
    return "caption"


@dataclass
class ModelSpec:
    """model_registry 一行的内存表示。"""

    model_id: str
    model_name: str
    model_type: str
    backend: str
    endpoint_url: str | None = None
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)
    max_batch_size: int = 32
    timeout_sec: int = 60
    status: str = "ACTIVE"
    tags: dict[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    updated_at: str | None = None

    def __post_init__(self) -> None:
        if self.model_type not in MODEL_TYPES:
            raise ValueError(
                f"未知 model_type={self.model_type!r}；允许：{', '.join(MODEL_TYPES)}"
            )
        if self.backend not in BACKENDS:
            raise ValueError(
                f"未知 backend={self.backend!r}；允许：{', '.join(BACKENDS)}"
            )
        if self.status not in MODEL_STATUSES:
            raise ValueError(
                f"未知 status={self.status!r}；允许：{', '.join(MODEL_STATUSES)}"
            )

    @property
    def is_active(self) -> bool:
        return self.status == "ACTIVE"

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "model_name": self.model_name,
            "model_type": self.model_type,
            "backend": self.backend,
            "endpoint_url": self.endpoint_url,
            "input_schema": dict(self.input_schema),
            "output_schema": dict(self.output_schema),
            "max_batch_size": self.max_batch_size,
            "timeout_sec": self.timeout_sec,
            "status": self.status,
            "tags": dict(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelSpec":
        return cls(
            model_id=str(data["model_id"]),
            model_name=str(data.get("model_name") or data["model_id"]),
            model_type=str(data.get("model_type") or "mock"),
            backend=str(data.get("backend") or "mock"),
            endpoint_url=data.get("endpoint_url"),
            input_schema=dict(data.get("input_schema") or {}),
            output_schema=dict(data.get("output_schema") or {}),
            max_batch_size=int(data.get("max_batch_size") or 32),
            timeout_sec=int(data.get("timeout_sec") or 60),
            status=str(data.get("status") or "ACTIVE"),
            tags=dict(data.get("tags") or {}),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )

    def to_row_kwargs(self) -> dict[str, Any]:
        """转成 ModelRegistryRow 构造参数（jsonb 列直接放 dict）。"""
        return {
            "model_id": self.model_id,
            "model_name": self.model_name,
            "model_type": self.model_type,
            "backend": self.backend,
            "endpoint_url": self.endpoint_url,
            "input_schema_json": dict(self.input_schema),
            "output_schema_json": dict(self.output_schema),
            "max_batch_size": self.max_batch_size,
            "timeout_sec": self.timeout_sec,
            "status": self.status,
            "tags_json": dict(self.tags),
        }

    @classmethod
    def from_row(cls, row: Any) -> "ModelSpec":
        """从 ModelRegistryRow ORM 对象构造。"""
        return cls(
            model_id=row.model_id,
            model_name=row.model_name,
            model_type=row.model_type,
            backend=row.backend,
            endpoint_url=row.endpoint_url,
            input_schema=dict(row.input_schema_json or {}),
            output_schema=dict(row.output_schema_json or {}),
            max_batch_size=int(row.max_batch_size or 32),
            timeout_sec=int(row.timeout_sec or 60),
            status=row.status,
            tags=dict(row.tags_json or {}),
            created_at=row.created_at.isoformat() if getattr(row, "created_at", None) else None,
            updated_at=row.updated_at.isoformat() if getattr(row, "updated_at", None) else None,
        )


@dataclass
class InferenceSample:
    """单条推理输入样本。"""

    sample_id: str
    dataset_id: str | None = None
    version: str | None = None
    episode_id: str | None = None
    frame_id: str | None = None
    input_uri: str | None = None
    input_text: str | None = None
    input_refs: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "dataset_id": self.dataset_id,
            "version": self.version,
            "episode_id": self.episode_id,
            "frame_id": self.frame_id,
            "input_uri": self.input_uri,
            "input_text": self.input_text,
            "input_refs": list(self.input_refs),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "InferenceSample":
        return cls(
            sample_id=str(data["sample_id"]),
            dataset_id=data.get("dataset_id"),
            version=data.get("version"),
            episode_id=data.get("episode_id"),
            frame_id=data.get("frame_id"),
            input_uri=data.get("input_uri"),
            input_text=data.get("input_text"),
            input_refs=list(data.get("input_refs") or []),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass
class InferencePrediction:
    """单条推理输出。"""

    sample_id: str
    prediction_type: str
    prediction_json: dict[str, Any] = field(default_factory=dict)
    confidence: float | None = None
    latency_ms: float | None = None
    token_count: int | None = None
    status: str = PREDICTION_OK
    error_message: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == PREDICTION_OK

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "prediction_type": self.prediction_type,
            "prediction_json": dict(self.prediction_json),
            "confidence": self.confidence,
            "latency_ms": self.latency_ms,
            "token_count": self.token_count,
            "status": self.status,
            "error_message": self.error_message,
        }


@dataclass
class BackendHealth:
    """backend 健康检查结果。"""

    status: str  # PASS / FAIL
    backend: str
    model_id: str
    detail: str = ""
    latency_ms: float | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "PASS"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "backend": self.backend,
            "model_id": self.model_id,
            "detail": self.detail,
            "latency_ms": self.latency_ms,
            "error": self.error,
        }
