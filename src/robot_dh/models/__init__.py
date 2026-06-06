"""v1.9 模型注册 + 推理 backend 抽象。"""

from robot_dh.models.backends import (
    BackendNotAvailableError,
    BaseModelBackend,
    LocalCPUBackend,
    MockBackend,
    OpenAICompatibleBackend,
    get_backend,
)
from robot_dh.models.registry import ModelRegistry
from robot_dh.models.schemas import (
    BACKENDS,
    MODEL_TYPES,
    BackendHealth,
    InferencePrediction,
    InferenceSample,
    ModelSpec,
    prediction_type_for_task,
    task_type_for_model,
)

__all__ = [
    "ModelRegistry",
    "ModelSpec",
    "InferenceSample",
    "InferencePrediction",
    "BackendHealth",
    "BaseModelBackend",
    "MockBackend",
    "LocalCPUBackend",
    "OpenAICompatibleBackend",
    "BackendNotAvailableError",
    "get_backend",
    "MODEL_TYPES",
    "BACKENDS",
    "task_type_for_model",
    "prediction_type_for_task",
]
