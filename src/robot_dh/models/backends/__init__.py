"""推理 backend 实现集合。"""

from robot_dh.models.backends.base import (
    BackendNotAvailableError,
    BaseModelBackend,
    get_backend,
)
from robot_dh.models.backends.local_cpu import LocalCPUBackend
from robot_dh.models.backends.mock import MockBackend
from robot_dh.models.backends.openai_compatible import (
    ERR_BAD_RESPONSE,
    ERR_ENDPOINT_UNAVAILABLE,
    ERR_TIMEOUT,
    OpenAIBackendError,
    OpenAICompatibleBackend,
)

__all__ = [
    "BaseModelBackend",
    "BackendNotAvailableError",
    "get_backend",
    "MockBackend",
    "LocalCPUBackend",
    "OpenAICompatibleBackend",
    "OpenAIBackendError",
    "ERR_ENDPOINT_UNAVAILABLE",
    "ERR_TIMEOUT",
    "ERR_BAD_RESPONSE",
]
