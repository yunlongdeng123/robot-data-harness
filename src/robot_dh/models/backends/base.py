"""推理 backend 抽象 + 工厂。

每个 backend 无状态：health / predict_batch 都显式接收 ModelSpec，方便一个进程内复用
同一个 backend 实例服务多个模型。get_backend() 按 ModelSpec.backend 分发。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from robot_dh.models.schemas import (
    BackendHealth,
    InferencePrediction,
    InferenceSample,
    ModelSpec,
)


class BaseModelBackend(ABC):
    """所有推理 backend 的基类。"""

    name: str = "base"

    @abstractmethod
    def health(self, model: ModelSpec) -> BackendHealth:
        """轻量健康检查；不应抛异常，失败时返回 status=FAIL 的 BackendHealth。"""

    @abstractmethod
    def predict_batch(
        self,
        samples: list[InferenceSample],
        model: ModelSpec,
        config: dict[str, Any],
    ) -> list[InferencePrediction]:
        """对一个 batch 的样本推理；返回与 samples 等长、同序的预测列表。

        约定：单样本失败不抛异常，而是返回 status=FAILED 的 InferencePrediction，
        由上层 runner 决定是否 fail-fast。
        """


class BackendNotAvailableError(RuntimeError):
    """请求的 backend 在主项目中不可直接执行（如 autodl_worker）。"""


def get_backend(model: ModelSpec) -> BaseModelBackend:
    """按 model.backend 返回 backend 实例。

    主项目内置 mock / local_cpu / openai_compatible 三类；
    autodl_worker 由 workers/autodl_inference_worker 执行，http_json 暂未在主项目实现。
    """
    backend = (model.backend or "").lower()
    if backend == "mock":
        from robot_dh.models.backends.mock import MockBackend

        return MockBackend()
    if backend == "local_cpu":
        from robot_dh.models.backends.local_cpu import LocalCPUBackend

        return LocalCPUBackend()
    if backend == "openai_compatible":
        from robot_dh.models.backends.openai_compatible import OpenAICompatibleBackend

        return OpenAICompatibleBackend()
    if backend == "autodl_worker":
        raise BackendNotAvailableError(
            "backend=autodl_worker 由 workers/autodl_inference_worker 以 pull-based 方式执行，"
            "主项目不直接调用；本地请用 mock / local_cpu / openai_compatible。"
        )
    raise BackendNotAvailableError(
        f"backend={model.backend!r} 暂未在主项目实现；支持 mock / local_cpu / openai_compatible。"
    )
