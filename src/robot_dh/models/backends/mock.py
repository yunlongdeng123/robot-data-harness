"""MockBackend：确定性假推理，不调用任何外部服务。

用途：链路联调 / CI / 单测。同一份输入永远产出同样的预测，便于断言。
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from robot_dh.models.backends.base import BaseModelBackend
from robot_dh.models.schemas import (
    BackendHealth,
    InferencePrediction,
    InferenceSample,
    ModelSpec,
    PREDICTION_OK,
)

# embedding 维度（mock 固定 16 维，见 v1_9_promptB 第四节）。
MOCK_EMBEDDING_DIM = 16


def _stable_unit_float(seed: str) -> float:
    """由字符串稳定映射到 [0, 1) 的 float。"""
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    # 取前 8 字节当无符号整数再归一化。
    value = int.from_bytes(digest[:8], "big")
    return (value % 1_000_000) / 1_000_000.0


def _mock_embedding(sample_id: str, dim: int = MOCK_EMBEDDING_DIM) -> list[float]:
    """基于 sample_id hash 生成确定性 dim 维向量，元素落在 [-1, 1)。"""
    out: list[float] = []
    for i in range(dim):
        out.append(round(_stable_unit_float(f"{sample_id}:{i}") * 2.0 - 1.0, 6))
    return out


class MockBackend(BaseModelBackend):
    name = "mock"

    def health(self, model: ModelSpec) -> BackendHealth:
        return BackendHealth(
            status="PASS",
            backend=self.name,
            model_id=model.model_id,
            detail="mock backend 始终可用（无外部依赖）",
            latency_ms=0.0,
        )

    def predict_batch(
        self,
        samples: list[InferenceSample],
        model: ModelSpec,
        config: dict[str, Any],
    ) -> list[InferencePrediction]:
        out: list[InferencePrediction] = []
        for sample in samples:
            out.append(self._predict_one(sample, model))
        return out

    def _predict_one(self, sample: InferenceSample, model: ModelSpec) -> InferencePrediction:
        started = time.perf_counter()
        model_type = (model.model_type or "mock").lower()

        if model_type == "embedding":
            vec = _mock_embedding(sample.sample_id)
            latency = (time.perf_counter() - started) * 1000.0
            return InferencePrediction(
                sample_id=sample.sample_id,
                prediction_type="embedding",
                prediction_json={"embedding": vec, "dim": len(vec)},
                confidence=None,
                latency_ms=latency,
                status=PREDICTION_OK,
            )

        if model_type in ("anomaly_scorer",):
            score = self._anomaly_score(sample)
            latency = (time.perf_counter() - started) * 1000.0
            return InferencePrediction(
                sample_id=sample.sample_id,
                prediction_type="anomaly_score",
                prediction_json={
                    "anomaly_score": score,
                    "label": "anomaly" if score >= 0.5 else "normal",
                },
                confidence=score,
                latency_ms=latency,
                status=PREDICTION_OK,
            )

        # caption / vlm / llm / mock 默认走确定性 caption。
        dataset = sample.dataset_id or "unknown"
        text = f"A robot manipulation episode from dataset {dataset}."
        latency = (time.perf_counter() - started) * 1000.0
        return InferencePrediction(
            sample_id=sample.sample_id,
            prediction_type="caption",
            prediction_json={"text": text},
            confidence=round(0.5 + 0.5 * _stable_unit_float(sample.sample_id), 6),
            latency_ms=latency,
            token_count=len(text.split()),
            status=PREDICTION_OK,
        )

    @staticmethod
    def _anomaly_score(sample: InferenceSample) -> float:
        """优先用 metadata 里的 quality_score / max_velocity_mps 推导；缺省走 sample_id hash。"""
        meta = sample.metadata or {}
        q = meta.get("quality_score")
        v = meta.get("max_velocity_mps")
        if q is not None:
            try:
                # quality 越低越异常。
                return round(max(0.0, min(1.0, 1.0 - float(q))), 6)
            except (TypeError, ValueError):
                pass
        if v is not None:
            try:
                # 速度越高越异常（2.0 m/s 归一化到 1.0）。
                return round(max(0.0, min(1.0, float(v) / 2.0)), 6)
            except (TypeError, ValueError):
                pass
        return round(_stable_unit_float(sample.sample_id), 6)
