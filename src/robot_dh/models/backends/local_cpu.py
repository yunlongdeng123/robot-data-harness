"""LocalCPUBackend：纯 CPU 的轻量本地推理，不引入 sklearn / torch。

- anomaly_scorer：基于已有质量指标（quality_score / max_velocity_mps / null_rate）打分。
- embedding：基于字符 hash 的确定性向量（不是语义向量，仅占位 / 联调用）。
- caption：从 metadata 拼一句结构化描述。

刻意只用标准库 + hash，保证无任何重依赖、确定性、可在 CI / 无网环境跑通。
"""

from __future__ import annotations

import hashlib
import math
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

LOCAL_EMBEDDING_DIM = 32


def _hash_bytes(seed: str) -> bytes:
    return hashlib.sha256(seed.encode("utf-8")).digest()


def _hashing_embedding(text: str, dim: int = LOCAL_EMBEDDING_DIM) -> list[float]:
    """feature hashing：把 token 散列到 dim 维并 L2 归一化（确定性，无需训练）。"""
    vec = [0.0] * dim
    tokens = [t for t in text.replace("/", " ").replace(":", " ").split() if t]
    if not tokens:
        tokens = [text or "empty"]
    for tok in tokens:
        h = _hash_bytes(tok)
        idx = h[0] % dim
        sign = 1.0 if (h[1] & 1) == 0 else -1.0
        vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 0:
        vec = [round(v / norm, 6) for v in vec]
    return vec


class LocalCPUBackend(BaseModelBackend):
    name = "local_cpu"

    def health(self, model: ModelSpec) -> BackendHealth:
        return BackendHealth(
            status="PASS",
            backend=self.name,
            model_id=model.model_id,
            detail="local_cpu backend 可用（纯标准库，无外部依赖）",
            latency_ms=0.0,
        )

    def predict_batch(
        self,
        samples: list[InferenceSample],
        model: ModelSpec,
        config: dict[str, Any],
    ) -> list[InferencePrediction]:
        return [self._predict_one(s, model) for s in samples]

    def _predict_one(self, sample: InferenceSample, model: ModelSpec) -> InferencePrediction:
        started = time.perf_counter()
        model_type = (model.model_type or "").lower()

        if model_type == "embedding":
            basis = sample.input_text or sample.input_uri or sample.sample_id
            vec = _hashing_embedding(basis)
            return InferencePrediction(
                sample_id=sample.sample_id,
                prediction_type="embedding",
                prediction_json={"embedding": vec, "dim": len(vec), "method": "feature_hashing"},
                latency_ms=(time.perf_counter() - started) * 1000.0,
                status=PREDICTION_OK,
            )

        if model_type == "anomaly_scorer":
            score, factors = self._rule_anomaly_score(sample)
            return InferencePrediction(
                sample_id=sample.sample_id,
                prediction_type="anomaly_score",
                prediction_json={
                    "anomaly_score": score,
                    "label": "anomaly" if score >= 0.5 else "normal",
                    "factors": factors,
                },
                confidence=score,
                latency_ms=(time.perf_counter() - started) * 1000.0,
                status=PREDICTION_OK,
            )

        # caption fallback：从 metadata 拼描述。
        dataset = sample.dataset_id or "unknown"
        episode = sample.episode_id or "?"
        text = f"Episode {episode} of dataset {dataset} processed by local_cpu rule captioner."
        return InferencePrediction(
            sample_id=sample.sample_id,
            prediction_type="caption",
            prediction_json={"text": text},
            confidence=0.5,
            latency_ms=(time.perf_counter() - started) * 1000.0,
            token_count=len(text.split()),
            status=PREDICTION_OK,
        )

    @staticmethod
    def _rule_anomaly_score(sample: InferenceSample) -> tuple[float, dict[str, Any]]:
        """规则打分：综合 quality_score（越低越异常）、max_velocity_mps、null_rate。"""
        meta = sample.metadata or {}
        factors: dict[str, Any] = {}
        score = 0.0
        weight = 0.0

        q = meta.get("quality_score")
        if q is not None:
            try:
                qf = max(0.0, min(1.0, float(q)))
                score += (1.0 - qf) * 0.5
                weight += 0.5
                factors["quality_score"] = qf
            except (TypeError, ValueError):
                pass

        v = meta.get("max_velocity_mps")
        if v is not None:
            try:
                vf = max(0.0, min(1.0, float(v) / 2.0))
                score += vf * 0.3
                weight += 0.3
                factors["max_velocity_mps"] = float(v)
            except (TypeError, ValueError):
                pass

        nr = meta.get("null_rate")
        if nr is not None:
            try:
                nf = max(0.0, min(1.0, float(nr)))
                score += nf * 0.2
                weight += 0.2
                factors["null_rate"] = nf
            except (TypeError, ValueError):
                pass

        if weight <= 0:
            # 无可用指标时退到确定性 hash，保证仍有输出。
            h = _hash_bytes(sample.sample_id)
            return round((h[0] % 1000) / 1000.0, 6), {"source": "hash_fallback"}
        return round(max(0.0, min(1.0, score / weight)), 6), factors
