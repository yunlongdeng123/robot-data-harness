"""推理指标：吞吐 / 时延分位 / 错误率聚合。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from robot_dh.models.schemas import InferencePrediction, PREDICTION_OK


def percentile(values: list[float], q: float) -> float | None:
    """线性插值分位数；空列表返回 None。q ∈ [0, 1]。"""
    clean = sorted(v for v in values if v is not None)
    if not clean:
        return None
    if len(clean) == 1:
        return float(clean[0])
    idx = q * (len(clean) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(clean) - 1)
    frac = idx - lo
    return float(clean[lo] + (clean[hi] - clean[lo]) * frac)


@dataclass
class InferenceMetrics:
    """单次推理（或 benchmark 单组合）的聚合指标。"""

    total_samples: int
    succeeded_samples: int
    failed_samples: int
    duration_sec: float
    samples_per_sec: float | None
    avg_latency_ms: float | None
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    p99_latency_ms: float | None
    error_rate: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_samples": self.total_samples,
            "succeeded_samples": self.succeeded_samples,
            "failed_samples": self.failed_samples,
            "duration_sec": round(self.duration_sec, 6),
            "samples_per_sec": _round(self.samples_per_sec),
            "avg_latency_ms": _round(self.avg_latency_ms),
            "p50_latency_ms": _round(self.p50_latency_ms),
            "p95_latency_ms": _round(self.p95_latency_ms),
            "p99_latency_ms": _round(self.p99_latency_ms),
            "error_rate": round(self.error_rate, 6),
        }


def compute_metrics(
    predictions: Iterable[InferencePrediction],
    duration_sec: float,
) -> InferenceMetrics:
    """从预测列表 + 墙钟耗时聚合指标。"""
    preds = list(predictions)
    total = len(preds)
    succeeded = sum(1 for p in preds if p.status == PREDICTION_OK)
    failed = total - succeeded
    latencies = [p.latency_ms for p in preds if p.latency_ms is not None]
    avg = (sum(latencies) / len(latencies)) if latencies else None
    sps = (total / duration_sec) if duration_sec > 0 else None
    error_rate = (failed / total) if total > 0 else 0.0
    return InferenceMetrics(
        total_samples=total,
        succeeded_samples=succeeded,
        failed_samples=failed,
        duration_sec=duration_sec,
        samples_per_sec=sps,
        avg_latency_ms=avg,
        p50_latency_ms=percentile(latencies, 0.50),
        p95_latency_ms=percentile(latencies, 0.95),
        p99_latency_ms=percentile(latencies, 0.99),
        error_rate=error_rate,
    )


def _round(value: float | None, ndigits: int = 4) -> float | None:
    return round(value, ndigits) if value is not None else None
