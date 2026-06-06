"""inference_report.json 数据类与构造。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from robot_dh.inference.metrics import InferenceMetrics


@dataclass
class InferenceReport:
    """单个推理任务的报告（落 output_uri/inference_report.json）。"""

    job_id: str
    model_id: str
    input_uri: str
    output_uri: str
    task_type: str
    backend: str
    total_samples: int
    succeeded_samples: int
    failed_samples: int
    duration_sec: float
    samples_per_sec: float | None
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    p99_latency_ms: float | None
    error_rate: float
    status: str
    error_summary: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "model_id": self.model_id,
            "input_uri": self.input_uri,
            "output_uri": self.output_uri,
            "task_type": self.task_type,
            "backend": self.backend,
            "total_samples": self.total_samples,
            "succeeded_samples": self.succeeded_samples,
            "failed_samples": self.failed_samples,
            "duration_sec": round(self.duration_sec, 6),
            "samples_per_sec": _round(self.samples_per_sec),
            "p50_latency_ms": _round(self.p50_latency_ms),
            "p95_latency_ms": _round(self.p95_latency_ms),
            "p99_latency_ms": _round(self.p99_latency_ms),
            "error_rate": round(self.error_rate, 6),
            "status": self.status,
            "error_summary": dict(self.error_summary),
        }


def build_report(
    *,
    job_id: str,
    model_id: str,
    input_uri: str,
    output_uri: str,
    task_type: str,
    backend: str,
    metrics: InferenceMetrics,
    status: str,
    error_summary: dict[str, int],
) -> InferenceReport:
    return InferenceReport(
        job_id=job_id,
        model_id=model_id,
        input_uri=input_uri,
        output_uri=output_uri,
        task_type=task_type,
        backend=backend,
        total_samples=metrics.total_samples,
        succeeded_samples=metrics.succeeded_samples,
        failed_samples=metrics.failed_samples,
        duration_sec=metrics.duration_sec,
        samples_per_sec=metrics.samples_per_sec,
        p50_latency_ms=metrics.p50_latency_ms,
        p95_latency_ms=metrics.p95_latency_ms,
        p99_latency_ms=metrics.p99_latency_ms,
        error_rate=metrics.error_rate,
        status=status,
        error_summary=dict(error_summary),
    )


def _round(value: float | None, ndigits: int = 4) -> float | None:
    return round(value, ndigits) if value is not None else None
