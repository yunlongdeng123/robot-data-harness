"""推理 benchmark：对 concurrency × batch_size 网格做小规模推理并汇总吞吐 / 时延。

产物（output_uri 下）：benchmark_report.json / benchmark_report.html / benchmark_results.csv。
DB 可用时每个组合写一行 inference_benchmark_runs。
"""

from __future__ import annotations

import csv
import io
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robot_dh.ai_tasks.events import (
    AiTaskEvent,
    EVENT_BENCHMARK_FINISHED,
    EVENT_BENCHMARK_STARTED,
)
from robot_dh.ai_tasks.store import AiTaskStore, resolve_optional_engine
from robot_dh.inference.batch import InferenceInputBuilder, iter_batches, run_batches_concurrent
from robot_dh.inference.metrics import InferenceMetrics, compute_metrics
from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import join_uri
from robot_dh.models.backends.base import get_backend
from robot_dh.models.registry import ModelRegistry
from robot_dh.models.schemas import task_type_for_model
from robot_dh.warehouse.models import InferenceBenchmarkRunRow

LOG = logging.getLogger(__name__)

BENCHMARK_REPORT_JSON = "benchmark_report.json"
BENCHMARK_REPORT_HTML = "benchmark_report.html"
BENCHMARK_RESULTS_CSV = "benchmark_results.csv"


@dataclass
class BenchmarkCombo:
    benchmark_id: str
    concurrency: int
    batch_size: int
    metrics: InferenceMetrics
    started_at: datetime
    finished_at: datetime
    status: str = "SUCCEEDED"

    def to_dict(self) -> dict[str, Any]:
        d = {
            "benchmark_id": self.benchmark_id,
            "concurrency": self.concurrency,
            "batch_size": self.batch_size,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
        }
        d.update(self.metrics.to_dict())
        return d


@dataclass
class BenchmarkResult:
    model_id: str
    backend: str
    workload_name: str
    input_uri: str
    output_uri: str
    combos: list[BenchmarkCombo] = field(default_factory=list)
    report_uri: str = ""
    html_uri: str = ""
    csv_uri: str = ""

    def to_dict(self) -> dict[str, Any]:
        best = self.best_combo()
        return {
            "model_id": self.model_id,
            "backend": self.backend,
            "workload_name": self.workload_name,
            "input_uri": self.input_uri,
            "output_uri": self.output_uri,
            "combo_count": len(self.combos),
            "best_samples_per_sec": best.metrics.samples_per_sec if best else None,
            "best_combo": {"concurrency": best.concurrency, "batch_size": best.batch_size} if best else None,
            "combos": [c.to_dict() for c in self.combos],
        }

    def best_combo(self) -> BenchmarkCombo | None:
        ranked = [c for c in self.combos if c.metrics.samples_per_sec is not None]
        if not ranked:
            return None
        return max(ranked, key=lambda c: c.metrics.samples_per_sec or 0.0)


def run_benchmark(
    *,
    input_uri: str,
    model_id: str,
    output_uri: str,
    concurrency_list: list[int],
    batch_size_list: list[int],
    limit: int = 200,
    workload_name: str | None = None,
    task_type: str | None = None,
    db_uri: str | None = None,
    local_only: bool = False,
    registry: ModelRegistry | None = None,
) -> BenchmarkResult:
    """跑 benchmark 网格并写报告。"""
    registry = registry or ModelRegistry(db_uri=db_uri, local_only=local_only)
    spec = registry.get(model_id)
    if spec is None:
        raise ValueError(f"模型未注册：{model_id}")
    resolved_task = task_type or task_type_for_model(spec.model_type)

    engine = None if local_only else resolve_optional_engine(db_uri)
    events = AiTaskStore(db_uri=db_uri, local_only=local_only)
    backend = get_backend(spec)

    samples = InferenceInputBuilder(input_uri=input_uri, split="all", limit=limit).build()
    workload = workload_name or f"{model_id}-{resolved_task}"

    events.emit(AiTaskEvent(
        event_type=EVENT_BENCHMARK_STARTED, model_id=model_id,
        payload={"input_uri": input_uri, "concurrency": concurrency_list, "batch_size": batch_size_list},
    ))

    result = BenchmarkResult(
        model_id=model_id, backend=spec.backend, workload_name=workload,
        input_uri=input_uri, output_uri=output_uri,
    )
    config: dict[str, Any] = {"timeout_sec": spec.timeout_sec}
    for concurrency in concurrency_list:
        for batch_size in batch_size_list:
            combo = _run_combo(
                backend=backend, spec=spec, samples=samples,
                concurrency=concurrency, batch_size=batch_size, config=config,
            )
            result.combos.append(combo)
            if engine is not None:
                _write_benchmark_pg(engine, result, combo, resolved_task)

    # 写报告。
    store = create_lake_store(output_uri)
    payload = result.to_dict()
    result.report_uri = store.write_json(join_uri(output_uri, BENCHMARK_REPORT_JSON), payload)
    result.csv_uri = store.write_text(join_uri(output_uri, BENCHMARK_RESULTS_CSV), _to_csv(result))
    result.html_uri = store.write_text(join_uri(output_uri, BENCHMARK_REPORT_HTML), _to_html(result))

    events.emit(AiTaskEvent(
        event_type=EVENT_BENCHMARK_FINISHED, model_id=model_id,
        payload={"combo_count": len(result.combos), "report_uri": result.report_uri},
    ))
    return result


def _run_combo(
    *,
    backend: Any,
    spec: Any,
    samples: list[Any],
    concurrency: int,
    batch_size: int,
    config: dict[str, Any],
) -> BenchmarkCombo:
    batches = list(iter_batches(samples, batch_size))

    def predict_fn(batch: list[Any]) -> list[Any]:
        return backend.predict_batch(batch, spec, config)

    started = datetime.now(timezone.utc)
    results = run_batches_concurrent(batches, predict_fn, max_workers=concurrency)
    finished = datetime.now(timezone.utc)
    preds = [p for batch_preds in results for p in batch_preds]
    duration = (finished - started).total_seconds()
    metrics = compute_metrics(preds, duration)
    return BenchmarkCombo(
        benchmark_id=f"bench-{uuid.uuid4().hex[:16]}",
        concurrency=concurrency, batch_size=batch_size,
        metrics=metrics, started_at=started, finished_at=finished,
    )


def _write_benchmark_pg(
    engine: Engine,
    result: BenchmarkResult,
    combo: BenchmarkCombo,
    task_type: str,
) -> None:
    m = combo.metrics
    try:
        with Session(engine, expire_on_commit=False, future=True) as session:
            session.add(
                InferenceBenchmarkRunRow(
                    benchmark_id=combo.benchmark_id,
                    model_id=result.model_id,
                    backend=result.backend,
                    workload_name=result.workload_name,
                    input_uri=result.input_uri,
                    status=combo.status,
                    concurrency=combo.concurrency,
                    batch_size=combo.batch_size,
                    duration_sec=m.duration_sec,
                    total_samples=m.total_samples,
                    succeeded_samples=m.succeeded_samples,
                    failed_samples=m.failed_samples,
                    samples_per_sec=m.samples_per_sec,
                    p50_latency_ms=m.p50_latency_ms,
                    p95_latency_ms=m.p95_latency_ms,
                    p99_latency_ms=m.p99_latency_ms,
                    error_rate=m.error_rate,
                    cost_estimate_json=None,
                    metrics_json={"task_type": task_type, **m.to_dict()},
                    started_at=combo.started_at,
                    finished_at=combo.finished_at,
                )
            )
            session.commit()
    except SQLAlchemyError as err:
        LOG.warning("inference_benchmark_runs PG 写入失败：%s", err)


_CSV_COLUMNS = [
    "benchmark_id", "concurrency", "batch_size", "total_samples",
    "samples_per_sec", "avg_latency_ms", "p50_latency_ms", "p95_latency_ms",
    "p99_latency_ms", "error_rate", "duration_sec", "status",
]


def _to_csv(result: BenchmarkResult) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for combo in result.combos:
        writer.writerow(combo.to_dict())
    return buf.getvalue()


def _to_html(result: BenchmarkResult) -> str:
    rows = []
    for combo in result.combos:
        d = combo.to_dict()
        cells = "".join(f"<td>{d.get(col, '')}</td>" for col in _CSV_COLUMNS)
        rows.append(f"<tr>{cells}</tr>")
    headers = "".join(f"<th>{col}</th>" for col in _CSV_COLUMNS)
    best = result.best_combo()
    best_line = (
        f"best samples/sec={best.metrics.samples_per_sec} @ concurrency={best.concurrency}, batch={best.batch_size}"
        if best else "no data"
    )
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>inference benchmark {result.model_id}</title></head><body>"
        f"<h1>Inference Benchmark: {result.model_id}</h1>"
        f"<p>backend={result.backend} workload={result.workload_name}</p>"
        f"<p>input_uri={result.input_uri}</p>"
        f"<p><b>{best_line}</b></p>"
        f"<table border='1' cellpadding='4' cellspacing='0'><thead><tr>{headers}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></body></html>"
    )
