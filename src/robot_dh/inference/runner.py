"""推理任务编排：build samples -> batch -> backend -> 写产物 + 回流 PG。

产物（output_uri 下）：
    predictions.parquet / failed_samples.parquet / inference_report.json / _manifest.json

DB 可用时回流：inference_jobs / inference_outputs / inference_failures / ai_task_events。
DB 不可用时仍完整产出本地产物（local-first）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from robot_dh.ai_tasks.events import (
    AiTaskEvent,
    EVENT_INFERENCE_BATCH_FINISHED,
    EVENT_INFERENCE_JOB_CREATED,
    EVENT_INFERENCE_JOB_FAILED,
    EVENT_INFERENCE_JOB_FINISHED,
    EVENT_INFERENCE_JOB_STARTED,
)
from robot_dh.ai_tasks.state import JOB_RUNNING, evaluate_job_outcome
from robot_dh.ai_tasks.store import AiTaskStore, resolve_optional_engine
from robot_dh.inference.batch import (
    InferenceInputBuilder,
    iter_batches,
    run_batches_concurrent,
)
from robot_dh.inference.failures import failure_from, write_failed_samples, write_failures_pg
from robot_dh.inference.job import InferenceJob, new_job_id, write_job_pg
from robot_dh.inference.metrics import compute_metrics
from robot_dh.inference.outputs import (
    record_from_prediction,
    write_outputs_pg,
    write_predictions,
)
from robot_dh.inference.report import build_report
from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import join_uri
from robot_dh.models.registry import ModelRegistry
from robot_dh.models.schemas import (
    InferencePrediction,
    InferenceSample,
    ModelSpec,
    PREDICTION_OK,
    task_type_for_model,
)

LOG = logging.getLogger(__name__)

DEFAULT_MAX_ERROR_RATE = 0.5
MANIFEST_FILENAME = "_manifest.json"
REPORT_FILENAME = "inference_report.json"


class InferenceJobError(RuntimeError):
    """推理任务无法启动（模型缺失 / 输入不可用）。"""


@dataclass
class InferenceRunResult:
    job: InferenceJob
    report: dict[str, Any]
    predictions_uri: str
    failed_samples_uri: str
    report_uri: str
    manifest_uri: str
    exit_code: int
    warn: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job.job_id,
            "status": self.job.status,
            "exit_code": self.exit_code,
            "warn": self.warn,
            "predictions_uri": self.predictions_uri,
            "failed_samples_uri": self.failed_samples_uri,
            "report_uri": self.report_uri,
            "manifest_uri": self.manifest_uri,
            "report": self.report,
        }


def run_inference(
    *,
    input_uri: str,
    model_id: str,
    output_uri: str,
    task_type: str | None = None,
    split: str = "all",
    limit: int | None = None,
    batch_size: int | None = None,
    max_workers: int = 1,
    retry: int = 0,
    timeout_sec: int | None = None,
    fail_fast: bool = False,
    max_error_rate: float = DEFAULT_MAX_ERROR_RATE,
    dataset_id: str | None = None,
    version: str | None = None,
    db_uri: str | None = None,
    local_only: bool = False,
    record_to_registry: bool = False,
    registry: ModelRegistry | None = None,
) -> InferenceRunResult:
    """执行一次批量推理，返回结果（含 exit_code）。"""
    registry = registry or ModelRegistry(db_uri=db_uri, local_only=local_only)
    spec = registry.get(model_id)
    if spec is None:
        raise InferenceJobError(f"模型未注册：{model_id}（先 robot-dh model register）")

    resolved_task = task_type or task_type_for_model(spec.model_type)
    engine = None if local_only else resolve_optional_engine(db_uri)
    events = AiTaskStore(db_uri=db_uri, local_only=local_only)

    effective_batch = int(batch_size or spec.max_batch_size or 32)
    job = InferenceJob(
        job_id=new_job_id(),
        model_id=model_id,
        input_uri=input_uri,
        output_uri=output_uri,
        task_type=resolved_task,
        dataset_id=dataset_id,
        version=version,
        batch_size=effective_batch,
        max_workers=max_workers,
        input_format="parquet",
        output_format="parquet",
        config={
            "split": split,
            "limit": limit,
            "retry": retry,
            "timeout_sec": timeout_sec or spec.timeout_sec,
            "fail_fast": fail_fast,
            "max_error_rate": max_error_rate,
            "backend": spec.backend,
            "record_to_registry": record_to_registry,
        },
    )
    if engine is not None:
        write_job_pg(engine, job)
    events.emit(AiTaskEvent(
        event_type=EVENT_INFERENCE_JOB_CREATED,
        job_id=job.job_id, model_id=model_id, dataset_id=dataset_id, version=version,
        payload={"input_uri": input_uri, "output_uri": output_uri, "task_type": resolved_task},
    ))

    # ---------- 构建样本 ----------
    builder = InferenceInputBuilder(
        input_uri=input_uri, split=split, limit=limit,
        dataset_id=dataset_id, version=version,
    )
    samples = builder.build()
    job.total_samples = len(samples)
    job.status = JOB_RUNNING
    job.started_at = datetime.now(timezone.utc)
    if engine is not None:
        write_job_pg(engine, job)
    events.emit(AiTaskEvent(
        event_type=EVENT_INFERENCE_JOB_STARTED,
        job_id=job.job_id, model_id=model_id,
        payload={"total_samples": job.total_samples},
    ))

    # ---------- 推理 ----------
    started = datetime.now(timezone.utc)
    predictions, fail_fast_triggered = _execute(
        samples=samples, spec=spec, job=job, retry=retry,
        max_workers=max_workers, fail_fast=fail_fast, events=events,
    )
    duration = (datetime.now(timezone.utc) - started).total_seconds()

    # ---------- 聚合 + 记录 ----------
    output_records = [
        record_from_prediction(
            job_id=job.job_id, model_id=model_id, sample=s, prediction=p,
            output_uri=output_uri,
        )
        for s, p in zip(samples, predictions)
    ]
    failed_records = [
        failure_from(job_id=job.job_id, model_id=model_id, sample=s, prediction=p)
        for s, p in zip(samples, predictions)
        if p.status != PREDICTION_OK
    ]
    metrics = compute_metrics(predictions, duration)
    error_summary: dict[str, int] = {}
    for fr in failed_records:
        key = fr.error_type or "UNKNOWN"
        error_summary[key] = error_summary.get(key, 0) + 1

    outcome = evaluate_job_outcome(
        total=metrics.total_samples, failed=metrics.failed_samples,
        max_error_rate=max_error_rate, fail_fast_triggered=fail_fast_triggered,
    )
    job.processed_samples = metrics.total_samples
    job.failed_samples = metrics.failed_samples
    job.status = outcome.status
    job.finished_at = datetime.now(timezone.utc)
    job.duration_sec = duration
    job.metrics = metrics.to_dict()
    if metrics.failed_samples:
        job.error_message = f"{metrics.failed_samples} samples failed; top={_top_error(error_summary)}"

    # ---------- 写产物 ----------
    predictions_uri = write_predictions(output_uri, output_records)
    failed_uri = write_failed_samples(output_uri, failed_records)
    report = build_report(
        job_id=job.job_id, model_id=model_id, input_uri=input_uri, output_uri=output_uri,
        task_type=resolved_task, backend=spec.backend, metrics=metrics,
        status=job.status, error_summary=error_summary,
    )
    store = create_lake_store(output_uri)
    report_uri = store.write_json(join_uri(output_uri, REPORT_FILENAME), report.to_dict())
    manifest = _build_manifest(job, predictions_uri, failed_uri, report_uri)
    manifest_uri = store.write_json(join_uri(output_uri, MANIFEST_FILENAME), manifest)

    # ---------- 回流 PG ----------
    if engine is not None:
        write_outputs_pg(engine, output_records)
        write_failures_pg(engine, failed_records)
        write_job_pg(engine, job)

    final_event = EVENT_INFERENCE_JOB_FINISHED if outcome.exit_code == 0 else EVENT_INFERENCE_JOB_FAILED
    events.emit(AiTaskEvent(
        event_type=final_event, job_id=job.job_id, model_id=model_id,
        payload={"status": job.status, **metrics.to_dict()},
    ))
    if outcome.exit_code != 0:
        events.record_dead_letter(
            task_type="inference_job", task_id=None, job_id=job.job_id,
            reason=job.error_message or "error_rate exceeded", payload={"error_summary": error_summary},
        )

    return InferenceRunResult(
        job=job, report=report.to_dict(),
        predictions_uri=predictions_uri, failed_samples_uri=failed_uri,
        report_uri=report_uri, manifest_uri=manifest_uri,
        exit_code=outcome.exit_code, warn=outcome.warn,
    )


def _execute(
    *,
    samples: list[InferenceSample],
    spec: ModelSpec,
    job: InferenceJob,
    retry: int,
    max_workers: int,
    fail_fast: bool,
    events: AiTaskStore,
) -> tuple[list[InferencePrediction], bool]:
    """执行推理，返回 (与 samples 同序的 predictions, 是否触发 fail_fast)。"""
    from robot_dh.models.backends.base import get_backend

    backend = get_backend(spec)
    config = dict(job.config)
    batches = list(iter_batches(samples, job.batch_size or 32))

    def predict_one_batch(batch: list[InferenceSample]) -> list[InferencePrediction]:
        return _predict_with_retry(backend, batch, spec, config, retry)

    if fail_fast:
        # fail-fast 走顺序执行，首个失败样本即停（其余样本标记为未执行的失败）。
        predictions: list[InferencePrediction] = []
        triggered = False
        done = 0
        for idx, batch in enumerate(batches):
            preds = predict_one_batch(batch)
            predictions.extend(preds)
            done += len(batch)
            events.emit(AiTaskEvent(
                event_type=EVENT_INFERENCE_BATCH_FINISHED, job_id=job.job_id,
                model_id=spec.model_id, payload={"batch_index": idx, "processed": done},
            ))
            if any(p.status != PREDICTION_OK for p in preds):
                triggered = True
                remaining = samples[len(predictions):]
                predictions.extend(_skipped_predictions(remaining, spec))
                break
        return predictions, triggered

    results = run_batches_concurrent(batches, predict_one_batch, max_workers=max_workers)
    predictions = [p for batch_preds in results for p in batch_preds]
    for idx, batch_preds in enumerate(results):
        events.emit(AiTaskEvent(
            event_type=EVENT_INFERENCE_BATCH_FINISHED, job_id=job.job_id,
            model_id=spec.model_id, payload={"batch_index": idx, "batch_size": len(batch_preds)},
        ))
    return predictions, False


def _predict_with_retry(
    backend: Any,
    batch: list[InferenceSample],
    spec: ModelSpec,
    config: dict[str, Any],
    retry: int,
) -> list[InferencePrediction]:
    """对一个 batch 推理；对失败且可重试的样本最多重试 retry 次。"""
    preds = backend.predict_batch(batch, spec, config)
    by_id = {p.sample_id: p for p in preds}
    for _ in range(max(0, retry)):
        retry_samples = [s for s in batch if by_id.get(s.sample_id) and by_id[s.sample_id].status != PREDICTION_OK]
        if not retry_samples:
            break
        again = backend.predict_batch(retry_samples, spec, config)
        for p in again:
            by_id[p.sample_id] = p
    return [by_id[s.sample_id] for s in batch]


def _skipped_predictions(samples: list[InferenceSample], spec: ModelSpec) -> list[InferencePrediction]:
    from robot_dh.models.schemas import PREDICTION_FAILED, prediction_type_for_task

    ptype = prediction_type_for_task(task_type_for_model(spec.model_type))
    return [
        InferencePrediction(
            sample_id=s.sample_id, prediction_type=ptype, prediction_json={},
            status=PREDICTION_FAILED, error_message="SKIPPED: fail-fast 提前终止",
        )
        for s in samples
    ]


def _top_error(error_summary: dict[str, int]) -> str | None:
    if not error_summary:
        return None
    return max(error_summary.items(), key=lambda kv: kv[1])[0]


def _build_manifest(job: InferenceJob, predictions_uri: str, failed_uri: str, report_uri: str) -> dict[str, Any]:
    return {
        "schema_version": "1.9",
        "kind": "inference_job",
        "job_id": job.job_id,
        "model_id": job.model_id,
        "task_type": job.task_type,
        "dataset_id": job.dataset_id,
        "version": job.version,
        "input_uri": job.input_uri,
        "output_uri": job.output_uri,
        "status": job.status,
        "total_samples": job.total_samples,
        "processed_samples": job.processed_samples,
        "failed_samples": job.failed_samples,
        "files": [
            {"path": "predictions.parquet", "uri": predictions_uri},
            {"path": "failed_samples.parquet", "uri": failed_uri},
            {"path": "inference_report.json", "uri": report_uri},
        ],
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }
