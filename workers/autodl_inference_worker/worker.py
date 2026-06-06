"""AutoDL pull-based 推理 worker（v1.9 可选脚手架）。

定位（见 docs/autodl_gpu_worker.md）：
- AutoDL 实例只做 GPU inference worker，不当 K8s / DB / MinIO 节点。
- 从腾讯云 PostgreSQL 拉 status=QUEUED 且 model_id 匹配的 inference_jobs。
- 用 openai_compatible backend（vLLM endpoint）执行，predictions 写回 MinIO（output_uri）。
- 更新 inference_jobs 状态；失败写 inference_failures。
- --dry-run 只打印将处理哪些 job，不访问 GPU、不改状态。

本文件复用主项目 robot_dh（worker 机器需 `pip install -e robot-data-harness`）。
核心逻辑拆成可被测试 import 的函数；不依赖真实 GPU / vLLM。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from robot_dh.ai_tasks.state import JOB_FAILED, JOB_QUEUED, JOB_RUNNING, JOB_SUCCEEDED
from robot_dh.ai_tasks.store import resolve_optional_engine
from robot_dh.inference.batch import InferenceInputBuilder, iter_batches
from robot_dh.inference.failures import failure_from, write_failed_samples, write_failures_pg
from robot_dh.inference.job import row_to_dict
from robot_dh.inference.metrics import compute_metrics
from robot_dh.inference.outputs import record_from_prediction, write_outputs_pg, write_predictions
from robot_dh.inference.report import build_report
from robot_dh.lake.store import create_lake_store
from robot_dh.lake.uri import join_uri
from robot_dh.models.backends.base import get_backend
from robot_dh.models.registry import ModelRegistry
from robot_dh.models.schemas import ModelSpec, PREDICTION_OK, task_type_for_model
from robot_dh.warehouse.models import InferenceJobRow


class WorkerConfigError(RuntimeError):
    """worker 必需环境变量缺失。"""


@dataclass
class WorkerConfig:
    db_uri: str
    model_id: str
    poll_interval_sec: int = 10
    max_jobs: int = 1
    openai_base_url: str | None = None
    openai_model: str | None = None
    s3_endpoint: str | None = None
    s3_lake_bucket: str | None = None

    def to_safe_dict(self) -> dict[str, Any]:
        """打印用：脱敏，不含 api_key / secret。"""
        return {
            "model_id": self.model_id,
            "poll_interval_sec": self.poll_interval_sec,
            "max_jobs": self.max_jobs,
            "db_host": _host_of(self.db_uri),
            "openai_base_url": self.openai_base_url or "(unset)",
            "openai_model": self.openai_model or "(unset)",
            "s3_endpoint": self.s3_endpoint or "(unset)",
            "s3_lake_bucket": self.s3_lake_bucket or "(unset)",
        }


def _host_of(uri: str) -> str:
    if not uri:
        return "(unset)"
    tail = uri.split("@")[-1]
    return tail.split("/")[0]


def load_worker_config(
    args: argparse.Namespace,
    env: dict[str, str] | None = None,
) -> WorkerConfig:
    """从 env + args 构造 WorkerConfig；缺关键 env 抛 WorkerConfigError。"""
    env = dict(env if env is not None else os.environ)
    db_uri = env.get("ROBOT_DH_DB_URI", "").strip()
    if not db_uri:
        raise WorkerConfigError(
            "缺少 ROBOT_DH_DB_URI；worker 需要连接腾讯云 PostgreSQL 拉取 inference_jobs。"
        )
    model_id = (getattr(args, "model_id", None) or env.get("ROBOT_DH_WORKER_MODEL_ID") or "").strip()
    if not model_id:
        raise WorkerConfigError("缺少 --model-id（或 ROBOT_DH_WORKER_MODEL_ID）。")
    base_url = env.get("ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL") or None
    # 非 dry-run 且无 base_url 时给出明确告警（真正执行 openai_compatible 需要 endpoint）。
    if not getattr(args, "dry_run", False) and not base_url:
        raise WorkerConfigError(
            "非 dry-run 模式需要 ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL 指向 vLLM endpoint；"
            "仅验证拉取逻辑请加 --dry-run。"
        )
    return WorkerConfig(
        db_uri=db_uri,
        model_id=model_id,
        poll_interval_sec=int(getattr(args, "poll_interval_sec", 10)),
        max_jobs=int(getattr(args, "max_jobs", 1)),
        openai_base_url=base_url,
        openai_model=env.get("ROBOT_DH_OPENAI_COMPATIBLE_MODEL") or None,
        s3_endpoint=env.get("ROBOT_DH_S3_ENDPOINT_URL") or None,
        s3_lake_bucket=env.get("ROBOT_DH_S3_LAKE_BUCKET") or None,
    )


def list_queued_jobs(engine: Engine, model_id: str, limit: int = 20) -> list[dict[str, Any]]:
    """列出匹配 model_id 的 QUEUED 任务（dry-run 用）。"""
    with Session(engine, expire_on_commit=False, future=True) as session:
        rows = session.execute(
            select(InferenceJobRow)
            .where(InferenceJobRow.status == JOB_QUEUED, InferenceJobRow.model_id == model_id)
            .order_by(InferenceJobRow.priority.desc(), InferenceJobRow.created_at.asc())
            .limit(limit)
        ).scalars().all()
        return [row_to_dict(r) for r in rows]


def claim_next_job(engine: Engine, model_id: str) -> dict[str, Any] | None:
    """原子认领下一个 QUEUED 任务（UPDATE ... WHERE status=QUEUED），避免多 worker 抢同一个。"""
    now = datetime.now(timezone.utc)
    with Session(engine, expire_on_commit=False, future=True) as session:
        candidate = session.execute(
            select(InferenceJobRow)
            .where(InferenceJobRow.status == JOB_QUEUED, InferenceJobRow.model_id == model_id)
            .order_by(InferenceJobRow.priority.desc(), InferenceJobRow.created_at.asc())
            .limit(1)
        ).scalars().first()
        if candidate is None:
            return None
        result = session.execute(
            update(InferenceJobRow)
            .where(InferenceJobRow.job_id == candidate.job_id, InferenceJobRow.status == JOB_QUEUED)
            .values(status=JOB_RUNNING, started_at=now, updated_at=now)
        )
        if result.rowcount != 1:
            session.rollback()
            return None
        session.commit()
        claimed = session.get(InferenceJobRow, candidate.job_id)
        return row_to_dict(claimed) if claimed else None


def _resolve_spec(engine: Engine, job: dict[str, Any], config: WorkerConfig) -> ModelSpec:
    """优先用 model_registry 里的 spec；缺失则按 env 构造 openai_compatible spec。"""
    spec = ModelRegistry(db_uri=config.db_uri).get(job["model_id"])
    if spec is not None:
        return spec
    task_type = job.get("task_type") or "caption"
    model_type = "embedding" if task_type == "embedding" else "llm"
    return ModelSpec(
        model_id=job["model_id"],
        model_name=job["model_id"],
        model_type=model_type,
        backend="openai_compatible",
        endpoint_url=config.openai_base_url,
    )


def run_claimed_job(engine: Engine, job: dict[str, Any], config: WorkerConfig) -> dict[str, Any]:
    """执行一个已认领（RUNNING）的任务，更新同一行状态。返回执行摘要。"""
    spec = _resolve_spec(engine, job, config)
    backend = get_backend(spec)
    started = datetime.now(timezone.utc)
    samples = InferenceInputBuilder(input_uri=job["input_uri"], split="all").build()
    batch_size = int(job.get("batch_size") or spec.max_batch_size or 32)

    predictions = []
    for batch in iter_batches(samples, batch_size):
        predictions.extend(backend.predict_batch(batch, spec, {"timeout_sec": spec.timeout_sec}))
    duration = (datetime.now(timezone.utc) - started).total_seconds()

    records = [
        record_from_prediction(job_id=job["job_id"], model_id=spec.model_id, sample=s,
                               prediction=p, output_uri=job["output_uri"])
        for s, p in zip(samples, predictions)
    ]
    failed = [
        failure_from(job_id=job["job_id"], model_id=spec.model_id, sample=s, prediction=p)
        for s, p in zip(samples, predictions) if p.status != PREDICTION_OK
    ]
    metrics = compute_metrics(predictions, duration)
    write_predictions(job["output_uri"], records)
    write_failed_samples(job["output_uri"], failed)
    status = JOB_SUCCEEDED if metrics.error_rate <= 0.5 else JOB_FAILED
    report = build_report(
        job_id=job["job_id"], model_id=spec.model_id, input_uri=job["input_uri"],
        output_uri=job["output_uri"], task_type=job.get("task_type") or task_type_for_model(spec.model_type),
        backend=spec.backend, metrics=metrics, status=status,
        error_summary=_error_summary(failed),
    )
    store = create_lake_store(job["output_uri"])
    store.write_json(join_uri(job["output_uri"], "inference_report.json"), report.to_dict())

    _finalize_job(engine, job["job_id"], status, metrics, duration)
    write_outputs_pg(engine, records)
    write_failures_pg(engine, failed)
    return {"job_id": job["job_id"], "status": status, **metrics.to_dict()}


def _error_summary(failed: list[Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for fr in failed:
        key = fr.error_type or "UNKNOWN"
        out[key] = out.get(key, 0) + 1
    return out


def _finalize_job(engine: Engine, job_id: str, status: str, metrics: Any, duration: float) -> None:
    now = datetime.now(timezone.utc)
    with Session(engine, expire_on_commit=False, future=True) as session:
        session.execute(
            update(InferenceJobRow)
            .where(InferenceJobRow.job_id == job_id)
            .values(
                status=status,
                processed_samples=metrics.total_samples,
                failed_samples=metrics.failed_samples,
                finished_at=now,
                updated_at=now,
                duration_sec=duration,
                metrics_json=metrics.to_dict(),
            )
        )
        session.commit()


def run_worker(config: WorkerConfig, *, dry_run: bool) -> int:
    """worker 主循环：拉取 -> 执行最多 max_jobs 个任务。返回处理的任务数。"""
    engine = resolve_optional_engine(config.db_uri)
    if engine is None:
        raise WorkerConfigError(f"无法连接 DB：{_host_of(config.db_uri)}")

    if dry_run:
        queued = list_queued_jobs(engine, config.model_id, limit=config.max_jobs)
        print(json.dumps({
            "dry_run": True,
            "model_id": config.model_id,
            "would_process": [
                {"job_id": j["job_id"], "input_uri": j["input_uri"], "output_uri": j["output_uri"]}
                for j in queued
            ],
        }, ensure_ascii=False, indent=2))
        return 0

    processed = 0
    while processed < config.max_jobs:
        job = claim_next_job(engine, config.model_id)
        if job is None:
            break
        summary = run_claimed_job(engine, job, config)
        print(json.dumps(summary, ensure_ascii=False))
        processed += 1
    return processed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="autodl_inference_worker")
    parser.add_argument("--poll-interval-sec", type=int, default=10)
    parser.add_argument("--max-jobs", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--model-id", type=str, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        config = load_worker_config(args)
    except WorkerConfigError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 2
    print(f"[worker] config={json.dumps(config.to_safe_dict(), ensure_ascii=False)}", file=sys.stderr)
    try:
        run_worker(config, dry_run=args.dry_run)
    except WorkerConfigError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
