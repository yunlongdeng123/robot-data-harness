"""单个 shard 的执行：对 shard 中的每个 dataset 调用 etl_run，并把性能 / 事件落到 DB+本地。

支持 max_workers 并发；--fail-fast 时遇到任何失败立即返回。
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from robot_dh.etl.runner import etl_run
from robot_dh.perf.io_stats import (
    measure_uri_bytes,
    sum_file_rows,
    sum_file_sizes,
)
from robot_dh.perf.profiler import EtlProfiler, PerfRecord
from robot_dh.perf.writer import write_perf_json
from robot_dh.runtime.events import RuntimeEventLogger, utcnow_iso
from robot_dh.sharding.io import write_json_uri
from robot_dh.sharding.models import EtlPlan, PlanDataset, PlanShard, ShardSummary
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)


def _resolve_shard(plan: EtlPlan, shard_id: int | str) -> PlanShard | None:
    if isinstance(shard_id, str) and shard_id.isdigit():
        shard_id = int(shard_id)
    return plan.get_shard(shard_id)


def _dataset_phase_perf(
    *,
    plan_id: str,
    shard: PlanShard,
    dataset: PlanDataset,
    result: Any,
    error: str | None,
    started: float,
    duration_sec: float,
) -> list[PerfRecord]:
    """根据 etl_run 的 EtlRunResult 拆出 normalize / build-features / build-ads 三阶段 perf。"""
    out: list[PerfRecord] = []
    run_id = f"{plan_id}::{shard.shard_id}"

    if result is None:
        # 整体失败：写一条聚合 perf 记录
        rec = PerfRecord(
            job_id=f"{shard.shard_id}::{dataset.dataset_id}",
            run_id=run_id,
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            phase="etl_run",
            input_uri=dataset.dataset_uri,
            output_uri=None,
            input_bytes=dataset.input_bytes,
            duration_sec=duration_sec,
            status="FAIL",
            error_message=error,
        )
        out.append(rec)
        return out

    norm = result.normalize
    feat = result.features
    ads = result.ads

    if norm is not None:
        rec = PerfRecord(
            job_id=norm.job_id,
            run_id=run_id,
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            phase="normalize",
            input_uri=dataset.dataset_uri,
            output_uri=norm.output_uri,
            input_bytes=dataset.input_bytes,
            output_bytes=sum_file_sizes(norm.files or []),
            input_rows=int(norm.num_samples),
            output_rows=sum_file_rows(norm.files or []),
            duration_sec=float(norm.duration_job_sec),
            status="OK",
        )
        out.append(rec)
    if feat is not None:
        rec = PerfRecord(
            job_id=feat.job_id,
            run_id=run_id,
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            phase="build_features",
            input_uri=norm.output_uri if norm else None,
            output_uri=feat.output_uri,
            input_bytes=sum_file_sizes(norm.files or []) if norm else 0,
            output_bytes=sum_file_sizes(feat.files or []),
            input_rows=int(norm.num_samples) if norm else 0,
            output_rows=sum_file_rows(feat.files or []),
            duration_sec=float(feat.duration_job_sec),
            status=str(feat.job_status),
        )
        out.append(rec)
    if ads is not None:
        rec = PerfRecord(
            job_id=ads.job_id,
            run_id=run_id,
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            phase="build_ads",
            input_uri=feat.output_uri if feat else None,
            output_uri=ads.output_uri,
            input_bytes=sum_file_sizes(feat.files or []) if feat else 0,
            output_bytes=sum_file_sizes(ads.files or []),
            input_rows=int(feat.num_press_events) if feat else 0,
            output_rows=sum_file_rows(ads.files or []),
            duration_sec=float(ads.duration_job_sec),
            status="OK",
        )
        out.append(rec)
    return out


def _run_one_dataset(
    *,
    plan: EtlPlan,
    shard: PlanShard,
    dataset: PlanDataset,
    work_dir: Path,
    events: RuntimeEventLogger,
    warehouse: WarehouseService,
    features_config_path: Path | None,
    ads_config_path: Path | None,
    build_ads_layer: bool,
) -> dict[str, Any]:
    job_id = f"{shard.shard_id}::{dataset.dataset_id}-{dataset.version}"
    events.emit(
        "dataset_etl_started",
        payload={
            "plan_id": plan.plan_id,
            "shard_id": shard.shard_id,
            "dataset_uri": dataset.dataset_uri,
        },
        job_id=job_id,
        run_id=plan.plan_id,
        dataset_id=dataset.dataset_id,
        version=dataset.version,
    )

    started = time.time()
    error_message: str | None = None
    status = "OK"
    result = None
    try:
        result = etl_run(
            dataset_uri=dataset.dataset_uri,
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            lake_root_uri=plan.lake_root,
            build_ads_layer=build_ads_layer,
            features_config_path=features_config_path,
            ads_config_path=ads_config_path,
            job_id=job_id,
            warehouse=warehouse,
        )
        status = result.status
        error_message = result.error
    except Exception as err:  # noqa: BLE001
        status = "FAIL"
        error_message = str(err)
        LOG.exception("dataset etl failed: %s", err)

    duration_sec = time.time() - started
    perf_records = _dataset_phase_perf(
        plan_id=plan.plan_id,
        shard=shard,
        dataset=dataset,
        result=result,
        error=error_message,
        started=started,
        duration_sec=duration_sec,
    )
    dataset_perf_dir = work_dir / "perf" / dataset.dataset_id / dataset.version
    dataset_perf_dir.mkdir(parents=True, exist_ok=True)
    for rec in perf_records:
        write_perf_json(rec, dataset_perf_dir)
        warehouse.record_etl_perf_run(rec)

    events.emit(
        "dataset_etl_finished",
        payload={
            "plan_id": plan.plan_id,
            "shard_id": shard.shard_id,
            "status": status,
            "duration_sec": duration_sec,
            "error_message": error_message,
        },
        job_id=job_id,
        run_id=plan.plan_id,
        dataset_id=dataset.dataset_id,
        version=dataset.version,
    )

    return {
        "dataset_id": dataset.dataset_id,
        "version": dataset.version,
        "dataset_uri": dataset.dataset_uri,
        "status": status,
        "duration_sec": duration_sec,
        "error_message": error_message,
        "etl_result": result.to_dict() if result is not None else None,
    }


def run_shard(
    *,
    plan: EtlPlan,
    shard_id: int | str,
    lake_root: str | None = None,
    work_dir: Path,
    output_summary_uri: str | None = None,
    max_workers: int = 1,
    fail_fast: bool = False,
    features_config_path: Path | None = None,
    ads_config_path: Path | None = None,
    build_ads_layer: bool = False,
    warehouse: WarehouseService | None = None,
    events: RuntimeEventLogger | None = None,
) -> ShardSummary:
    """执行 plan 中指定 shard，写本地 + S3 shard_summary.json，并把 perf/runtime events 落库。"""
    if lake_root and not plan.lake_root:
        plan.lake_root = lake_root
    if lake_root:
        plan.lake_root = lake_root

    shard = _resolve_shard(plan, shard_id)
    if shard is None:
        # SKIP：plan 中不存在该 shard
        summary = ShardSummary(
            plan_id=plan.plan_id,
            shard_id=str(shard_id),
            shard_index=int(shard_id) if isinstance(shard_id, int) or (isinstance(shard_id, str) and shard_id.isdigit()) else -1,
            status="SKIPPED",
            total=0,
            succeeded=0,
            failed=0,
            skipped=0,
            duration_sec=0.0,
            started_at=utcnow_iso(),
            finished_at=utcnow_iso(),
            runs=[],
        )
        work_dir.mkdir(parents=True, exist_ok=True)
        local_summary = work_dir / "shard_summary.json"
        local_summary.write_text(_json(summary.to_dict()))
        if output_summary_uri:
            write_json_uri(output_summary_uri, summary.to_dict())
            summary.summary_uri = output_summary_uri
        return summary

    warehouse = warehouse or WarehouseService(soft=True)
    events = events or RuntimeEventLogger(warehouse=warehouse)

    work_dir = work_dir.expanduser().resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    started_iso = utcnow_iso()
    events.emit(
        "etl_shard_started",
        payload={"plan_id": plan.plan_id, "shard_id": shard.shard_id, "dataset_count": len(shard.datasets)},
        job_id=shard.shard_id,
        run_id=plan.plan_id,
    )
    warehouse.record_etl_shard(
        plan_id=plan.plan_id,
        shard_id=shard.shard_id,
        shard_index=shard.shard_index,
        status="RUNNING",
        dataset_count=len(shard.datasets),
        input_bytes=int(shard.total_bytes),
        started_at=datetime.now(timezone.utc),
    )

    runs: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0
    skipped = 0

    def _job(dataset: PlanDataset) -> dict[str, Any]:
        return _run_one_dataset(
            plan=plan,
            shard=shard,
            dataset=dataset,
            work_dir=work_dir,
            events=events,
            warehouse=warehouse,
            features_config_path=features_config_path,
            ads_config_path=ads_config_path,
            build_ads_layer=build_ads_layer,
        )

    if max_workers <= 1 or len(shard.datasets) <= 1:
        for dataset in shard.datasets:
            run = _job(dataset)
            runs.append(run)
            if run["status"] in {"OK", "WARN"}:
                succeeded += 1
            elif run["status"] == "SKIPPED":
                skipped += 1
            else:
                failed += 1
                if fail_fast:
                    break
    else:
        with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as pool:
            future_map = {pool.submit(_job, ds): ds for ds in shard.datasets}
            for fut in as_completed(future_map):
                run = fut.result()
                runs.append(run)
                if run["status"] in {"OK", "WARN"}:
                    succeeded += 1
                elif run["status"] == "SKIPPED":
                    skipped += 1
                else:
                    failed += 1
                    if fail_fast:
                        for f in future_map:
                            if not f.done():
                                f.cancel()
                        break

    elapsed = time.time() - started
    final_status = "OK"
    if failed > 0 and succeeded == 0 and len(shard.datasets) > 0:
        final_status = "FAIL"
    elif failed > 0:
        final_status = "WARN"
    summary = ShardSummary(
        plan_id=plan.plan_id,
        shard_id=shard.shard_id,
        shard_index=shard.shard_index,
        status=final_status,
        total=len(shard.datasets),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        duration_sec=elapsed,
        started_at=started_iso,
        finished_at=utcnow_iso(),
        runs=runs,
    )

    local_summary = work_dir / "shard_summary.json"
    local_summary.write_text(_json(summary.to_dict()))
    if output_summary_uri:
        write_json_uri(output_summary_uri, summary.to_dict())
        summary.summary_uri = output_summary_uri
    else:
        summary.summary_uri = local_summary.as_posix()

    warehouse.record_etl_shard(
        plan_id=plan.plan_id,
        shard_id=shard.shard_id,
        shard_index=shard.shard_index,
        status=final_status,
        finished_at=datetime.now(timezone.utc),
        duration_sec=elapsed,
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        summary_uri=summary.summary_uri,
        metrics={"total": len(shard.datasets)},
    )

    events.emit(
        "etl_shard_finished",
        payload={
            "plan_id": plan.plan_id,
            "shard_id": shard.shard_id,
            "status": final_status,
            "total": len(shard.datasets),
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "duration_sec": elapsed,
        },
        job_id=shard.shard_id,
        run_id=plan.plan_id,
    )
    return summary


def _json(payload: Any) -> str:
    import json

    return json.dumps(payload, indent=2, ensure_ascii=False)
