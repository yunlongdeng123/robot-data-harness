"""把 PerfRecord 写入本地 JSON 与 PostgreSQL `etl_perf_runs`。

DB 路径采用 soft 写入：表缺失 / 连接失败时仅 warning，不影响本地落盘。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from robot_dh.perf.io_stats import sum_file_rows, sum_file_sizes
from robot_dh.perf.profiler import PerfRecord, perf_filename
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)


def write_perf_json(record: PerfRecord, work_dir: Path) -> Path:
    work_dir = work_dir.expanduser().resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    out_path = work_dir / perf_filename(record)
    out_path.write_text(json.dumps(record.to_dict(), indent=2, ensure_ascii=False))
    return out_path


def write_perf_record_to_db(
    record: PerfRecord,
    *,
    warehouse: WarehouseService | None = None,
) -> int | None:
    """写入 PG `etl_perf_runs`；warehouse 缺省时按 soft 模式构造（DB 不可用时只 warning）。"""
    wh = warehouse or WarehouseService(soft=True)
    return wh.record_etl_perf_run(record)


def perf_records_from_etl_run(
    *,
    etl_result,  # robot_dh.etl.runner.EtlRunResult
    run_id: str | None = None,
    input_bytes_estimate: int = 0,
) -> list[PerfRecord]:
    """根据 etl_run 返回值产出 normalize / build_features / build_ads 三段 PerfRecord。"""
    out: list[PerfRecord] = []
    if etl_result is None:
        return out
    run_id = run_id or etl_result.job_id
    norm = etl_result.normalize
    feat = etl_result.features
    ads = etl_result.ads
    if norm is not None:
        out.append(
            PerfRecord(
                job_id=norm.job_id,
                run_id=run_id,
                dataset_id=etl_result.dataset_id,
                version=etl_result.version,
                phase="normalize",
                input_uri=etl_result.raw_uri,
                output_uri=norm.output_uri,
                input_bytes=int(input_bytes_estimate or 0),
                output_bytes=sum_file_sizes(norm.files or []),
                input_rows=int(norm.num_samples),
                output_rows=sum_file_rows(norm.files or []),
                duration_sec=float(norm.duration_job_sec),
                status="OK",
            )
        )
    if feat is not None:
        out.append(
            PerfRecord(
                job_id=feat.job_id,
                run_id=run_id,
                dataset_id=etl_result.dataset_id,
                version=etl_result.version,
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
        )
    if ads is not None:
        out.append(
            PerfRecord(
                job_id=ads.job_id,
                run_id=run_id,
                dataset_id=etl_result.dataset_id,
                version=etl_result.version,
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
        )
    return out


def emit_perf_records(
    records: list[PerfRecord],
    *,
    work_dir: Path | None,
    warehouse: WarehouseService | None = None,
) -> None:
    """把一批 PerfRecord 落盘 + 写 DB（soft）。"""
    if work_dir is not None:
        for rec in records:
            write_perf_json(rec, work_dir)
    wh = warehouse
    for rec in records:
        write_perf_record_to_db(rec, warehouse=wh)
