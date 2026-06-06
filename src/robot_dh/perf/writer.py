"""把 PerfRecord 写入本地 JSON 与 PostgreSQL `etl_perf_runs`。

DB 路径采用 soft 写入：表缺失 / 连接失败时仅 warning，不影响本地落盘。
当远端 `etl_perf_runs` schema 漂移（infra 端 migration 滞后于主项目 ORM）触发
``V15SchemaMissingError`` 时，按 ``ROBOT_DH_PERF_RECORD_ON_SCHEMA_MISMATCH`` 切换策略：

- ``soft``（默认）：把 record 落到本地 pending 目录 + 可选 S3 mirror，业务 step 仍 exit 0；
  待 infra 跑完 migration 后用 ``robot-dh perf reingest-pending`` 批量回灌。
- ``loud``：保持旧行为（向上抛 ``V15SchemaMissingError``），CI 守门用。

详细背景：``docs/history/v1_6_etl_perf_runs_schema_align_request.md`` §4.2。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from robot_dh.perf.io_stats import sum_file_rows, sum_file_sizes
from robot_dh.perf.pending import (
    PendingPerfStore,
    archive_pending_file,
    list_pending_files,
    resolve_local_archive_dir,
    resolve_local_pending_dir,
)
from robot_dh.perf.profiler import PerfRecord, perf_filename
from robot_dh.warehouse.service import (
    LakeMetadataUnavailableError,
    V15SchemaMissingError,
    WarehouseService,
)

LOG = logging.getLogger(__name__)

PERF_FAIL_MODE_ENV = "ROBOT_DH_PERF_RECORD_ON_SCHEMA_MISMATCH"


def _should_fail_loud() -> bool:
    """ENV 控制 schema 漂移时的行为；默认 soft（fallback 到 pending）。"""
    return os.environ.get(PERF_FAIL_MODE_ENV, "soft").strip().lower() == "loud"


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
    pending_store: PendingPerfStore | None = None,
) -> int | None:
    """写 PG `etl_perf_runs`；schema 漂移时按 ENV 决定 fallback 到 pending 或 raise。"""
    wh = warehouse or WarehouseService(soft=True)
    try:
        return wh.record_etl_perf_run(record)
    except V15SchemaMissingError as err:
        if _should_fail_loud():
            raise
        store = pending_store or PendingPerfStore.from_env()
        uris = store.emit(record, reason=str(err))
        LOG.error(
            "perf record schema mismatch, deferred to pending store: local=%s s3=%s. "
            "Run schema migration on infra side then `robot-dh perf reingest-pending` to backfill.",
            uris.get("local"),
            uris.get("s3", "skipped"),
        )
        return None


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
        # v1.8 修复：
        # 1) status 从 NormalizeResult 透传（OK / SKIPPED / RESUMED / WARN），
        #    不再硬编码 "OK"，避免和 runner.py 顶层口径不一致；
        # 2) metrics 合并 NormalizeResult.metrics（sub-stage profile 数据），
        #    避免 PG 里这条记录的 metrics_json 永远是空字典 / 失去诊断信息。
        norm_status = str(getattr(norm, "status", "OK") or "OK")
        norm_metrics: dict[str, Any] = {}
        for k, v in (getattr(norm, "metrics", None) or {}).items():
            norm_metrics[str(k)] = v
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
                status=norm_status,
                metrics=norm_metrics,
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
    pending_store: PendingPerfStore | None = None,
) -> None:
    """把一批 PerfRecord 落盘 + 写 DB（soft）。

    schema 漂移时不阻塞，按 ENV `ROBOT_DH_PERF_RECORD_ON_SCHEMA_MISMATCH` 切 soft/loud。
    """
    if work_dir is not None:
        for rec in records:
            write_perf_json(rec, work_dir)
    if not records:
        return
    wh = warehouse
    # 同一批共享一个 pending_store，避免每条 record 都重新嗅探 S3 环境
    store = pending_store
    for rec in records:
        try:
            write_perf_record_to_db(rec, warehouse=wh, pending_store=store)
        except V15SchemaMissingError:
            if _should_fail_loud():
                raise


def reingest_pending_perf_records(
    *,
    pending_dir: Path | None = None,
    archive_dir: Path | None = None,
    warehouse: WarehouseService | None = None,
    pending_store: PendingPerfStore | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """把 pending 目录里的 perf record 批量回灌 PG，成功的搬到 archive。

    遇到 schema 仍然漂移（``V15SchemaMissingError`` / ``LakeMetadataUnavailableError``）
    立即停止，保留余下文件，等下一次 migration / 重试。文件读不出来或单条 DB 写失败
    走 ``failed`` 计数继续往下，不阻塞后续记录。

    返回 ``{"scanned", "ingested", "archived", "failed", "skipped", "aborted_reason"}``。
    """
    pending_dir = resolve_local_pending_dir(pending_dir)
    archive_dir = resolve_local_archive_dir(archive_dir)
    wh = warehouse or WarehouseService(soft=False)

    counters: dict[str, Any] = {
        "scanned": 0,
        "ingested": 0,
        "archived": 0,
        "failed": 0,
        "skipped": 0,
        "aborted_reason": None,
        "pending_dir": str(pending_dir),
        "archive_dir": str(archive_dir),
    }

    if not pending_dir.is_dir():
        return counters

    files = list_pending_files(pending_dir)
    if not files:
        return counters

    s3_client: Any = None
    s3_bucket: str | None = None
    if pending_store is not None:
        s3_client = getattr(pending_store, "_s3_client", None)
        s3_bucket = getattr(pending_store, "_s3_bucket", None)
    else:
        # archive 用与 emit 同源的 S3 mirror，缺配置时退化为纯本地
        try:
            inferred = PendingPerfStore.from_env()
            s3_client = getattr(inferred, "_s3_client", None)
            s3_bucket = getattr(inferred, "_s3_bucket", None)
        except Exception:
            s3_client = None
            s3_bucket = None

    for path in files:
        counters["scanned"] += 1
        try:
            payload = json.loads(path.read_text())
        except Exception as err:
            LOG.error("skip unreadable pending record %s: %s", path, err)
            counters["failed"] += 1
            continue

        # `_pending` 是 emit 时塞进去的元信息，写回 PG 前剥掉
        payload_for_db = {k: v for k, v in payload.items() if k != "_pending"}

        if dry_run:
            counters["skipped"] += 1
            continue

        try:
            wh.record_etl_perf_run(payload_for_db)
        except (V15SchemaMissingError, LakeMetadataUnavailableError) as err:
            LOG.error(
                "schema still mismatched, aborting reingest at %s: %s",
                path,
                err,
            )
            counters["failed"] += 1
            counters["aborted_reason"] = str(err)
            return counters
        except Exception as err:
            LOG.error("reingest failed for %s: %s", path, err)
            counters["failed"] += 1
            continue

        counters["ingested"] += 1
        try:
            archive_pending_file(
                path,
                pending_dir=pending_dir,
                archive_dir=archive_dir,
                s3_client=s3_client,
                s3_bucket=s3_bucket,
            )
            counters["archived"] += 1
        except Exception as err:
            LOG.warning("archive move failed for %s: %s", path, err)

    return counters
