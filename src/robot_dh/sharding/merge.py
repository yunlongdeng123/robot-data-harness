"""合并所有 shard_summary.json 为 plan-level summary。"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from robot_dh.runtime.events import RuntimeEventLogger, utcnow_iso
from robot_dh.sharding.io import list_local_or_s3, read_json_uri, write_json_uri
from robot_dh.sharding.models import EtlPlan, ShardSummary
from robot_dh.warehouse.service import WarehouseService

LOG = logging.getLogger(__name__)


def _find_shard_summaries(shard_results_uri: str) -> list[str]:
    items = list_local_or_s3(shard_results_uri)
    return [u for u in items if u.endswith("shard_summary.json")]


def merge_shard_summaries(
    *,
    plan: EtlPlan,
    shard_results_uri: str,
    output_uri: str,
    warehouse: WarehouseService | None = None,
    events: RuntimeEventLogger | None = None,
) -> dict[str, Any]:
    """汇总 shard_results_uri 下所有 shard_summary.json，输出 plan summary 到 output_uri。"""
    warehouse = warehouse or WarehouseService(soft=True)
    events = events or RuntimeEventLogger(warehouse=warehouse)

    summary_uris = _find_shard_summaries(shard_results_uri)
    summaries: list[ShardSummary] = []
    for uri in sorted(summary_uris):
        try:
            payload = read_json_uri(uri)
            summaries.append(ShardSummary.from_dict(payload))
        except Exception as err:
            LOG.warning("merge: failed to read %s: %s", uri, err)

    total = 0
    succeeded = 0
    failed = 0
    skipped = 0
    duration_sec = 0.0
    failed_datasets: list[dict[str, Any]] = []
    per_shard_stats: list[dict[str, Any]] = []

    for s in summaries:
        total += s.total
        succeeded += s.succeeded
        failed += s.failed
        skipped += s.skipped
        duration_sec = max(duration_sec, s.duration_sec)
        per_shard_stats.append(
            {
                "shard_id": s.shard_id,
                "shard_index": s.shard_index,
                "status": s.status,
                "total": s.total,
                "succeeded": s.succeeded,
                "failed": s.failed,
                "skipped": s.skipped,
                "duration_sec": s.duration_sec,
                "summary_uri": s.summary_uri,
            }
        )
        for run in s.runs:
            if run.get("status") == "FAIL":
                failed_datasets.append(
                    {
                        "shard_id": s.shard_id,
                        "dataset_id": run.get("dataset_id"),
                        "version": run.get("version"),
                        "error_message": run.get("error_message"),
                    }
                )

    payload = {
        "plan_id": plan.plan_id,
        "lake_root": plan.lake_root,
        "root_uri": plan.root_uri,
        "created_at": utcnow_iso(),
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "duration": duration_sec,
        "failed_datasets": failed_datasets,
        "per_shard_stats": per_shard_stats,
        "expected_shards": len(plan.shards),
        "found_shards": len(summaries),
    }

    write_json_uri(output_uri, payload)

    events.emit(
        "merge_summary_written",
        payload={"output_uri": output_uri, "expected": len(plan.shards), "found": len(summaries)},
        run_id=plan.plan_id,
    )
    return payload
