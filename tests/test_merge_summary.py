from __future__ import annotations

import json
from pathlib import Path

from robot_dh.data.synthetic import generate_demo_dataset
from robot_dh.sharding.merge import merge_shard_summaries
from robot_dh.sharding.models import EtlPlan, PlanDataset, PlanShard, ShardSummary


def _write_shard_summary(dir_path: Path, summary: ShardSummary) -> None:
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / "shard_summary.json").write_text(json.dumps(summary.to_dict(), indent=2))


def test_merge_aggregates_per_shard_stats(tmp_path: Path) -> None:
    plan = EtlPlan(
        plan_id="plan-merge-test",
        created_at="2025-01-01T00:00:00Z",
        root_uri=tmp_path.as_posix(),
        lake_root=(tmp_path / "lake").as_posix(),
        target_shard_size_bytes=4 * 1024 * 1024,
        total_datasets=3,
        total_bytes=0,
        shards=[
            PlanShard(shard_id="shard-0", shard_index=0, datasets=[]),
            PlanShard(shard_id="shard-1", shard_index=1, datasets=[]),
        ],
    )
    a = tmp_path / "results" / "shard_0"
    b = tmp_path / "results" / "shard_1"
    _write_shard_summary(
        a,
        ShardSummary(
            plan_id="plan-merge-test",
            shard_id="shard-0",
            shard_index=0,
            status="OK",
            total=2,
            succeeded=2,
            failed=0,
            skipped=0,
            duration_sec=12.5,
            started_at="2025-01-01T00:00:00Z",
            finished_at="2025-01-01T00:00:12Z",
            runs=[{"status": "OK", "dataset_id": "alpha", "version": "v1"}],
        ),
    )
    _write_shard_summary(
        b,
        ShardSummary(
            plan_id="plan-merge-test",
            shard_id="shard-1",
            shard_index=1,
            status="WARN",
            total=2,
            succeeded=1,
            failed=1,
            skipped=0,
            duration_sec=20.0,
            started_at="2025-01-01T00:00:00Z",
            finished_at="2025-01-01T00:00:20Z",
            runs=[{"status": "FAIL", "dataset_id": "beta", "version": "v1", "error_message": "boom"}],
        ),
    )

    output = tmp_path / "plan_summary.json"
    payload = merge_shard_summaries(
        plan=plan,
        shard_results_uri=(tmp_path / "results").as_posix(),
        output_uri=output.as_posix(),
    )

    assert payload["total"] == 4
    assert payload["succeeded"] == 3
    assert payload["failed"] == 1
    assert payload["found_shards"] == 2
    assert any(item["dataset_id"] == "beta" for item in payload["failed_datasets"])
    assert output.is_file()
