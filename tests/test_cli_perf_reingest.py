"""CLI ``robot-dh perf reingest-pending`` 行为覆盖。"""

from __future__ import annotations

import json
from unittest.mock import patch

from robot_dh.cli import main
from robot_dh.perf.pending import PendingPerfStore
from robot_dh.perf.profiler import PerfRecord


def _make_perf(job_id: str = "perf-cli") -> PerfRecord:
    return PerfRecord(
        job_id=job_id,
        run_id="run-1",
        dataset_id="bridgedata_v2_scale30",
        version="v1",
        phase="normalize",
        status="OK",
        duration_sec=1.0,
    )


def test_cli_perf_reingest_pending_dry_run(tmp_path, capsys):
    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    store = PendingPerfStore(local_dir=pending)
    store.emit(_make_perf("perf-z"), reason="cli dry-run test")

    rc = main(
        [
            "perf",
            "reingest-pending",
            "--pending-dir",
            str(pending),
            "--archive-dir",
            str(archive),
            "--dry-run",
        ]
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["scanned"] == 1
    assert payload["skipped"] == 1
    assert payload["ingested"] == 0
    assert payload["failed"] == 0
    # dry-run 不能挪文件
    assert len(list(pending.rglob("*.json"))) == 1


def test_cli_perf_reingest_pending_ingests_and_returns_zero(tmp_path, capsys):
    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    store = PendingPerfStore(local_dir=pending)
    store.emit(_make_perf("perf-ok-1"), reason="cli ingest test")
    store.emit(_make_perf("perf-ok-2"), reason="cli ingest test")

    fake_record_etl_perf_run = lambda payload: 1  # noqa: E731

    with patch(
        "robot_dh.warehouse.service.WarehouseService.record_etl_perf_run",
        side_effect=fake_record_etl_perf_run,
    ):
        rc = main(
            [
                "perf",
                "reingest-pending",
                "--pending-dir",
                str(pending),
                "--archive-dir",
                str(archive),
            ]
        )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["scanned"] == 2
    assert payload["ingested"] == 2
    assert payload["archived"] == 2
    assert payload["failed"] == 0
    assert list(pending.rglob("*.json")) == []
    assert len(list(archive.rglob("*.json"))) == 2


def test_cli_perf_reingest_returns_nonzero_when_failures(tmp_path, capsys):
    from robot_dh.warehouse.service import V15SchemaMissingError

    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    store = PendingPerfStore(local_dir=pending)
    store.emit(_make_perf("perf-fail"), reason="cli fail test")

    def _still_drifted(_payload):
        raise V15SchemaMissingError("column still missing")

    with patch(
        "robot_dh.warehouse.service.WarehouseService.record_etl_perf_run",
        side_effect=_still_drifted,
    ):
        rc = main(
            [
                "perf",
                "reingest-pending",
                "--pending-dir",
                str(pending),
                "--archive-dir",
                str(archive),
            ]
        )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["failed"] >= 1
    assert payload["aborted_reason"] is not None
    # 失败保留文件
    assert len(list(pending.rglob("*.json"))) == 1
