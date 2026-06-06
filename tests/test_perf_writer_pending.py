"""F2 软降级 + reingest 行为覆盖：模拟 etl_perf_runs schema 漂移场景。

具体场景出自 ``docs/history/v1_6_etl_perf_runs_schema_align_request.md`` §4.2 推荐方案 A。
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from robot_dh.perf.pending import PendingPerfStore
from robot_dh.perf.profiler import PerfRecord
from robot_dh.perf.writer import (
    PERF_FAIL_MODE_ENV,
    emit_perf_records,
    reingest_pending_perf_records,
    write_perf_record_to_db,
)
from robot_dh.warehouse.service import (
    LakeMetadataUnavailableError,
    V15SchemaMissingError,
)


def _make_perf(job_id: str = "perf-1", phase: str = "normalize") -> PerfRecord:
    return PerfRecord(
        job_id=job_id,
        run_id="run-1",
        dataset_id="bridgedata_v2_scale30",
        version="v1",
        phase=phase,
        status="OK",
        duration_sec=12.3,
    )


def _schema_drift_error() -> V15SchemaMissingError:
    return V15SchemaMissingError(
        "warehouse record_etl_perf_run schema mismatch: "
        'column "started_at" of relation "etl_perf_runs" does not exist'
    )


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch, tmp_path):
    # 默认走 soft；测试需要 loud 时再单独设置
    monkeypatch.delenv(PERF_FAIL_MODE_ENV, raising=False)
    # 兜底防止落到用户 $HOME
    monkeypatch.setenv("ROBOT_DH_PERF_PENDING_DIR", str(tmp_path / "_pending_isolation"))
    monkeypatch.setenv("ROBOT_DH_PERF_ARCHIVE_DIR", str(tmp_path / "_archive_isolation"))


def test_write_perf_record_soft_fallback_to_local_pending(tmp_path):
    pending = tmp_path / "pending"
    wh = MagicMock()
    wh.record_etl_perf_run.side_effect = _schema_drift_error()
    store = PendingPerfStore(local_dir=pending)

    rid = write_perf_record_to_db(_make_perf(), warehouse=wh, pending_store=store)

    assert rid is None
    files = list(pending.rglob("*.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload["phase"] == "normalize"
    assert payload["job_id"] == "perf-1"
    assert payload["_pending"]["reason"].startswith("warehouse record_etl_perf_run schema mismatch")
    # 路径分段：<dataset>/<version>/<phase>/<job>-<salt>.json
    rel = files[0].relative_to(pending)
    parts = rel.parts
    assert parts[:3] == ("bridgedata_v2_scale30", "v1", "normalize")
    assert parts[3].startswith("perf-1-") and parts[3].endswith(".json")


def test_write_perf_record_loud_raises(tmp_path, monkeypatch):
    pending = tmp_path / "pending"
    monkeypatch.setenv(PERF_FAIL_MODE_ENV, "loud")
    wh = MagicMock()
    wh.record_etl_perf_run.side_effect = _schema_drift_error()
    store = PendingPerfStore(local_dir=pending)

    with pytest.raises(V15SchemaMissingError):
        write_perf_record_to_db(_make_perf(), warehouse=wh, pending_store=store)
    assert list(pending.rglob("*.json")) == []


def test_emit_perf_records_does_not_abort_on_schema_drift(tmp_path):
    pending = tmp_path / "pending"
    work_dir = tmp_path / "perf_work"
    wh = MagicMock()
    wh.record_etl_perf_run.side_effect = _schema_drift_error()
    store = PendingPerfStore(local_dir=pending)
    records = [_make_perf("perf-a", "normalize"), _make_perf("perf-b", "build_features")]

    emit_perf_records(records, work_dir=work_dir, warehouse=wh, pending_store=store)

    assert wh.record_etl_perf_run.call_count == 2
    json_files = sorted(p.name for p in work_dir.iterdir())
    assert json_files == ["build_features_perf.json", "normalize_perf.json"]
    pending_files = list(pending.rglob("*.json"))
    assert len(pending_files) == 2


def test_emit_perf_records_loud_propagates(tmp_path, monkeypatch):
    pending = tmp_path / "pending"
    work_dir = tmp_path / "perf_work"
    monkeypatch.setenv(PERF_FAIL_MODE_ENV, "loud")
    wh = MagicMock()
    wh.record_etl_perf_run.side_effect = _schema_drift_error()
    store = PendingPerfStore(local_dir=pending)

    with pytest.raises(V15SchemaMissingError):
        emit_perf_records([_make_perf()], work_dir=work_dir, warehouse=wh, pending_store=store)


def test_reingest_pending_moves_to_archive(tmp_path):
    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    store = PendingPerfStore(local_dir=pending)
    store.emit(_make_perf("perf-x"), reason="test schema mismatch")
    store.emit(_make_perf("perf-y"), reason="test schema mismatch")
    assert len(list(pending.rglob("*.json"))) == 2

    wh = MagicMock()
    wh.record_etl_perf_run.return_value = 42

    stats = reingest_pending_perf_records(
        pending_dir=pending,
        archive_dir=archive,
        warehouse=wh,
    )

    assert stats["scanned"] == 2
    assert stats["ingested"] == 2
    assert stats["archived"] == 2
    assert stats["failed"] == 0
    assert stats["aborted_reason"] is None
    assert list(pending.rglob("*.json")) == []
    assert len(list(archive.rglob("*.json"))) == 2

    # archive 后 emit 时塞进去的 `_pending` 不应回灌到 PG
    for call in wh.record_etl_perf_run.call_args_list:
        payload = call.args[0]
        assert "_pending" not in payload
        assert payload["phase"] == "normalize"


def test_reingest_pending_abort_on_schema_still_missing(tmp_path):
    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    store = PendingPerfStore(local_dir=pending)
    store.emit(_make_perf("perf-x"), reason="test")
    store.emit(_make_perf("perf-y"), reason="test")

    wh = MagicMock()
    wh.record_etl_perf_run.side_effect = _schema_drift_error()

    stats = reingest_pending_perf_records(
        pending_dir=pending,
        archive_dir=archive,
        warehouse=wh,
    )

    assert stats["ingested"] == 0
    assert stats["failed"] == 1
    assert stats["aborted_reason"] is not None
    # 立刻中断，两个文件都保留
    assert len(list(pending.rglob("*.json"))) == 2


def test_reingest_pending_abort_on_lake_metadata_unavailable(tmp_path):
    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    store = PendingPerfStore(local_dir=pending)
    store.emit(_make_perf("perf-x"), reason="test")

    wh = MagicMock()
    wh.record_etl_perf_run.side_effect = LakeMetadataUnavailableError("no PG")

    stats = reingest_pending_perf_records(
        pending_dir=pending,
        archive_dir=archive,
        warehouse=wh,
    )
    assert stats["failed"] == 1
    assert stats["aborted_reason"] == "no PG"
    assert len(list(pending.rglob("*.json"))) == 1


def test_reingest_dry_run_does_not_change_files(tmp_path):
    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    store = PendingPerfStore(local_dir=pending)
    store.emit(_make_perf("perf-x"), reason="test")

    wh = MagicMock()

    stats = reingest_pending_perf_records(
        pending_dir=pending,
        archive_dir=archive,
        warehouse=wh,
        dry_run=True,
    )

    assert stats["scanned"] == 1
    assert stats["skipped"] == 1
    assert stats["ingested"] == 0
    wh.record_etl_perf_run.assert_not_called()
    assert len(list(pending.rglob("*.json"))) == 1
    assert not archive.exists() or not list(archive.rglob("*.json"))


def test_reingest_empty_pending_dir_is_noop(tmp_path):
    pending = tmp_path / "pending_empty"
    archive = tmp_path / "archive"
    wh = MagicMock()
    stats = reingest_pending_perf_records(
        pending_dir=pending,
        archive_dir=archive,
        warehouse=wh,
    )
    assert stats == {
        "scanned": 0,
        "ingested": 0,
        "archived": 0,
        "failed": 0,
        "skipped": 0,
        "aborted_reason": None,
        "pending_dir": str(pending),
        "archive_dir": str(archive),
    }
    wh.record_etl_perf_run.assert_not_called()


def test_pending_store_s3_mirror_failure_does_not_break_local(tmp_path):
    pending = tmp_path / "pending"
    s3 = MagicMock()
    s3.upload_file.side_effect = RuntimeError("network down")
    store = PendingPerfStore(
        local_dir=pending,
        s3_client=s3,
        s3_bucket="robot-dh-artifacts",
    )

    uris = store.emit(_make_perf(), reason="test")

    assert Path(uris["local"]).is_file()
    assert uris["s3"] == "skipped"  # mirror 失败，但本地必落已成功


def test_pending_store_s3_mirror_success(tmp_path):
    pending = tmp_path / "pending"
    s3 = MagicMock()
    store = PendingPerfStore(
        local_dir=pending,
        s3_client=s3,
        s3_bucket="robot-dh-artifacts",
    )

    uris = store.emit(_make_perf("perf-mirror"), reason="test")

    assert s3.upload_file.call_count == 1
    assert uris["s3"].startswith("s3://robot-dh-artifacts/perf-records-pending/")
    assert uris["s3"].endswith(".json")


def test_reingest_archive_uses_s3_copy_then_delete(tmp_path):
    pending = tmp_path / "pending"
    archive = tmp_path / "archive"
    s3 = MagicMock()
    store = PendingPerfStore(
        local_dir=pending,
        s3_client=s3,
        s3_bucket="robot-dh-artifacts",
    )
    store.emit(_make_perf("perf-archive-s3"), reason="test")

    wh = MagicMock()
    wh.record_etl_perf_run.return_value = 1

    stats = reingest_pending_perf_records(
        pending_dir=pending,
        archive_dir=archive,
        warehouse=wh,
        pending_store=store,
    )

    assert stats["ingested"] == 1
    assert stats["archived"] == 1
    # copy_object + delete_object 必须都被调用
    assert s3.copy_object.call_count == 1
    assert s3.delete_object.call_count == 1
    copy_kwargs = s3.copy_object.call_args.kwargs
    assert copy_kwargs["Bucket"] == "robot-dh-artifacts"
    assert copy_kwargs["Key"].startswith("perf-records-archived/")
    assert copy_kwargs["CopySource"]["Key"].startswith("perf-records-pending/")
