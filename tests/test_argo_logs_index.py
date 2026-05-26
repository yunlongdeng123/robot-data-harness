"""v1.7：argo logs index 派生 archive_log_uri / metrics 写回 PG。"""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from robot_dh.argo.logs_index import (
    build_log_records,
    index_archive_logs,
    write_log_records_to_pg,
)


def _fake_workflow_payload() -> dict[str, Any]:
    return {
        "metadata": {"name": "wf-demo", "namespace": "robot-dh"},
        "status": {
            "phase": "Succeeded",
            "nodes": {
                "wf-demo-dag": {
                    "id": "wf-demo-dag",
                    "name": "wf-demo.dag",
                    "displayName": "dag",
                    "type": "DAG",
                    "phase": "Succeeded",
                },
                "wf-demo-step1": {
                    "id": "wf-demo-step1",
                    "name": "wf-demo[0].step1",
                    "displayName": "step1",
                    "type": "Pod",
                    "phase": "Succeeded",
                    "templateName": "qc-contract-run",
                    "startedAt": "2026-05-25T10:00:00Z",
                    "finishedAt": "2026-05-25T10:01:00Z",
                    "outputs": {"exitCode": 0},
                },
                "wf-demo-step2": {
                    "id": "wf-demo-step2",
                    "name": "wf-demo[1].step2",
                    "displayName": "step2",
                    "type": "Pod",
                    "phase": "Failed",
                    "templateName": "etl-phase",
                    "startedAt": "2026-05-25T10:01:00Z",
                    "finishedAt": "2026-05-25T10:02:00Z",
                    "outputs": {"exitCode": 137},
                    "message": "OOMKilled",
                },
            },
        },
    }


def test_build_log_records_emits_one_per_pod() -> None:
    records = build_log_records(
        _fake_workflow_payload(),
        archive_root="s3://robot-dh-artifacts/argo-logs",
    )
    pod_records = [r for r in records if r.node_type == "Pod"]
    assert len(pod_records) == 2
    uris = {r.step_name: r.archive_log_uri for r in pod_records}
    assert uris["step1"] == "s3://robot-dh-artifacts/argo-logs/robot-dh/wf-demo/wf-demo-step1/main.log"
    assert uris["step2"] == "s3://robot-dh-artifacts/argo-logs/robot-dh/wf-demo/wf-demo-step2/main.log"


def test_build_log_records_handles_file_archive_root() -> None:
    records = build_log_records(
        _fake_workflow_payload(),
        archive_root="file:///mnt/local-data/robot-dh-local/logs/argo",
    )
    pod = [r for r in records if r.step_name == "step1"][0]
    assert pod.archive_log_uri.startswith("file:///mnt/local-data/robot-dh-local/logs/argo/")
    assert pod.archive_log_uri.endswith("/wf-demo-step1/main.log")


def test_write_log_records_to_pg_calls_upsert_with_archive_uri() -> None:
    records = build_log_records(
        _fake_workflow_payload(),
        archive_root="s3://robot-dh-artifacts/argo-logs",
    )
    warehouse = mock.MagicMock()
    warehouse.upsert_workflow_step.return_value = 1
    written, skipped = write_log_records_to_pg(
        records,
        workflow_name="wf-demo",
        namespace="robot-dh",
        warehouse=warehouse,
    )
    assert written == 2
    assert skipped == 1  # DAG node skipped
    call_kwargs = warehouse.upsert_workflow_step.call_args_list[0].kwargs
    assert call_kwargs["workflow_name"] == "wf-demo"
    assert call_kwargs["metrics"]["archive_log_uri"].startswith("s3://robot-dh-artifacts/argo-logs/")


def test_index_archive_logs_dry_run_does_not_write() -> None:
    result = index_archive_logs(
        workflow_name="wf-demo",
        namespace="robot-dh",
        workflow_payload=_fake_workflow_payload(),
        archive_root="s3://robot-dh-artifacts/argo-logs",
        dry_run=True,
    )
    assert result.written_steps == 0
    assert result.skipped_steps == len(result.records)


def test_write_log_records_continues_when_upsert_raises() -> None:
    records = build_log_records(
        _fake_workflow_payload(),
        archive_root="s3://robot-dh-artifacts/argo-logs",
    )
    warehouse = mock.MagicMock()
    warehouse.upsert_workflow_step.side_effect = RuntimeError("metrics col missing")
    written, skipped = write_log_records_to_pg(
        records,
        workflow_name="wf-demo",
        namespace="robot-dh",
        warehouse=warehouse,
    )
    # 异常 -> warning + 跳过；不抛
    assert written == 0
    assert skipped >= 2
