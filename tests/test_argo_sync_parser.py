"""argo workflow JSON parser：脱离 kubectl 单元测试。"""

from __future__ import annotations

from datetime import timezone

from robot_dh.argo.sync import parse_workflow_json


def _fake_workflow_json() -> dict:
    return {
        "metadata": {
            "name": "robot-dh-multisource-scale30-abcde",
            "namespace": "robot-dh",
            "uid": "uid-1234",
        },
        "spec": {
            "workflowTemplateRef": {"name": "robot-dh-multisource-scale30"},
            "arguments": {
                "parameters": [
                    {"name": "version", "value": "v1"},
                    {"name": "quality_threshold", "value": "80"},
                ],
            },
        },
        "status": {
            "phase": "Succeeded",
            "startedAt": "2026-05-23T01:00:00Z",
            "finishedAt": "2026-05-23T03:30:00Z",
            "progress": "10/10",
            "nodes": {
                "n1": {
                    "type": "DAG",
                    "displayName": "main",
                    "templateName": "main",
                    "phase": "Succeeded",
                    "startedAt": "2026-05-23T01:00:00Z",
                    "finishedAt": "2026-05-23T03:30:00Z",
                },
                "n2": {
                    "type": "Pod",
                    "displayName": "droid-qc",
                    "templateName": "qc-contract-run",
                    "phase": "Succeeded",
                    "startedAt": "2026-05-23T01:05:00Z",
                    "finishedAt": "2026-05-23T01:30:00Z",
                },
                "n3": {
                    "type": "Pod",
                    "displayName": "droid-normalize",
                    "templateName": "etl-phase",
                    "phase": "Failed",
                    "startedAt": "2026-05-23T01:30:00Z",
                    "finishedAt": "2026-05-23T03:00:00Z",
                    "message": "deadline exceeded",
                },
            },
        },
    }


def test_parse_workflow_json_extracts_run_and_steps() -> None:
    parsed = parse_workflow_json(_fake_workflow_json())
    run = parsed["workflow_run"]
    steps = parsed["steps"]
    assert run["workflow_name"] == "robot-dh-multisource-scale30-abcde"
    assert run["status"] == "Succeeded"
    assert run["workflow_template"] == "robot-dh-multisource-scale30"
    assert run["parameters"]["version"] == "v1"
    assert run["duration_sec"] == 9000.0  # 2.5h
    assert len(steps) == 3
    failed = [s for s in steps if s["phase"] == "Failed"]
    assert failed and failed[0]["step_name"] == "droid-normalize"
    assert failed[0]["message"] == "deadline exceeded"


def test_parse_workflow_json_handles_missing_fields() -> None:
    parsed = parse_workflow_json({"metadata": {"name": "x"}})
    assert parsed["workflow_run"]["workflow_name"] == "x"
    assert parsed["steps"] == []
