"""argo workflow metadata sync：从 kubectl 取 workflow JSON -> 写 PG workflow_runs / workflow_steps。

依赖：本机/容器内能 `kubectl get workflow -o json`；不可用时给出清晰错误。

提供 sync_from_kubectl(workflow_name, namespace) 与 sync_from_json(payload)。
JSON parser 与 kubectl 解耦，便于纯单元测试。
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from robot_dh.warehouse.robot_platform import PlatformWarehouse

LOG = logging.getLogger(__name__)


def _parse_iso(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


@dataclass(slots=True)
class WorkflowSyncResult:
    workflow_name: str
    workflow_namespace: str
    status: str | None
    steps: int


def parse_workflow_json(payload: dict[str, Any]) -> dict[str, Any]:
    """把 Argo workflow CR 的 JSON 抽成 (workflow_run, workflow_steps[])。

    输入 `payload` 是 kubectl get workflow <name> -o json 的结果。
    """
    metadata = payload.get("metadata") or {}
    spec = payload.get("spec") or {}
    status = payload.get("status") or {}

    workflow_name = str(metadata.get("name") or "")
    workflow_uid = metadata.get("uid")
    namespace = str(metadata.get("namespace") or "robot-dh")
    workflow_template = (spec.get("workflowTemplateRef") or {}).get("name")
    parameters = {p["name"]: p.get("value") for p in (spec.get("arguments") or {}).get("parameters", [])}

    started_at = _parse_iso(status.get("startedAt"))
    finished_at = _parse_iso(status.get("finishedAt"))
    duration_sec: float | None = None
    if started_at and finished_at:
        duration_sec = (finished_at - started_at).total_seconds()

    workflow_run = {
        "workflow_name": workflow_name,
        "workflow_uid": workflow_uid,
        "workflow_namespace": namespace,
        "workflow_template": workflow_template,
        "workflow_type": "argo",
        "status": status.get("phase"),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_sec": duration_sec,
        "parameters": parameters,
        "metrics": {
            "nodeCount": len(status.get("nodes") or {}),
            "progress": status.get("progress"),
        },
        "workflow_doc": {"raw_json_size": len(json.dumps(payload))},
    }

    steps: list[dict[str, Any]] = []
    nodes = status.get("nodes") or {}
    for node_id, node in nodes.items():
        if node.get("type") not in ("Pod", "DAG", "Steps", "Container"):
            continue
        s_at = _parse_iso(node.get("startedAt"))
        f_at = _parse_iso(node.get("finishedAt"))
        d_sec: float | None = None
        if s_at and f_at:
            d_sec = (f_at - s_at).total_seconds()
        steps.append(
            {
                "workflow_name": workflow_name,
                "workflow_namespace": namespace,
                "step_name": node.get("displayName") or node.get("name") or node_id,
                "template_name": node.get("templateName"),
                "pod_name": node.get("id") if node.get("type") == "Pod" else None,
                "phase": node.get("phase"),
                "started_at": s_at,
                "finished_at": f_at,
                "duration_sec": d_sec,
                "metrics": {"node_id": node_id, "type": node.get("type")},
                "message": node.get("message"),
            }
        )

    return {"workflow_run": workflow_run, "steps": steps}


def fetch_workflow_via_kubectl(workflow_name: str, namespace: str = "robot-dh") -> dict[str, Any]:
    if shutil.which("kubectl") is None:
        raise RuntimeError(
            "kubectl not found on PATH; argo sync requires kubectl to fetch workflow JSON"
        )
    cmd = ["kubectl", "-n", namespace, "get", "workflow", workflow_name, "-o", "json"]
    LOG.info("argo sync: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"kubectl get workflow failed (rc={proc.returncode}): {proc.stderr}")
    return json.loads(proc.stdout)


def sync_workflow(
    *,
    payload: dict[str, Any],
    warehouse: PlatformWarehouse | None = None,
) -> WorkflowSyncResult:
    parsed = parse_workflow_json(payload)
    wh = warehouse or PlatformWarehouse(soft=True)
    wfr = parsed["workflow_run"]
    wh.upsert_workflow_run(
        workflow_name=wfr["workflow_name"],
        workflow_uid=wfr.get("workflow_uid"),
        workflow_namespace=wfr.get("workflow_namespace"),
        workflow_template=wfr.get("workflow_template"),
        workflow_type=wfr.get("workflow_type"),
        status=wfr.get("status"),
        started_at=wfr.get("started_at"),
        finished_at=wfr.get("finished_at"),
        duration_sec=wfr.get("duration_sec"),
        parameters=wfr.get("parameters"),
        metrics=wfr.get("metrics"),
        workflow_doc=wfr.get("workflow_doc"),
    )
    for step in parsed["steps"]:
        wh.upsert_workflow_step(
            workflow_name=step["workflow_name"],
            step_name=step["step_name"],
            workflow_namespace=step.get("workflow_namespace"),
            template_name=step.get("template_name"),
            pod_name=step.get("pod_name"),
            phase=step.get("phase"),
            started_at=step.get("started_at"),
            finished_at=step.get("finished_at"),
            duration_sec=step.get("duration_sec"),
            metrics=step.get("metrics"),
            message=step.get("message"),
        )
    return WorkflowSyncResult(
        workflow_name=wfr["workflow_name"],
        workflow_namespace=wfr.get("workflow_namespace") or "robot-dh",
        status=wfr.get("status"),
        steps=len(parsed["steps"]),
    )


def sync_from_kubectl(
    workflow_name: str, namespace: str = "robot-dh",
    warehouse: PlatformWarehouse | None = None,
) -> WorkflowSyncResult:
    payload = fetch_workflow_via_kubectl(workflow_name, namespace=namespace)
    return sync_workflow(payload=payload, warehouse=warehouse)
