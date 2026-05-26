"""argo logs index：从 Argo workflow JSON 派生每个 step pod 的
archive log URI / URL，并把这些字段写回 PG ``workflow_steps`` 的 ``metrics`` 列。

archive log key 模板（与 v1.6 ``argo/workflow-controller-configmap`` 保持一致）：

    argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log

调用方：

    from robot_dh.argo.logs_index import index_archive_logs
    result = index_archive_logs(workflow_name="...", namespace="robot-dh",
                                 archive_root="s3://robot-dh-artifacts/argo-logs")

如果 ``workflow_steps`` 表还没引入 ``archive_log_uri`` / ``pod_uid`` / ``container_state``
等列（v1.6 schema 还没），本模块**不抛**——把这些字段塞进 ``metrics`` JSON
字典里，并打 warning 一行。
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_archive_root(uri: str) -> tuple[str, str, str]:
    """``s3://bucket/prefix/with/slashes`` -> (scheme, bucket, prefix)."""
    if not uri:
        return ("", "", "")
    if uri.startswith("s3://"):
        rest = uri[len("s3://"):]
        if "/" in rest:
            bucket, prefix = rest.split("/", 1)
        else:
            bucket, prefix = rest, ""
        return ("s3", bucket, prefix.rstrip("/"))
    if uri.startswith("file://"):
        # 保留绝对路径的前导 ``/``（``file:///abs/...``）；``file://relative`` 不带 / 也兼容
        path = uri[len("file://"):]
        # 注意：file:///abs 拆完之后是 "/abs"，rstrip 不能去掉前导 "/"。
        return ("file", "", path.rstrip("/"))
    return ("", "", uri.rstrip("/"))


@dataclass(slots=True)
class StepLogRecord:
    step_name: str
    pod_name: str | None
    pod_uid: str | None
    node_id: str
    node_type: str | None
    template_name: str | None
    container_name: str
    phase: str | None
    exit_code: int | None
    message: str | None
    archive_log_uri: str | None
    archive_log_url: str | None
    retry_attempt: int | None
    started_at: datetime | None
    finished_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        for k in ("started_at", "finished_at"):
            v = out.get(k)
            if isinstance(v, datetime):
                out[k] = v.strftime("%Y-%m-%dT%H:%M:%SZ")
        return out

    def metrics_payload(self) -> dict[str, Any]:
        """要塞进 ``workflow_steps.metrics`` JSON 的可选字段。"""
        out: dict[str, Any] = {}
        for k in (
            "pod_uid", "node_id", "node_type", "container_name",
            "exit_code", "archive_log_uri", "archive_log_url",
            "retry_attempt",
        ):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        return out


@dataclass(slots=True)
class LogsIndexResult:
    workflow_name: str
    namespace: str
    archive_root: str
    generated_at: str
    written_steps: int = 0
    skipped_steps: int = 0
    records: list[StepLogRecord] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_name": self.workflow_name,
            "namespace": self.namespace,
            "archive_root": self.archive_root,
            "generated_at": self.generated_at,
            "written_steps": self.written_steps,
            "skipped_steps": self.skipped_steps,
            "records": [r.to_dict() for r in self.records],
        }


def _parse_iso(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def build_log_records(
    workflow_payload: dict[str, Any],
    *,
    archive_root: str,
    container_name: str = "main",
) -> list[StepLogRecord]:
    """从 ``kubectl get workflow -o json`` 的 dict 中派生 StepLogRecord 列表。"""
    metadata = workflow_payload.get("metadata") or {}
    status = workflow_payload.get("status") or {}
    workflow_name = str(metadata.get("name") or "")
    namespace = str(metadata.get("namespace") or "")
    scheme, bucket, prefix = _split_archive_root(archive_root)

    nodes = status.get("nodes") or {}
    out: list[StepLogRecord] = []
    for node_id, node in nodes.items():
        node_type = node.get("type")
        if node_type not in ("Pod", "DAG", "Steps", "Container"):
            continue
        # 真实 step pod (Pod 类型) 才有 archive log；DAG / Steps 节点是逻辑节点。
        pod_name: str | None = None
        archive_log_uri: str | None = None
        archive_log_url: str | None = None
        if node_type == "Pod":
            pod_name = node.get("id") or node_id
            # prefix 形如 "argo-logs"（s3）或 "/mnt/.../argo"（file）。
            # s3 走相对 key（无前导 /）；file 必须保留绝对路径前导 /。
            prefix_clean = prefix.rstrip("/")
            tail = f"{namespace}/{workflow_name}/{pod_name}/{container_name}.log"
            if scheme == "file" and prefix_clean.startswith("/"):
                key = f"{prefix_clean}/{tail}"
            else:
                key = f"{prefix_clean}/{tail}".lstrip("/")
            if scheme == "s3":
                archive_log_uri = f"s3://{bucket}/{key}"
                archive_log_url = f"/buckets/{bucket}/browse/{key}"
            elif scheme == "file":
                archive_log_uri = f"file://{key}"
            else:
                archive_log_uri = key

        exit_code: int | None = None
        outputs = node.get("outputs") or {}
        if isinstance(outputs.get("exitCode"), (int, str)):
            try:
                exit_code = int(outputs["exitCode"])
            except (TypeError, ValueError):
                exit_code = None

        out.append(
            StepLogRecord(
                step_name=str(node.get("displayName") or node.get("name") or node_id),
                pod_name=pod_name,
                pod_uid=node.get("podUID"),  # Argo 不一定填；v1.6 schema 没列，metrics 兜底
                node_id=node_id,
                node_type=node_type,
                template_name=node.get("templateName"),
                container_name=container_name,
                phase=node.get("phase"),
                exit_code=exit_code,
                message=node.get("message"),
                archive_log_uri=archive_log_uri,
                archive_log_url=archive_log_url,
                retry_attempt=None,
                started_at=_parse_iso(node.get("startedAt")),
                finished_at=_parse_iso(node.get("finishedAt")),
            )
        )
    return out


def write_log_records_to_pg(
    records: list[StepLogRecord],
    *,
    workflow_name: str,
    namespace: str,
    warehouse: Any | None = None,
) -> tuple[int, int]:
    """把 archive_log_uri / pod_uid 等塞进 ``workflow_steps.metrics``。

    返回 (written, skipped)。
    """
    if warehouse is None:
        try:
            from robot_dh.warehouse.robot_platform import PlatformWarehouse

            warehouse = PlatformWarehouse(soft=True)
        except Exception as err:  # noqa: BLE001
            LOG.warning("argo logs index: PG unavailable, dry-run only: %s", err)
            return (0, len(records))

    written = 0
    skipped = 0
    for r in records:
        if r.node_type != "Pod":
            skipped += 1
            continue
        try:
            warehouse.upsert_workflow_step(
                workflow_name=workflow_name,
                step_name=r.step_name,
                workflow_namespace=namespace,
                template_name=r.template_name,
                pod_name=r.pod_name,
                phase=r.phase,
                started_at=r.started_at,
                finished_at=r.finished_at,
                metrics=r.metrics_payload(),
                message=r.message,
            )
            written += 1
        except Exception as err:  # noqa: BLE001
            LOG.warning(
                "argo logs index: upsert step %s failed (schema may need upgrade): %s",
                r.step_name, err,
            )
            skipped += 1
    return written, skipped


def index_archive_logs(
    *,
    workflow_name: str,
    namespace: str = "robot-dh",
    archive_root: str = "s3://robot-dh-artifacts/argo-logs",
    container_name: str = "main",
    workflow_payload: dict[str, Any] | None = None,
    from_json_path: str | Path | None = None,
    warehouse: Any | None = None,
    dry_run: bool = False,
) -> LogsIndexResult:
    """主入口。

    ``workflow_payload`` / ``from_json_path`` / kubectl 三选一。
    """
    if workflow_payload is None:
        if from_json_path is not None:
            workflow_payload = json.loads(Path(from_json_path).read_text(encoding="utf-8"))
        else:
            from robot_dh.argo.sync import fetch_workflow_via_kubectl

            workflow_payload = fetch_workflow_via_kubectl(workflow_name, namespace=namespace)

    records = build_log_records(
        workflow_payload,
        archive_root=archive_root,
        container_name=container_name,
    )
    if dry_run:
        return LogsIndexResult(
            workflow_name=workflow_name,
            namespace=namespace,
            archive_root=archive_root,
            generated_at=_now_iso(),
            written_steps=0,
            skipped_steps=len(records),
            records=records,
        )

    written, skipped = write_log_records_to_pg(
        records,
        workflow_name=workflow_name,
        namespace=namespace,
        warehouse=warehouse,
    )
    return LogsIndexResult(
        workflow_name=workflow_name,
        namespace=namespace,
        archive_root=archive_root,
        generated_at=_now_iso(),
        written_steps=written,
        skipped_steps=skipped,
        records=records,
    )
