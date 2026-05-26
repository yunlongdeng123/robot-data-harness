"""v1.6.4 Argo sync 与 lineage report；v1.7 archive logs index。"""

from robot_dh.argo.logs_index import (
    LogsIndexResult,
    StepLogRecord,
    build_log_records,
    index_archive_logs,
)
from robot_dh.argo.sync import (
    WorkflowSyncResult,
    fetch_workflow_via_kubectl,
    parse_workflow_json,
    sync_from_kubectl,
    sync_workflow,
)

__all__ = [
    "WorkflowSyncResult",
    "fetch_workflow_via_kubectl",
    "parse_workflow_json",
    "sync_from_kubectl",
    "sync_workflow",
    "LogsIndexResult",
    "StepLogRecord",
    "build_log_records",
    "index_archive_logs",
]
