"""lineage report：扫已有 workflow_steps + lineage_edges -> 一个 JSON 摘要。

只读；用于 Argo workflow 最后一步把整条 DAG 的输入输出 + 状态序列化到 lake/lineage/。
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from robot_dh.lake.store import create_lake_store
from robot_dh.warehouse.robot_platform import PlatformWarehouse

LOG = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class LineageReport:
    workflow_name: str
    workflow_namespace: str
    workflow_status: str | None
    steps_total: int
    steps_failed: int
    qc_runs: list[dict[str, Any]] = field(default_factory=list)
    ml_ready_datasets: list[dict[str, Any]] = field(default_factory=list)
    asset_profiles: list[dict[str, Any]] = field(default_factory=list)
    workflow_steps: list[dict[str, Any]] = field(default_factory=list)
    workflow_run: dict[str, Any] | None = None
    generated_at: str = field(default_factory=_utcnow_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_lineage_report(
    *,
    workflow_name: str,
    workflow_namespace: str = "robot-dh",
    warehouse: PlatformWarehouse | None = None,
) -> LineageReport:
    wh = warehouse or PlatformWarehouse(soft=True)

    workflow_run: dict[str, Any] | None = None
    try:
        workflow_run = wh.get_workflow_run(
            workflow_name=workflow_name, workflow_namespace=workflow_namespace,
        )
    except Exception as err:
        LOG.warning("lineage report: get_workflow_run failed: %s", err)

    steps: list[dict[str, Any]] = []
    try:
        steps = wh.list_workflow_steps(
            workflow_name=workflow_name, workflow_namespace=workflow_namespace, limit=500,
        )
    except Exception as err:
        LOG.warning("lineage report: list_workflow_steps failed: %s", err)
    failed = sum(1 for s in steps if s.get("phase") == "Failed")

    qc_runs: list[dict[str, Any]] = []
    try:
        qc_runs = wh.list_qc_contract_runs(limit=20)
    except Exception as err:
        LOG.warning("lineage report: list_qc_contract_runs failed: %s", err)

    ml_ready: list[dict[str, Any]] = []
    try:
        ml_ready = wh.list_ml_ready_datasets(limit=20)
    except Exception as err:
        LOG.warning("lineage report: list_ml_ready_datasets failed: %s", err)

    profiles: list[dict[str, Any]] = []
    try:
        profiles = wh.list_asset_profiles(limit=20)
    except Exception as err:
        LOG.warning("lineage report: list_asset_profiles failed: %s", err)

    return LineageReport(
        workflow_name=workflow_name,
        workflow_namespace=workflow_namespace,
        workflow_status=(workflow_run or {}).get("status"),
        steps_total=len(steps),
        steps_failed=failed,
        qc_runs=qc_runs,
        ml_ready_datasets=ml_ready,
        asset_profiles=profiles,
        workflow_steps=steps,
        workflow_run=workflow_run,
    )


def write_lineage_report(report: LineageReport, output_uri: str) -> str:
    """把 LineageReport 写到 output_uri；返回写入 uri。"""
    store = create_lake_store(output_uri)
    payload = report.to_dict()
    store.write_json(output_uri, payload)
    return output_uri
