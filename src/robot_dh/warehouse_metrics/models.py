"""warehouse_metrics 报告 / 任务的数据类。

设计：
- 用 dataclass，避免引入 pydantic（项目已尽量轻依赖）。
- BackfillTask / SlaCheck 在 quality_ops 复用，统一放这里方便 import。
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass
class LayerBuildResult:
    """单条 build SQL / 简化口径的执行结果。"""

    layer: str
    sql_file: str
    status: str
    duration_sec: float
    affected_rows: int | str = "unknown"
    error: str | None = None
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WarehouseBuildReport:
    """warehouse build 整体报告。"""

    start_date: str
    end_date: str
    layers: list[str]
    backend: str
    schema: str
    dry_run: bool
    started_at: str
    finished_at: str
    duration_sec: float
    status: str
    results: list[LayerBuildResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "layers": list(self.layers),
            "backend": self.backend,
            "schema": self.schema,
            "dry_run": self.dry_run,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_sec": round(self.duration_sec, 4),
            "status": self.status,
            "results": [r.to_dict() for r in self.results],
            "warnings": list(self.warnings),
        }


@dataclass
class BackfillTaskInput:
    """backfill_tasks 一行的构造参数。"""

    task_id: str
    plan_id: str
    dataset_id: str | None
    version: str | None
    dataset_family: str | None
    dt: date | None
    phase: str | None
    input_uri: str | None
    output_uri: str | None
    recommended_command: str
    status: str = "PLANNED"
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if isinstance(d.get("dt"), date):
            d["dt"] = d["dt"].isoformat()
        return d


@dataclass
class BackfillPlanInput:
    """backfill_plans 一行的构造参数。"""

    plan_id: str
    from_date: date | None
    to_date: date | None
    dataset_id: str | None
    version: str | None
    phase: str | None
    reason: str | None
    status: str
    task_count: int
    created_by: str | None
    plan_json: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for k in ("from_date", "to_date"):
            v = d.get(k)
            if isinstance(v, date):
                d[k] = v.isoformat()
        return d


def jsonable(value: Any) -> Any:
    """递归把 datetime / date / set / Path 转为 json 可序列化值。"""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, set):
        return [jsonable(v) for v in sorted(value, key=str)]
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if hasattr(value, "to_dict"):
        return jsonable(value.to_dict())
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)
