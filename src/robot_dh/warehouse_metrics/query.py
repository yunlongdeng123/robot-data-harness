"""warehouse 查询。

只读、白名单：表名 / 列名 必须在注册表里，避免 SQL injection。
WHERE 子句允许简单条件（``col = 'value'`` / ``col IN ('a','b')``），实际通过 SQLAlchemy text + bindparams 绑定。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from robot_dh.registry import get_engine, init_db, resolve_db_uri
from robot_dh.warehouse.models import (
    ensure_lake_tables,
    WAREHOUSE_METRICS_TABLES,
)
from robot_dh.warehouse_metrics.config import WarehouseMetricsConfig

LOG = logging.getLogger(__name__)

# 表名 / 列名校验：仅允许小写字母、数字、下划线
_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# 注册：表名 -> {"layer": ..., "primary_key": [...] }
WAREHOUSE_TABLE_REGISTRY: dict[str, dict[str, Any]] = {
    "dim_dataset": {"layer": "dim", "primary_key": ["dataset_key"]},
    "fact_etl_run": {"layer": "fact", "primary_key": ["run_key"]},
    "fact_qc_rule_result": {"layer": "fact", "primary_key": ["rule_result_key"]},
    "fact_workflow_step": {"layer": "fact", "primary_key": ["step_key"]},
    "fact_asset_profile": {"layer": "fact", "primary_key": ["asset_profile_key"]},
    "dws_dataset_quality_daily": {"layer": "dws", "primary_key": ["dt", "dataset_id", "version"]},
    "dws_rule_failure_daily": {"layer": "dws", "primary_key": ["dt", "dataset_family", "contract_id", "rule_id", "severity"]},
    "dws_workflow_ops_daily": {"layer": "dws", "primary_key": ["dt", "workflow_type"]},
    "ads_quality_dashboard": {"layer": "ads", "primary_key": ["dt", "dataset_id", "version"]},
    "ads_workflow_ops_dashboard": {"layer": "ads", "primary_key": ["dt", "workflow_type"]},
    "backfill_plans": {"layer": "backfill", "primary_key": ["plan_id"]},
    "backfill_tasks": {"layer": "backfill", "primary_key": ["task_id"]},
    "sla_policies": {"layer": "sla", "primary_key": ["policy_id"]},
    "sla_checks": {"layer": "sla", "primary_key": ["check_id"]},
    "dataset_partition_readiness": {"layer": "sla", "primary_key": ["readiness_key"]},
    # v1.9 推理数据平面（只读查询）。
    "model_registry": {"layer": "inference", "primary_key": ["model_id"]},
    "inference_jobs": {"layer": "inference", "primary_key": ["job_id"]},
    "inference_outputs": {"layer": "inference", "primary_key": ["output_id"]},
    "inference_failures": {"layer": "inference", "primary_key": ["failure_id"]},
    "distillation_datasets": {"layer": "inference", "primary_key": ["distill_id"]},
    "inference_benchmark_runs": {"layer": "inference", "primary_key": ["benchmark_id"]},
    "dws_inference_job_daily": {"layer": "inference", "primary_key": ["dt", "model_id", "task_type"]},
    "ads_inference_dashboard": {"layer": "inference", "primary_key": ["dt", "model_id", "task_type"]},
}


class WarehouseTableNotKnownError(ValueError):
    """请求的 table 不在 v1.8 注册表中。"""


@dataclass
class QueryRequest:
    table: str
    limit: int = 100
    where: str | None = None
    order_by: str | None = None


class WarehouseQueryService:
    """v1.8 查询服务（只读）。"""

    def __init__(
        self,
        *,
        config: WarehouseMetricsConfig | None = None,
        db_uri: str | None = None,
    ) -> None:
        self._config = config or WarehouseMetricsConfig()
        self._db_uri = db_uri
        self._engine: Engine | None = None

    def get_engine(self) -> Engine:
        if self._engine is None:
            resolved = resolve_db_uri(self._db_uri)
            engine = get_engine(resolved)
            if engine.dialect.name == "sqlite":
                init_db(resolved)
                ensure_lake_tables(engine)
            self._engine = engine
        return self._engine

    def list_tables(self) -> list[dict[str, Any]]:
        engine = self.get_engine()
        existing = set(inspect(engine).get_table_names())
        return [
            {
                "table": name,
                "layer": info["layer"],
                "primary_key": list(info["primary_key"]),
                "present": name in existing,
            }
            for name, info in WAREHOUSE_TABLE_REGISTRY.items()
        ]

    def query(self, request: QueryRequest) -> list[dict[str, Any]]:
        if request.table not in WAREHOUSE_TABLE_REGISTRY:
            raise WarehouseTableNotKnownError(
                f"table '{request.table}' not in v1.8 registry; known={sorted(WAREHOUSE_TABLE_REGISTRY)}"
            )
        if not _IDENT_RE.match(request.table):
            raise WarehouseTableNotKnownError(f"illegal table name: {request.table}")

        limit = max(1, min(int(request.limit), 10000))
        sql_parts = [f"SELECT * FROM {self._qualified(request.table)}"]
        bind_params: dict[str, Any] = {}
        if request.where:
            where_clause, where_params = _parse_where(request.where)
            sql_parts.append(f"WHERE {where_clause}")
            bind_params.update(where_params)
        if request.order_by:
            order_col = _validate_order(request.order_by)
            sql_parts.append(f"ORDER BY {order_col}")
        sql_parts.append(f"LIMIT {limit}")
        sql_str = " ".join(sql_parts)

        engine = self.get_engine()
        existing = set(inspect(engine).get_table_names())
        if request.table not in existing:
            raise WarehouseTableNotKnownError(
                f"table '{request.table}' missing in DB; please run infra migration first"
            )
        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql_str), bind_params)
                cols = list(result.keys())
                return [dict(zip(cols, row)) for row in result.fetchmany(limit)]
        except SQLAlchemyError as err:
            raise RuntimeError(f"query failed: {err}") from err

    def _qualified(self, table: str) -> str:
        engine = self.get_engine()
        if engine.dialect.name == "postgresql":
            return f"{self._config.schema}.{table}"
        return table


def _parse_where(where: str) -> tuple[str, dict[str, Any]]:
    """解析简单的 WHERE 子句：

    支持：
        col = 'value'
        col IN ('a','b','c')
        col1 = 'v1' AND col2 = 'v2'

    实现：拆 AND，逐条 token 化，再用 bindparam 绑定。
    """
    parts = re.split(r"\s+AND\s+", where.strip(), flags=re.IGNORECASE)
    sql_pieces: list[str] = []
    bind_params: dict[str, Any] = {}
    for idx, frag in enumerate(parts):
        frag = frag.strip()
        in_match = re.match(r"^([a-z][a-z0-9_]*)\s+IN\s*\(([^()]+)\)\s*$", frag, flags=re.IGNORECASE)
        if in_match:
            col = in_match.group(1).lower()
            if not _IDENT_RE.match(col):
                raise WarehouseTableNotKnownError(f"illegal column in WHERE: {col}")
            values_raw = in_match.group(2)
            values = [v.strip().strip("'\"") for v in values_raw.split(",") if v.strip()]
            if not values:
                raise WarehouseTableNotKnownError("empty IN value list")
            placeholders = []
            for j, v in enumerate(values):
                p = f"p_{idx}_{j}"
                placeholders.append(f":{p}")
                bind_params[p] = v
            sql_pieces.append(f"{col} IN ({', '.join(placeholders)})")
            continue
        eq_match = re.match(r"^([a-z][a-z0-9_]*)\s*=\s*'([^']*)'\s*$", frag, flags=re.IGNORECASE)
        if eq_match:
            col = eq_match.group(1).lower()
            if not _IDENT_RE.match(col):
                raise WarehouseTableNotKnownError(f"illegal column in WHERE: {col}")
            p = f"p_{idx}"
            bind_params[p] = eq_match.group(2)
            sql_pieces.append(f"{col} = :{p}")
            continue
        raise WarehouseTableNotKnownError(
            f"unsupported WHERE fragment '{frag}'; only \"col = 'value'\" / \"col IN ('a','b')\" supported (joined by AND)"
        )
    return " AND ".join(sql_pieces), bind_params


def _validate_order(order_by: str) -> str:
    """允许 ``col`` / ``col ASC`` / ``col DESC``，禁止 ``;`` / ``--``。"""
    m = re.match(r"^([a-z][a-z0-9_]*)(?:\s+(ASC|DESC))?$", order_by.strip(), flags=re.IGNORECASE)
    if not m:
        raise WarehouseTableNotKnownError(f"illegal ORDER BY: {order_by}")
    col = m.group(1).lower()
    direction = (m.group(2) or "ASC").upper()
    return f"{col} {direction}"
