"""v1.8 warehouse 配置。

来源优先级：
    显式参数 > ROBOT_DH_WAREHOUSE_* 环境变量 > configs/warehouse.yaml > 内置默认值

设计要点：
    - schema 默认 'public'，远端 PostgreSQL 用 ROBOT_DH_WAREHOUSE_SCHEMA 覆盖。
    - output_root 支持本地路径 / file:// / s3://，未配置时默认 'file:///tmp/robot-dh/warehouse-export'。
    - YAML 容错：不存在或缺字段时不报错，回退到默认。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_WAREHOUSE_SCHEMA = "public"
DEFAULT_WAREHOUSE_OUTPUT_ROOT = "file:///tmp/robot-dh/warehouse-export"
DEFAULT_BUILD_LAYERS = ("dim", "fact", "dws", "ads")


@dataclass(frozen=True)
class WarehouseMetricsConfig:
    """v1.8 warehouse 运行时配置。

    Attributes:
        schema: 远端 PostgreSQL 模式名，SQLite 时被忽略。
        output_root: warehouse export 默认根，支持 file:// / s3://。
        default_layers: warehouse build 默认层级。
        sql_root: warehouse/sql/{ddl,dml} 所在目录，默认仓库根下的 warehouse/sql。
        timezone: dt 列计算用的时区，默认 UTC。
    """

    schema: str = DEFAULT_WAREHOUSE_SCHEMA
    output_root: str = DEFAULT_WAREHOUSE_OUTPUT_ROOT
    default_layers: tuple[str, ...] = DEFAULT_BUILD_LAYERS
    sql_root: Path = Path("warehouse/sql")
    timezone: str = "UTC"


def _load_yaml(path: Path) -> dict[str, Any]:
    """安全加载 yaml，文件不存在 / 解析失败时返回空 dict。"""
    if not path.exists():
        return {}
    try:
        import yaml
    except ImportError:
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def load_warehouse_metrics_config(
    *,
    config_path: Path | str | None = None,
    schema: str | None = None,
    output_root: str | None = None,
    sql_root: Path | str | None = None,
) -> WarehouseMetricsConfig:
    """合并 YAML / env / 显式参数；显式参数最高优先级。"""
    cfg_path = Path(config_path) if config_path else Path("configs/warehouse.yaml")
    raw = _load_yaml(cfg_path)
    yaml_warehouse: dict[str, Any] = {}
    if isinstance(raw, dict):
        yaml_warehouse = raw.get("warehouse", {}) if isinstance(raw.get("warehouse"), dict) else raw

    resolved_schema = (
        schema
        or os.environ.get("ROBOT_DH_WAREHOUSE_SCHEMA")
        or yaml_warehouse.get("schema")
        or DEFAULT_WAREHOUSE_SCHEMA
    )
    resolved_output = (
        output_root
        or os.environ.get("ROBOT_DH_WAREHOUSE_OUTPUT_ROOT")
        or yaml_warehouse.get("output_root")
        or DEFAULT_WAREHOUSE_OUTPUT_ROOT
    )
    resolved_sql_root_raw = (
        sql_root
        or os.environ.get("ROBOT_DH_WAREHOUSE_SQL_ROOT")
        or yaml_warehouse.get("sql_root")
        or "warehouse/sql"
    )
    resolved_sql_root = Path(resolved_sql_root_raw)
    layers_raw = yaml_warehouse.get("default_layers") or list(DEFAULT_BUILD_LAYERS)
    layers = tuple(str(layer).lower() for layer in layers_raw if str(layer).strip())
    if not layers:
        layers = DEFAULT_BUILD_LAYERS

    return WarehouseMetricsConfig(
        schema=str(resolved_schema),
        output_root=str(resolved_output),
        default_layers=layers,
        sql_root=resolved_sql_root,
        timezone=str(yaml_warehouse.get("timezone") or "UTC"),
    )
