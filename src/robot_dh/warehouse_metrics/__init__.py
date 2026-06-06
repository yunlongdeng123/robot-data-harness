"""robot_dh v1.8 数仓指标层。

模块组织：
- config        v1.8 warehouse 全局配置（schema / output_root / 默认参数）
- dates         build window 工具（单日 / from-to）
- sql_runner    SQL 模板加载、参数渲染、dry-run、事务执行
- models        Python 端聚合的简化口径 + Pydantic-like 数据类
- builder       dim/fact/dws/ads 串接的物化 builder
- query         通用 SELECT
- exporter      query 结果导出 parquet / csv / json + manifest

PostgreSQL 是真生产路径；SQLite 用于单测，走简化的 Python builder。
"""

from robot_dh.warehouse_metrics.config import (
    DEFAULT_WAREHOUSE_SCHEMA,
    WarehouseMetricsConfig,
    load_warehouse_metrics_config,
)
from robot_dh.warehouse_metrics.dates import (
    DateRange,
    iter_dates,
    parse_date_range,
)
from robot_dh.warehouse_metrics.sql_runner import (
    SqlExecution,
    SqlTemplateRunner,
    SqlExecutionError,
)
from robot_dh.warehouse_metrics.builder import (
    WarehouseBuildReport,
    WarehouseBuilder,
    LayerBuildResult,
)
from robot_dh.warehouse_metrics.query import (
    WarehouseQueryService,
    WarehouseTableNotKnownError,
    WAREHOUSE_TABLE_REGISTRY,
)
from robot_dh.warehouse_metrics.exporter import (
    ExportManifest,
    WarehouseExporter,
)

__all__ = [
    "DEFAULT_WAREHOUSE_SCHEMA",
    "WarehouseMetricsConfig",
    "load_warehouse_metrics_config",
    "DateRange",
    "iter_dates",
    "parse_date_range",
    "SqlExecution",
    "SqlTemplateRunner",
    "SqlExecutionError",
    "WarehouseBuildReport",
    "WarehouseBuilder",
    "LayerBuildResult",
    "WarehouseQueryService",
    "WarehouseTableNotKnownError",
    "WAREHOUSE_TABLE_REGISTRY",
    "ExportManifest",
    "WarehouseExporter",
]
