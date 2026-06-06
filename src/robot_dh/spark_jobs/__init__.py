"""v1.8 Spark local mode 离线数仓宽表层。

仅作为可选的离线开发流程对齐层：
- 读取 warehouse export 产出的 parquet（fact_etl_run / fact_qc_rule_result /
  fact_workflow_step / dim_dataset），不接 PostgreSQL；
- 基于 SparkSQL 计算 dws_dataset_quality_daily / ads_quality_dashboard 宽表；
- 输出 parquet + `_manifest.json`，方便接 Grafana / Trino / Superset / DuckDB。

不要在这里 import pyspark，让 pyspark 是真正可选依赖。需要时由 session.py
惰性 import 并抛出可读错误。
"""

from __future__ import annotations

from robot_dh.spark_jobs.quality_ads import (
    BuildQualityAdsResult,
    build_quality_ads,
)
from robot_dh.spark_jobs.session import (
    SparkUnavailableError,
    build_local_spark_session,
)

__all__ = [
    "BuildQualityAdsResult",
    "build_quality_ads",
    "SparkUnavailableError",
    "build_local_spark_session",
]
