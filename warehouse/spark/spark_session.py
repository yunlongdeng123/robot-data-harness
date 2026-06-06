"""warehouse/spark 入口的 SparkSession 工厂。

直接 re-export `robot_dh.spark_jobs.session.build_local_spark_session`，
保证用户在 `warehouse/spark/` 子目录内也能 import 同一份实现。
"""

from __future__ import annotations

from robot_dh.spark_jobs.session import (
    SparkUnavailableError,
    build_local_spark_session,
)

__all__ = ["SparkUnavailableError", "build_local_spark_session"]
