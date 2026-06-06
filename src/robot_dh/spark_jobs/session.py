"""Spark local session 工厂。

仅 import 时不触发 pyspark；真正调用时再 lazy import，缺包时给清晰错误。

设计要点：
- master 固定 local[*]，绝不连 cluster；不开 hive support；不依赖 Hadoop / Yarn。
- 关闭 UI（spark.ui.enabled=false）减少端口冲突。
- driver memory 默认 2g，可通过 ROBOT_DH_SPARK_DRIVER_MEMORY 覆盖。
- s3a 默认不启用；本地优先 file:// URI；如需走 S3，由调用方在 Python 层 boto3
  download_dir 后再喂给 Spark（避免在 SparkSession 里加 hadoop-aws）。
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

LOG = logging.getLogger(__name__)

if TYPE_CHECKING:  # 仅为类型检查；运行期不强依赖 pyspark
    from pyspark.sql import SparkSession


class SparkUnavailableError(RuntimeError):
    """pyspark 未安装时抛出，附带安装指引。"""


def _require_pyspark() -> Any:
    """Lazy import pyspark；缺包时抛 SparkUnavailableError。"""
    try:
        import pyspark  # noqa: F401
        from pyspark.sql import SparkSession

        return SparkSession
    except ModuleNotFoundError as err:  # 缺 pyspark
        raise SparkUnavailableError(
            "pyspark 未安装。请安装可选依赖：\n"
            "    pip install -e \".[spark]\"\n"
            "或在 CI / Docker 里单独 pip install 'pyspark>=3.5,<4'。"
        ) from err


def build_local_spark_session(
    app_name: str = "robot-dh-quality-ads",
    driver_memory: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> "SparkSession":
    """构造 local[*] SparkSession。

    Args:
        app_name: Spark app name，仅作日志标识。
        driver_memory: driver JVM heap，默认读取 ROBOT_DH_SPARK_DRIVER_MEMORY 或 2g。
        extra_conf: 调用方追加的 spark conf。

    Returns:
        SparkSession 实例。调用方负责 stop()。
    """
    SparkSession = _require_pyspark()

    mem = driver_memory or os.environ.get("ROBOT_DH_SPARK_DRIVER_MEMORY", "2g")
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", mem)
        .config("spark.sql.session.timeZone", os.environ.get("ROBOT_DH_TIMEZONE", "UTC"))
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    spark = builder.getOrCreate()
    LOG.info(
        "spark local session ready: app=%s driver_memory=%s timezone=%s",
        app_name,
        mem,
        spark.conf.get("spark.sql.session.timeZone"),
    )
    return spark
