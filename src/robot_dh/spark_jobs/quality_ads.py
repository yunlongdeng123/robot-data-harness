"""SparkSQL build：DWS dataset_quality_daily + ADS quality_dashboard。

调用流程：
1. CLI 或 Makefile 调用 build_quality_ads(input_uri, output_uri, dt)；
2. 启动 local[*] SparkSession；
3. 读取 input_uri 下的 4 张 parquet（warehouse export 产出，按 dt 分区或单文件均可）：
       fact_etl_run / fact_qc_rule_result / fact_workflow_step / dim_dataset
4. 注册为 Spark temp view；
5. 加载 SparkSQL 模板（warehouse/spark/sql/*.sql），渲染 {{ dt }} 后 spark.sql() 执行；
6. 把 DWS 结果再注册为 temp view 后跑 ADS；
7. 输出两张 parquet：
       <output>/dws_dataset_quality_daily/dt=<dt>/part-*.parquet
       <output>/ads_quality_dashboard/dt=<dt>/part-*.parquet
8. 同时写 _manifest.json 描述 source / row_count / 列等元信息。

设计要点：
- SparkSession 不强制 import；通过 session.py 惰性加载，缺包给清晰错误。
- 输入路径如果是 file://，本地 Spark 直接读；其他 scheme 暂不支持（promptC 明确不引入 hadoop-aws）。
- 4 张输入表如果缺失某一张，按"等价于空表"处理；ADS 仍能产出（指标可能为 NULL）。
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from robot_dh.spark_jobs.session import (
    SparkUnavailableError,
    build_local_spark_session,
)

LOG = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


SOURCE_TABLES: tuple[str, ...] = (
    "fact_etl_run",
    "fact_qc_rule_result",
    "fact_workflow_step",
    "dim_dataset",
)

DEFAULT_SQL_ROOT = Path(__file__).resolve().parents[3] / "warehouse" / "spark" / "sql"


@dataclass(frozen=True)
class BuildQualityAdsResult:
    """单次 build 的结构化结果。"""

    dt: str
    input_uri: str
    output_uri: str
    dws_path: str
    ads_path: str
    dws_row_count: int
    ads_row_count: int
    sources_present: dict[str, bool]
    manifest_path: str
    duration_sec: float
    extras: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dt": self.dt,
            "input_uri": self.input_uri,
            "output_uri": self.output_uri,
            "dws_path": self.dws_path,
            "ads_path": self.ads_path,
            "dws_row_count": self.dws_row_count,
            "ads_row_count": self.ads_row_count,
            "sources_present": dict(self.sources_present),
            "manifest_path": self.manifest_path,
            "duration_sec": round(self.duration_sec, 3),
            "extras": dict(self.extras),
        }


def _normalize_uri_to_path(uri: str) -> Path:
    """把 file:// URI 转 Path；非 file:// 抛错。"""
    parsed = urlparse(uri)
    if parsed.scheme in ("", "file"):
        if parsed.scheme == "file":
            return Path(parsed.path)
        return Path(uri)
    raise ValueError(
        f"unsupported scheme={parsed.scheme!r} for Spark local mode (uri={uri!r}). "
        f"promptC 仅支持 file:// 与本地路径；如需 s3://，请先 boto3 download_dir 到本地。"
    )


def _try_load_parquet(
    spark: "SparkSession", input_root: Path, table: str
) -> tuple["DataFrame | None", bool]:
    """加载 <input_root>/<table> 下的 parquet。

    关键：warehouse export 用 `dt=<export_date>` 命名子目录表示"导出于哪一天"，
    而**不是**按 fact 表的 dt 列做物理分区——bridgedata_v2_scale30 / droid_lerobot_scale30
    在 fact_qc_rule_result.parquet 内的 dt='2026-05-25'，但写在了 `dt=2026-05-26/` 下。
    如果让 Spark 走默认的 partition discovery，目录上的 `dt=2026-05-26` 会覆盖行级 dt，
    导致 `WHERE dt = DATE('2026-05-26')` 误把昨日数据算到今天。
    用 recursiveFileLookup=true 显式禁用 partition discovery，让 Spark 只读 parquet 自身。
    """
    table_dir = input_root / table
    if not table_dir.exists():
        LOG.warning("source table dir not found: %s", table_dir)
        return None, False
    try:
        df = (
            spark.read
            .option("mergeSchema", "true")
            .option("recursiveFileLookup", "true")
            .parquet(str(table_dir))
        )
        n = df.count()
        LOG.info("loaded source table=%s rows=%d", table, n)
        return df, True
    except Exception as err:  # parquet 损坏 / 空目录
        LOG.warning("failed to read parquet for %s under %s: %s", table, table_dir, err)
        return None, False


def _empty_view(spark: "SparkSession", table: str) -> "DataFrame":
    """缺源表时建一张完全为空的占位 view，保持 SparkSQL 不抛 AnalysisException。"""
    from pyspark.sql.types import (
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schemas: dict[str, StructType] = {
        "fact_etl_run": StructType([
            StructField("run_key", StringType()),
            StructField("job_id", StringType()),
            StructField("run_id", StringType()),
            StructField("dataset_id", StringType()),
            StructField("version", StringType()),
            StructField("dataset_family", StringType()),
            StructField("phase", StringType()),
            StructField("status", StringType()),
            StructField("started_at", TimestampType()),
            StructField("finished_at", TimestampType()),
            StructField("dt", DateType()),
            StructField("duration_sec", DoubleType()),
            StructField("input_bytes", LongType()),
            StructField("output_bytes", LongType()),
            StructField("input_rows", LongType()),
            StructField("output_rows", LongType()),
            StructField("peak_memory_mb", DoubleType()),
            StructField("error_message", StringType()),
            StructField("archive_log_uri", StringType()),
            StructField("created_at", TimestampType()),
        ]),
        "fact_qc_rule_result": StructType([
            StructField("rule_result_key", StringType()),
            StructField("run_id", StringType()),
            StructField("contract_id", StringType()),
            StructField("dataset_id", StringType()),
            StructField("version", StringType()),
            StructField("dataset_family", StringType()),
            StructField("rule_id", StringType()),
            StructField("severity", StringType()),
            StructField("status", StringType()),
            StructField("metric", StringType()),
            StructField("op", StringType()),
            StructField("threshold_value", StringType()),
            StructField("actual_value", StringType()),
            StructField("dt", DateType()),
            StructField("created_at", TimestampType()),
        ]),
        "fact_workflow_step": StructType([
            StructField("step_key", StringType()),
            StructField("workflow_name", StringType()),
            StructField("workflow_namespace", StringType()),
            StructField("workflow_type", StringType()),
            StructField("step_name", StringType()),
            StructField("template_name", StringType()),
            StructField("pod_name", StringType()),
            StructField("phase", StringType()),
            StructField("dataset_id", StringType()),
            StructField("version", StringType()),
            StructField("dataset_family", StringType()),
            StructField("started_at", TimestampType()),
            StructField("finished_at", TimestampType()),
            StructField("dt", DateType()),
            StructField("duration_sec", DoubleType()),
            StructField("exit_code", IntegerType()),
            StructField("container_reason", StringType()),
            StructField("archive_log_uri", StringType()),
            StructField("archive_log_url", StringType()),
            StructField("created_at", TimestampType()),
        ]),
        "dim_dataset": StructType([
            StructField("dataset_key", StringType()),
            StructField("dataset_id", StringType()),
            StructField("version", StringType()),
            StructField("dataset_family", StringType()),
            StructField("source_uri", StringType()),
            StructField("raw_uri", StringType()),
            StructField("ods_uri", StringType()),
            StructField("dwd_uri", StringType()),
            StructField("ads_uri", StringType()),
            StructField("ml_ready_uri", StringType()),
            StructField("first_seen_at", TimestampType()),
            StructField("latest_status", StringType()),
            StructField("latest_quality_score", DoubleType()),
            StructField("is_active", IntegerType()),
            StructField("updated_at", TimestampType()),
        ]),
    }
    schema = schemas[table]
    return spark.createDataFrame([], schema)


def _render_sql(sql_text: str, params: dict[str, str]) -> str:
    """把 {{ key }} 占位符替换为 params[key]。

    安全：仅匹配 `{{ alnum_or_underscore }}`，且 value 仅允许 [A-Za-z0-9_\\-:.]，
    用于 dt 等元数据，不允许嵌入任意 SQL 片段。
    """
    safe_value_re = re.compile(r"^[A-Za-z0-9_\-:.]+$")
    placeholder_re = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
    missing: list[str] = []

    def _sub(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in params:
            missing.append(key)
            return match.group(0)
        v = str(params[key])
        if not safe_value_re.match(v):
            raise ValueError(
                f"unsafe value for placeholder {{{{ {key} }}}}: {v!r} 不符合 [A-Za-z0-9_\\-:.] 限制"
            )
        return v

    rendered = placeholder_re.sub(_sub, sql_text)
    if missing:
        raise KeyError(f"SparkSQL 模板缺少占位符 value: {sorted(set(missing))}")
    return rendered


def _load_sql_template(name: str, sql_root: Path | None = None) -> str:
    """从 warehouse/spark/sql/<name>.sql 加载文本。"""
    root = sql_root or DEFAULT_SQL_ROOT
    path = root / name
    if not path.exists():
        raise FileNotFoundError(f"SparkSQL 模板不存在：{path}")
    return path.read_text(encoding="utf-8")


def build_quality_ads(
    input_uri: str,
    output_uri: str,
    dt: str,
    *,
    sql_root: Path | None = None,
    extra_conf: dict[str, str] | None = None,
    keep_session: bool = False,
    existing_spark: "SparkSession | None" = None,
) -> BuildQualityAdsResult:
    """读取 warehouse export 4 张 parquet，跑 SparkSQL，输出 DWS + ADS parquet。

    Args:
        input_uri: file:// 或本地路径，下面应有 <input>/<table>/[dt=*/]/*.parquet 子树。
        output_uri: file:// 或本地路径，会建立 <output>/dws_dataset_quality_daily/dt=<dt>/ 等。
        dt: YYYY-MM-DD，按该日切片聚合。
        sql_root: SparkSQL 模板根目录，默认 warehouse/spark/sql。
        extra_conf: 额外 Spark conf。
        keep_session: 完成后不 stop()，用于测试串联多次 build。
        existing_spark: 复用已有 SparkSession（测试用）。

    Returns:
        BuildQualityAdsResult。
    """
    started = datetime.now(timezone.utc)
    input_root = _normalize_uri_to_path(input_uri).resolve()
    output_root = _normalize_uri_to_path(output_uri).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    spark = existing_spark or build_local_spark_session(extra_conf=extra_conf)
    sources_present: dict[str, bool] = {}
    try:
        for table in SOURCE_TABLES:
            df, present = _try_load_parquet(spark, input_root, table)
            sources_present[table] = present
            if not present:
                df = _empty_view(spark, table)
            df.createOrReplaceTempView(table)

        dws_sql = _render_sql(
            _load_sql_template("build_dws_dataset_quality_daily.sql", sql_root),
            {"dt": dt},
        )
        ads_sql_template = _load_sql_template("build_ads_quality_dashboard.sql", sql_root)

        LOG.info("running DWS SparkSQL for dt=%s", dt)
        dws_df = spark.sql(dws_sql)
        dws_df.createOrReplaceTempView("dws_dataset_quality_daily")

        dws_path = output_root / "dws_dataset_quality_daily" / f"dt={dt}"
        ads_path = output_root / "ads_quality_dashboard" / f"dt={dt}"
        dws_df.coalesce(1).write.mode("overwrite").parquet(str(dws_path))
        dws_row_count = int(dws_df.count())

        ads_sql = _render_sql(ads_sql_template, {"dt": dt})
        LOG.info("running ADS SparkSQL for dt=%s", dt)
        ads_df = spark.sql(ads_sql)
        ads_df.coalesce(1).write.mode("overwrite").parquet(str(ads_path))
        ads_row_count = int(ads_df.count())

        finished = datetime.now(timezone.utc)
        manifest_payload = {
            "tool": "robot-dh spark build-quality-ads",
            "version": "v1.8.promptC",
            "dt": dt,
            "input_uri": str(input_root),
            "output_uri": str(output_root),
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "duration_sec": round((finished - started).total_seconds(), 3),
            "sources_present": sources_present,
            "outputs": [
                {
                    "table": "dws_dataset_quality_daily",
                    "path": str(dws_path),
                    "row_count": dws_row_count,
                    "format": "parquet",
                },
                {
                    "table": "ads_quality_dashboard",
                    "path": str(ads_path),
                    "row_count": ads_row_count,
                    "format": "parquet",
                },
            ],
            "spark_app_id": spark.sparkContext.applicationId,
            "spark_master": spark.sparkContext.master,
        }
        manifest_path = output_root / "_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return BuildQualityAdsResult(
            dt=dt,
            input_uri=str(input_root),
            output_uri=str(output_root),
            dws_path=str(dws_path),
            ads_path=str(ads_path),
            dws_row_count=dws_row_count,
            ads_row_count=ads_row_count,
            sources_present=sources_present,
            manifest_path=str(manifest_path),
            duration_sec=(finished - started).total_seconds(),
            extras={
                "spark_master": spark.sparkContext.master,
                "spark_app_id": spark.sparkContext.applicationId,
            },
        )
    finally:
        if not keep_session and existing_spark is None:
            spark.stop()


__all__ = [
    "BuildQualityAdsResult",
    "SOURCE_TABLES",
    "DEFAULT_SQL_ROOT",
    "SparkUnavailableError",
    "build_quality_ads",
]
