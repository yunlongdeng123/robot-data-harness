"""DWS dataset_quality_daily 单步 Python 入口。

可以独立运行（用于 Notebook / IDE 单元跑），也可以在 build_ads 整体流程中串联。
"""

from __future__ import annotations

import argparse
from pathlib import Path

from robot_dh.spark_jobs.quality_ads import (
    DEFAULT_SQL_ROOT,
    SOURCE_TABLES,
    _empty_view,
    _load_sql_template,
    _normalize_uri_to_path,
    _render_sql,
    _try_load_parquet,
)
from robot_dh.spark_jobs.session import build_local_spark_session


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Spark local mode：构建 DWS dataset_quality_daily 单张 parquet。"
    )
    parser.add_argument("--input", required=True, help="file:// 或本地路径，warehouse export 根")
    parser.add_argument("--output", required=True, help="file:// 或本地路径，output 根")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--sql-root", type=Path, default=None)
    args = parser.parse_args()

    input_root = _normalize_uri_to_path(args.input).resolve()
    output_root = _normalize_uri_to_path(args.output).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    spark = build_local_spark_session(app_name="robot-dh-dws-quality-daily")
    try:
        for table in SOURCE_TABLES:
            df, present = _try_load_parquet(spark, input_root, table)
            if not present:
                df = _empty_view(spark, table)
            df.createOrReplaceTempView(table)
        sql_text = _load_sql_template(
            "build_dws_dataset_quality_daily.sql", args.sql_root or DEFAULT_SQL_ROOT
        )
        sql_text = _render_sql(sql_text, {"dt": args.date})
        dws_df = spark.sql(sql_text)
        target = output_root / "dws_dataset_quality_daily" / f"dt={args.date}"
        dws_df.coalesce(1).write.mode("overwrite").parquet(str(target))
        print(f"written DWS parquet to {target}  rows={dws_df.count()}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
