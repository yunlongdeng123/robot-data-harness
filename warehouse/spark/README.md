# `warehouse/spark/`：v1.8 SparkSQL local mode 离线数仓宽表

> 这是 **可选** 模块。仅在你想对齐离线数仓开发流程、需要把 parquet 进一步落到 Grafana / Trino / Superset / DuckDB 时启用。
> 不要在这里搭 Hadoop / Hive / Operator。

## 1. 这层在做什么

- 读 `warehouse export` 已经落到本地的 4 张 parquet：`fact_etl_run / fact_qc_rule_result / fact_workflow_step / dim_dataset`。
- 用 **SparkSQL local mode**（`master=local[*]`）跑两条 SQL：
  1. `build_dws_dataset_quality_daily.sql` → 数据集 × 天 维度的宽表；
  2. `build_ads_quality_dashboard.sql` → 大屏宽表 + `quality_score / alert_level / top_failed_rule`。
- 输出仍然是 parquet（外加 `_manifest.json`），可以被任何外部 BI / DuckDB / Trino 直接 query。

## 2. 跟 PostgreSQL warehouse build 的区别

| 维度 | PostgreSQL warehouse build | SparkSQL local mode |
|---|---|---|
| 用途 | 元数据指标 **在线** 查询、FastAPI 喂数 | parquet **离线** 宽表 / 大屏 / BI |
| 输入 | 上游业务表（v1.4-v1.7） | warehouse export 产出的 parquet |
| 输出 | 远端 PostgreSQL 14 张 v1.8 表 | 本地 parquet + `_manifest.json` |
| 调用 | `robot-dh warehouse build --date YYYY-MM-DD` | `robot-dh spark build-quality-ads ...` |
| 依赖 | psycopg / sqlalchemy（**默认装**） | pyspark（**可选**） |

## 3. 安装

`pyspark` 是 **optional extra**：

```bash
pip install -e ".[spark]"
```

`pyspark>=3.5,<4`，本身自带 JVM 通信桥，不需要单独装 JDK 之外的 Hadoop。

## 4. 运行

前置：先用 `robot-dh warehouse export` 把 4 张表导出到本地（任意你方便的目录），例如：

```bash
for t in dim_dataset fact_etl_run fact_qc_rule_result fact_workflow_step; do
  robot-dh warehouse export \
    --table "$t" --date 2026-05-25 --format parquet \
    --output "file:///mnt/local-data/robot-dh-local/lake/warehouse/$t/dt=2026-05-25"
done
```

然后跑 Spark build：

```bash
robot-dh spark build-quality-ads \
  --input  file:///mnt/local-data/robot-dh-local/lake/warehouse \
  --output file:///mnt/local-data/robot-dh-local/lake/spark_ads \
  --date 2026-05-25
```

产出：

```
/mnt/local-data/robot-dh-local/lake/spark_ads/
├── _manifest.json
├── dws_dataset_quality_daily/dt=2026-05-25/part-00000-*.snappy.parquet
└── ads_quality_dashboard/dt=2026-05-25/part-00000-*.snappy.parquet
```

## 5. 我应该用哪一边？

| 你的目标 | 推荐路径 |
|---|---|
| FastAPI / 在线指标查询 | **PostgreSQL warehouse build** |
| Grafana / Superset / Trino 大屏（直接读 parquet） | **SparkSQL local mode** |
| 想要"开发流程对齐离线数仓" | **SparkSQL local mode** |
| 团队没装 pyspark | **PostgreSQL warehouse build** |

两条路径可以同时存在，互不冲突。

## 6. 限制

- **不支持 `s3://` 直接读**。promptC 明确不引入 `hadoop-aws`；如果数据在 S3，请先用 `boto3 download` / `mc cp` 拉到本地再喂给 Spark。
- driver memory 默认 2g；可通过 `ROBOT_DH_SPARK_DRIVER_MEMORY=4g` 覆盖。
- Spark UI 默认关闭（`spark.ui.enabled=false`），避免端口冲突。
- 时区默认 UTC；通过 `ROBOT_DH_TIMEZONE` 改。

## 7. 文件清单

| 文件 | 作用 |
|---|---|
| `sql/build_dws_dataset_quality_daily.sql` | DWS 单日宽表 SparkSQL |
| `sql/build_ads_quality_dashboard.sql` | ADS 大屏宽表 + `quality_score` + `alert_level` |
| `build_dws_dataset_quality_daily.py` | DWS Python 入口（thin wrapper，方便 IDE / Notebook 调用） |
| `build_ads_quality_dashboard.py` | ADS Python 入口（thin wrapper） |
| `spark_session.py` | SparkSession 工厂（直接 re-export `robot_dh.spark_jobs.session.build_local_spark_session`） |
