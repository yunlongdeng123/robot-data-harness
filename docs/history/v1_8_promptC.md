你是资深 PySpark / SparkSQL / 数据仓库工程师。当前 robot-data-harness 已完成 v1.8 Warehouse Metrics & Quality Ops：

- DIM / FACT / DWS / ADS 数仓指标层
- warehouse build / query / export CLI
- quality summary / report
- backfill plan / SLA check
- PostgreSQL warehouse tables
- 本地 devscale file lake
- 远端 MinIO warehouse export

现在新增可选 SparkSQL local mode 模块，用于对齐离线大数据数仓开发流程。不要搭 Hadoop，不要搭 Hive，不要引入重型集群。只使用 Spark local mode 读取 parquet，生成 DWS / ADS parquet。

============================================================
一、目标
============================================================

新增：

1. PySpark local mode ADS builder。
2. SparkSQL 文件。
3. CLI：
   robot-dh spark build-quality-ads
4. Makefile target。
5. README 中说明这是可选大数据开发适配层。

============================================================
二、目录结构
============================================================

新增：

warehouse/
  spark/
    README.md
    spark_session.py
    build_dws_dataset_quality_daily.py
    build_ads_quality_dashboard.py
    sql/
      build_dws_dataset_quality_daily.sql
      build_ads_quality_dashboard.sql

src/robot_dh/spark_jobs/
  __init__.py
  quality_ads.py
  session.py

tests/
  test_spark_quality_ads_optional.py

============================================================
三、依赖
============================================================

pyproject.toml 新增 optional extra：

[project.optional-dependencies]
spark = [
  "pyspark>=3.5,<4"
]

不要让默认安装强制安装 pyspark。
README 写明：

pip install -e ".[spark]"

============================================================
四、CLI
============================================================

新增：

robot-dh spark build-quality-ads \
  --input file:///mnt/local-data/robot-dh-local/lake/warehouse \
  --output file:///mnt/local-data/robot-dh-local/lake/spark_ads \
  --date 2026-05-25

行为：
  1. 启动 Spark local mode：
     master=local[*]
  2. 读取 warehouse export 中的 parquet：
     fact_etl_run
     fact_qc_rule_result
     fact_workflow_step
     dim_dataset
  3. 构建：
     dws_dataset_quality_daily
     ads_quality_dashboard
  4. 输出 parquet。
  5. 写 _manifest.json。
  6. 如果 pyspark 未安装，给清晰错误：
     Please install with pip install -e ".[spark]"

============================================================
五、SparkSQL 逻辑
============================================================

build_dws_dataset_quality_daily.sql：
  - 按 dt / dataset_id / version 聚合
  - qc_pass_rate
  - etl_success_rate
  - workflow_success_rate
  - p95_duration
  - total bytes

build_ads_quality_dashboard.sql：
  - 生成 quality_score
  - alert_level
  - top_failed_rule 可简化

============================================================
六、测试
============================================================

test_spark_quality_ads_optional.py：
  - 如果 pyspark 未安装，skip。
  - 构造小 parquet。
  - 运行 build-quality-ads。
  - 检查输出 parquet 存在。

============================================================
七、Makefile
============================================================

新增：

make spark-install
make spark-build-quality-ads-local
make spark-test

============================================================
八、Go exporter 对齐，可选
============================================================

如果 go/robot-dh-exporter 已存在，更新它读取 v1.8 表：

新增指标：
  robot_dh_warehouse_rows_total{table}
  robot_dh_ads_quality_score{dataset_family,dataset_id}
  robot_dh_ads_qc_pass_rate{dataset_family,dataset_id}
  robot_dh_ads_etl_success_rate{dataset_family,dataset_id}
  robot_dh_backfill_tasks_total{status,phase}
  robot_dh_sla_checks_total{status,policy_id}

要求：
  - 表不存在不 panic。
  - 不破坏已有指标。

============================================================
九、FastAPI 对齐
============================================================

确保以下接口存在并可用：

GET /warehouse/tables
GET /warehouse/query
GET /quality/summary
GET /backfill/plans
GET /sla/checks

如果 Prompt B 已实现，只补测试和 README。

============================================================
十、README
============================================================

新增 v1.8 SparkSQL optional 章节：

说明：
  - 为什么只用 Spark local mode。
  - 不需要 Hadoop / Hive。
  - 目的是对齐离线数仓开发流程。
  - 如何安装 spark extra。
  - 如何运行。
  - 和 PostgreSQL warehouse build 的区别：
      PostgreSQL build 用于元数据指标在线查询。
      Spark build 用于 parquet 离线宽表产出。

============================================================
十一、验收命令
============================================================

本地：

pip install -e ".[spark]"

robot-dh spark build-quality-ads \
  --input file:///mnt/local-data/robot-dh-local/lake/warehouse \
  --output file:///mnt/local-data/robot-dh-local/lake/spark_ads \
  --date 2026-05-25

make spark-test

可选 exporter：

cd go/robot-dh-exporter
go test ./...

请开始实现。不要引入 Hadoop / Hive / Kafka / Operator。不要留 TODO，不要写伪代码。