你是资深数据研发工程师、机器人数据平台工程师、Python / SQL 工程师。当前仓库是 robot-data-harness，已完成 v1.7：

- Local-First Robot Data Platform Runtime
- Windows D 盘 devscale 数据同步
- kind robot-dh-dev cluster
- local file URI 支持
- DROID / LeRobot、robomimic、BridgeData adapter
- QC Contract
- Argo Local 20 节点 devscale DAG
- heartbeat / checkpoint / archive log
- workflow_steps / qc_contract_runs / asset_profiles
- ML-ready export
- Go exporter
- FastAPI 查询接口

当前状态：
  v1.7 已经不缺 K8s / Argo 复杂度。v1.8 不要继续堆复杂 DAG，不要引入 Kafka，不要做 Go Operator，不要做复杂前端。

v1.8 目标：
  Warehouse Metrics & Quality Ops

核心目标：
  1. 建设 SQL 数仓指标层：DIM / FACT / DWS / ADS。
  2. 实现 warehouse build / query / export CLI。
  3. 实现数据质量日报 / Top 失败规则 / workflow SLA 汇总。
  4. 实现 backfill plan / run / status 和 SLA check。
  5. 让项目更贴合数据开发 / 数据研发 / 数仓 ETL 岗位。
  6. 可选补 SparkSQL local mode，放到后续 Prompt C。

============================================================
一、总体要求
============================================================

1. 不重写现有 v1.7 功能。
2. 保持所有已有 CLI 向后兼容。
3. 新增功能必须支持：
   - 本地 SQLite / file mode 测试。
   - 远端 PostgreSQL 正式模式。
   - 本地 devscale lake file URI。
   - 远端 s3://robot-lake/warehouse 输出。
4. 没有远端 PostgreSQL / MinIO 时，make test 必须通过。
5. PostgreSQL v1.8 表不存在时，给清晰错误：
   请先在 infra 项目执行 ./scripts/39_pg_apply_v1_8_schema.sh
6. 不引入 Kafka。
7. 不新增复杂 Argo DAG。
8. 不做 Go Operator。
9. 不做复杂 React 前端。

============================================================
二、新增目录结构
============================================================

新增：

warehouse/
  sql/
    ddl/
      001_dim_dataset.sql
      002_fact_etl_run.sql
      003_fact_qc_rule_result.sql
      004_fact_workflow_step.sql
      005_fact_asset_profile.sql
      006_dws_dataset_quality_daily.sql
      007_dws_rule_failure_daily.sql
      008_dws_workflow_ops_daily.sql
      009_ads_quality_dashboard.sql
      010_ads_workflow_ops_dashboard.sql
      011_backfill_sla.sql

    dml/
      build_dim_dataset.sql
      build_fact_etl_run.sql
      build_fact_qc_rule_result.sql
      build_fact_workflow_step.sql
      build_fact_asset_profile.sql
      build_dws_dataset_quality_daily.sql
      build_dws_rule_failure_daily.sql
      build_dws_workflow_ops_daily.sql
      build_ads_quality_dashboard.sql
      build_ads_workflow_ops_dashboard.sql

src/robot_dh/warehouse_metrics/
  __init__.py
  config.py
  sql_runner.py
  builder.py
  exporter.py
  query.py
  models.py
  dates.py

src/robot_dh/quality_ops/
  __init__.py
  summary.py
  report.py
  sla.py
  backfill.py
  templates/
    quality_summary.html.j2
    sla_report.html.j2
    backfill_plan.md.j2

configs/
  warehouse.yaml
  sla_policies.yaml

tests/
  test_warehouse_sql_runner.py
  test_warehouse_builder_local.py
  test_warehouse_exporter.py
  test_quality_summary.py
  test_quality_report.py
  test_backfill_plan.py
  test_sla_check.py
  test_postgres_v1_8_optional.py

docs/
  v1_8_warehouse_metrics.md
  v1_8_quality_ops.md
  v1_8_backfill_sla.md

============================================================
三、SQL Runner
============================================================

实现 SqlTemplateRunner。

功能：
  1. 加载 warehouse/sql 下的 SQL 文件。
  2. 支持参数渲染：
     {{ dt }}
     {{ start_date }}
     {{ end_date }}
     {{ schema }}
  3. 支持 dry-run。
  4. 支持 transaction。
  5. 支持 PostgreSQL。
  6. 测试模式支持 SQLite，但可以只覆盖简单 SQL。
  7. 每个 SQL 执行后记录：
     sql_file
     duration_sec
     affected_rows 或 unknown
     status
  8. 错误信息要包含 SQL 文件名。

新增 CLI：

robot-dh warehouse sql run \
  --file warehouse/sql/dml/build_dim_dataset.sql \
  --dt 2026-05-25 \
  --dry-run

============================================================
四、Warehouse build
============================================================

新增 CLI：

robot-dh warehouse init

行为：
  - 本地 SQLite 测试模式可以创建简化表。
  - PostgreSQL 正式模式不自动 destructive alter。
  - 如果表不存在，提示运行 infra migration。
  - 可选执行 warehouse/sql/ddl，但默认对远端只检查，不强行创建，除非传 --apply-ddl。

robot-dh warehouse build \
  --date 2026-05-25

robot-dh warehouse build \
  --from-date 2026-05-01 \
  --to-date 2026-05-25

参数：
  --layers dim,fact,dws,ads
  --dry-run
  --force
  --output-root file:///mnt/local-data/robot-dh-local/lake/warehouse 或 s3://robot-lake/warehouse

行为：
  1. 按顺序执行：
     dim -> fact -> dws -> ads
  2. 每个层级写入 PostgreSQL 表。
  3. 可选把结果导出为 parquet / csv。
  4. 生成 build report：
     warehouse_build_report.json
  5. 生成 _manifest.json。
  6. 如果某些源表为空，不失败，输出 WARN。
  7. 支持按日期重跑。
  8. 支持本地 devscale file output。

执行顺序：
  build_dim_dataset.sql
  build_fact_etl_run.sql
  build_fact_qc_rule_result.sql
  build_fact_workflow_step.sql
  build_fact_asset_profile.sql
  build_dws_dataset_quality_daily.sql
  build_dws_rule_failure_daily.sql
  build_dws_workflow_ops_daily.sql
  build_ads_quality_dashboard.sql
  build_ads_workflow_ops_dashboard.sql

============================================================
五、Warehouse query / export
============================================================

新增 CLI：

robot-dh warehouse query \
  --table ads_quality_dashboard \
  --limit 20

支持：
  --where "dt='2026-05-25'"
  --output json|table|csv

新增 CLI：

robot-dh warehouse export \
  --table ads_quality_dashboard \
  --date 2026-05-25 \
  --format parquet \
  --output file:///mnt/local-data/robot-dh-local/lake/warehouse/ads/ads_quality_dashboard/dt=2026-05-25

支持：
  format: parquet, csv, json
  output: local path, file://, s3://

输出时写：
  _manifest.json

Manifest 字段：
  table
  dt
  format
  row_count
  output_uri
  created_at
  source_tables
  checksum_sha256 optional

============================================================
六、核心 SQL 逻辑
============================================================

请实现 SQL 文件。

要求不要依赖复杂 PostgreSQL 特性，优先清晰可读。

------------------------------------------------------------
dim_dataset
------------------------------------------------------------

来源：
  dataset_versions
  asset_profiles
  ml_ready_datasets
  quality_snapshots

逻辑：
  - dataset_key = dataset_id || ':' || version
  - 汇总 dataset_family、raw_uri、ods_uri、dwd_uri、ml_ready_uri
  - latest_status 来自最近 quality_snapshots 或 dataset_versions
  - latest_quality_score 来自最近 quality_snapshots.metrics_json 或 avg_quality_score
  - upsert

------------------------------------------------------------
fact_etl_run
------------------------------------------------------------

来源：
  etl_perf_runs
  etl_jobs

逻辑：
  - phase、status、duration、input/output bytes、rows、peak memory
  - dt = date(started_at or created_at)
  - run_key = md5(job_id/run_id/phase/dataset/version)
  - upsert

------------------------------------------------------------
fact_qc_rule_result
------------------------------------------------------------

来源：
  qc_contract_runs

逻辑：
  - 展开 contract_report 中 rules_json 或 failed_rules_json / warning_rules_json。
  - 如果只有 metrics_json，没有 rules_json，也要生成 summary rule：
      rule_id='contract_status'
      status=qc_contract_runs.status
  - 需要兼容 metrics_json 字段结构不固定。
  - 不因某条 JSON 结构异常导致整个 build 失败，异常计入 warning。

------------------------------------------------------------
fact_workflow_step
------------------------------------------------------------

来源：
  workflow_steps

逻辑：
  - 提取 step_name、pod_name、phase、duration、exit_code、archive_log_uri/url。
  - dt = date(started_at or created_at)
  - step_key = md5(workflow_name/step_name/pod_name)

------------------------------------------------------------
fact_asset_profile
------------------------------------------------------------

来源：
  asset_profiles

逻辑：
  - rows、bytes、files_count、episodes_count、videos_count
  - dt = date(created_at)

------------------------------------------------------------
dws_dataset_quality_daily
------------------------------------------------------------

来源：
  dim_dataset
  fact_etl_run
  fact_qc_rule_result
  fact_workflow_step
  ml_ready_datasets

指标：
  qc_run_count
  qc_pass_count
  qc_warn_count
  qc_fail_count
  qc_pass_rate
  etl_run_count
  etl_success_count
  etl_fail_count
  etl_success_rate
  workflow_count
  workflow_success_count
  workflow_fail_count
  workflow_success_rate
  avg_quality_score
  ml_ready_rows
  total_input_bytes
  total_output_bytes
  p95_etl_duration_sec
  p95_workflow_step_duration_sec

------------------------------------------------------------
dws_rule_failure_daily
------------------------------------------------------------

来源：
  fact_qc_rule_result

指标：
  rule_id
  severity
  run_count
  pass_count
  warn_count
  fail_count
  fail_rate

------------------------------------------------------------
dws_workflow_ops_daily
------------------------------------------------------------

来源：
  workflow_runs
  fact_workflow_step
  task_heartbeats

指标：
  workflow_count
  success_count
  failed_count
  success_rate
  avg_duration_sec
  p95_duration_sec
  deadline_exceeded_count
  oom_count
  nonzero_exit_count

------------------------------------------------------------
ads_quality_dashboard
------------------------------------------------------------

来源：
  dws_dataset_quality_daily
  dws_rule_failure_daily

字段：
  overall_status:
    FAIL if qc_pass_rate < 0.8 or etl_success_rate < 0.8
    WARN if qc_pass_rate < 0.95
    PASS otherwise

  quality_score:
    100 * qc_pass_rate * 0.5
    + 100 * etl_success_rate * 0.3
    + 100 * workflow_success_rate * 0.2

  alert_level:
    CRITICAL / WARN / OK

------------------------------------------------------------
ads_workflow_ops_dashboard
------------------------------------------------------------

来源：
  dws_workflow_ops_daily

字段：
  success_rate
  p95_duration_sec
  stale_heartbeat_count
  oom_count
  deadline_exceeded_count
  alert_level
  alert_reason

============================================================
七、Quality summary / report
============================================================

新增 CLI：

robot-dh quality summary \
  --date 2026-05-25 \
  --output json

robot-dh quality report \
  --date 2026-05-25 \
  --output runs/quality_report

输出目录：

quality_report/
  quality_summary.html
  quality_summary.json
  rule_failure_top10.csv
  dataset_quality_daily.parquet
  workflow_sla_summary.csv
  abnormal_partitions.csv
  archive_log_index.csv

报告内容：
  - 数据集数量
  - Workflow 成功率
  - ETL 成功率
  - QC 通过率
  - Top 10 失败规则
  - p95 step duration
  - stale heartbeat count
  - ML-ready rows
  - raw bytes / dwd bytes
  - alert level
  - archive log links

Jinja2 模板：
  src/robot_dh/quality_ops/templates/quality_summary.html.j2

要求：
  - 没有数据时也能生成空报告。
  - 报告不要报错。
  - CSV / parquet 可选，如果 pyarrow 可用就输出 parquet，否则 warning。

============================================================
八、Backfill
============================================================

新增 CLI：

robot-dh backfill plan \
  --from-date 2026-05-01 \
  --to-date 2026-05-07 \
  --dataset droid_lerobot_dev1g \
  --phase normalize \
  --output runs/backfill/plan.json

支持：
  --reason "failed workflow"
  --status-filter FAILED,WARN
  --dry-run

行为：
  1. 从 ads_quality_dashboard、fact_etl_run、fact_workflow_step、sla_checks 查询失败或缺失分区。
  2. 生成 backfill plan。
  3. 写 backfill_plans / backfill_tasks。
  4. 每个 task 生成 recommended_command，例如：
     robot-dh etl run --dataset ... --phase normalize --resume
  5. 输出 plan.json 和 plan.md。

新增 CLI：

robot-dh backfill run \
  --plan-id <plan_id> \
  --max-parallel 2

v1.8 中 backfill run 可以先做轻量实现：
  - 默认只打印 recommended commands。
  - 传 --execute 才实际执行。
  - 执行时必须支持 --dry-run。
  - 不直接提交 Argo，除非后续版本扩展。

新增 CLI：

robot-dh backfill status --plan-id <plan_id>

============================================================
九、SLA
============================================================

配置：

configs/sla_policies.yaml

示例：

policies:
  - policy_id: devscale_daily_ready
    policy_name: Devscale Daily Ready
    dataset_pattern: "*dev*"
    dataset_family: null
    deadline_hour: 23
    required_outputs:
      - qc_contract
      - dwd
      - ads
      - ml_ready
    min_qc_pass_rate: 0.8
    min_etl_success_rate: 0.8
    max_failed_workflows: 0

新增 CLI：

robot-dh sla check \
  --date 2026-05-25 \
  --policy configs/sla_policies.yaml

行为：
  1. 读取 policy。
  2. 查询 ads_quality_dashboard、ml_ready_datasets、qc_contract_runs、workflow_runs。
  3. 生成 sla_checks。
  4. 输出 JSON。
  5. 状态：
     PASS / WARN / FAIL

新增 CLI：

robot-dh sla report \
  --date 2026-05-25 \
  --output runs/sla_report

输出：
  sla_report.html
  sla_report.json
  sla_failed_datasets.csv

模板：
  src/robot_dh/quality_ops/templates/sla_report.html.j2

============================================================
十、FastAPI 增强
============================================================

新增只读接口：

GET /warehouse/tables
GET /warehouse/query?table=ads_quality_dashboard&limit=20
GET /quality/summary?date=YYYY-MM-DD
GET /quality/report/latest
GET /backfill/plans
GET /backfill/plans/{plan_id}
GET /sla/checks?date=YYYY-MM-DD

要求：
  - DB 不可用返回 503。
  - 不在 API 中执行重型 warehouse build。
  - 不暴露 secret。
  - 响应模型清晰。

============================================================
十一、测试要求
============================================================

所有测试必须在无远端服务时通过。

新增测试：

test_warehouse_sql_runner.py:
  - SQL 文件加载
  - 参数渲染
  - dry run
  - 错误信息包含 SQL 文件名

test_warehouse_builder_local.py:
  - 使用临时 SQLite 或 fake repository
  - build dim/fact/dws/ads 流程不报错
  - 空数据也能生成 report

test_warehouse_exporter.py:
  - 导出 CSV / JSON
  - pyarrow 可用时导出 parquet
  - manifest 完整

test_quality_summary.py:
  - 从 fake ads 数据生成 summary

test_quality_report.py:
  - 生成 HTML / JSON / CSV

test_backfill_plan.py:
  - 根据 fake failed fact_etl_run 生成 backfill task
  - recommended_command 正确

test_sla_check.py:
  - PASS / WARN / FAIL policy 判断

test_postgres_v1_8_optional.py:
  - 设置 ROBOT_DH_TEST_POSTGRES_URI 时跑 PostgreSQL 集成测试
  - 未设置时 skip

============================================================
十二、Makefile
============================================================

新增 target：

make warehouse-init
make warehouse-build-local
make warehouse-query
make warehouse-export-local
make quality-summary
make quality-report
make backfill-plan
make sla-check
make v1-8-warehouse-smoke

v1-8-warehouse-smoke：
  - warehouse init/check
  - warehouse build 当前日期
  - warehouse query ads_quality_dashboard
  - quality report
  - backfill plan dry-run
  - sla check

============================================================
十三、README / docs
============================================================

README 新增 v1.8 章节：

标题：
  v1.8 Warehouse Metrics & Quality Ops

内容：
  1. 为什么 v1.8 不继续堆 K8s。
  2. DIM / FACT / DWS / ADS 分层。
  3. 核心表说明。
  4. CLI 使用。
  5. 质量日报。
  6. backfill / SLA。
  7. 和 v1.7 Argo local workflow 的关系。
  8. 简历 bullet 示例。

docs/v1_8_warehouse_metrics.md：
  - 表设计
  - SQL 构建顺序
  - 指标口径

docs/v1_8_quality_ops.md：
  - 报告内容
  - TopN 失败规则
  - 数据质量运营视角

docs/v1_8_backfill_sla.md：
  - backfill plan / task
  - SLA policy
  - 失败重跑策略

============================================================
十四、验收命令
============================================================

本地：

make test
make warehouse-init
make warehouse-build-local
make warehouse-query
make quality-summary
make quality-report
make backfill-plan
make sla-check
make v1-8-warehouse-smoke

远端：

source client/robot-dh-v1-8.env

robot-dh warehouse build --date 2026-05-25
robot-dh warehouse query --table ads_quality_dashboard --limit 20
robot-dh warehouse export \
  --table ads_quality_dashboard \
  --date 2026-05-25 \
  --format parquet \
  --output s3://robot-lake/warehouse/ads/ads_quality_dashboard/dt=2026-05-25

robot-dh quality report \
  --date 2026-05-25 \
  --output runs/quality_report

robot-dh backfill plan \
  --from-date 2026-05-01 \
  --to-date 2026-05-07 \
  --phase normalize \
  --dry-run

robot-dh sla check \
  --date 2026-05-25 \
  --policy configs/sla_policies.yaml

请开始实现。代码必须模块化、类型清晰、错误信息明确。不要留 TODO，不要写伪代码。