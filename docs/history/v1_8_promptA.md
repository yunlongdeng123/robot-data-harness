你是资深数据基建工程师、PostgreSQL 数仓工程师、DevOps/SRE 工程师。当前项目是 robot-dh-infra，运行在腾讯云 Ubuntu 服务器上，为 robot-data-harness 提供远端 PostgreSQL / MinIO / Redis 基础设施。

请在现有 v1.7 infra 基础上升级到 v1.8。不要删除已有数据，不要 drop 已有表，不要自动格式化磁盘，不要暴露真实密码到日志。

============================================================
一、完整背景
============================================================

主项目：
  robot-data-harness

当前主项目 v1.7 已完成：
  - Local-First Robot Data Platform Runtime
  - Windows D 盘 devscale 数据同步
  - kind robot-dh-dev cluster
  - local file URI 读写
  - DROID / LeRobot、robomimic、BridgeData adapter
  - QC Contract
  - Argo Local 20 节点 devscale DAG
  - heartbeat / checkpoint / archive logs
  - workflow_steps / qc_contract_runs / asset_profiles
  - ML-ready export
  - Go exporter
  - FastAPI 查询接口

当前 infra 已有 PostgreSQL 元数据表，大致包括：
  - datasets / runs
  - lake_assets
  - etl_jobs
  - lineage_edges
  - dataset_versions
  - quality_snapshots
  - etl_perf_runs
  - etl_shards
  - benchmark_runs
  - benchmark_cases
  - runtime_events
  - argo_workflow_runs
  - qc_contracts
  - qc_contract_runs
  - workflow_runs
  - workflow_steps
  - asset_profiles
  - ml_ready_datasets
  - dataset_partitions
  - task_heartbeats
  - openlineage_events

v1.8 目标：
  Warehouse Metrics & Quality Ops

核心目标：
  1. 新增 DIM / FACT / DWS / ADS 数仓指标层。
  2. 新增补数 backfill 和 SLA 检查元数据表。
  3. 提供幂等 SQL migration。
  4. 提供 smoke test 和每日质量运营报告脚本。
  5. 不影响 v1.7 的 Argo / QC / ML-ready 链路。
  6. 不引入 Kafka / Operator / 大型前端。

============================================================
二、本次交付物
============================================================

新增或修改：

postgres/
  migrations/
    006_v1_8_warehouse_quality_ops.sql

scripts/
  39_pg_apply_v1_8_schema.sh
  40_pg_v1_8_smoke_test.sh
  41_warehouse_table_counts.sh
  42_quality_ops_daily_report.sh
  43_sla_ops_report.sh
  44_export_v1_8_client_env.sh

client/
  robot-dh-v1-8.env.example
  k8s-v1-8-secret.example.yaml
  k8s-create-v1-8-secret.example.sh

docs/
  v1_8_warehouse_schema.md
  v1_8_quality_ops_runbook.md
  v1_8_backfill_sla_notes.md

README.md 更新 v1.8 章节。

============================================================
三、PostgreSQL v1.8 schema
============================================================

新增 migration：

postgres/migrations/006_v1_8_warehouse_quality_ops.sql

只允许：
  CREATE TABLE IF NOT EXISTS
  CREATE INDEX IF NOT EXISTS
  CREATE VIEW IF NOT EXISTS
  GRANT

禁止：
  DROP TABLE
  TRUNCATE
  destructive ALTER
  删除已有数据

------------------------------------------------------------
1. DIM 层
------------------------------------------------------------

dim_dataset:
  dataset_key text primary key
  dataset_id text not null
  version text not null
  dataset_family text
  source_uri text
  raw_uri text
  ods_uri text
  dwd_uri text
  ads_uri text
  ml_ready_uri text
  first_seen_at timestamptz
  latest_status text
  latest_quality_score double precision
  is_active boolean default true
  updated_at timestamptz default now()

索引：
  dim_dataset(dataset_id, version)
  dim_dataset(dataset_family)
  dim_dataset(latest_status)

------------------------------------------------------------
2. FACT 层
------------------------------------------------------------

fact_etl_run:
  run_key text primary key
  job_id text
  run_id text
  dataset_id text
  version text
  dataset_family text
  phase text
  status text
  started_at timestamptz
  finished_at timestamptz
  dt date
  duration_sec double precision
  input_bytes bigint
  output_bytes bigint
  input_rows bigint
  output_rows bigint
  peak_memory_mb double precision
  error_message text
  archive_log_uri text
  created_at timestamptz default now()

索引：
  fact_etl_run(dt, dataset_id, phase)
  fact_etl_run(status, dt)
  fact_etl_run(dataset_family, dt)

fact_qc_rule_result:
  rule_result_key text primary key
  run_id text
  contract_id text
  dataset_id text
  version text
  dataset_family text
  rule_id text
  severity text
  status text
  metric text
  op text
  threshold_value text
  actual_value text
  dt date
  created_at timestamptz default now()

索引：
  fact_qc_rule_result(dt, dataset_id)
  fact_qc_rule_result(contract_id, rule_id, status)
  fact_qc_rule_result(dataset_family, status, dt)

fact_workflow_step:
  step_key text primary key
  workflow_name text
  workflow_namespace text
  workflow_type text
  step_name text
  template_name text
  pod_name text
  phase text
  dataset_id text
  version text
  dataset_family text
  started_at timestamptz
  finished_at timestamptz
  dt date
  duration_sec double precision
  exit_code int
  container_reason text
  archive_log_uri text
  archive_log_url text
  created_at timestamptz default now()

索引：
  fact_workflow_step(dt, workflow_name)
  fact_workflow_step(phase, dt)
  fact_workflow_step(dataset_id, version, dt)
  fact_workflow_step(step_name, phase)

fact_asset_profile:
  asset_profile_key text primary key
  profile_id text
  dataset_id text
  version text
  dataset_family text
  asset_uri text
  asset_format text
  layer text
  bytes bigint
  rows bigint
  files_count int
  episodes_count int
  videos_count int
  schema_hash text
  null_rate double precision
  status text
  dt date
  created_at timestamptz default now()

索引：
  fact_asset_profile(dt, dataset_id, layer)
  fact_asset_profile(asset_format, status)
  fact_asset_profile(dataset_family, dt)

------------------------------------------------------------
3. DWS 层
------------------------------------------------------------

dws_dataset_quality_daily:
  dt date not null
  dataset_id text not null
  version text not null
  dataset_family text
  qc_run_count int default 0
  qc_pass_count int default 0
  qc_warn_count int default 0
  qc_fail_count int default 0
  qc_pass_rate double precision
  etl_run_count int default 0
  etl_success_count int default 0
  etl_fail_count int default 0
  etl_success_rate double precision
  workflow_count int default 0
  workflow_success_count int default 0
  workflow_fail_count int default 0
  workflow_success_rate double precision
  avg_quality_score double precision
  ml_ready_rows bigint default 0
  total_input_bytes bigint default 0
  total_output_bytes bigint default 0
  p95_etl_duration_sec double precision
  p95_workflow_step_duration_sec double precision
  stale_heartbeat_count int default 0
  updated_at timestamptz default now()
  primary key (dt, dataset_id, version)

索引：
  dws_dataset_quality_daily(dataset_family, dt)
  dws_dataset_quality_daily(qc_pass_rate)
  dws_dataset_quality_daily(etl_success_rate)

dws_rule_failure_daily:
  dt date not null
  dataset_family text
  contract_id text
  rule_id text
  severity text
  run_count int default 0
  pass_count int default 0
  warn_count int default 0
  fail_count int default 0
  fail_rate double precision
  updated_at timestamptz default now()
  primary key (dt, dataset_family, contract_id, rule_id, severity)

索引：
  dws_rule_failure_daily(dt, fail_rate)
  dws_rule_failure_daily(rule_id, dt)

dws_workflow_ops_daily:
  dt date not null
  workflow_type text not null
  workflow_count int default 0
  success_count int default 0
  failed_count int default 0
  running_count int default 0
  success_rate double precision
  avg_duration_sec double precision
  p95_duration_sec double precision
  deadline_exceeded_count int default 0
  oom_count int default 0
  nonzero_exit_count int default 0
  updated_at timestamptz default now()
  primary key (dt, workflow_type)

------------------------------------------------------------
4. ADS 层
------------------------------------------------------------

ads_quality_dashboard:
  dt date not null
  dataset_id text not null
  version text not null
  dataset_family text
  overall_status text
  quality_score double precision
  qc_pass_rate double precision
  etl_success_rate double precision
  workflow_success_rate double precision
  top_failed_rule text
  top_failed_rule_count int
  p95_duration_sec double precision
  ml_ready_rows bigint
  raw_bytes bigint
  dwd_bytes bigint
  alert_level text
  alert_reason text
  updated_at timestamptz default now()
  primary key (dt, dataset_id, version)

ads_workflow_ops_dashboard:
  dt date not null
  workflow_type text not null
  workflow_count int
  success_count int
  failed_count int
  success_rate double precision
  avg_duration_sec double precision
  p95_duration_sec double precision
  stale_heartbeat_count int
  oom_count int
  deadline_exceeded_count int
  alert_level text
  alert_reason text
  updated_at timestamptz default now()
  primary key (dt, workflow_type)

------------------------------------------------------------
5. Backfill / SLA
------------------------------------------------------------

backfill_plans:
  plan_id text primary key
  from_date date
  to_date date
  dataset_id text
  version text
  phase text
  reason text
  status text
  task_count int default 0
  created_by text
  created_at timestamptz default now()
  updated_at timestamptz default now()
  plan_json jsonb

backfill_tasks:
  task_id text primary key
  plan_id text not null
  dataset_id text
  version text
  dataset_family text
  dt date
  phase text
  input_uri text
  output_uri text
  recommended_command text
  status text
  attempts int default 0
  last_error text
  started_at timestamptz
  finished_at timestamptz
  created_at timestamptz default now()
  updated_at timestamptz default now()

索引：
  backfill_tasks(plan_id, status)
  backfill_tasks(dataset_id, dt, phase)

sla_policies:
  policy_id text primary key
  policy_name text not null
  dataset_pattern text
  dataset_family text
  deadline_hour int
  required_outputs_json jsonb
  min_qc_pass_rate double precision
  min_etl_success_rate double precision
  max_failed_workflows int
  enabled boolean default true
  created_at timestamptz default now()
  updated_at timestamptz default now()

sla_checks:
  check_id text primary key
  policy_id text
  dt date
  dataset_id text
  version text
  status text
  qc_pass_rate double precision
  etl_success_rate double precision
  workflow_success_rate double precision
  missing_outputs_json jsonb
  failed_reason text
  checked_at timestamptz default now()
  metrics_json jsonb

索引：
  sla_checks(dt, status)
  sla_checks(dataset_id, version, dt)
  sla_checks(policy_id, dt)

dataset_partition_readiness:
  readiness_key text primary key
  dt date
  dataset_id text
  version text
  partition_id text
  phase text
  output_uri text
  is_ready boolean
  quality_status text
  row_count bigint
  bytes bigint
  checked_at timestamptz default now()

============================================================
四、脚本要求
============================================================

scripts/39_pg_apply_v1_8_schema.sh:
  - 幂等执行 006_v1_8_warehouse_quality_ops.sql。
  - 不 drop、不 truncate。
  - 应用后列出 v1.8 新表。
  - 退出码非 0 表示失败。

scripts/40_pg_v1_8_smoke_test.sh:
  - 使用 app user 测试读写权限。
  - 覆盖 dim_dataset、fact_etl_run、fact_qc_rule_result、dws_dataset_quality_daily、ads_quality_dashboard、backfill_plans、sla_checks。
  - 插入 smoke 数据后清理。
  - 不影响真实数据。

scripts/41_warehouse_table_counts.sh:
  - 输出 v1.8 所有 DIM/FACT/DWS/ADS/BACKFILL/SLA 表 row count。
  - 输出 Markdown 和 JSON：
      /data/robot-dh/logs/v1_8_warehouse_counts_YYYYmmdd_HHMMSS.md
      /data/robot-dh/logs/v1_8_warehouse_counts_YYYYmmdd_HHMMSS.json

scripts/42_quality_ops_daily_report.sh:
  - 查询 ads_quality_dashboard、ads_workflow_ops_dashboard、dws_rule_failure_daily。
  - 输出当天质量运营 Markdown 报告：
      /data/robot-dh/logs/v1_8_quality_ops_daily_YYYYmmdd.md
  - 如果表为空，正常输出空报告，不失败。

scripts/43_sla_ops_report.sh:
  - 查询 sla_checks、backfill_plans、backfill_tasks。
  - 输出 SLA / backfill 运营报告。
  - 如果表为空，不失败。

scripts/44_export_v1_8_client_env.sh:
  - 生成 client/robot-dh-v1-8.env.example 或真实 env。
  - 默认脱敏。
  - 传 --show-secrets 才输出真实文件。
  - 增加：
      ROBOT_DH_PLATFORM_VERSION=1.8
      ROBOT_DH_WAREHOUSE_SCHEMA=public
      ROBOT_DH_WAREHOUSE_OUTPUT_ROOT=s3://robot-lake/warehouse
  - chmod 600。
  - 不打印密码。

============================================================
五、文档要求
============================================================

docs/v1_8_warehouse_schema.md：
  - 解释 DIM / FACT / DWS / ADS 分层。
  - 解释每张表来源和用途。
  - 解释如何从 v1.7 元数据生成指标层。
  - 解释质量看板和 SLA 表。

docs/v1_8_quality_ops_runbook.md：
  - 初始化 schema。
  - smoke test。
  - 表计数。
  - 质量日报。
  - 常见问题。

docs/v1_8_backfill_sla_notes.md：
  - backfill plan / task 设计。
  - SLA policy 设计。
  - 如何和 checkpoint / Argo workflow 结合。
  - 目前 v1.8 只做轻量 backfill，不实现大型调度器。

README.md：
  - 新增 v1.8 Warehouse Metrics & Quality Ops 章节。
  - 新增验收命令。

============================================================
六、验收命令
============================================================

用户手动执行：

cd /opt/robot-dh-infra

./scripts/06_healthcheck.sh
./scripts/39_pg_apply_v1_8_schema.sh
./scripts/40_pg_v1_8_smoke_test.sh
./scripts/41_warehouse_table_counts.sh
./scripts/42_quality_ops_daily_report.sh
./scripts/43_sla_ops_report.sh
./scripts/44_export_v1_8_client_env.sh

验收标准：
  - 所有脚本可重复执行。
  - 不删除已有数据。
  - 不 drop / truncate 已有表。
  - 不暴露密码。
  - v1.8 新表存在。
  - app user 可读写新表。
  - 报告脚本能在空表和有数据两种情况下运行。

请开始实现。所有 shell 脚本使用 set -euo pipefail。不要留 TODO，不要写伪代码。