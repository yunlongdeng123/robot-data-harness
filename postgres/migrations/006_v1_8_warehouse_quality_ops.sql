-- v1.8 warehouse metrics & quality ops schema.
-- 在 v1.3~v1.7 业务表基础上新增数仓分层（DIM / FACT / DWS / ADS）+ 补数（backfill）+ SLA 表。
--
-- 仅做：
--   CREATE TABLE IF NOT EXISTS
--   CREATE INDEX IF NOT EXISTS
--   GRANT
--
-- 禁止：
--   DROP TABLE / TRUNCATE / destructive ALTER / 删除已有数据
--
-- 与 001/002/003/004/005 迁移完全并存，可重复执行。
--
-- 设计要点：
--   1. 全部新表主键使用 text（业务侧生成 key），不引入 bigserial，因此不需要 SEQUENCE GRANT。
--   2. dws_/ads_ 表使用复合主键代替业务唯一约束，配合 ON CONFLICT (dt, ...) DO UPDATE 做 UPSERT。
--   3. fact_* 表 dt 列冗余存储日期，方便按天分区查询；事实上的真源是 started_at / finished_at。
--   4. dim_dataset 由主项目 robot-data-harness 在 ingest / register 阶段写入，infra 仓库只建表不写入。

BEGIN;

-- ============================================================
-- 1. DIM 层
-- ============================================================

-- dim_dataset：dataset 维度宽表，单 (dataset_id, version) 一条最新画像。
-- dataset_key 由主项目生成，建议格式 'dataset:<dataset_id>:<version>'。
-- raw_uri / ods_uri / dwd_uri / ads_uri / ml_ready_uri 指向各分层根目录，缺位时为 NULL。
CREATE TABLE IF NOT EXISTS dim_dataset (
  dataset_key text PRIMARY KEY,
  dataset_id text NOT NULL,
  version text NOT NULL,
  dataset_family text,
  source_uri text,
  raw_uri text,
  ods_uri text,
  dwd_uri text,
  ads_uri text,
  ml_ready_uri text,
  first_seen_at timestamptz,
  latest_status text,
  latest_quality_score double precision,
  is_active boolean DEFAULT TRUE,
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_dataset_dataset_id_version
  ON dim_dataset (dataset_id, version);
CREATE INDEX IF NOT EXISTS idx_dim_dataset_dataset_family
  ON dim_dataset (dataset_family);
CREATE INDEX IF NOT EXISTS idx_dim_dataset_latest_status
  ON dim_dataset (latest_status);

-- ============================================================
-- 2. FACT 层
-- ============================================================

-- fact_etl_run：单次 etl run 事实表，由主项目从 etl_perf_runs / etl_jobs 物化得到。
-- run_key 建议格式 '<job_id>:<run_id>'；同一 run 多次重跑用 attempts 维度，不在本表展开。
CREATE TABLE IF NOT EXISTS fact_etl_run (
  run_key text PRIMARY KEY,
  job_id text,
  run_id text,
  dataset_id text,
  version text,
  dataset_family text,
  phase text,
  status text,
  started_at timestamptz,
  finished_at timestamptz,
  dt date,
  duration_sec double precision,
  input_bytes bigint,
  output_bytes bigint,
  input_rows bigint,
  output_rows bigint,
  peak_memory_mb double precision,
  error_message text,
  archive_log_uri text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_etl_run_dt_dataset_phase
  ON fact_etl_run (dt, dataset_id, phase);
CREATE INDEX IF NOT EXISTS idx_fact_etl_run_status_dt
  ON fact_etl_run (status, dt);
CREATE INDEX IF NOT EXISTS idx_fact_etl_run_family_dt
  ON fact_etl_run (dataset_family, dt);

-- fact_qc_rule_result：QC contract 单条规则结果事实表。
-- 一次 qc_contract_runs 通常会展开为 N 行（N=规则数），每行 rule_id 唯一对应 contract 中的一条规则。
-- threshold_value / actual_value 用 text 存储以兼容 numeric/string/enum 三类规则。
CREATE TABLE IF NOT EXISTS fact_qc_rule_result (
  rule_result_key text PRIMARY KEY,
  run_id text,
  contract_id text,
  dataset_id text,
  version text,
  dataset_family text,
  rule_id text,
  severity text,
  status text,
  metric text,
  op text,
  threshold_value text,
  actual_value text,
  dt date,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_qc_rule_result_dt_dataset
  ON fact_qc_rule_result (dt, dataset_id);
CREATE INDEX IF NOT EXISTS idx_fact_qc_rule_result_contract_rule_status
  ON fact_qc_rule_result (contract_id, rule_id, status);
CREATE INDEX IF NOT EXISTS idx_fact_qc_rule_result_family_status_dt
  ON fact_qc_rule_result (dataset_family, status, dt);

-- fact_workflow_step：workflow step 级事实表（含 Argo 与非 Argo workflow）。
-- step_key 建议格式 '<workflow_namespace>:<workflow_name>:<step_name>:<finished_at_epoch>'。
-- archive_log_url 是给前端展示的 https/http 链接，archive_log_uri 是 s3:// 协议根。
CREATE TABLE IF NOT EXISTS fact_workflow_step (
  step_key text PRIMARY KEY,
  workflow_name text,
  workflow_namespace text,
  workflow_type text,
  step_name text,
  template_name text,
  pod_name text,
  phase text,
  dataset_id text,
  version text,
  dataset_family text,
  started_at timestamptz,
  finished_at timestamptz,
  dt date,
  duration_sec double precision,
  exit_code int,
  container_reason text,
  archive_log_uri text,
  archive_log_url text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_workflow_step_dt_workflow
  ON fact_workflow_step (dt, workflow_name);
CREATE INDEX IF NOT EXISTS idx_fact_workflow_step_phase_dt
  ON fact_workflow_step (phase, dt);
CREATE INDEX IF NOT EXISTS idx_fact_workflow_step_dataset_version_dt
  ON fact_workflow_step (dataset_id, version, dt);
CREATE INDEX IF NOT EXISTS idx_fact_workflow_step_step_phase
  ON fact_workflow_step (step_name, phase);

-- fact_asset_profile：asset 画像事实表，从 v1.6 asset_profiles 物化（去重 by profile_id）。
-- 与 asset_profiles 的差别：本表强约束 layer + 加 dt 维度，方便按天聚合到 dws/ads。
CREATE TABLE IF NOT EXISTS fact_asset_profile (
  asset_profile_key text PRIMARY KEY,
  profile_id text,
  dataset_id text,
  version text,
  dataset_family text,
  asset_uri text,
  asset_format text,
  layer text,
  bytes bigint,
  rows bigint,
  files_count int,
  episodes_count int,
  videos_count int,
  schema_hash text,
  null_rate double precision,
  status text,
  dt date,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_asset_profile_dt_dataset_layer
  ON fact_asset_profile (dt, dataset_id, layer);
CREATE INDEX IF NOT EXISTS idx_fact_asset_profile_format_status
  ON fact_asset_profile (asset_format, status);
CREATE INDEX IF NOT EXISTS idx_fact_asset_profile_family_dt
  ON fact_asset_profile (dataset_family, dt);

-- ============================================================
-- 3. DWS 层
-- ============================================================

-- dws_dataset_quality_daily：dataset 日度宽表，主键 (dt, dataset_id, version)。
-- 由主项目离线 job 从 fact_* 聚合写入；infra 不直接写。
-- UPSERT 写入：ON CONFLICT (dt, dataset_id, version) DO UPDATE。
CREATE TABLE IF NOT EXISTS dws_dataset_quality_daily (
  dt date NOT NULL,
  dataset_id text NOT NULL,
  version text NOT NULL,
  dataset_family text,
  qc_run_count int DEFAULT 0,
  qc_pass_count int DEFAULT 0,
  qc_warn_count int DEFAULT 0,
  qc_fail_count int DEFAULT 0,
  qc_pass_rate double precision,
  etl_run_count int DEFAULT 0,
  etl_success_count int DEFAULT 0,
  etl_fail_count int DEFAULT 0,
  etl_success_rate double precision,
  workflow_count int DEFAULT 0,
  workflow_success_count int DEFAULT 0,
  workflow_fail_count int DEFAULT 0,
  workflow_success_rate double precision,
  avg_quality_score double precision,
  ml_ready_rows bigint DEFAULT 0,
  total_input_bytes bigint DEFAULT 0,
  total_output_bytes bigint DEFAULT 0,
  p95_etl_duration_sec double precision,
  p95_workflow_step_duration_sec double precision,
  stale_heartbeat_count int DEFAULT 0,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (dt, dataset_id, version)
);

CREATE INDEX IF NOT EXISTS idx_dws_dataset_quality_daily_family_dt
  ON dws_dataset_quality_daily (dataset_family, dt);
CREATE INDEX IF NOT EXISTS idx_dws_dataset_quality_daily_qc_pass_rate
  ON dws_dataset_quality_daily (qc_pass_rate);
CREATE INDEX IF NOT EXISTS idx_dws_dataset_quality_daily_etl_success_rate
  ON dws_dataset_quality_daily (etl_success_rate);

-- dws_rule_failure_daily：按 dataset_family + rule_id 的失败聚合，做"哪条规则最容易挂"看板。
CREATE TABLE IF NOT EXISTS dws_rule_failure_daily (
  dt date NOT NULL,
  dataset_family text NOT NULL,
  contract_id text NOT NULL,
  rule_id text NOT NULL,
  severity text NOT NULL,
  run_count int DEFAULT 0,
  pass_count int DEFAULT 0,
  warn_count int DEFAULT 0,
  fail_count int DEFAULT 0,
  fail_rate double precision,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (dt, dataset_family, contract_id, rule_id, severity)
);

CREATE INDEX IF NOT EXISTS idx_dws_rule_failure_daily_dt_fail_rate
  ON dws_rule_failure_daily (dt, fail_rate);
CREATE INDEX IF NOT EXISTS idx_dws_rule_failure_daily_rule_dt
  ON dws_rule_failure_daily (rule_id, dt);

-- dws_workflow_ops_daily：workflow_type 维度的日度运营指标。
-- workflow_type 例 'normalize' / 'build_features' / 'qc' / 'export_ml_ready'。
CREATE TABLE IF NOT EXISTS dws_workflow_ops_daily (
  dt date NOT NULL,
  workflow_type text NOT NULL,
  workflow_count int DEFAULT 0,
  success_count int DEFAULT 0,
  failed_count int DEFAULT 0,
  running_count int DEFAULT 0,
  success_rate double precision,
  avg_duration_sec double precision,
  p95_duration_sec double precision,
  deadline_exceeded_count int DEFAULT 0,
  oom_count int DEFAULT 0,
  nonzero_exit_count int DEFAULT 0,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (dt, workflow_type)
);

-- ============================================================
-- 4. ADS 层
-- ============================================================

-- ads_quality_dashboard：质量看板表，看板 / FastAPI 查询直接打这张表。
-- 一行 = 一份 (dt, dataset_id, version) 的"是否健康"摘要 + 报警等级。
-- alert_level 推荐值：none / info / warning / critical。
CREATE TABLE IF NOT EXISTS ads_quality_dashboard (
  dt date NOT NULL,
  dataset_id text NOT NULL,
  version text NOT NULL,
  dataset_family text,
  overall_status text,
  quality_score double precision,
  qc_pass_rate double precision,
  etl_success_rate double precision,
  workflow_success_rate double precision,
  top_failed_rule text,
  top_failed_rule_count int,
  p95_duration_sec double precision,
  ml_ready_rows bigint,
  raw_bytes bigint,
  dwd_bytes bigint,
  alert_level text,
  alert_reason text,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (dt, dataset_id, version)
);

-- ads_workflow_ops_dashboard：workflow 运营看板。
CREATE TABLE IF NOT EXISTS ads_workflow_ops_dashboard (
  dt date NOT NULL,
  workflow_type text NOT NULL,
  workflow_count int,
  success_count int,
  failed_count int,
  success_rate double precision,
  avg_duration_sec double precision,
  p95_duration_sec double precision,
  stale_heartbeat_count int,
  oom_count int,
  deadline_exceeded_count int,
  alert_level text,
  alert_reason text,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (dt, workflow_type)
);

-- ============================================================
-- 5. Backfill / SLA
-- ============================================================

-- backfill_plans：补数计划。plan_json 用 jsonb 存原始计划（含期望的输入 partition list / 资源 hint）。
-- 注意：v1.8 故意不实现调度器，本表只做"计划元数据登记"，实际触发由主项目 Argo / CLI 完成。
CREATE TABLE IF NOT EXISTS backfill_plans (
  plan_id text PRIMARY KEY,
  from_date date,
  to_date date,
  dataset_id text,
  version text,
  phase text,
  reason text,
  status text,
  task_count int DEFAULT 0,
  created_by text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  plan_json jsonb
);

-- backfill_tasks：补数任务实例，一个 plan 对应 N 个 task；task 粒度通常 = (dataset_id, version, dt, phase)。
-- recommended_command 由主项目 plan 生成阶段写入，方便 Argo 模板或人工 dry-run。
CREATE TABLE IF NOT EXISTS backfill_tasks (
  task_id text PRIMARY KEY,
  plan_id text NOT NULL,
  dataset_id text,
  version text,
  dataset_family text,
  dt date,
  phase text,
  input_uri text,
  output_uri text,
  recommended_command text,
  status text,
  attempts int DEFAULT 0,
  last_error text,
  started_at timestamptz,
  finished_at timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_backfill_tasks_plan_status
  ON backfill_tasks (plan_id, status);
CREATE INDEX IF NOT EXISTS idx_backfill_tasks_dataset_dt_phase
  ON backfill_tasks (dataset_id, dt, phase);

-- sla_policies：SLA 策略配置。dataset_pattern 用 SQL LIKE 风格匹配 dataset_id。
-- required_outputs_json：期望的输出 URI 列表（jsonb 数组），sla_checks 写入时与之比对得到 missing_outputs_json。
CREATE TABLE IF NOT EXISTS sla_policies (
  policy_id text PRIMARY KEY,
  policy_name text NOT NULL,
  dataset_pattern text,
  dataset_family text,
  deadline_hour int,
  required_outputs_json jsonb,
  min_qc_pass_rate double precision,
  min_etl_success_rate double precision,
  max_failed_workflows int,
  enabled boolean DEFAULT TRUE,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- sla_checks：单次 SLA 校验结果。status 推荐值 pass / warn / fail / skipped。
-- missing_outputs_json：本次未达成的输出列表；metrics_json：本次实际抓到的全部指标（用于回放）。
CREATE TABLE IF NOT EXISTS sla_checks (
  check_id text PRIMARY KEY,
  policy_id text,
  dt date,
  dataset_id text,
  version text,
  status text,
  qc_pass_rate double precision,
  etl_success_rate double precision,
  workflow_success_rate double precision,
  missing_outputs_json jsonb,
  failed_reason text,
  checked_at timestamptz DEFAULT now(),
  metrics_json jsonb
);

CREATE INDEX IF NOT EXISTS idx_sla_checks_dt_status
  ON sla_checks (dt, status);
CREATE INDEX IF NOT EXISTS idx_sla_checks_dataset_version_dt
  ON sla_checks (dataset_id, version, dt);
CREATE INDEX IF NOT EXISTS idx_sla_checks_policy_dt
  ON sla_checks (policy_id, dt);

-- dataset_partition_readiness：分区就绪登记，readiness_key 建议格式
-- '<dataset_id>:<version>:<phase>:<partition_id>'。
-- 用于 SLA / backfill 判断"今天 dataset 是否齐"，与 v1.6 dataset_partitions 互补：
-- dataset_partitions 关注分片拆分元数据，本表关注分区"是否到岗"。
CREATE TABLE IF NOT EXISTS dataset_partition_readiness (
  readiness_key text PRIMARY KEY,
  dt date,
  dataset_id text,
  version text,
  partition_id text,
  phase text,
  output_uri text,
  is_ready boolean,
  quality_status text,
  row_count bigint,
  bytes bigint,
  checked_at timestamptz DEFAULT now()
);

-- ============================================================
-- 6. GRANT 给应用账号 robot_dh_app
-- ============================================================
--
-- 全部 v1.8 表均使用 text 主键，不存在 bigserial / SEQUENCE，
-- 因此本次 migration 不需要 GRANT USAGE/SELECT ON SEQUENCE。

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
  dim_dataset,
  fact_etl_run, fact_qc_rule_result, fact_workflow_step, fact_asset_profile,
  dws_dataset_quality_daily, dws_rule_failure_daily, dws_workflow_ops_daily,
  ads_quality_dashboard, ads_workflow_ops_dashboard,
  backfill_plans, backfill_tasks,
  sla_policies, sla_checks,
  dataset_partition_readiness
TO robot_dh_app;

COMMIT;
