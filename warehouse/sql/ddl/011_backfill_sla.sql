-- v1.8 backfill / SLA 表族。
-- 主键全部 text；plan_json / missing_outputs_json / metrics_json 为 jsonb，本地 SQLite 走 JSON fallback。

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
