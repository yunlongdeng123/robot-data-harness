-- v1.8 FACT：workflow step 级事实表。
-- step_key 由主项目生成（推荐 '<ns>:<workflow_name>:<step_name>:<finished_at_epoch>'）。

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
