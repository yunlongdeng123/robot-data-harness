-- v1.8 DWS：dataset 日度宽表。
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
