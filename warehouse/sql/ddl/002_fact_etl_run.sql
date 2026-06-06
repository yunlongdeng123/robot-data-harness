-- v1.8 FACT：etl run 事实表。
-- run_key 由主项目生成（推荐 md5(job_id|run_id|phase|dataset|version)）。

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
