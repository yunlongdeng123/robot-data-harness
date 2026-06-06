-- v1.8 DWS：workflow_type 维度日度运营指标。

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
