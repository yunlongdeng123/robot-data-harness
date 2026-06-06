-- v1.8 ADS：workflow ops 运营看板。

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
