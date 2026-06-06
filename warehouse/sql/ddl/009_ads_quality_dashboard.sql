-- v1.8 ADS：质量看板。
-- alert_level 推荐值 OK / WARN / CRITICAL；与 dws 同 (dt, dataset_id, version) 维度。

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
