-- v1.8 DWS：rule 失败率日度聚合，按 (dt, dataset_family, contract_id, rule_id, severity) 维度。

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
