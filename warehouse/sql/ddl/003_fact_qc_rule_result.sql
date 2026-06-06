-- v1.8 FACT：QC contract 单条规则结果事实表。
-- threshold_value / actual_value 用 text 兼容 numeric / string / enum 三类规则。

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
