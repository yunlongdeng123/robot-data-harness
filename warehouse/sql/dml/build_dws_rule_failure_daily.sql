-- 物化 dws_rule_failure_daily：按 (dt, dataset_family, contract_id, rule_id, severity) 维度聚合 fact_qc_rule_result。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

INSERT INTO {{ schema }}.dws_rule_failure_daily (
    dt, dataset_family, contract_id, rule_id, severity,
    run_count, pass_count, warn_count, fail_count, fail_rate,
    updated_at
)
SELECT
    f.dt,
    coalesce(f.dataset_family, 'unknown')                                  AS dataset_family,
    coalesce(f.contract_id, 'unknown')                                     AS contract_id,
    f.rule_id,
    coalesce(f.severity, 'unknown')                                        AS severity,
    count(*)                                                               AS run_count,
    count(*) FILTER (WHERE upper(coalesce(f.status, '')) = 'PASS')         AS pass_count,
    count(*) FILTER (WHERE upper(coalesce(f.status, '')) = 'WARN')         AS warn_count,
    count(*) FILTER (WHERE upper(coalesce(f.status, '')) = 'FAIL')         AS fail_count,
    CASE WHEN count(*) > 0
         THEN count(*) FILTER (WHERE upper(coalesce(f.status, '')) = 'FAIL')::double precision / count(*)
         ELSE NULL END                                                     AS fail_rate,
    now()
FROM {{ schema }}.fact_qc_rule_result f
WHERE f.dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
  AND f.rule_id IS NOT NULL
  AND f.rule_id <> 'contract_status'
GROUP BY 1, 2, 3, 4, 5
ON CONFLICT (dt, dataset_family, contract_id, rule_id, severity) DO UPDATE SET
    run_count   = EXCLUDED.run_count,
    pass_count  = EXCLUDED.pass_count,
    warn_count  = EXCLUDED.warn_count,
    fail_count  = EXCLUDED.fail_count,
    fail_rate   = EXCLUDED.fail_rate,
    updated_at  = now();
