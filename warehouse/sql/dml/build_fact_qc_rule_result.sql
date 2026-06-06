-- 物化 fact_qc_rule_result：展开 qc_contract_runs 的全量规则结果。
--
-- v1.8 修复（远端 PG 现网诊断）：
--   1. WHERE 用 coalesce(started_at, created_at)::date，避免历史 18 行 started_at=NULL
--      被全过滤导致 fact_qc_rule_result=0。
--   2. metrics_json -> '_rule_results' 是 v1.8 起约定的全量规则数组（含 PASS）。
--      DML 从这里展开，DWS 算 pass_rate 不再被 “只记 fail/warn” 卡住。
--   3. failed_rules_json / warning_rules_json 兼容三种历史格式：
--      a. v1.8 起的 JSON array
--      b. v1.7 历史的 {"items": [...]} 包裹
--      c. NULL
--      统一通过 _expand_rules CTE + jsonb_typeof 判定，保持幂等。
--   4. 仍然保留 summary_rules（每个 run 一行 rule_id='contract_status'），方便审计。
--   5. rule_result_key = md5(run_id|severity|rule_id|metric)，对同 rule 多次出现幂等。
--
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

WITH runs AS (
    SELECT
        qcr.run_id,
        qcr.contract_id,
        qcr.dataset_id,
        qcr.version,
        qcr.dataset_family,
        qcr.status                                                         AS contract_status,
        (COALESCE(qcr.started_at, qcr.created_at) AT TIME ZONE 'UTC')::date AS dt,
        qcr.metrics_json,
        qcr.failed_rules_json,
        qcr.warning_rules_json
    FROM {{ schema }}.qc_contract_runs qcr
    WHERE (COALESCE(qcr.started_at, qcr.created_at) AT TIME ZONE 'UTC')::date BETWEEN
            CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
),
-- v1.8：metrics_json._rule_results 是全量规则数组（含 PASS / WARN / FAIL），
-- DML 直接展开它，确保 pass_count > 0 才能算 pass_rate。
all_rules_from_metrics AS (
    SELECT
        runs.run_id,
        runs.contract_id,
        runs.dataset_id,
        runs.version,
        runs.dataset_family,
        runs.dt,
        rule_obj.value ->> 'rule_id'                                       AS rule_id,
        rule_obj.value ->> 'severity'                                      AS severity,
        UPPER(COALESCE(rule_obj.value ->> 'status', 'PASS'))               AS status,
        rule_obj.value ->> 'metric'                                        AS metric,
        rule_obj.value ->> 'op'                                            AS op,
        (rule_obj.value ->> 'threshold')                                   AS threshold_value,
        (rule_obj.value ->> 'actual')                                      AS actual_value
    FROM runs
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(COALESCE(runs.metrics_json -> '_rule_results', '[]'::jsonb)) = 'array'
                THEN COALESCE(runs.metrics_json -> '_rule_results', '[]'::jsonb)
            ELSE '[]'::jsonb
        END
    ) AS rule_obj
),
-- 兼容历史：失败规则可能是 array 也可能是 {"items": [...]}。
_failed_array AS (
    SELECT
        runs.run_id, runs.contract_id, runs.dataset_id, runs.version,
        runs.dataset_family, runs.dt,
        CASE
            WHEN jsonb_typeof(runs.failed_rules_json) = 'array'
                THEN COALESCE(runs.failed_rules_json, '[]'::jsonb)
            WHEN jsonb_typeof(runs.failed_rules_json) = 'object'
                THEN COALESCE(runs.failed_rules_json -> 'items', '[]'::jsonb)
            ELSE '[]'::jsonb
        END AS rules
    FROM runs
    -- 只在 metrics_json._rule_results 缺失时用 failed/warning 兜底，避免重复行。
    WHERE NOT EXISTS (
        SELECT 1 FROM all_rules_from_metrics arm WHERE arm.run_id = runs.run_id
    )
),
fail_rules AS (
    SELECT
        fa.run_id, fa.contract_id, fa.dataset_id, fa.version, fa.dataset_family, fa.dt,
        rule_obj.value ->> 'rule_id'    AS rule_id,
        rule_obj.value ->> 'severity'   AS severity,
        'FAIL'                          AS status,
        rule_obj.value ->> 'metric'     AS metric,
        rule_obj.value ->> 'op'         AS op,
        rule_obj.value ->> 'threshold'  AS threshold_value,
        rule_obj.value ->> 'actual'     AS actual_value
    FROM _failed_array fa
    CROSS JOIN LATERAL jsonb_array_elements(fa.rules) AS rule_obj
),
_warn_array AS (
    SELECT
        runs.run_id, runs.contract_id, runs.dataset_id, runs.version,
        runs.dataset_family, runs.dt,
        CASE
            WHEN jsonb_typeof(runs.warning_rules_json) = 'array'
                THEN COALESCE(runs.warning_rules_json, '[]'::jsonb)
            WHEN jsonb_typeof(runs.warning_rules_json) = 'object'
                THEN COALESCE(runs.warning_rules_json -> 'items', '[]'::jsonb)
            ELSE '[]'::jsonb
        END AS rules
    FROM runs
    WHERE NOT EXISTS (
        SELECT 1 FROM all_rules_from_metrics arm WHERE arm.run_id = runs.run_id
    )
),
warn_rules AS (
    SELECT
        wa.run_id, wa.contract_id, wa.dataset_id, wa.version, wa.dataset_family, wa.dt,
        rule_obj.value ->> 'rule_id'    AS rule_id,
        rule_obj.value ->> 'severity'   AS severity,
        'WARN'                          AS status,
        rule_obj.value ->> 'metric'     AS metric,
        rule_obj.value ->> 'op'         AS op,
        rule_obj.value ->> 'threshold'  AS threshold_value,
        rule_obj.value ->> 'actual'     AS actual_value
    FROM _warn_array wa
    CROSS JOIN LATERAL jsonb_array_elements(wa.rules) AS rule_obj
),
-- contract_status 行：每个 run 一行 summary，rule_id='contract_status'。
summary_rules AS (
    SELECT
        runs.run_id, runs.contract_id, runs.dataset_id, runs.version, runs.dataset_family, runs.dt,
        'contract_status'  AS rule_id,
        'summary'          AS severity,
        runs.contract_status AS status,
        NULL               AS metric,
        NULL               AS op,
        NULL               AS threshold_value,
        NULL               AS actual_value
    FROM runs
),
all_rules AS (
    SELECT * FROM all_rules_from_metrics
    UNION ALL
    SELECT * FROM fail_rules
    UNION ALL
    SELECT * FROM warn_rules
    UNION ALL
    SELECT * FROM summary_rules
)
INSERT INTO {{ schema }}.fact_qc_rule_result (
    rule_result_key,
    run_id,
    contract_id,
    dataset_id,
    version,
    dataset_family,
    rule_id,
    severity,
    status,
    metric,
    op,
    threshold_value,
    actual_value,
    dt,
    created_at
)
SELECT
    md5(
        coalesce(run_id, '') || '|' ||
        coalesce(severity, '') || '|' ||
        coalesce(rule_id, '') || '|' ||
        coalesce(metric, '')
    )                                                                      AS rule_result_key,
    run_id,
    contract_id,
    dataset_id,
    version,
    dataset_family,
    rule_id,
    severity,
    status,
    metric,
    op,
    threshold_value,
    actual_value,
    dt,
    now()                                                                  AS created_at
FROM all_rules
WHERE rule_id IS NOT NULL
ON CONFLICT (rule_result_key) DO UPDATE SET
    status            = EXCLUDED.status,
    metric            = EXCLUDED.metric,
    op                = EXCLUDED.op,
    threshold_value   = EXCLUDED.threshold_value,
    actual_value      = EXCLUDED.actual_value,
    created_at        = now();
