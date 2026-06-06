-- v1.8 promptC：Spark local mode ADS 大屏宽表。
-- 输入：上一步 build_dws_dataset_quality_daily 的结果（已注册为 temp view dws_dataset_quality_daily）
--      + 可选 fact_qc_rule_result 用于 top_failed_rule 简化版（按 rule_id 出现次数最多者）
-- 输出：ads_quality_dashboard 单行 = (dt, dataset_id, version)
-- 占位符：{{ dt }} 由 Python 渲染替换。
--
-- quality_score 口径：
--   60 * qc_pass_rate + 30 * etl_success_rate + 10 * workflow_success_rate
-- alert_level 口径：
--   FAIL  if qc_fail_count + etl_fail_count + workflow_fail_count > 0
--   WARN  if qc_warn_count > 0 OR qc_pass_rate < 0.95
--   OK    otherwise

WITH top_rule AS (
    SELECT
        dt,
        dataset_id,
        version,
        rule_id,
        COUNT(*) AS fail_cnt,
        ROW_NUMBER() OVER (
            PARTITION BY dt, dataset_id, version
            ORDER BY COUNT(*) DESC
        ) AS rk
    FROM fact_qc_rule_result
    WHERE dt = DATE('{{ dt }}')
      AND UPPER(COALESCE(status, '')) IN ('FAIL', 'FAILED', 'ERROR')
    GROUP BY dt, dataset_id, version, rule_id
)
SELECT
    d.dt                                                                     AS dt,
    d.dataset_id                                                             AS dataset_id,
    d.version                                                                AS version,
    d.dataset_family                                                         AS dataset_family,
    CASE
        WHEN COALESCE(d.qc_fail_count, 0) + COALESCE(d.etl_fail_count, 0) + COALESCE(d.workflow_fail_count, 0) > 0
            THEN 'FAIL'
        WHEN COALESCE(d.qc_warn_count, 0) > 0
            THEN 'WARN'
        ELSE 'PASS'
    END                                                                      AS overall_status,
    -- quality_score：[0, 100]，三项权重 60/30/10
    ROUND(
        60.0 * COALESCE(d.qc_pass_rate, 0)
      + 30.0 * COALESCE(d.etl_success_rate, 0)
      + 10.0 * COALESCE(d.workflow_success_rate, 0)
    , 2)                                                                     AS quality_score,
    d.qc_pass_rate                                                           AS qc_pass_rate,
    d.etl_success_rate                                                       AS etl_success_rate,
    d.workflow_success_rate                                                  AS workflow_success_rate,
    tr.rule_id                                                               AS top_failed_rule,
    CAST(tr.fail_cnt AS INT)                                                 AS top_failed_rule_count,
    d.p95_etl_duration_sec                                                   AS p95_duration_sec,
    d.ml_ready_rows                                                          AS ml_ready_rows,
    d.total_input_bytes                                                      AS raw_bytes,
    d.total_output_bytes                                                     AS dwd_bytes,
    CASE
        WHEN COALESCE(d.qc_fail_count, 0) + COALESCE(d.etl_fail_count, 0) + COALESCE(d.workflow_fail_count, 0) > 0
            THEN 'CRITICAL'
        WHEN COALESCE(d.qc_warn_count, 0) > 0 OR COALESCE(d.qc_pass_rate, 1.0) < 0.95
            THEN 'WARNING'
        ELSE 'OK'
    END                                                                      AS alert_level,
    CONCAT_WS(' ',
        CASE WHEN COALESCE(d.qc_fail_count, 0) > 0 THEN CONCAT('qc_fail=', d.qc_fail_count) ELSE NULL END,
        CASE WHEN COALESCE(d.etl_fail_count, 0) > 0 THEN CONCAT('etl_fail=', d.etl_fail_count) ELSE NULL END,
        CASE WHEN COALESCE(d.workflow_fail_count, 0) > 0 THEN CONCAT('wf_fail=', d.workflow_fail_count) ELSE NULL END,
        CASE WHEN tr.rule_id IS NOT NULL THEN CONCAT('top_rule=', tr.rule_id) ELSE NULL END
    )                                                                        AS alert_reason,
    CURRENT_TIMESTAMP()                                                      AS updated_at
FROM dws_dataset_quality_daily d
LEFT JOIN top_rule tr
    ON  d.dt = tr.dt
    AND d.dataset_id = tr.dataset_id
    AND d.version = tr.version
    AND tr.rk = 1
WHERE d.dt = DATE('{{ dt }}')
