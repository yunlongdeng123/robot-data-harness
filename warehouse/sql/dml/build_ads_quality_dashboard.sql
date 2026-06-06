-- 物化 ads_quality_dashboard：从 dws_dataset_quality_daily + dws_rule_failure_daily 聚合得到看板行。
-- alert_level 推荐值：OK / WARN / CRITICAL（小写形式 critical/warn/ok 同样合法，由读端规一化）。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

WITH dws AS (
    SELECT *
    FROM {{ schema }}.dws_dataset_quality_daily
    WHERE dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
),
top_rule AS (
    SELECT DISTINCT ON (dt, dataset_family)
        dt, dataset_family, rule_id, fail_count
    FROM {{ schema }}.dws_rule_failure_daily
    WHERE dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
      AND fail_count > 0
    ORDER BY dt, dataset_family, fail_count DESC, rule_id
),
raw_bytes AS (
    SELECT
        f.dt, f.dataset_id, f.version,
        sum(coalesce(f.bytes, 0))                                          AS raw_bytes
    FROM {{ schema }}.fact_asset_profile f
    WHERE f.dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
      AND f.layer IN ('raw', 'ods')
    GROUP BY 1, 2, 3
),
dwd_bytes AS (
    SELECT
        f.dt, f.dataset_id, f.version,
        sum(coalesce(f.bytes, 0))                                          AS dwd_bytes
    FROM {{ schema }}.fact_asset_profile f
    WHERE f.dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
      AND f.layer = 'dwd'
    GROUP BY 1, 2, 3
)
INSERT INTO {{ schema }}.ads_quality_dashboard (
    dt, dataset_id, version, dataset_family,
    overall_status, quality_score,
    qc_pass_rate, etl_success_rate, workflow_success_rate,
    top_failed_rule, top_failed_rule_count,
    p95_duration_sec, ml_ready_rows,
    raw_bytes, dwd_bytes,
    alert_level, alert_reason,
    updated_at
)
SELECT
    d.dt, d.dataset_id, d.version, d.dataset_family,
    CASE
        WHEN coalesce(d.qc_pass_rate,  1.0) < 0.8 OR coalesce(d.etl_success_rate, 1.0) < 0.8 THEN 'FAIL'
        WHEN coalesce(d.qc_pass_rate,  1.0) < 0.95 THEN 'WARN'
        ELSE 'PASS'
    END                                                                    AS overall_status,
    (100.0 * coalesce(d.qc_pass_rate,        1.0) * 0.5)
        + (100.0 * coalesce(d.etl_success_rate,  1.0) * 0.3)
        + (100.0 * coalesce(d.workflow_success_rate, 1.0) * 0.2)           AS quality_score,
    d.qc_pass_rate,
    d.etl_success_rate,
    d.workflow_success_rate,
    tr.rule_id                                                             AS top_failed_rule,
    tr.fail_count                                                          AS top_failed_rule_count,
    d.p95_workflow_step_duration_sec                                       AS p95_duration_sec,
    d.ml_ready_rows,
    rb.raw_bytes,
    db.dwd_bytes,
    CASE
        WHEN coalesce(d.qc_pass_rate, 1.0) < 0.8 OR coalesce(d.etl_success_rate, 1.0) < 0.8 THEN 'CRITICAL'
        WHEN coalesce(d.qc_pass_rate, 1.0) < 0.95 THEN 'WARN'
        ELSE 'OK'
    END                                                                    AS alert_level,
    CASE
        WHEN coalesce(d.qc_pass_rate, 1.0) < 0.8        THEN 'qc_pass_rate<0.8'
        WHEN coalesce(d.etl_success_rate, 1.0) < 0.8    THEN 'etl_success_rate<0.8'
        WHEN coalesce(d.qc_pass_rate, 1.0) < 0.95       THEN 'qc_pass_rate<0.95'
        ELSE NULL
    END                                                                    AS alert_reason,
    now()
FROM dws d
LEFT JOIN top_rule tr  ON tr.dt  = d.dt AND tr.dataset_family IS NOT DISTINCT FROM d.dataset_family
LEFT JOIN raw_bytes rb ON rb.dt  = d.dt AND rb.dataset_id = d.dataset_id AND rb.version = d.version
LEFT JOIN dwd_bytes db ON db.dt  = d.dt AND db.dataset_id = d.dataset_id AND db.version = d.version
ON CONFLICT (dt, dataset_id, version) DO UPDATE SET
    dataset_family         = EXCLUDED.dataset_family,
    overall_status         = EXCLUDED.overall_status,
    quality_score          = EXCLUDED.quality_score,
    qc_pass_rate           = EXCLUDED.qc_pass_rate,
    etl_success_rate       = EXCLUDED.etl_success_rate,
    workflow_success_rate  = EXCLUDED.workflow_success_rate,
    top_failed_rule        = EXCLUDED.top_failed_rule,
    top_failed_rule_count  = EXCLUDED.top_failed_rule_count,
    p95_duration_sec       = EXCLUDED.p95_duration_sec,
    ml_ready_rows          = EXCLUDED.ml_ready_rows,
    raw_bytes              = EXCLUDED.raw_bytes,
    dwd_bytes              = EXCLUDED.dwd_bytes,
    alert_level            = EXCLUDED.alert_level,
    alert_reason           = EXCLUDED.alert_reason,
    updated_at             = now();
