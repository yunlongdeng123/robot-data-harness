-- 物化 ads_inference_dashboard：从 dws_inference_job_daily 推导看板 + 告警。
-- top_error_type 取自 inference_failures（按 job 关联的 dt/model_id/task_type 聚合）。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

WITH dws AS (
    SELECT *
    FROM {{ schema }}.dws_inference_job_daily
    WHERE dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
),
top_err AS (
    SELECT DISTINCT ON (dt, model_id, task_type)
        dt, model_id, task_type, error_type, error_count
    FROM (
        SELECT
            CAST(COALESCE(j.finished_at, j.started_at, j.created_at) AS date) AS dt,
            j.model_id,
            COALESCE(j.task_type, 'unknown')                                  AS task_type,
            COALESCE(f.error_type, 'UNKNOWN')                                 AS error_type,
            count(*)                                                          AS error_count
        FROM {{ schema }}.inference_failures f
        JOIN {{ schema }}.inference_jobs j ON j.job_id = f.job_id
        WHERE CAST(COALESCE(j.finished_at, j.started_at, j.created_at) AS date)
              BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
        GROUP BY dt, j.model_id, COALESCE(j.task_type, 'unknown'), COALESCE(f.error_type, 'UNKNOWN')
    ) e
    ORDER BY dt, model_id, task_type, error_count DESC, error_type
)
INSERT INTO {{ schema }}.ads_inference_dashboard (
    dt, model_id, backend, task_type,
    overall_status, job_count, success_rate, total_samples,
    samples_per_sec, p95_latency_ms, error_rate, top_error_type,
    alert_level, alert_reason, updated_at
)
SELECT
    d.dt, d.model_id, d.backend, d.task_type,
    CASE
        WHEN COALESCE(d.success_rate, 1.0) < 0.8 OR COALESCE(d.error_rate, 0.0) > 0.2 THEN 'FAIL'
        WHEN COALESCE(d.success_rate, 1.0) < 0.95 THEN 'WARN'
        ELSE 'PASS'
    END                                                                    AS overall_status,
    d.job_count,
    d.success_rate,
    d.total_samples,
    d.samples_per_sec,
    d.p95_latency_ms,
    d.error_rate,
    te.error_type                                                          AS top_error_type,
    CASE
        WHEN COALESCE(d.success_rate, 1.0) < 0.8 OR COALESCE(d.error_rate, 0.0) > 0.2 THEN 'CRITICAL'
        WHEN COALESCE(d.success_rate, 1.0) < 0.95 THEN 'WARN'
        ELSE 'OK'
    END                                                                    AS alert_level,
    CASE
        WHEN COALESCE(d.success_rate, 1.0) < 0.8  THEN 'success_rate<0.8'
        WHEN COALESCE(d.error_rate, 0.0) > 0.2    THEN 'error_rate>0.2'
        WHEN COALESCE(d.success_rate, 1.0) < 0.95 THEN 'success_rate<0.95'
        ELSE NULL
    END                                                                    AS alert_reason,
    now()
FROM dws d
LEFT JOIN top_err te ON te.dt = d.dt AND te.model_id = d.model_id AND te.task_type = d.task_type
ON CONFLICT (dt, model_id, task_type) DO UPDATE SET
    backend         = EXCLUDED.backend,
    overall_status  = EXCLUDED.overall_status,
    job_count       = EXCLUDED.job_count,
    success_rate    = EXCLUDED.success_rate,
    total_samples   = EXCLUDED.total_samples,
    samples_per_sec = EXCLUDED.samples_per_sec,
    p95_latency_ms  = EXCLUDED.p95_latency_ms,
    error_rate      = EXCLUDED.error_rate,
    top_error_type  = EXCLUDED.top_error_type,
    alert_level     = EXCLUDED.alert_level,
    alert_reason    = EXCLUDED.alert_reason,
    updated_at      = now();
