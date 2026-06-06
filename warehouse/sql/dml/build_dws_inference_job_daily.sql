-- 物化 dws_inference_job_daily：从 inference_jobs (+ inference_outputs 时延) 按
-- (dt, model_id, task_type) 聚合。dt 取 COALESCE(finished_at, started_at, created_at)::date。
-- backend 取自 model_registry。参数：{{ schema }} / {{ start_date }} / {{ end_date }}

WITH jobs AS (
    SELECT
        CAST(COALESCE(j.finished_at, j.started_at, j.created_at) AS date) AS dt,
        j.model_id,
        COALESCE(j.task_type, 'unknown')                                  AS task_type,
        j.status,
        COALESCE(j.total_samples, 0)                                      AS total_samples,
        COALESCE(j.processed_samples, 0)                                  AS processed_samples,
        COALESCE(j.failed_samples, 0)                                     AS failed_samples,
        COALESCE(j.duration_sec, 0)                                       AS duration_sec
    FROM {{ schema }}.inference_jobs j
    WHERE CAST(COALESCE(j.finished_at, j.started_at, j.created_at) AS date)
          BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
),
agg AS (
    SELECT
        dt,
        model_id,
        task_type,
        count(*)                                                          AS job_count,
        count(*) FILTER (WHERE upper(status) IN ('SUCCEEDED', 'OK'))      AS success_count,
        count(*) FILTER (WHERE upper(status) IN ('FAILED', 'DEAD_LETTER')) AS fail_count,
        sum(total_samples)                                                AS total_samples,
        sum(processed_samples)                                            AS processed_samples,
        sum(failed_samples)                                               AS failed_samples,
        sum(duration_sec)                                                 AS duration_sec
    FROM jobs
    GROUP BY dt, model_id, task_type
),
lat AS (
    SELECT
        CAST(COALESCE(j.finished_at, j.started_at, j.created_at) AS date) AS dt,
        j.model_id,
        COALESCE(j.task_type, 'unknown')                                  AS task_type,
        avg(o.latency_ms)                                                 AS avg_latency_ms,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY o.latency_ms)        AS p95_latency_ms
    FROM {{ schema }}.inference_outputs o
    JOIN {{ schema }}.inference_jobs j ON j.job_id = o.job_id
    WHERE CAST(COALESCE(j.finished_at, j.started_at, j.created_at) AS date)
          BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
      AND o.latency_ms IS NOT NULL
    GROUP BY dt, j.model_id, COALESCE(j.task_type, 'unknown')
)
INSERT INTO {{ schema }}.dws_inference_job_daily (
    dt, model_id, backend, task_type,
    job_count, success_count, fail_count, success_rate,
    total_samples, samples_per_sec, avg_latency_ms, p95_latency_ms, error_rate,
    updated_at
)
SELECT
    a.dt,
    a.model_id,
    m.backend                                                            AS backend,
    a.task_type,
    a.job_count,
    a.success_count,
    a.fail_count,
    CASE WHEN a.job_count > 0 THEN a.success_count::double precision / a.job_count END AS success_rate,
    a.total_samples,
    CASE WHEN a.duration_sec > 0 THEN a.processed_samples::double precision / a.duration_sec END AS samples_per_sec,
    l.avg_latency_ms,
    l.p95_latency_ms,
    CASE WHEN a.total_samples > 0 THEN a.failed_samples::double precision / a.total_samples ELSE 0 END AS error_rate,
    now()
FROM agg a
LEFT JOIN {{ schema }}.model_registry m ON m.model_id = a.model_id
LEFT JOIN lat l ON l.dt = a.dt AND l.model_id = a.model_id AND l.task_type = a.task_type
ON CONFLICT (dt, model_id, task_type) DO UPDATE SET
    backend         = EXCLUDED.backend,
    job_count       = EXCLUDED.job_count,
    success_count   = EXCLUDED.success_count,
    fail_count      = EXCLUDED.fail_count,
    success_rate    = EXCLUDED.success_rate,
    total_samples   = EXCLUDED.total_samples,
    samples_per_sec = EXCLUDED.samples_per_sec,
    avg_latency_ms  = EXCLUDED.avg_latency_ms,
    p95_latency_ms  = EXCLUDED.p95_latency_ms,
    error_rate      = EXCLUDED.error_rate,
    updated_at      = now();
