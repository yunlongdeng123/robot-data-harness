-- 物化 fact_etl_run：以 etl_perf_runs 为主源。
-- run_key = md5(job_id|run_id|phase|dataset_id|version)，幂等 UPSERT。
--
-- v1.8 修复：etl_perf_runs 在远端 infra 现网只有 created_at + duration_sec，没有
-- started_at / finished_at；新版本 ORM 写入路径已把这两个值兜底放进 metrics_json，
-- 这里优先用 metrics_json 取值，否则按 created_at + duration_sec 衍生：
--   started_at  := metrics_json->>'started_at'  ?? created_at
--   finished_at := metrics_json->>'finished_at' ?? created_at + duration_sec
-- dt 用 coalesce(started_at_value, created_at) 截断到日。
--
-- 参数：
--   {{ schema }}
--   {{ start_date }} / {{ end_date }} (含端点；为空时由 builder 转成 'epoch'..'infinity')

WITH src AS (
    SELECT
        epr.*,
        COALESCE(
            (epr.metrics_json ->> 'started_at')::timestamptz,
            epr.created_at
        )                                                              AS started_at_value,
        COALESCE(
            (epr.metrics_json ->> 'finished_at')::timestamptz,
            epr.created_at + (coalesce(epr.duration_sec, 0) * interval '1 second')
        )                                                              AS finished_at_value
    FROM {{ schema }}.etl_perf_runs epr
    WHERE (epr.created_at AT TIME ZONE 'UTC')::date BETWEEN
            CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
      AND epr.dataset_id IS NOT NULL
      AND epr.version IS NOT NULL
)
INSERT INTO {{ schema }}.fact_etl_run (
    run_key,
    job_id,
    run_id,
    dataset_id,
    version,
    dataset_family,
    phase,
    status,
    started_at,
    finished_at,
    dt,
    duration_sec,
    input_bytes,
    output_bytes,
    input_rows,
    output_rows,
    peak_memory_mb,
    error_message,
    archive_log_uri,
    created_at
)
SELECT
    md5(
        coalesce(src.job_id, '') || '|' ||
        coalesce(src.run_id, '') || '|' ||
        coalesce(src.phase, '') || '|' ||
        coalesce(src.dataset_id, '') || '|' ||
        coalesce(src.version, '')
    )                                                              AS run_key,
    src.job_id,
    src.run_id,
    src.dataset_id,
    src.version,
    NULL                                                           AS dataset_family,
    src.phase,
    src.status,
    src.started_at_value                                           AS started_at,
    src.finished_at_value                                          AS finished_at,
    (src.started_at_value AT TIME ZONE 'UTC')::date                AS dt,
    src.duration_sec,
    src.input_bytes,
    src.output_bytes,
    src.input_rows,
    src.output_rows,
    src.peak_memory_mb,
    src.error_message,
    (src.metrics_json ->> 'archive_log_uri')                       AS archive_log_uri,
    now()                                                          AS created_at
FROM src
ON CONFLICT (run_key) DO UPDATE SET
    status            = EXCLUDED.status,
    finished_at       = EXCLUDED.finished_at,
    duration_sec      = EXCLUDED.duration_sec,
    input_bytes       = EXCLUDED.input_bytes,
    output_bytes      = EXCLUDED.output_bytes,
    input_rows        = EXCLUDED.input_rows,
    output_rows       = EXCLUDED.output_rows,
    peak_memory_mb    = EXCLUDED.peak_memory_mb,
    error_message     = EXCLUDED.error_message,
    archive_log_uri   = EXCLUDED.archive_log_uri,
    created_at        = now();
