-- 物化 fact_workflow_step：从 workflow_steps 抽取 step 级数据。
-- step_key = md5(workflow_namespace|workflow_name|step_name|pod_name)。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

INSERT INTO {{ schema }}.fact_workflow_step (
    step_key,
    workflow_name,
    workflow_namespace,
    workflow_type,
    step_name,
    template_name,
    pod_name,
    phase,
    dataset_id,
    version,
    dataset_family,
    started_at,
    finished_at,
    dt,
    duration_sec,
    exit_code,
    container_reason,
    archive_log_uri,
    archive_log_url,
    created_at
)
SELECT
    md5(
        coalesce(ws.workflow_namespace, '') || '|' ||
        coalesce(ws.workflow_name, '') || '|' ||
        coalesce(ws.step_name, '') || '|' ||
        coalesce(ws.pod_name, '')
    )                                                                      AS step_key,
    ws.workflow_name,
    ws.workflow_namespace,
    wr.workflow_type,
    ws.step_name,
    ws.template_name,
    ws.pod_name,
    ws.phase,
    ws.dataset_id,
    ws.version,
    ws.dataset_family,
    ws.started_at,
    ws.finished_at,
    (ws.started_at AT TIME ZONE 'UTC')::date                               AS dt,
    ws.duration_sec,
    CAST(ws.metrics_json ->> 'exit_code' AS int)                           AS exit_code,
    ws.metrics_json ->> 'container_reason'                                 AS container_reason,
    ws.metrics_json ->> 'archive_log_uri'                                  AS archive_log_uri,
    ws.metrics_json ->> 'archive_log_url'                                  AS archive_log_url,
    now()                                                                  AS created_at
FROM {{ schema }}.workflow_steps ws
LEFT JOIN {{ schema }}.workflow_runs wr
       ON wr.workflow_namespace IS NOT DISTINCT FROM ws.workflow_namespace
      AND wr.workflow_name      = ws.workflow_name
WHERE (ws.started_at AT TIME ZONE 'UTC')::date BETWEEN
        CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
ON CONFLICT (step_key) DO UPDATE SET
    phase             = EXCLUDED.phase,
    finished_at       = EXCLUDED.finished_at,
    duration_sec      = EXCLUDED.duration_sec,
    exit_code         = EXCLUDED.exit_code,
    container_reason  = EXCLUDED.container_reason,
    archive_log_uri   = EXCLUDED.archive_log_uri,
    archive_log_url   = EXCLUDED.archive_log_url,
    created_at        = now();
