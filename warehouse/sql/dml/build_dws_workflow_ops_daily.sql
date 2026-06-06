-- 物化 dws_workflow_ops_daily：从 fact_workflow_step + workflow_runs + task_heartbeats 聚合到 (dt, workflow_type)。
-- workflow_type 落空时归到 'unknown'。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

WITH step_agg AS (
    SELECT
        fws.dt,
        coalesce(fws.workflow_type, 'unknown')                             AS workflow_type,
        count(DISTINCT fws.workflow_name)                                  AS workflow_count,
        count(DISTINCT fws.workflow_name) FILTER (WHERE upper(coalesce(fws.phase, '')) = 'SUCCEEDED')
                                                                           AS success_count,
        count(DISTINCT fws.workflow_name) FILTER (WHERE upper(coalesce(fws.phase, '')) IN ('FAILED', 'ERROR'))
                                                                           AS failed_count,
        count(DISTINCT fws.workflow_name) FILTER (WHERE upper(coalesce(fws.phase, '')) IN ('RUNNING', 'PENDING'))
                                                                           AS running_count,
        avg(fws.duration_sec)                                              AS avg_duration_sec,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY fws.duration_sec)     AS p95_duration_sec,
        count(*) FILTER (WHERE upper(coalesce(fws.container_reason, '')) = 'DEADLINEEXCEEDED')
                                                                           AS deadline_exceeded_count,
        count(*) FILTER (WHERE upper(coalesce(fws.container_reason, '')) = 'OOMKILLED')
                                                                           AS oom_count,
        count(*) FILTER (WHERE coalesce(fws.exit_code, 0) <> 0)            AS nonzero_exit_count
    FROM {{ schema }}.fact_workflow_step fws
    WHERE fws.dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
    GROUP BY 1, 2
)
INSERT INTO {{ schema }}.dws_workflow_ops_daily (
    dt, workflow_type,
    workflow_count, success_count, failed_count, running_count,
    success_rate, avg_duration_sec, p95_duration_sec,
    deadline_exceeded_count, oom_count, nonzero_exit_count,
    updated_at
)
SELECT
    s.dt, s.workflow_type,
    coalesce(s.workflow_count, 0),
    coalesce(s.success_count, 0),
    coalesce(s.failed_count, 0),
    coalesce(s.running_count, 0),
    CASE WHEN coalesce(s.workflow_count, 0) > 0
         THEN s.success_count::double precision / s.workflow_count
         ELSE NULL END                                                     AS success_rate,
    s.avg_duration_sec,
    s.p95_duration_sec,
    coalesce(s.deadline_exceeded_count, 0),
    coalesce(s.oom_count, 0),
    coalesce(s.nonzero_exit_count, 0),
    now()
FROM step_agg s
ON CONFLICT (dt, workflow_type) DO UPDATE SET
    workflow_count          = EXCLUDED.workflow_count,
    success_count           = EXCLUDED.success_count,
    failed_count            = EXCLUDED.failed_count,
    running_count           = EXCLUDED.running_count,
    success_rate            = EXCLUDED.success_rate,
    avg_duration_sec        = EXCLUDED.avg_duration_sec,
    p95_duration_sec        = EXCLUDED.p95_duration_sec,
    deadline_exceeded_count = EXCLUDED.deadline_exceeded_count,
    oom_count               = EXCLUDED.oom_count,
    nonzero_exit_count      = EXCLUDED.nonzero_exit_count,
    updated_at              = now();
