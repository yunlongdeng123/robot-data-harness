-- 物化 ads_workflow_ops_dashboard：以 dws_workflow_ops_daily 为唯一源。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

INSERT INTO {{ schema }}.ads_workflow_ops_dashboard (
    dt, workflow_type,
    workflow_count, success_count, failed_count,
    success_rate, avg_duration_sec, p95_duration_sec,
    stale_heartbeat_count, oom_count, deadline_exceeded_count,
    alert_level, alert_reason,
    updated_at
)
SELECT
    d.dt, d.workflow_type,
    d.workflow_count, d.success_count, d.failed_count,
    d.success_rate, d.avg_duration_sec, d.p95_duration_sec,
    0                                                                       AS stale_heartbeat_count,
    d.oom_count, d.deadline_exceeded_count,
    CASE
        WHEN coalesce(d.success_rate, 1.0) < 0.8 OR coalesce(d.oom_count, 0) > 0
             OR coalesce(d.deadline_exceeded_count, 0) > 0 THEN 'CRITICAL'
        WHEN coalesce(d.success_rate, 1.0) < 0.95 THEN 'WARN'
        ELSE 'OK'
    END                                                                    AS alert_level,
    CASE
        WHEN coalesce(d.success_rate, 1.0) < 0.8        THEN 'success_rate<0.8'
        WHEN coalesce(d.oom_count, 0) > 0               THEN 'oom_kill'
        WHEN coalesce(d.deadline_exceeded_count, 0) > 0 THEN 'deadline_exceeded'
        WHEN coalesce(d.success_rate, 1.0) < 0.95       THEN 'success_rate<0.95'
        ELSE NULL
    END                                                                    AS alert_reason,
    now()
FROM {{ schema }}.dws_workflow_ops_daily d
WHERE d.dt BETWEEN CAST('{{ start_date }}' AS date) AND CAST('{{ end_date }}' AS date)
ON CONFLICT (dt, workflow_type) DO UPDATE SET
    workflow_count          = EXCLUDED.workflow_count,
    success_count           = EXCLUDED.success_count,
    failed_count            = EXCLUDED.failed_count,
    success_rate            = EXCLUDED.success_rate,
    avg_duration_sec        = EXCLUDED.avg_duration_sec,
    p95_duration_sec        = EXCLUDED.p95_duration_sec,
    stale_heartbeat_count   = EXCLUDED.stale_heartbeat_count,
    oom_count               = EXCLUDED.oom_count,
    deadline_exceeded_count = EXCLUDED.deadline_exceeded_count,
    alert_level             = EXCLUDED.alert_level,
    alert_reason            = EXCLUDED.alert_reason,
    updated_at              = now();
