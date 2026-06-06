-- 物化 dws_dataset_quality_daily：聚合 fact_etl_run / fact_qc_rule_result / fact_workflow_step / fact_asset_profile + ml_ready_datasets。
-- 维度：(dt, dataset_id, version)。
-- 参数：{{ schema }} / {{ start_date }} / {{ end_date }}

WITH date_range AS (
    SELECT
        CAST('{{ start_date }}' AS date) AS start_dt,
        CAST('{{ end_date }}'   AS date) AS end_dt
),
etl_agg AS (
    -- v1.8 修复：与 runner.py / cli.py 语义对齐——
    -- 1) WARN 在业务上是 "带警告的成功"（cli.py: `return 0 if status in {OK, WARN} else 1`），
    --    必须计入 etl_success_count，否则 features 单步 WARN 会把 etl_success_rate 拖到 67%/80%
    --    触发 CRITICAL 告警，制造"明明每步都退出 0 但 dashboard 报警"的假阳性。
    -- 2) RUNNING / PENDING / STARTED 等非终态不进分母——历史上 normalize 内部因
    --    EtlProfiler 退出时机错位写入的 RUNNING 孤儿（已通过 normalize.py 修复，
    --    PG 里仍可能有遗留行）会把成功率算成 N/(N+1)；任何未来 heartbeat /
    --    early-write 也以此口径兜底，保证 dashboard 不再被中间态污染。
    SELECT
        f.dt,
        f.dataset_id,
        f.version,
        f.dataset_family,
        count(*)                                                           AS etl_run_count,
        count(*) FILTER (WHERE upper(coalesce(f.status, '')) IN ('OK', 'WARN', 'SUCCESS', 'SUCCEEDED'))
                                                                           AS etl_success_count,
        count(*) FILTER (WHERE upper(coalesce(f.status, '')) IN ('FAIL', 'FAILED', 'ERROR'))
                                                                           AS etl_fail_count,
        sum(coalesce(f.input_bytes, 0))                                    AS total_input_bytes,
        sum(coalesce(f.output_bytes, 0))                                   AS total_output_bytes,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY f.duration_sec)       AS p95_etl_duration_sec
    FROM {{ schema }}.fact_etl_run f
    WHERE f.dt BETWEEN (SELECT start_dt FROM date_range) AND (SELECT end_dt FROM date_range)
      AND f.dataset_id IS NOT NULL
      AND f.version    IS NOT NULL
      AND upper(coalesce(f.status, '')) NOT IN ('RUNNING', 'PENDING', 'STARTED')
    GROUP BY 1, 2, 3, 4
),
qc_agg AS (
    SELECT
        f.dt,
        f.dataset_id,
        f.version,
        count(*)                                                           AS qc_run_count,
        count(*) FILTER (WHERE upper(coalesce(f.status, '')) = 'PASS')     AS qc_pass_count,
        count(*) FILTER (WHERE upper(coalesce(f.status, '')) = 'WARN')     AS qc_warn_count,
        count(*) FILTER (WHERE upper(coalesce(f.status, '')) = 'FAIL')     AS qc_fail_count
    FROM {{ schema }}.fact_qc_rule_result f
    WHERE f.dt BETWEEN (SELECT start_dt FROM date_range) AND (SELECT end_dt FROM date_range)
      AND f.rule_id = 'contract_status'
      AND f.dataset_id IS NOT NULL
      AND f.version    IS NOT NULL
    GROUP BY 1, 2, 3
),
wf_agg AS (
    -- fact_workflow_step 里有 archive-logs-index / argo-sync 这种与 dataset 无关的 step，
    -- 这里只保留有 dataset_id / version 的行；否则会把 NULL 写到 dws.dataset_id 主键。
    SELECT
        f.dt,
        f.dataset_id,
        f.version,
        count(DISTINCT f.workflow_name)                                    AS workflow_count,
        count(DISTINCT f.workflow_name) FILTER (WHERE upper(coalesce(f.phase, '')) = 'SUCCEEDED')
                                                                           AS workflow_success_count,
        count(DISTINCT f.workflow_name) FILTER (WHERE upper(coalesce(f.phase, '')) IN ('FAILED', 'ERROR'))
                                                                           AS workflow_fail_count,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY f.duration_sec)       AS p95_workflow_step_duration_sec
    FROM {{ schema }}.fact_workflow_step f
    WHERE f.dt BETWEEN (SELECT start_dt FROM date_range) AND (SELECT end_dt FROM date_range)
      AND f.dataset_id IS NOT NULL
      AND f.version    IS NOT NULL
    GROUP BY 1, 2, 3
),
ml_agg AS (
    SELECT
        (mlr.created_at AT TIME ZONE 'UTC')::date                          AS dt,
        mlr.dataset_id,
        mlr.version,
        max(coalesce(mlr.num_train, 0) + coalesce(mlr.num_val, 0) + coalesce(mlr.num_test, 0))
                                                                           AS ml_ready_rows
    FROM {{ schema }}.ml_ready_datasets mlr
    WHERE (mlr.created_at AT TIME ZONE 'UTC')::date BETWEEN
            (SELECT start_dt FROM date_range) AND (SELECT end_dt FROM date_range)
      AND mlr.dataset_id IS NOT NULL
      AND mlr.version    IS NOT NULL
    GROUP BY 1, 2, 3
),
quality_agg AS (
    SELECT
        (qs.created_at AT TIME ZONE 'UTC')::date                           AS dt,
        qs.dataset_id,
        qs.version,
        avg(qs.quality_score)                                              AS avg_quality_score
    FROM {{ schema }}.quality_snapshots qs
    WHERE (qs.created_at AT TIME ZONE 'UTC')::date BETWEEN
            (SELECT start_dt FROM date_range) AND (SELECT end_dt FROM date_range)
      AND qs.dataset_id IS NOT NULL
      AND qs.version    IS NOT NULL
    GROUP BY 1, 2, 3
),
all_keys AS (
    -- 兜底 NOT NULL：dws_dataset_quality_daily 主键是 (dt, dataset_id, version)，三列均 NOT NULL。
    SELECT * FROM (
        SELECT dt, dataset_id, version, dataset_family FROM etl_agg
        UNION
        SELECT dt, dataset_id, version, NULL::text       FROM qc_agg
        UNION
        SELECT dt, dataset_id, version, NULL::text       FROM wf_agg
        UNION
        SELECT dt, dataset_id, version, NULL::text       FROM ml_agg
    ) u
    WHERE dataset_id IS NOT NULL
      AND version    IS NOT NULL
)
INSERT INTO {{ schema }}.dws_dataset_quality_daily (
    dt, dataset_id, version, dataset_family,
    qc_run_count, qc_pass_count, qc_warn_count, qc_fail_count, qc_pass_rate,
    etl_run_count, etl_success_count, etl_fail_count, etl_success_rate,
    workflow_count, workflow_success_count, workflow_fail_count, workflow_success_rate,
    avg_quality_score, ml_ready_rows,
    total_input_bytes, total_output_bytes,
    p95_etl_duration_sec, p95_workflow_step_duration_sec,
    stale_heartbeat_count,
    updated_at
)
SELECT
    k.dt, k.dataset_id, k.version,
    coalesce(e.dataset_family, k.dataset_family)                           AS dataset_family,
    coalesce(q.qc_run_count, 0),
    coalesce(q.qc_pass_count, 0),
    coalesce(q.qc_warn_count, 0),
    coalesce(q.qc_fail_count, 0),
    CASE WHEN coalesce(q.qc_run_count, 0) > 0
         THEN q.qc_pass_count::double precision / q.qc_run_count
         ELSE NULL END                                                     AS qc_pass_rate,
    coalesce(e.etl_run_count, 0),
    coalesce(e.etl_success_count, 0),
    coalesce(e.etl_fail_count, 0),
    CASE WHEN coalesce(e.etl_run_count, 0) > 0
         THEN e.etl_success_count::double precision / e.etl_run_count
         ELSE NULL END                                                     AS etl_success_rate,
    coalesce(w.workflow_count, 0),
    coalesce(w.workflow_success_count, 0),
    coalesce(w.workflow_fail_count, 0),
    CASE WHEN coalesce(w.workflow_count, 0) > 0
         THEN w.workflow_success_count::double precision / w.workflow_count
         ELSE NULL END                                                     AS workflow_success_rate,
    qsa.avg_quality_score,
    coalesce(m.ml_ready_rows, 0),
    coalesce(e.total_input_bytes, 0),
    coalesce(e.total_output_bytes, 0),
    e.p95_etl_duration_sec,
    w.p95_workflow_step_duration_sec,
    0                                                                       AS stale_heartbeat_count,
    now()
FROM all_keys k
LEFT JOIN etl_agg      e   ON e.dt = k.dt AND e.dataset_id = k.dataset_id AND e.version = k.version
LEFT JOIN qc_agg       q   ON q.dt = k.dt AND q.dataset_id = k.dataset_id AND q.version = k.version
LEFT JOIN wf_agg       w   ON w.dt = k.dt AND w.dataset_id = k.dataset_id AND w.version = k.version
LEFT JOIN ml_agg       m   ON m.dt = k.dt AND m.dataset_id = k.dataset_id AND m.version = k.version
LEFT JOIN quality_agg  qsa ON qsa.dt = k.dt AND qsa.dataset_id = k.dataset_id AND qsa.version = k.version
ON CONFLICT (dt, dataset_id, version) DO UPDATE SET
    dataset_family                  = EXCLUDED.dataset_family,
    qc_run_count                    = EXCLUDED.qc_run_count,
    qc_pass_count                   = EXCLUDED.qc_pass_count,
    qc_warn_count                   = EXCLUDED.qc_warn_count,
    qc_fail_count                   = EXCLUDED.qc_fail_count,
    qc_pass_rate                    = EXCLUDED.qc_pass_rate,
    etl_run_count                   = EXCLUDED.etl_run_count,
    etl_success_count               = EXCLUDED.etl_success_count,
    etl_fail_count                  = EXCLUDED.etl_fail_count,
    etl_success_rate                = EXCLUDED.etl_success_rate,
    workflow_count                  = EXCLUDED.workflow_count,
    workflow_success_count          = EXCLUDED.workflow_success_count,
    workflow_fail_count             = EXCLUDED.workflow_fail_count,
    workflow_success_rate           = EXCLUDED.workflow_success_rate,
    avg_quality_score               = EXCLUDED.avg_quality_score,
    ml_ready_rows                   = EXCLUDED.ml_ready_rows,
    total_input_bytes               = EXCLUDED.total_input_bytes,
    total_output_bytes              = EXCLUDED.total_output_bytes,
    p95_etl_duration_sec            = EXCLUDED.p95_etl_duration_sec,
    p95_workflow_step_duration_sec  = EXCLUDED.p95_workflow_step_duration_sec,
    updated_at                      = now();
