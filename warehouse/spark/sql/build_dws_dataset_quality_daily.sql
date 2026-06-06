-- v1.8 promptC：Spark local mode DWS 宽表。
-- 输入：warehouse export 产出的 4 张 parquet（已注册为 temp view）
--   fact_etl_run / fact_qc_rule_result / fact_workflow_step / dim_dataset
-- 输出：dws_dataset_quality_daily 单行 = (dt, dataset_id, version)
-- 占位符：{{ dt }} 由 Python 渲染替换；保持 single-quote 包裹。
--
-- 与远端 PostgreSQL 版本（warehouse/sql/dml/build_dws_dataset_quality_daily.sql）
-- 计算口径一致：
--   1) etl_success_count 必须包含 WARN（runner.py / cli.py 把 WARN 也视为退出码 0 的成功），
--      只认 'SUCCEEDED' 会让 features 单步 WARN 把 etl_success_rate 拖到 0/67% 触发假阳性 CRITICAL。
--   2) etl_run_count 分母必须排除 RUNNING / PENDING / STARTED 等非终态，防止 heartbeat /
--      early-write 的中间状态污染成功率。
--   3) qc 维度必须只看 rule_id='contract_status' 的 contract-level 结果；统计全部 rule
--      会把 PASS / WARN / FAIL 多次计数，造成 qc_pass_rate 漂移。

WITH
etl_agg AS (
    SELECT
        dt,
        dataset_id,
        version,
        ANY_VALUE(dataset_family)                                          AS dataset_family,
        COUNT(*)                                                            AS etl_run_count,
        SUM(CASE WHEN UPPER(COALESCE(status, '')) IN ('OK', 'WARN', 'SUCCESS', 'SUCCEEDED') THEN 1 ELSE 0 END) AS etl_success_count,
        SUM(CASE WHEN UPPER(COALESCE(status, '')) IN ('FAILED', 'FAIL', 'ERROR') THEN 1 ELSE 0 END) AS etl_fail_count,
        SUM(COALESCE(input_bytes, 0))                                       AS total_input_bytes,
        SUM(COALESCE(output_bytes, 0))                                      AS total_output_bytes,
        SUM(COALESCE(output_rows, 0))                                       AS ml_ready_rows,
        PERCENTILE_APPROX(duration_sec, 0.95)                               AS p95_etl_duration_sec
    FROM fact_etl_run
    WHERE dt = DATE('{{ dt }}')
      AND UPPER(COALESCE(status, '')) NOT IN ('RUNNING', 'PENDING', 'STARTED')
    GROUP BY dt, dataset_id, version
),
qc_agg AS (
    SELECT
        dt,
        dataset_id,
        version,
        COUNT(*)                                                            AS qc_run_count,
        SUM(CASE WHEN UPPER(COALESCE(status, '')) = 'PASS' THEN 1 ELSE 0 END) AS qc_pass_count,
        SUM(CASE WHEN UPPER(COALESCE(status, '')) = 'WARN' THEN 1 ELSE 0 END) AS qc_warn_count,
        SUM(CASE WHEN UPPER(COALESCE(status, '')) IN ('FAIL', 'FAILED', 'ERROR') THEN 1 ELSE 0 END) AS qc_fail_count
    FROM fact_qc_rule_result
    WHERE dt = DATE('{{ dt }}')
      AND rule_id = 'contract_status'
    GROUP BY dt, dataset_id, version
),
wf_agg AS (
    SELECT
        dt,
        dataset_id,
        version,
        COUNT(*)                                                            AS workflow_count,
        SUM(CASE WHEN UPPER(COALESCE(phase, '')) IN ('SUCCEEDED', 'SUCCESS') THEN 1 ELSE 0 END) AS workflow_success_count,
        SUM(CASE WHEN UPPER(COALESCE(phase, '')) IN ('FAILED', 'ERROR') THEN 1 ELSE 0 END) AS workflow_fail_count,
        PERCENTILE_APPROX(duration_sec, 0.95)                               AS p95_workflow_step_duration_sec
    FROM fact_workflow_step
    WHERE dt = DATE('{{ dt }}')
      AND dataset_id IS NOT NULL
    GROUP BY dt, dataset_id, version
)
SELECT
    COALESCE(e.dt, q.dt, w.dt)                                              AS dt,
    COALESCE(e.dataset_id, q.dataset_id, w.dataset_id)                      AS dataset_id,
    COALESCE(e.version, q.version, w.version)                               AS version,
    e.dataset_family                                                        AS dataset_family,
    COALESCE(q.qc_run_count, 0)                                             AS qc_run_count,
    COALESCE(q.qc_pass_count, 0)                                            AS qc_pass_count,
    COALESCE(q.qc_warn_count, 0)                                            AS qc_warn_count,
    COALESCE(q.qc_fail_count, 0)                                            AS qc_fail_count,
    CASE
        WHEN COALESCE(q.qc_run_count, 0) = 0 THEN NULL
        ELSE CAST(q.qc_pass_count AS DOUBLE) / q.qc_run_count
    END                                                                     AS qc_pass_rate,
    COALESCE(e.etl_run_count, 0)                                            AS etl_run_count,
    COALESCE(e.etl_success_count, 0)                                        AS etl_success_count,
    COALESCE(e.etl_fail_count, 0)                                           AS etl_fail_count,
    CASE
        WHEN COALESCE(e.etl_run_count, 0) = 0 THEN NULL
        ELSE CAST(e.etl_success_count AS DOUBLE) / e.etl_run_count
    END                                                                     AS etl_success_rate,
    COALESCE(w.workflow_count, 0)                                           AS workflow_count,
    COALESCE(w.workflow_success_count, 0)                                   AS workflow_success_count,
    COALESCE(w.workflow_fail_count, 0)                                      AS workflow_fail_count,
    CASE
        WHEN COALESCE(w.workflow_count, 0) = 0 THEN NULL
        ELSE CAST(w.workflow_success_count AS DOUBLE) / w.workflow_count
    END                                                                     AS workflow_success_rate,
    CAST(NULL AS DOUBLE)                                                    AS avg_quality_score,
    COALESCE(e.ml_ready_rows, 0)                                            AS ml_ready_rows,
    COALESCE(e.total_input_bytes, 0)                                        AS total_input_bytes,
    COALESCE(e.total_output_bytes, 0)                                       AS total_output_bytes,
    e.p95_etl_duration_sec                                                  AS p95_etl_duration_sec,
    w.p95_workflow_step_duration_sec                                        AS p95_workflow_step_duration_sec,
    0                                                                       AS stale_heartbeat_count,
    CURRENT_TIMESTAMP()                                                     AS updated_at
FROM etl_agg e
FULL OUTER JOIN qc_agg q ON e.dt = q.dt AND e.dataset_id = q.dataset_id AND e.version = q.version
FULL OUTER JOIN wf_agg w ON COALESCE(e.dt, q.dt) = w.dt
                        AND COALESCE(e.dataset_id, q.dataset_id) = w.dataset_id
                        AND COALESCE(e.version, q.version) = w.version
