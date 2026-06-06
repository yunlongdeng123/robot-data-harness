#!/usr/bin/env bash
# robot-dh-infra：v1.9 推理运营报告。
#
# 查询 inference_jobs / inference_outputs / inference_failures / inference_benchmark_runs，
# 输出 Markdown 与 JSON 两份报告到 /data/robot-dh/logs/。
# 表为空时输出零值报告，不失败。
#
# 产物：
#   /data/robot-dh/logs/v1_9_inference_ops_YYYYmmdd_HHMMSS.md
#   /data/robot-dh/logs/v1_9_inference_ops_YYYYmmdd_HHMMSS.json
#
# 可覆盖环境变量：
#   ROBOT_DH_PG_CONTAINER   PG 容器名（默认 robot-dh-postgres）
#   ROBOT_DH_PG_APP_USER    PG 应用账号（默认 robot_dh_app）
#   ROBOT_DH_PG_DB          数据库名（默认 robot_dh）
#   ROBOT_DH_LOG_DIR        报告输出目录（默认 /data/robot-dh/logs）
set -euo pipefail

PG_CONTAINER="${ROBOT_DH_PG_CONTAINER:-robot-dh-postgres}"
PG_APP_USER="${ROBOT_DH_PG_APP_USER:-robot_dh_app}"
PG_DB="${ROBOT_DH_PG_DB:-robot_dh}"
LOG_DIR="${ROBOT_DH_LOG_DIR:-/data/robot-dh/logs}"

TS="$(date -u +%Y%m%d_%H%M%S)"
TS_HUMAN="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
MD_FILE="${LOG_DIR}/v1_9_inference_ops_${TS}.md"
JSON_FILE="${LOG_DIR}/v1_9_inference_ops_${TS}.json"

log() { printf '[47_inference_ops_report] %s\n' "$*"; }
die() { printf '[47_inference_ops_report] ERROR: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker 未安装"
docker inspect "$PG_CONTAINER" >/dev/null 2>&1 || die "PG 容器不存在：$PG_CONTAINER"
mkdir -p "$LOG_DIR"

# 只读查询走 app 账号。
psql_scalar() {
  docker exec -i "$PG_CONTAINER" \
    psql -v ON_ERROR_STOP=1 -X -q -tA -U "$PG_APP_USER" -d "$PG_DB" -c "$1"
}
psql_table() {
  docker exec -i "$PG_CONTAINER" \
    psql -v ON_ERROR_STOP=1 -X -q -P pager=off -U "$PG_APP_USER" -d "$PG_DB" -c "$1"
}

log "生成推理运营报告（db=${PG_DB}）..."

# ---------- JSON 报告：一条 json_build_object 查询返回完整对象 ----------
json_payload="$(psql_scalar "
SELECT json_build_object(
  'generated_at', '${TS_HUMAN}',
  'database', '${PG_DB}',
  'inference_jobs', json_build_object(
    'total', (SELECT count(*) FROM inference_jobs),
    'by_status', (SELECT coalesce(json_object_agg(status, c), '{}'::json)
                  FROM (SELECT status, count(*) c FROM inference_jobs GROUP BY status) s),
    'by_task_type', (SELECT coalesce(json_object_agg(task_type, c), '{}'::json)
                     FROM (SELECT task_type, count(*) c FROM inference_jobs GROUP BY task_type) s),
    'total_samples', (SELECT coalesce(sum(total_samples), 0) FROM inference_jobs),
    'processed_samples', (SELECT coalesce(sum(processed_samples), 0) FROM inference_jobs),
    'failed_samples', (SELECT coalesce(sum(failed_samples), 0) FROM inference_jobs)
  ),
  'inference_outputs', json_build_object(
    'total', (SELECT count(*) FROM inference_outputs),
    'by_prediction_type', (SELECT coalesce(json_object_agg(coalesce(prediction_type, '(null)'), c), '{}'::json)
                           FROM (SELECT prediction_type, count(*) c FROM inference_outputs GROUP BY prediction_type) s),
    'avg_latency_ms', (SELECT round(avg(latency_ms)::numeric, 2) FROM inference_outputs),
    'avg_confidence', (SELECT round(avg(confidence)::numeric, 4) FROM inference_outputs)
  ),
  'inference_failures', json_build_object(
    'total', (SELECT count(*) FROM inference_failures),
    'retryable', (SELECT count(*) FROM inference_failures WHERE retryable),
    'non_retryable', (SELECT count(*) FROM inference_failures WHERE NOT retryable),
    'by_error_type', (SELECT coalesce(json_object_agg(coalesce(error_type, '(null)'), c), '{}'::json)
                      FROM (SELECT error_type, count(*) c FROM inference_failures GROUP BY error_type) s)
  ),
  'inference_benchmark_runs', json_build_object(
    'total', (SELECT count(*) FROM inference_benchmark_runs),
    'by_backend', (SELECT coalesce(json_object_agg(coalesce(backend, '(null)'), c), '{}'::json)
                   FROM (SELECT backend, count(*) c FROM inference_benchmark_runs GROUP BY backend) s),
    'latest', (SELECT coalesce(json_agg(row_to_json(b)), '[]'::json)
               FROM (SELECT benchmark_id, model_id, backend, workload_name, status,
                            samples_per_sec, p95_latency_ms, error_rate, created_at
                     FROM inference_benchmark_runs
                     ORDER BY created_at DESC NULLS LAST LIMIT 10) b)
  )
);
")"
printf '%s\n' "$json_payload" > "$JSON_FILE"

# ---------- Markdown 报告 ----------
jobs_total="$(psql_scalar "SELECT count(*) FROM inference_jobs;")"
outputs_total="$(psql_scalar "SELECT count(*) FROM inference_outputs;")"
failures_total="$(psql_scalar "SELECT count(*) FROM inference_failures;")"
bench_total="$(psql_scalar "SELECT count(*) FROM inference_benchmark_runs;")"

{
  echo "# v1.9 推理运营报告"
  echo
  echo "- 生成时间（UTC）：${TS_HUMAN}"
  echo "- 数据库：${PG_DB}"
  echo "- 概览：inference_jobs=${jobs_total} · inference_outputs=${outputs_total} · inference_failures=${failures_total} · benchmark_runs=${bench_total}"
  echo
  echo "## inference_jobs（按状态 / 任务类型）"
  echo
  echo '```'
  psql_table "
SELECT status, count(*) AS jobs,
       coalesce(sum(total_samples),0)     AS total_samples,
       coalesce(sum(processed_samples),0) AS processed_samples,
       coalesce(sum(failed_samples),0)    AS failed_samples
FROM inference_jobs GROUP BY status ORDER BY status;"
  echo
  psql_table "
SELECT task_type, count(*) AS jobs
FROM inference_jobs GROUP BY task_type ORDER BY jobs DESC;"
  echo '```'
  echo
  echo "## inference_outputs（按预测类型）"
  echo
  echo '```'
  psql_table "
SELECT coalesce(prediction_type,'(null)') AS prediction_type,
       count(*) AS outputs,
       round(avg(latency_ms)::numeric, 2) AS avg_latency_ms,
       round(avg(confidence)::numeric, 4) AS avg_confidence
FROM inference_outputs GROUP BY prediction_type ORDER BY outputs DESC;"
  echo '```'
  echo
  echo "## inference_failures（按错误类型）"
  echo
  echo '```'
  psql_table "
SELECT coalesce(error_type,'(null)') AS error_type,
       count(*) AS failures,
       count(*) FILTER (WHERE retryable)     AS retryable,
       count(*) FILTER (WHERE NOT retryable) AS non_retryable
FROM inference_failures GROUP BY error_type ORDER BY failures DESC;"
  echo '```'
  echo
  echo "## inference_benchmark_runs（最近 10 次）"
  echo
  echo '```'
  psql_table "
SELECT benchmark_id, coalesce(backend,'(null)') AS backend,
       coalesce(workload_name,'(null)') AS workload, status,
       round(samples_per_sec::numeric, 2) AS samples_per_sec,
       round(p95_latency_ms::numeric, 2)  AS p95_latency_ms,
       round(error_rate::numeric, 4)      AS error_rate,
       created_at
FROM inference_benchmark_runs
ORDER BY created_at DESC NULLS LAST LIMIT 10;"
  echo '```'
} > "$MD_FILE"

log "OK：报告已生成"
log "  Markdown：${MD_FILE}"
log "  JSON：    ${JSON_FILE}"
