#!/usr/bin/env bash
# robot-dh-infra：v1.9 推理数据平面 smoke test。
#
# 用应用账号 robot_dh_app 对 6 张关键表做 INSERT -> UPDATE -> DELETE，验证读写权限，
# 插入的 smoke 数据用 __smoke__ 前缀隔离并在结束前删除，绝不影响真实数据。
# 覆盖：model_registry / inference_jobs / inference_outputs /
#       distillation_datasets / inference_benchmark_runs / ai_task_events。
#
# 可覆盖环境变量：
#   ROBOT_DH_PG_CONTAINER   PG 容器名（默认 robot-dh-postgres）
#   ROBOT_DH_PG_APP_USER    PG 应用账号（默认 robot_dh_app）
#   ROBOT_DH_PG_DB          数据库名（默认 robot_dh）
set -euo pipefail

PG_CONTAINER="${ROBOT_DH_PG_CONTAINER:-robot-dh-postgres}"
PG_APP_USER="${ROBOT_DH_PG_APP_USER:-robot_dh_app}"
PG_DB="${ROBOT_DH_PG_DB:-robot_dh}"

# 唯一 smoke key，避免与真实数据或并发 smoke 冲突。
SMOKE_ID="__smoke__$(date -u +%Y%m%d%H%M%S)_$$"

log() { printf '[46_pg_inference_smoke_test] %s\n' "$*"; }
die() { printf '[46_pg_inference_smoke_test] ERROR: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker 未安装"
docker inspect "$PG_CONTAINER" >/dev/null 2>&1 || die "PG 容器不存在：$PG_CONTAINER"

psql_app() {
  docker exec -i "$PG_CONTAINER" \
    psql -v ON_ERROR_STOP=1 -X -q -U "$PG_APP_USER" -d "$PG_DB" "$@"
}

# 兜底清理：即便中途失败，也把本次 SMOKE_ID 的残留行删干净。
cleanup() {
  docker exec -i "$PG_CONTAINER" \
    psql -X -q -U "$PG_APP_USER" -d "$PG_DB" \
    -v sid="$SMOKE_ID" >/dev/null 2>&1 <<'SQL' || true
DELETE FROM inference_outputs       WHERE output_id   = :'sid';
DELETE FROM inference_jobs          WHERE job_id      = :'sid';
DELETE FROM model_registry          WHERE model_id    = :'sid';
DELETE FROM distillation_datasets   WHERE distill_id  = :'sid';
DELETE FROM inference_benchmark_runs WHERE benchmark_id = :'sid';
DELETE FROM ai_task_events          WHERE event_id    = :'sid';
SQL
}
trap cleanup EXIT

log "smoke key = ${SMOKE_ID}（app=${PG_APP_USER} db=${PG_DB}）"

# 单事务内 INSERT -> UPDATE -> DELETE，全程 ON_ERROR_STOP=1；
# DELETE 在 COMMIT 前完成，因此即使 COMMIT 也不残留 smoke 行。
psql_app -v sid="$SMOKE_ID" <<'SQL'
BEGIN;

-- model_registry
INSERT INTO model_registry (model_id, model_name, model_type, backend, status, tags_json)
VALUES (:'sid', 'smoke model', 'mock', 'mock', 'ACTIVE', '{"smoke":true}'::jsonb);
UPDATE model_registry SET status = 'INACTIVE', updated_at = now() WHERE model_id = :'sid';

-- inference_jobs
INSERT INTO inference_jobs (job_id, model_id, input_uri, output_uri, task_type, status)
VALUES (:'sid', :'sid', 'file:///tmp/smoke_in', 'file:///tmp/smoke_out', 'caption', 'CREATED');
UPDATE inference_jobs SET status = 'SUCCEEDED', processed_samples = 1, finished_at = now() WHERE job_id = :'sid';

-- inference_outputs
INSERT INTO inference_outputs (output_id, job_id, model_id, prediction_type, prediction_json, status)
VALUES (:'sid', :'sid', :'sid', 'caption', '{"text":"smoke"}'::jsonb, 'OK');
UPDATE inference_outputs SET confidence = 0.99 WHERE output_id = :'sid';

-- distillation_datasets
INSERT INTO distillation_datasets (distill_id, distill_format, output_uri, status)
VALUES (:'sid', 'caption_sft', 's3://robot-lake/distill/__smoke__', 'CREATED');
UPDATE distillation_datasets SET status = 'READY', num_train = 1, updated_at = now() WHERE distill_id = :'sid';

-- inference_benchmark_runs
INSERT INTO inference_benchmark_runs (benchmark_id, model_id, backend, workload_name, status, total_samples)
VALUES (:'sid', :'sid', 'mock', 'smoke', 'SUCCEEDED', 1);
UPDATE inference_benchmark_runs SET samples_per_sec = 1.0, error_rate = 0.0 WHERE benchmark_id = :'sid';

-- ai_task_events
INSERT INTO ai_task_events (event_id, event_type, job_id, model_id, payload_json)
VALUES (:'sid', 'SMOKE_TEST', :'sid', :'sid', '{"smoke":true}'::jsonb);

-- 读权限校验
SELECT count(*) AS model_registry_visible        FROM model_registry        WHERE model_id    = :'sid';
SELECT count(*) AS inference_jobs_visible         FROM inference_jobs         WHERE job_id      = :'sid';
SELECT count(*) AS inference_outputs_visible      FROM inference_outputs      WHERE output_id   = :'sid';
SELECT count(*) AS distillation_datasets_visible  FROM distillation_datasets  WHERE distill_id  = :'sid';
SELECT count(*) AS inference_benchmark_visible    FROM inference_benchmark_runs WHERE benchmark_id = :'sid';
SELECT count(*) AS ai_task_events_visible         FROM ai_task_events         WHERE event_id    = :'sid';

-- 清理（仍在事务内）
DELETE FROM inference_outputs       WHERE output_id    = :'sid';
DELETE FROM inference_jobs          WHERE job_id       = :'sid';
DELETE FROM model_registry          WHERE model_id     = :'sid';
DELETE FROM distillation_datasets   WHERE distill_id   = :'sid';
DELETE FROM inference_benchmark_runs WHERE benchmark_id = :'sid';
DELETE FROM ai_task_events          WHERE event_id     = :'sid';

COMMIT;
SQL

# 显式确认 6 张表都已无残留（兜底）。
remaining="$(psql_app -tA -v sid="$SMOKE_ID" <<'SQL'
SELECT
  (SELECT count(*) FROM model_registry        WHERE model_id    = :'sid') +
  (SELECT count(*) FROM inference_jobs         WHERE job_id      = :'sid') +
  (SELECT count(*) FROM inference_outputs      WHERE output_id   = :'sid') +
  (SELECT count(*) FROM distillation_datasets  WHERE distill_id  = :'sid') +
  (SELECT count(*) FROM inference_benchmark_runs WHERE benchmark_id = :'sid') +
  (SELECT count(*) FROM ai_task_events         WHERE event_id    = :'sid');
SQL
)"
remaining="${remaining//[[:space:]]/}"
[[ "$remaining" == "0" ]] || die "smoke 数据未清理干净，残留 ${remaining} 行（SMOKE_ID=${SMOKE_ID}）"

log "OK：6 张关键表 INSERT/UPDATE/DELETE 读写权限正常，smoke 数据已清理。"
