#!/usr/bin/env bash
# robot-dh-infra：v1.9 蒸馏数据集统计报告。
#
# 查询 distillation_datasets，输出 Markdown 与 JSON 报告到 /data/robot-dh/logs/。
# 表为空时输出零值报告，不失败。
#
# 产物：
#   /data/robot-dh/logs/v1_9_distill_datasets_YYYYmmdd_HHMMSS.md
#   /data/robot-dh/logs/v1_9_distill_datasets_YYYYmmdd_HHMMSS.json
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
MD_FILE="${LOG_DIR}/v1_9_distill_datasets_${TS}.md"
JSON_FILE="${LOG_DIR}/v1_9_distill_datasets_${TS}.json"

log() { printf '[48_distill_dataset_report] %s\n' "$*"; }
die() { printf '[48_distill_dataset_report] ERROR: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || die "docker 未安装"
docker inspect "$PG_CONTAINER" >/dev/null 2>&1 || die "PG 容器不存在：$PG_CONTAINER"
mkdir -p "$LOG_DIR"

psql_scalar() {
  docker exec -i "$PG_CONTAINER" \
    psql -v ON_ERROR_STOP=1 -X -q -tA -U "$PG_APP_USER" -d "$PG_DB" -c "$1"
}
psql_table() {
  docker exec -i "$PG_CONTAINER" \
    psql -v ON_ERROR_STOP=1 -X -q -P pager=off -U "$PG_APP_USER" -d "$PG_DB" -c "$1"
}

log "生成蒸馏数据集报告（db=${PG_DB}）..."

# ---------- JSON ----------
json_payload="$(psql_scalar "
SELECT json_build_object(
  'generated_at', '${TS_HUMAN}',
  'database', '${PG_DB}',
  'total', (SELECT count(*) FROM distillation_datasets),
  'by_status', (SELECT coalesce(json_object_agg(status, c), '{}'::json)
                FROM (SELECT status, count(*) c FROM distillation_datasets GROUP BY status) s),
  'by_distill_format', (SELECT coalesce(json_object_agg(distill_format, c), '{}'::json)
                        FROM (SELECT distill_format, count(*) c FROM distillation_datasets GROUP BY distill_format) s),
  'sample_totals', json_build_object(
    'num_train', (SELECT coalesce(sum(num_train), 0) FROM distillation_datasets),
    'num_val',   (SELECT coalesce(sum(num_val), 0)   FROM distillation_datasets),
    'num_test',  (SELECT coalesce(sum(num_test), 0)  FROM distillation_datasets)
  ),
  'by_teacher_model', (SELECT coalesce(json_object_agg(coalesce(teacher_model_id, '(null)'), c), '{}'::json)
                       FROM (SELECT teacher_model_id, count(*) c FROM distillation_datasets GROUP BY teacher_model_id) s),
  'latest', (SELECT coalesce(json_agg(row_to_json(d)), '[]'::json)
             FROM (SELECT distill_id, dataset_id, version, teacher_model_id, distill_format,
                          status, num_train, num_val, num_test, output_uri, created_at
                   FROM distillation_datasets
                   ORDER BY created_at DESC NULLS LAST LIMIT 10) d)
);
")"
printf '%s\n' "$json_payload" > "$JSON_FILE"

# ---------- Markdown ----------
total="$(psql_scalar "SELECT count(*) FROM distillation_datasets;")"

{
  echo "# v1.9 蒸馏数据集报告"
  echo
  echo "- 生成时间（UTC）：${TS_HUMAN}"
  echo "- 数据库：${PG_DB}"
  echo "- 蒸馏数据集总数：${total}"
  echo
  echo "## 按状态"
  echo
  echo '```'
  psql_table "
SELECT status, count(*) AS datasets,
       coalesce(sum(num_train),0) AS num_train,
       coalesce(sum(num_val),0)   AS num_val,
       coalesce(sum(num_test),0)  AS num_test
FROM distillation_datasets GROUP BY status ORDER BY status;"
  echo '```'
  echo
  echo "## 按蒸馏格式（distill_format）"
  echo
  echo '```'
  psql_table "
SELECT distill_format, count(*) AS datasets,
       coalesce(sum(num_train),0) AS num_train
FROM distillation_datasets GROUP BY distill_format ORDER BY datasets DESC;"
  echo '```'
  echo
  echo "## 按 teacher 模型"
  echo
  echo '```'
  psql_table "
SELECT coalesce(teacher_model_id,'(null)') AS teacher_model_id, count(*) AS datasets
FROM distillation_datasets GROUP BY teacher_model_id ORDER BY datasets DESC;"
  echo '```'
  echo
  echo "## 最近 10 个蒸馏数据集"
  echo
  echo '```'
  psql_table "
SELECT distill_id, coalesce(dataset_id,'(null)') AS dataset_id,
       coalesce(version,'(null)') AS version,
       coalesce(teacher_model_id,'(null)') AS teacher_model_id,
       distill_format, status,
       num_train, num_val, num_test, created_at
FROM distillation_datasets
ORDER BY created_at DESC NULLS LAST LIMIT 10;"
  echo '```'
} > "$MD_FILE"

log "OK：报告已生成"
log "  Markdown：${MD_FILE}"
log "  JSON：    ${JSON_FILE}"
