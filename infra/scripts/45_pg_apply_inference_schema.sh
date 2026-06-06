#!/usr/bin/env bash
# robot-dh-infra：幂等应用 v1.9 推理数据平面 schema（007_inference_data_plane.sql）。
#
# 运行位置：腾讯云 Ubuntu 服务器 /opt/robot-dh-infra（PostgreSQL 跑在 docker 容器 robot-dh-postgres）。
# 用管理员账号执行 DDL；只做 CREATE TABLE/INDEX IF NOT EXISTS + GRANT，不 drop、不 truncate。
# 应用后列出 v1.9 新表。任何一步失败立即非 0 退出。
#
# 部署：把本仓库 infra/scripts/*.sh rsync 到 /opt/robot-dh-infra/scripts/ 后执行，
#       此时 scripts/ 与 postgres/ 同级，下面的 INFRA_ROOT 自动指向仓库根。
#
# 可覆盖环境变量：
#   ROBOT_DH_INFRA_ROOT     仓库根（默认 = 脚本所在目录的上一级）
#   ROBOT_DH_PG_CONTAINER   PG 容器名（默认 robot-dh-postgres）
#   ROBOT_DH_PG_ADMIN_USER  PG 管理员账号（默认 robot_dh_admin）
#   ROBOT_DH_PG_DB          数据库名（默认 robot_dh）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_ROOT="${ROBOT_DH_INFRA_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
MIGRATION_FILE="${INFRA_ROOT}/postgres/migrations/007_inference_data_plane.sql"

PG_CONTAINER="${ROBOT_DH_PG_CONTAINER:-robot-dh-postgres}"
PG_ADMIN_USER="${ROBOT_DH_PG_ADMIN_USER:-robot_dh_admin}"
PG_DB="${ROBOT_DH_PG_DB:-robot_dh}"

# v1.9 新表清单，用于应用后核对。
V1_9_TABLES=(
  model_registry
  inference_jobs
  inference_outputs
  inference_failures
  distillation_datasets
  inference_benchmark_runs
  ai_task_events
  dead_letter_tasks
  dws_inference_job_daily
  ads_inference_dashboard
)

log() { printf '[45_pg_apply_inference_schema] %s\n' "$*"; }
die() { printf '[45_pg_apply_inference_schema] ERROR: %s\n' "$*" >&2; exit 1; }

[[ -f "$MIGRATION_FILE" ]] || die "找不到 migration 文件：$MIGRATION_FILE"
command -v docker >/dev/null 2>&1 || die "docker 未安装"
docker inspect "$PG_CONTAINER" >/dev/null 2>&1 || die "PG 容器不存在：$PG_CONTAINER"

# 以管理员身份执行 psql；ON_ERROR_STOP=1 保证任何 SQL 错误都非 0 退出。
psql_admin() {
  docker exec -i "$PG_CONTAINER" \
    psql -v ON_ERROR_STOP=1 -X -q -U "$PG_ADMIN_USER" -d "$PG_DB" "$@"
}

log "应用 migration：$MIGRATION_FILE"
log "目标：container=$PG_CONTAINER db=$PG_DB admin=$PG_ADMIN_USER"

# 整个 007 文件自带 BEGIN/COMMIT，幂等可重复执行。
psql_admin -f - < "$MIGRATION_FILE"

log "应用完成，核对 v1.9 新表："

# 构造 IN (...) 列表用于 information_schema 查询。
in_list=""
for t in "${V1_9_TABLES[@]}"; do
  in_list+="'${t}',"
done
in_list="${in_list%,}"

psql_admin -P pager=off -c "
SELECT c.relname AS table_name,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid) AS index_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'r'
  AND c.relname IN (${in_list})
ORDER BY c.relname;
"

# 缺表则失败（防止部分建表）。
present="$(psql_admin -tA -c "
SELECT count(*) FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname='public' AND c.relkind='r' AND c.relname IN (${in_list});
")"
present="${present//[[:space:]]/}"
expected="${#V1_9_TABLES[@]}"
if [[ "$present" != "$expected" ]]; then
  die "v1.9 新表数量不符：期望 ${expected}，实际 ${present}"
fi

log "OK：v1.9 全部 ${expected} 张表就绪。"
