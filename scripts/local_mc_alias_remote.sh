#!/usr/bin/env bash
#
# scripts/local_mc_alias_remote.sh
#
# v1.7 Local-First Data Runtime - 为 mc 注册 robotdh-remote alias，
# 指向腾讯云 MinIO（与 robot-dh-platform secret 一致的 endpoint/AK/SK）。
#
# 必需环境变量（从 client/robot-dh-v1-6.env 加载）：
#   ROBOT_DH_S3_ENDPOINT_URL    e.g. https://minio.example.com:9000
#   ROBOT_DH_S3_ACCESS_KEY
#   ROBOT_DH_S3_SECRET_KEY
#
# 可选：
#   ROBOT_DH_S3_DATA_BUCKET     默认 robot-datasets，用于 list test
#   ROBOT_DH_MC_ALIAS_NAME      默认 robotdh-remote
#
# 用法:
#   source client/robot-dh-v1-6.env
#   ./scripts/local_mc_alias_remote.sh
#   ./scripts/local_mc_alias_remote.sh --dry-run

set -euo pipefail

ALIAS_NAME="${ROBOT_DH_MC_ALIAS_NAME:-robotdh-remote}"
DATA_BUCKET="${ROBOT_DH_S3_DATA_BUCKET:-robot-datasets}"
DRY_RUN="false"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --alias) ALIAS_NAME="$2"; shift 2 ;;
    --bucket) DATA_BUCKET="$2"; shift 2 ;;
    --dry-run) DRY_RUN="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

if ! command -v mc >/dev/null 2>&1; then
  echo "ERROR: 未找到 mc。请安装 MinIO Client：https://min.io/docs/minio/linux/reference/minio-mc.html" >&2
  exit 2
fi

REQUIRED=(ROBOT_DH_S3_ENDPOINT_URL ROBOT_DH_S3_ACCESS_KEY ROBOT_DH_S3_SECRET_KEY)
MISSING=()
for v in "${REQUIRED[@]}"; do
  if [[ -z "${!v:-}" ]]; then
    MISSING+=("$v")
  fi
done
if (( ${#MISSING[@]} > 0 )); then
  echo "ERROR: 缺少必要环境变量：${MISSING[*]}" >&2
  echo "Hint: source client/robot-dh-v1-6.env 后重试。" >&2
  exit 3
fi

# 注意：本脚本**不**打印 secret，所有 mc alias set 的 stderr 也 mask 掉。
ENDPOINT="${ROBOT_DH_S3_ENDPOINT_URL}"
AK_PREFIX="${ROBOT_DH_S3_ACCESS_KEY:0:4}***"

echo "将注册 mc alias：${ALIAS_NAME} -> ${ENDPOINT}  (access_key=${AK_PREFIX})"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "(dry-run) mc alias set ${ALIAS_NAME} <endpoint> <AK> <SK>"
  exit 0
fi

# mc alias set 在 stdout 上不会泄露 secret，但为稳妥起见用 >/dev/null。
mc alias set "${ALIAS_NAME}" "${ENDPOINT}" \
  "${ROBOT_DH_S3_ACCESS_KEY}" "${ROBOT_DH_S3_SECRET_KEY}" \
  --api "S3v4" >/dev/null

echo "alias 注册成功，连通性测试..."
# 用 --json 让输出整洁；任何 error 直接 fail（set -e）。
if ! mc ls --json "${ALIAS_NAME}/${DATA_BUCKET}" >/dev/null 2>&1; then
  echo "WARNING: 列 bucket ${DATA_BUCKET} 失败；可能 endpoint / AK/SK 不正确，或者 bucket 不存在。" >&2
  echo "请用以下命令手动排查（不要在公开终端贴 AK/SK）：" >&2
  echo "  mc ls ${ALIAS_NAME}/${DATA_BUCKET} --debug 2>&1 | head -n 20" >&2
  exit 4
fi

echo "OK：mc alias ${ALIAS_NAME} 可访问 ${DATA_BUCKET}。"
echo "下一步：./scripts/local_plan_devscale_sync.sh"
