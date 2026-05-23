#!/usr/bin/env bash
# robot-dh v1.5 Argo Workflows Secret 创建脚本示例。
#
# 推荐用法（pass-through 模式）：
#   1) 在云端执行 ./scripts/31_argowf_remote_env_export.sh --show-secrets
#      生成 client/robot-dh-v1-5.env（chmod 600）
#   2) scp 到 WSL 后，在 WSL host：
#        set -a; source client/robot-dh-v1-5.env; set +a
#        ./client/k8s-create-argo-secret.example.sh
#      脚本会直接把已 source 的 ROBOT_DH_* 完整 URL/URI 写入 Secret。
#
# 兼容用法（component 模式）：
#   如果没有 source 完整 env，只设置了 PUBLIC_HOST / ROBOT_DH_APP_PASSWORD /
#   MINIO_APP_SECRET_KEY / REDIS_PASSWORD，脚本会按组件拼接 URI（fallback）。
#
# 不论哪种模式，apply 前会做硬校验：任何 CHANGE_ME* / 占位符 / 空值都会立即拒绝，
# 避免把垃圾 Secret 推到集群导致 Pod 启动后才报 EndpointConnectionError。
#
# 默认拒绝 127.0.0.1 / localhost / 占位符；WSL host 单进程测试可加 --allow-localhost。
set -euo pipefail

NAMESPACE=${NAMESPACE:-robot-dh}
SECRET_NAME=${SECRET_NAME:-robot-dh-v1-5-secrets}
ALLOW_LOCALHOST=0
DRY_RUN=0

usage() {
  cat <<EOF >&2
Usage: $0 [--allow-localhost] [--dry-run]

推荐：先 set -a; source client/robot-dh-v1-5.env; set +a

Pass-through 期望的变量（31_argowf_remote_env_export.sh 已全部输出）：
  ROBOT_DH_DB_URI               postgresql+psycopg://USER:PASS@HOST:5432/DB
  ROBOT_DH_S3_ENDPOINT_URL      http://HOST:9000
  ROBOT_DH_S3_ACCESS_KEY
  ROBOT_DH_S3_SECRET_KEY
  ROBOT_DH_S3_DATA_BUCKET
  ROBOT_DH_S3_ARTIFACT_BUCKET
  ROBOT_DH_S3_LAKE_BUCKET
  ROBOT_DH_REDIS_URL            redis://:PASS@HOST:6379/0
  ROBOT_DH_S3_REGION            可选，默认 us-east-1

Component fallback（只在 pass-through 缺值时使用）：
  PUBLIC_HOST            公网 IP 或 DNS
  ROBOT_DH_APP_PASSWORD  PostgreSQL 应用账号密码
  MINIO_APP_SECRET_KEY   MinIO 应用 secret
  REDIS_PASSWORD         Redis 密码
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-localhost) ALLOW_LOCALHOST=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown arg: $1" >&2; usage; exit 1 ;;
  esac
  shift
done

# 解析 host:port，用于占位符 / localhost 校验和最后的诊断输出
parse_host_from_uri() {
  # 输入形如 scheme://user:pass@host:port/path 或 scheme://host:port/path
  local input="$1"
  local without_scheme="${input#*://}"
  local hostpart="${without_scheme##*@}"
  hostpart="${hostpart%%/*}"
  printf '%s\n' "${hostpart%%:*}"
}

# Component fallback 默认值（仅在 pass-through 缺值时用到）
ROBOT_DH_APP_USER=${ROBOT_DH_APP_USER:-robot_dh_app}
POSTGRES_DB=${POSTGRES_DB:-robot_dh}
DB_PORT=${DB_PORT:-5432}
MINIO_APP_ACCESS_KEY_FALLBACK=${MINIO_APP_ACCESS_KEY:-robotdhapp}
S3_PORT=${S3_PORT:-9000}
REDIS_PORT=${REDIS_PORT:-6379}
ROBOT_DH_DATA_BUCKET_FALLBACK=${ROBOT_DH_DATA_BUCKET:-robot-datasets}
ROBOT_DH_ARTIFACT_BUCKET_FALLBACK=${ROBOT_DH_ARTIFACT_BUCKET:-robot-dh-artifacts}
ROBOT_DH_LAKE_BUCKET_FALLBACK=${ROBOT_DH_LAKE_BUCKET:-robot-lake}

# 注意：env 文件里通常是 ROBOT_DH_S3_DATA_BUCKET（带 S3_ 前缀），
# 但旧版 .env 用 ROBOT_DH_DATA_BUCKET。两个都兜底。
SECRET_DB_URI=${ROBOT_DH_DB_URI:-}
SECRET_S3_ENDPOINT=${ROBOT_DH_S3_ENDPOINT_URL:-}
SECRET_S3_REGION=${ROBOT_DH_S3_REGION:-us-east-1}
SECRET_S3_ACCESS=${ROBOT_DH_S3_ACCESS_KEY:-${MINIO_APP_ACCESS_KEY_FALLBACK}}
SECRET_S3_SECRET=${ROBOT_DH_S3_SECRET_KEY:-${MINIO_APP_SECRET_KEY:-}}
SECRET_S3_DATA_BUCKET=${ROBOT_DH_S3_DATA_BUCKET:-${ROBOT_DH_DATA_BUCKET_FALLBACK}}
SECRET_S3_ARTIFACT_BUCKET=${ROBOT_DH_S3_ARTIFACT_BUCKET:-${ROBOT_DH_ARTIFACT_BUCKET_FALLBACK}}
SECRET_S3_LAKE_BUCKET=${ROBOT_DH_S3_LAKE_BUCKET:-${ROBOT_DH_LAKE_BUCKET_FALLBACK}}
SECRET_REDIS_URL=${ROBOT_DH_REDIS_URL:-}

# 走 component fallback：仅当 ROBOT_DH_DB_URI / S3 endpoint / REDIS URL 任一缺失
if [[ -z "$SECRET_DB_URI" || -z "$SECRET_S3_ENDPOINT" || -z "$SECRET_REDIS_URL" ]]; then
  PUBLIC_HOST=${PUBLIC_HOST:-}
  if [[ -z "$PUBLIC_HOST" ]]; then
    echo "ERROR: 既未 source ROBOT_DH_* 完整 env，也未设置 PUBLIC_HOST。" >&2
    echo "       推荐做法：set -a; source client/robot-dh-v1-5.env; set +a" >&2
    exit 1
  fi
  ROBOT_DH_APP_PASSWORD=${ROBOT_DH_APP_PASSWORD:-}
  REDIS_PASSWORD=${REDIS_PASSWORD:-}
  [[ -z "$SECRET_DB_URI"     ]] && SECRET_DB_URI="postgresql+psycopg://${ROBOT_DH_APP_USER}:${ROBOT_DH_APP_PASSWORD}@${PUBLIC_HOST}:${DB_PORT}/${POSTGRES_DB}"
  [[ -z "$SECRET_S3_ENDPOINT" ]] && SECRET_S3_ENDPOINT="http://${PUBLIC_HOST}:${S3_PORT}"
  [[ -z "$SECRET_S3_SECRET"  ]] && SECRET_S3_SECRET=${MINIO_APP_SECRET_KEY:-}
  [[ -z "$SECRET_REDIS_URL"  ]] && SECRET_REDIS_URL="redis://:${REDIS_PASSWORD}@${PUBLIC_HOST}:${REDIS_PORT}/0"
fi

# 从最终 URL 反推 PUBLIC_HOST，用于 localhost / 占位符校验
DB_HOST=$(parse_host_from_uri "$SECRET_DB_URI")
S3_HOST=$(parse_host_from_uri "$SECRET_S3_ENDPOINT")
REDIS_HOST=$(parse_host_from_uri "$SECRET_REDIS_URL")

# 三个 host 必须一致；不一致会让 ufw / 安全组排查极难
if [[ "$DB_HOST" != "$S3_HOST" || "$DB_HOST" != "$REDIS_HOST" ]]; then
  echo "WARN: DB/S3/Redis host 不一致（DB=$DB_HOST S3=$S3_HOST Redis=$REDIS_HOST）。" >&2
  echo "      如果是有意的（多机部署），可忽略；否则请重新生成 env。" >&2
fi

# 不允许占位符 / 空 / CHANGE_ME，逐字段校验
declare -A FIELDS=(
  [ROBOT_DH_DB_URI]="$SECRET_DB_URI"
  [ROBOT_DH_S3_ENDPOINT_URL]="$SECRET_S3_ENDPOINT"
  [ROBOT_DH_S3_REGION]="$SECRET_S3_REGION"
  [ROBOT_DH_S3_ACCESS_KEY]="$SECRET_S3_ACCESS"
  [ROBOT_DH_S3_SECRET_KEY]="$SECRET_S3_SECRET"
  [ROBOT_DH_S3_DATA_BUCKET]="$SECRET_S3_DATA_BUCKET"
  [ROBOT_DH_S3_ARTIFACT_BUCKET]="$SECRET_S3_ARTIFACT_BUCKET"
  [ROBOT_DH_S3_LAKE_BUCKET]="$SECRET_S3_LAKE_BUCKET"
  [ROBOT_DH_REDIS_URL]="$SECRET_REDIS_URL"
)
bad=0
for key in "${!FIELDS[@]}"; do
  value="${FIELDS[$key]}"
  if [[ -z "$value" ]]; then
    echo "ERROR: $key 为空。" >&2
    bad=1
    continue
  fi
  if [[ "$value" == *PUBLIC_SERVER_IP_OR_DNS* || "$value" == *CHANGE_ME* ]]; then
    echo "ERROR: $key 仍然包含占位符（值已脱敏，请重新 source 真实 env）。" >&2
    bad=1
  fi
done
if [[ $bad -ne 0 ]]; then
  echo "Hint: 在云端执行 ./scripts/31_argowf_remote_env_export.sh --show-secrets" >&2
  echo "      再 scp 到 WSL，然后 set -a; source client/robot-dh-v1-5.env; set +a" >&2
  exit 1
fi

# Host 层面拒绝 localhost / 占位符（最常见的两类垃圾值）
if [[ $ALLOW_LOCALHOST -ne 1 ]]; then
  case "$DB_HOST" in
    127.0.0.1|localhost|::1|PUBLIC_SERVER_IP_OR_DNS)
      echo "ERROR: DB host=$DB_HOST 不能在 kind / Argo Pod 中工作。" >&2
      echo "       请改用云端公网 IP/DNS；仅 WSL host 单进程测试可加 --allow-localhost。" >&2
      exit 1
      ;;
  esac
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "ERROR: namespace $NAMESPACE 不存在。请先：" >&2
  echo "       kubectl create namespace $NAMESPACE" >&2
  echo "       或者：kubectl apply -f client/k8s-argo-secret.example.yaml" >&2
  exit 1
fi

# 拼好后交给 kubectl apply；不打印任何 secret 明文
kubectl_apply_cmd=(
  kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME"
  --from-literal=ROBOT_DH_DB_URI="$SECRET_DB_URI"
  --from-literal=ROBOT_DH_ARTIFACT_STORE='s3'
  --from-literal=ROBOT_DH_S3_ENDPOINT_URL="$SECRET_S3_ENDPOINT"
  --from-literal=ROBOT_DH_S3_REGION="$SECRET_S3_REGION"
  --from-literal=ROBOT_DH_S3_ACCESS_KEY="$SECRET_S3_ACCESS"
  --from-literal=ROBOT_DH_S3_SECRET_KEY="$SECRET_S3_SECRET"
  --from-literal=ROBOT_DH_S3_DATA_BUCKET="$SECRET_S3_DATA_BUCKET"
  --from-literal=ROBOT_DH_S3_ARTIFACT_BUCKET="$SECRET_S3_ARTIFACT_BUCKET"
  --from-literal=ROBOT_DH_S3_LAKE_BUCKET="$SECRET_S3_LAKE_BUCKET"
  --from-literal=ROBOT_DH_REDIS_URL="$SECRET_REDIS_URL"
  --dry-run=client
  -o yaml
)

if [[ $DRY_RUN -eq 1 ]]; then
  # 只校验最终输出包含哪些 key，不泄漏 value
  "${kubectl_apply_cmd[@]}" | awk '/^  [A-Z_]+:/ {print $1}'
  echo "Dry-run OK. DB host=$DB_HOST  S3 host=$S3_HOST  Redis host=$REDIS_HOST"
  exit 0
fi

"${kubectl_apply_cmd[@]}" | kubectl apply -f -

echo "Applied secret $SECRET_NAME in namespace $NAMESPACE."
echo "DB host=$DB_HOST  S3 host=$S3_HOST  Redis host=$REDIS_HOST"
echo "（其他凭据已脱敏，不会打印）"
