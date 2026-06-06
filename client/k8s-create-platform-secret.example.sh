#!/usr/bin/env bash
# robot-dh 平台 Secret 创建脚本示例（v1.9 AI Inference Data Plane Lite）。
#
# 推荐用法（pass-through 模式）：
#   1) 云端：./scripts/49_export_inference_client_env.sh --show-secrets
#   2) scp 到 WSL host，然后：
#        set -a; source client/robot-dh-platform.env; set +a
#        ./client/k8s-create-platform-secret.example.sh
#
# 兼容用法（component 模式）：
#   只设置 PUBLIC_HOST / ROBOT_DH_APP_PASSWORD / MINIO_APP_SECRET_KEY / REDIS_PASSWORD
#   时，脚本按组件拼接 URI。
#
# 不论哪种模式，apply 前会做硬校验：
#   - 任何 CHANGE_ME* / 占位符 / 空值都立即拒绝（OpenAI api_key 可选除外）
#   - 默认拒绝 127.0.0.1 / localhost；WSL host 单进程可加 --allow-localhost
#   - v1.6 三个前缀（QC / ML-ready / workflow tmp）+ v1.9 两个输出根（infer / distill）
#     必须以 s3:// 开头，且不能指向 raw / ods / dwd / ads / lineage
set -euo pipefail

NAMESPACE=${NAMESPACE:-robot-dh}
SECRET_NAME=${SECRET_NAME:-robot-dh-v1-6-secrets}
ALLOW_LOCALHOST=0
DRY_RUN=0

usage() {
  cat <<EOF >&2
Usage: $0 [--allow-localhost] [--dry-run]

推荐：先 set -a; source client/robot-dh-platform.env; set +a

Pass-through 期望的变量（49_export_inference_client_env.sh 已全部输出）：
  ROBOT_DH_PLATFORM_VERSION       1.9
  ROBOT_DH_DB_URI                 postgresql+psycopg://USER:PASS@HOST:5432/DB
  ROBOT_DH_S3_ENDPOINT_URL        http://HOST:9000
  ROBOT_DH_S3_ACCESS_KEY
  ROBOT_DH_S3_SECRET_KEY
  ROBOT_DH_S3_DATA_BUCKET
  ROBOT_DH_S3_ARTIFACT_BUCKET
  ROBOT_DH_S3_LAKE_BUCKET
  ROBOT_DH_REDIS_URL              redis://:PASS@HOST:6379/0
  ROBOT_DH_S3_REGION              可选，默认 us-east-1
  ROBOT_DH_QC_CONTRACT_BUCKET_PREFIX  s3://robot-lake/qc
  ROBOT_DH_ML_READY_ROOT              s3://robot-lake/ml-ready
  ROBOT_DH_WORKFLOW_TMP_PREFIX        s3://robot-lake/tmp/workflows
  ROBOT_DH_INFER_OUTPUT_ROOT          s3://robot-lake/infer
  ROBOT_DH_DISTILL_OUTPUT_ROOT        s3://robot-lake/distill
  ROBOT_DH_DEFAULT_INFER_BACKEND      mock
  ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL 可选，openai_compatible 后端才需要
  ROBOT_DH_OPENAI_COMPATIBLE_API_KEY  可选，openai_compatible 后端才需要

Component fallback（pass-through 缺值时使用）：
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

parse_host_from_uri() {
  local input="$1"
  local without_scheme="${input#*://}"
  local hostpart="${without_scheme##*@}"
  hostpart="${hostpart%%/*}"
  printf '%s\n' "${hostpart%%:*}"
}

# Component fallback 默认
ROBOT_DH_APP_USER=${ROBOT_DH_APP_USER:-robot_dh_app}
POSTGRES_DB=${POSTGRES_DB:-robot_dh}
DB_PORT=${DB_PORT:-5432}
MINIO_APP_ACCESS_KEY_FALLBACK=${MINIO_APP_ACCESS_KEY:-robotdhapp}
S3_PORT=${S3_PORT:-9000}
REDIS_PORT=${REDIS_PORT:-6379}
ROBOT_DH_DATA_BUCKET_FALLBACK=${ROBOT_DH_DATA_BUCKET:-robot-datasets}
ROBOT_DH_ARTIFACT_BUCKET_FALLBACK=${ROBOT_DH_ARTIFACT_BUCKET:-robot-dh-artifacts}
ROBOT_DH_LAKE_BUCKET_FALLBACK=${ROBOT_DH_LAKE_BUCKET:-robot-lake}

SECRET_PLATFORM=${ROBOT_DH_PLATFORM_VERSION:-1.9}
SECRET_DB_URI=${ROBOT_DH_DB_URI:-}
SECRET_S3_ENDPOINT=${ROBOT_DH_S3_ENDPOINT_URL:-}
SECRET_S3_REGION=${ROBOT_DH_S3_REGION:-us-east-1}
SECRET_S3_ACCESS=${ROBOT_DH_S3_ACCESS_KEY:-${MINIO_APP_ACCESS_KEY_FALLBACK}}
SECRET_S3_SECRET=${ROBOT_DH_S3_SECRET_KEY:-${MINIO_APP_SECRET_KEY:-}}
SECRET_S3_DATA_BUCKET=${ROBOT_DH_S3_DATA_BUCKET:-${ROBOT_DH_DATA_BUCKET_FALLBACK}}
SECRET_S3_ARTIFACT_BUCKET=${ROBOT_DH_S3_ARTIFACT_BUCKET:-${ROBOT_DH_ARTIFACT_BUCKET_FALLBACK}}
SECRET_S3_LAKE_BUCKET=${ROBOT_DH_S3_LAKE_BUCKET:-${ROBOT_DH_LAKE_BUCKET_FALLBACK}}
SECRET_REDIS_URL=${ROBOT_DH_REDIS_URL:-}
SECRET_QC_PREFIX=${ROBOT_DH_QC_CONTRACT_BUCKET_PREFIX:-s3://${SECRET_S3_LAKE_BUCKET}/qc}
SECRET_ML_READY=${ROBOT_DH_ML_READY_ROOT:-s3://${SECRET_S3_LAKE_BUCKET}/ml-ready}
SECRET_WORKFLOW_TMP=${ROBOT_DH_WORKFLOW_TMP_PREFIX:-s3://${SECRET_S3_LAKE_BUCKET}/tmp/workflows}

# v1.9 推理数据平面变量：输出根 / 默认后端必填；OpenAI-compatible 两项可选（可空）。
SECRET_INFER_OUTPUT_ROOT=${ROBOT_DH_INFER_OUTPUT_ROOT:-s3://${SECRET_S3_LAKE_BUCKET}/infer}
SECRET_DISTILL_OUTPUT_ROOT=${ROBOT_DH_DISTILL_OUTPUT_ROOT:-s3://${SECRET_S3_LAKE_BUCKET}/distill}
SECRET_DEFAULT_INFER_BACKEND=${ROBOT_DH_DEFAULT_INFER_BACKEND:-mock}
SECRET_OPENAI_BASE_URL=${ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL:-}
SECRET_OPENAI_API_KEY=${ROBOT_DH_OPENAI_COMPATIBLE_API_KEY:-}

# Component fallback
if [[ -z "$SECRET_DB_URI" || -z "$SECRET_S3_ENDPOINT" || -z "$SECRET_REDIS_URL" ]]; then
  PUBLIC_HOST=${PUBLIC_HOST:-}
  if [[ -z "$PUBLIC_HOST" ]]; then
    echo "ERROR: 既未 source ROBOT_DH_* 完整 env，也未设置 PUBLIC_HOST。" >&2
    echo "       推荐做法：set -a; source client/robot-dh-platform.env; set +a" >&2
    exit 1
  fi
  ROBOT_DH_APP_PASSWORD=${ROBOT_DH_APP_PASSWORD:-}
  REDIS_PASSWORD=${REDIS_PASSWORD:-}
  [[ -z "$SECRET_DB_URI"     ]] && SECRET_DB_URI="postgresql+psycopg://${ROBOT_DH_APP_USER}:${ROBOT_DH_APP_PASSWORD}@${PUBLIC_HOST}:${DB_PORT}/${POSTGRES_DB}"
  [[ -z "$SECRET_S3_ENDPOINT" ]] && SECRET_S3_ENDPOINT="http://${PUBLIC_HOST}:${S3_PORT}"
  [[ -z "$SECRET_S3_SECRET"  ]] && SECRET_S3_SECRET=${MINIO_APP_SECRET_KEY:-}
  [[ -z "$SECRET_REDIS_URL"  ]] && SECRET_REDIS_URL="redis://:${REDIS_PASSWORD}@${PUBLIC_HOST}:${REDIS_PORT}/0"
fi

DB_HOST=$(parse_host_from_uri "$SECRET_DB_URI")
S3_HOST=$(parse_host_from_uri "$SECRET_S3_ENDPOINT")
REDIS_HOST=$(parse_host_from_uri "$SECRET_REDIS_URL")

if [[ "$DB_HOST" != "$S3_HOST" || "$DB_HOST" != "$REDIS_HOST" ]]; then
  echo "WARN: DB/S3/Redis host 不一致（DB=$DB_HOST S3=$S3_HOST Redis=$REDIS_HOST）。" >&2
  echo "      如果是有意的（多机部署），可忽略；否则请重新生成 env。" >&2
fi

# 占位符 / CHANGE_ME / 空值校验
declare -A FIELDS=(
  [ROBOT_DH_PLATFORM_VERSION]="$SECRET_PLATFORM"
  [ROBOT_DH_DB_URI]="$SECRET_DB_URI"
  [ROBOT_DH_S3_ENDPOINT_URL]="$SECRET_S3_ENDPOINT"
  [ROBOT_DH_S3_REGION]="$SECRET_S3_REGION"
  [ROBOT_DH_S3_ACCESS_KEY]="$SECRET_S3_ACCESS"
  [ROBOT_DH_S3_SECRET_KEY]="$SECRET_S3_SECRET"
  [ROBOT_DH_S3_DATA_BUCKET]="$SECRET_S3_DATA_BUCKET"
  [ROBOT_DH_S3_ARTIFACT_BUCKET]="$SECRET_S3_ARTIFACT_BUCKET"
  [ROBOT_DH_S3_LAKE_BUCKET]="$SECRET_S3_LAKE_BUCKET"
  [ROBOT_DH_REDIS_URL]="$SECRET_REDIS_URL"
  [ROBOT_DH_QC_CONTRACT_BUCKET_PREFIX]="$SECRET_QC_PREFIX"
  [ROBOT_DH_ML_READY_ROOT]="$SECRET_ML_READY"
  [ROBOT_DH_WORKFLOW_TMP_PREFIX]="$SECRET_WORKFLOW_TMP"
  [ROBOT_DH_INFER_OUTPUT_ROOT]="$SECRET_INFER_OUTPUT_ROOT"
  [ROBOT_DH_DISTILL_OUTPUT_ROOT]="$SECRET_DISTILL_OUTPUT_ROOT"
  [ROBOT_DH_DEFAULT_INFER_BACKEND]="$SECRET_DEFAULT_INFER_BACKEND"
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
  echo "Hint: 在云端执行 ./scripts/49_export_inference_client_env.sh --show-secrets" >&2
  echo "      再 scp 到 WSL，然后 set -a; source client/robot-dh-platform.env; set +a" >&2
  exit 1
fi

# v1.6 三个前缀 + v1.9 两个输出根必须 s3:// 开头，且不能指向受保护层
for var in SECRET_QC_PREFIX SECRET_ML_READY SECRET_WORKFLOW_TMP \
           SECRET_INFER_OUTPUT_ROOT SECRET_DISTILL_OUTPUT_ROOT; do
  val="${!var}"
  if [[ "$val" != s3://* ]]; then
    echo "ERROR: $var 必须以 s3:// 开头（当前=$val）" >&2
    exit 1
  fi
  case "$val" in
    *://*/raw/*|*://*/ods/*|*://*/dwd/*|*://*/ads/*|*://*/lineage/*|*://*/manifests/*)
      echo "ERROR: $var 指向受保护数据层 ($val)，禁止；只允许 qc / ml-ready / tmp / infer / distill" >&2
      exit 1
      ;;
  esac
done

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
  echo "       kubectl apply -f client/k8s-platform-secret.example.yaml" >&2
  exit 1
fi

kubectl_apply_cmd=(
  kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME"
  --from-literal=ROBOT_DH_PLATFORM_VERSION="$SECRET_PLATFORM"
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
  --from-literal=ROBOT_DH_QC_CONTRACT_BUCKET_PREFIX="$SECRET_QC_PREFIX"
  --from-literal=ROBOT_DH_ML_READY_ROOT="$SECRET_ML_READY"
  --from-literal=ROBOT_DH_WORKFLOW_TMP_PREFIX="$SECRET_WORKFLOW_TMP"
  --from-literal=ROBOT_DH_INFER_OUTPUT_ROOT="$SECRET_INFER_OUTPUT_ROOT"
  --from-literal=ROBOT_DH_DISTILL_OUTPUT_ROOT="$SECRET_DISTILL_OUTPUT_ROOT"
  --from-literal=ROBOT_DH_DEFAULT_INFER_BACKEND="$SECRET_DEFAULT_INFER_BACKEND"
  --from-literal=ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL="$SECRET_OPENAI_BASE_URL"
  --from-literal=ROBOT_DH_OPENAI_COMPATIBLE_API_KEY="$SECRET_OPENAI_API_KEY"
  --dry-run=client
  -o yaml
)

if [[ $DRY_RUN -eq 1 ]]; then
  "${kubectl_apply_cmd[@]}" | awk '/^  [A-Z_]+:/ {print $1}'
  echo "Dry-run OK. DB host=$DB_HOST  S3 host=$S3_HOST  Redis host=$REDIS_HOST"
  exit 0
fi

"${kubectl_apply_cmd[@]}" | kubectl apply -f -

echo "Applied secret $SECRET_NAME in namespace $NAMESPACE."
echo "DB host=$DB_HOST  S3 host=$S3_HOST  Redis host=$REDIS_HOST"
echo "Platform version=$SECRET_PLATFORM  QC prefix=$SECRET_QC_PREFIX  ML-ready=$SECRET_ML_READY"
echo "Infer root=$SECRET_INFER_OUTPUT_ROOT  Distill root=$SECRET_DISTILL_OUTPUT_ROOT  default backend=$SECRET_DEFAULT_INFER_BACKEND"
echo "（其他凭据 / OpenAI api_key 已脱敏，不会打印）"
