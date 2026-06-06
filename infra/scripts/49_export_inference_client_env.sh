#!/usr/bin/env bash
# robot-dh-infra：导出 v1.9 客户端 env（含推理数据平面变量）。
#
# 默认脱敏：生成 client/robot-dh-platform.env.example（占位符，可提交 git）。
# 传 --show-secrets：从已 source 的真实环境变量生成 client/robot-dh-platform.env（chmod 600，禁止提交）。
# 两种模式都不会把密码 / API key 打印到 stdout。
#
# v1.9 在 v1.6 平台 env 基础上新增：
#   ROBOT_DH_PLATFORM_VERSION=1.9
#   ROBOT_DH_INFER_OUTPUT_ROOT=s3://robot-lake/infer
#   ROBOT_DH_DISTILL_OUTPUT_ROOT=s3://robot-lake/distill
#   ROBOT_DH_DEFAULT_INFER_BACKEND=mock
#   ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL=
#   ROBOT_DH_OPENAI_COMPATIBLE_API_KEY=
#
# 配置文件名保持版本无关（robot-dh-platform.env[.example]）：版本号只体现在
# ROBOT_DH_PLATFORM_VERSION 取值与注释里，文件名不随版本变化。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_ROOT="${ROBOT_DH_INFRA_ROOT:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
CLIENT_DIR="${INFRA_ROOT}/client"
OUT_EXAMPLE="${CLIENT_DIR}/robot-dh-platform.env.example"
OUT_REAL="${CLIENT_DIR}/robot-dh-platform.env"

SHOW_SECRETS=0

log() { printf '[49_export_inference_client_env] %s\n' "$*"; }
die() { printf '[49_export_inference_client_env] ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<EOF >&2
Usage: $0 [--show-secrets]

  默认（脱敏）：写 ${OUT_EXAMPLE}
  --show-secrets：写 ${OUT_REAL}（chmod 600），需先 source 真实凭据，例如：
      set -a; source /opt/robot-dh-infra/secrets/robot-dh.runtime.env; set +a
      $0 --show-secrets
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --show-secrets) SHOW_SECRETS=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "未知参数：$1" ;;
  esac
  shift
done

mkdir -p "$CLIENT_DIR"

# bucket 默认值（允许环境变量覆盖）。
DATA_BUCKET="${ROBOT_DH_S3_DATA_BUCKET:-robot-datasets}"
ARTIFACT_BUCKET="${ROBOT_DH_S3_ARTIFACT_BUCKET:-robot-dh-artifacts}"
BACKUP_BUCKET="${ROBOT_DH_S3_BACKUP_BUCKET:-robot-dh-backups}"
LAKE_BUCKET="${ROBOT_DH_S3_LAKE_BUCKET:-robot-lake}"
ARGO_NAMESPACE="${ROBOT_DH_ARGO_NAMESPACE:-robot-dh}"

# v1.6 三个前缀（沿用既有约定）。
QC_PREFIX="${ROBOT_DH_QC_CONTRACT_BUCKET_PREFIX:-s3://${LAKE_BUCKET}/qc}"
ML_READY_ROOT="${ROBOT_DH_ML_READY_ROOT:-s3://${LAKE_BUCKET}/ml-ready}"
WORKFLOW_TMP="${ROBOT_DH_WORKFLOW_TMP_PREFIX:-s3://${LAKE_BUCKET}/tmp/workflows}"

# v1.9 推理数据平面变量（默认非敏感，可直接进 .example）。
INFER_OUTPUT_ROOT="${ROBOT_DH_INFER_OUTPUT_ROOT:-s3://${LAKE_BUCKET}/infer}"
DISTILL_OUTPUT_ROOT="${ROBOT_DH_DISTILL_OUTPUT_ROOT:-s3://${LAKE_BUCKET}/distill}"
DEFAULT_INFER_BACKEND="${ROBOT_DH_DEFAULT_INFER_BACKEND:-mock}"
OPENAI_BASE_URL="${ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL:-}"
OPENAI_API_KEY="${ROBOT_DH_OPENAI_COMPATIBLE_API_KEY:-}"

if [[ $SHOW_SECRETS -eq 1 ]]; then
  DB_URI="${ROBOT_DH_DB_URI:-}"
  S3_ENDPOINT="${ROBOT_DH_S3_ENDPOINT_URL:-}"
  S3_REGION="${ROBOT_DH_S3_REGION:-us-east-1}"
  S3_ACCESS="${ROBOT_DH_S3_ACCESS_KEY:-robotdhapp}"
  S3_SECRET="${ROBOT_DH_S3_SECRET_KEY:-}"
  REDIS_URL="${ROBOT_DH_REDIS_URL:-}"

  # 真实模式：必填密钥不能为空、不能仍是占位符。
  for pair in \
    "ROBOT_DH_DB_URI=${DB_URI}" \
    "ROBOT_DH_S3_ENDPOINT_URL=${S3_ENDPOINT}" \
    "ROBOT_DH_S3_SECRET_KEY=${S3_SECRET}" \
    "ROBOT_DH_REDIS_URL=${REDIS_URL}"; do
    k="${pair%%=*}"; v="${pair#*=}"
    [[ -n "$v" ]] || die "${k} 为空：--show-secrets 需要先 source 真实凭据"
    case "$v" in
      *CHANGE_ME*|*PUBLIC_SERVER_IP_OR_DNS*)
        die "${k} 仍是占位符，请 source 真实凭据后重试" ;;
    esac
  done
  OUT_FILE="$OUT_REAL"
else
  # 脱敏模式：强制占位符，绝不读取真实密钥。
  DB_URI="postgresql+psycopg://robot_dh_app:CHANGE_ME_APP_PASSWORD@PUBLIC_SERVER_IP_OR_DNS:5432/robot_dh"
  S3_ENDPOINT="http://PUBLIC_SERVER_IP_OR_DNS:9000"
  S3_REGION="us-east-1"
  S3_ACCESS="robotdhapp"
  S3_SECRET="CHANGE_ME_MINIO_APP_SECRET"
  REDIS_URL="redis://:CHANGE_ME_REDIS_PASSWORD@PUBLIC_SERVER_IP_OR_DNS:6379/0"
  OPENAI_API_KEY=""
  OUT_FILE="$OUT_EXAMPLE"
fi

# 写文件（heredoc 内变量按上面解析结果展开）。
cat > "$OUT_FILE" <<EOF
# robot-dh 平台 client env（v1.9 AI Inference Data Plane Lite）。
#
# 用法：
#   - .example 文件只放占位符，可提交到 git。
#   - 真实凭据由 ./scripts/49_export_inference_client_env.sh --show-secrets
#     生成到 client/robot-dh-platform.env（chmod 600，**不要**提交到 git）。
#   - kind / Argo Pod 不能用 WSL 127.0.0.1 SSH tunnel，必须使用云端公网 IP/DNS。

# 版本 / namespace 标识
ROBOT_DH_PLATFORM_VERSION=1.9
ROBOT_DH_RELEASE_VERSION=v1.9
ROBOT_DH_ARGO_NAMESPACE=${ARGO_NAMESPACE}

# PostgreSQL：应用账号，覆盖 v1.3~v1.9 全部业务表
ROBOT_DH_DB_URI=${DB_URI}

# MinIO：S3 兼容协议
ROBOT_DH_ARTIFACT_STORE=s3
ROBOT_DH_S3_ENDPOINT_URL=${S3_ENDPOINT}
ROBOT_DH_S3_REGION=${S3_REGION}
ROBOT_DH_S3_ACCESS_KEY=${S3_ACCESS}
ROBOT_DH_S3_SECRET_KEY=${S3_SECRET}

# bucket
ROBOT_DH_S3_DATA_BUCKET=${DATA_BUCKET}
ROBOT_DH_S3_ARTIFACT_BUCKET=${ARTIFACT_BUCKET}
ROBOT_DH_S3_BACKUP_BUCKET=${BACKUP_BUCKET}
ROBOT_DH_S3_LAKE_BUCKET=${LAKE_BUCKET}

# Redis：事件总线 / 任务队列
ROBOT_DH_REDIS_URL=${REDIS_URL}

# v1.6：QC contract / ML-ready / workflow tmp 根前缀
ROBOT_DH_QC_CONTRACT_BUCKET_PREFIX=${QC_PREFIX}
ROBOT_DH_ML_READY_ROOT=${ML_READY_ROOT}
ROBOT_DH_WORKFLOW_TMP_PREFIX=${WORKFLOW_TMP}

# v1.9：推理数据平面（inference data plane）
ROBOT_DH_INFER_OUTPUT_ROOT=${INFER_OUTPUT_ROOT}
ROBOT_DH_DISTILL_OUTPUT_ROOT=${DISTILL_OUTPUT_ROOT}
ROBOT_DH_DEFAULT_INFER_BACKEND=${DEFAULT_INFER_BACKEND}
ROBOT_DH_OPENAI_COMPATIBLE_BASE_URL=${OPENAI_BASE_URL}
ROBOT_DH_OPENAI_COMPATIBLE_API_KEY=${OPENAI_API_KEY}
EOF

if [[ $SHOW_SECRETS -eq 1 ]]; then
  chmod 600 "$OUT_FILE"
  log "已写真实 env（chmod 600）：${OUT_FILE}"
  log "注意：该文件含明文密码 / API key，禁止提交 git，用完建议 shred -u。"
else
  log "已写脱敏模板：${OUT_FILE}"
fi

# 只打印非敏感摘要，绝不回显密码 / key。
log "platform_version=1.9  default_infer_backend=${DEFAULT_INFER_BACKEND}"
log "infer_output_root=${INFER_OUTPUT_ROOT}  distill_output_root=${DISTILL_OUTPUT_ROOT}"
if [[ -n "$OPENAI_BASE_URL" ]]; then
  log "openai_compatible_base_url=${OPENAI_BASE_URL}（api_key 已脱敏，不打印）"
else
  log "openai_compatible_base_url=（未配置；mock / local_cpu 后端无需）"
fi
