#!/usr/bin/env bash
#
# scripts/k8s_create_v1_5_secret_from_env.sh
#
# 从当前 shell 已加载的环境变量创建/更新
# k8s secret  robot-dh-v1-5-secrets (namespace=robot-dh)
#
# 用法:
#   source client/robot-dh-v1-5.env
#   ./scripts/k8s_create_v1_5_secret_from_env.sh
#
# 选项:
#   --namespace <ns>      自定义命名空间 (默认 robot-dh)
#   --name <name>         自定义 secret 名 (默认 robot-dh-v1-5-secrets)
#   --kubectl <cmd>       自定义 kubectl 命令
#   --allow-localhost     允许 endpoint 为 127.0.0.1 / localhost (默认拒绝)
#   --dry-run             只展示要执行的 kubectl, 不真正应用
#   -h | --help           帮助
#
set -euo pipefail

NAMESPACE="robot-dh"
SECRET_NAME="robot-dh-v1-5-secrets"
KUBECTL="kubectl"
ALLOW_LOCALHOST="false"
DRY_RUN="false"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --name) SECRET_NAME="$2"; shift 2 ;;
    --kubectl) KUBECTL="$2"; shift 2 ;;
    --allow-localhost) ALLOW_LOCALHOST="true"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "未知选项: $1" >&2; usage 1 ;;
  esac
done

REQUIRED_VARS=(
  ROBOT_DH_DB_URI
  ROBOT_DH_S3_ENDPOINT_URL
  ROBOT_DH_S3_ACCESS_KEY
  ROBOT_DH_S3_SECRET_KEY
  ROBOT_DH_S3_DATA_BUCKET
  ROBOT_DH_S3_ARTIFACT_BUCKET
  ROBOT_DH_S3_LAKE_BUCKET
)
OPTIONAL_VARS=(
  ROBOT_DH_REDIS_URL
  ROBOT_DH_ARTIFACT_STORE
  ROBOT_DH_S3_REGION
)

missing=()
for v in "${REQUIRED_VARS[@]}"; do
  if [[ -z "${!v-}" ]]; then
    missing+=("$v")
  fi
done
if (( ${#missing[@]} > 0 )); then
  echo "ERROR: 缺少必需环境变量:" >&2
  for v in "${missing[@]}"; do echo "  - $v" >&2; done
  echo "Hint: 请先执行 source client/robot-dh-v1-5.env" >&2
  exit 2
fi

endpoint_lc="$(printf '%s' "${ROBOT_DH_S3_ENDPOINT_URL}" | tr '[:upper:]' '[:lower:]')"
db_lc="$(printf '%s' "${ROBOT_DH_DB_URI}" | tr '[:upper:]' '[:lower:]')"
if [[ "$ALLOW_LOCALHOST" != "true" ]]; then
  case "$endpoint_lc" in
    *127.0.0.1*|*localhost*|*::1*)
      echo "ERROR: ROBOT_DH_S3_ENDPOINT_URL 指向 loopback；kind Pod 不能用 WSL tunnel。" >&2
      echo "       请改成云端实际地址，或追加 --allow-localhost 跳过。" >&2
      exit 3
      ;;
  esac
  case "$db_lc" in
    *@127.0.0.1*|*@localhost*|*@::1*)
      echo "ERROR: ROBOT_DH_DB_URI 指向 loopback；kind Pod 不能复用 WSL tunnel。" >&2
      exit 3
      ;;
  esac
fi

if ! command -v "${KUBECTL%% *}" >/dev/null 2>&1; then
  echo "ERROR: 未找到 kubectl: ${KUBECTL%% *}" >&2
  exit 4
fi

if ! ${KUBECTL} get namespace "${NAMESPACE}" >/dev/null 2>&1; then
  echo "命名空间 '${NAMESPACE}' 不存在，正在创建..."
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "(dry-run) ${KUBECTL} create namespace ${NAMESPACE}"
  else
    ${KUBECTL} create namespace "${NAMESPACE}"
  fi
fi

literal_args=()
present_keys=()
missing_optional=()
for v in "${REQUIRED_VARS[@]}"; do
  literal_args+=("--from-literal=${v}=${!v}")
  present_keys+=("${v}")
done

# Go pgx 不识别 SQLAlchemy 的 postgresql+psycopg scheme，额外写入标准 DSN。
exporter_db_uri="${ROBOT_DH_DB_URI}"
case "${exporter_db_uri}" in
  postgresql+psycopg://*)
    exporter_db_uri="postgresql://${exporter_db_uri#postgresql+psycopg://}"
    ;;
esac
literal_args+=("--from-literal=ROBOT_DH_EXPORTER_DB_URI=${exporter_db_uri}")
present_keys+=("ROBOT_DH_EXPORTER_DB_URI")

for v in "${OPTIONAL_VARS[@]}"; do
  if [[ -n "${!v-}" ]]; then
    literal_args+=("--from-literal=${v}=${!v}")
    present_keys+=("${v}")
  else
    missing_optional+=("${v}")
  fi
done

if [[ "$DRY_RUN" == "true" ]]; then
  echo "(dry-run) 将创建/更新 Secret ${NAMESPACE}/${SECRET_NAME}，键:"
  for k in "${present_keys[@]}"; do echo "  - ${k}"; done
  if (( ${#missing_optional[@]} > 0 )); then
    echo "  （可选键缺失: ${missing_optional[*]}）"
  fi
  exit 0
fi

${KUBECTL} -n "${NAMESPACE}" create secret generic "${SECRET_NAME}" \
  "${literal_args[@]}" \
  --dry-run=client -o yaml | ${KUBECTL} apply -f - >/dev/null

echo
echo "Secret ${NAMESPACE}/${SECRET_NAME} 已应用。已包含的键:"
for k in "${present_keys[@]}"; do echo "  - ${k}"; done
if (( ${#missing_optional[@]} > 0 )); then
  echo "可选键未设置（已跳过）:"
  for k in "${missing_optional[@]}"; do echo "  - ${k}"; done
fi

echo
echo "摘要（kubectl describe；不打印敏感值）:"
${KUBECTL} -n "${NAMESPACE}" describe secret "${SECRET_NAME}" | \
  awk '/^Name:|^Namespace:|^Type:|^Data$/{print; in_data=($0=="Data"); next} in_data && /^[[:space:]]*[A-Za-z0-9_.-]+:/{print}'
