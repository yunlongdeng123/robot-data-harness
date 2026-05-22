#!/usr/bin/env bash
#
# scripts/k8s_create_lake_secret_from_env.sh
#
# 从当前 shell 已加载的环境变量创建/更新
# k8s secret  robot-dh-lake-secrets (namespace=robot-dh)
#
# 用法:
#   source client/robot-dh-lake.env          # 先把变量加载进来
#   ./scripts/k8s_create_lake_secret_from_env.sh
#
# 选项:
#   --namespace <ns>      自定义命名空间 (默认 robot-dh)
#   --name <name>         自定义 secret 名 (默认 robot-dh-lake-secrets)
#   --kubectl <cmd>       自定义 kubectl 命令 (例如 "kubectl --context kind-robot-dh")
#   --allow-localhost     允许 endpoint 为 127.0.0.1 / localhost
#                         (默认拒绝，避免误把 WSL tunnel 注入到 kind Pod)
#   --dry-run             只展示要执行的 kubectl, 不真正应用
#   -h | --help           帮助
#
# 注意:
#   - 永远不会回显 secret 值。
#   - 最后只输出 kubectl describe secret 的 KEY / Type / 长度等安全摘要。
set -euo pipefail

NAMESPACE="robot-dh"
SECRET_NAME="robot-dh-lake-secrets"
KUBECTL="kubectl"
ALLOW_LOCALHOST="false"
DRY_RUN="false"

usage() {
    grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
    exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --namespace)
            NAMESPACE="$2"; shift 2 ;;
        --name)
            SECRET_NAME="$2"; shift 2 ;;
        --kubectl)
            KUBECTL="$2"; shift 2 ;;
        --allow-localhost)
            ALLOW_LOCALHOST="true"; shift ;;
        --dry-run)
            DRY_RUN="true"; shift ;;
        -h|--help)
            usage 0 ;;
        *)
            echo "未知选项: $1" >&2
            usage 1 ;;
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
    for v in "${missing[@]}"; do
        echo "  - $v" >&2
    done
    echo "Hint: 请先执行 source client/robot-dh-lake.env" >&2
    exit 2
fi

endpoint="${ROBOT_DH_S3_ENDPOINT_URL}"
endpoint_lc="$(printf '%s' "$endpoint" | tr '[:upper:]' '[:lower:]')"
if [[ "$ALLOW_LOCALHOST" != "true" ]]; then
    case "$endpoint_lc" in
        *127.0.0.1*|*localhost*|*::1*)
            echo "ERROR: ROBOT_DH_S3_ENDPOINT_URL='${endpoint}' 指向 loopback。" >&2
            echo "       kind Pod 无法访问 WSL 的 127.0.0.1 SSH tunnel；" >&2
            echo "       请改成云端实际地址（公网 IP 或 VPC 内网地址），" >&2
            echo "       或显式追加 --allow-localhost 跳过该检查（仅推荐 kind 内部 service）。" >&2
            exit 3
            ;;
    esac
fi

db_lc="$(printf '%s' "${ROBOT_DH_DB_URI}" | tr '[:upper:]' '[:lower:]')"
if [[ "$ALLOW_LOCALHOST" != "true" ]]; then
    case "$db_lc" in
        *@127.0.0.1*|*@localhost*|*@::1*)
            echo "ERROR: ROBOT_DH_DB_URI 指向 loopback；kind Pod 不能复用 WSL tunnel。" >&2
            echo "       请使用云端 PostgreSQL 的真实地址，或加 --allow-localhost。" >&2
            exit 3
            ;;
    esac
fi

if ! command -v "${KUBECTL%% *}" >/dev/null 2>&1; then
    echo "ERROR: 未找到 kubectl: ${KUBECTL%% *}" >&2
    exit 4
fi

if ! ${KUBECTL} get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    echo "命名空间 '${NAMESPACE}' 不存在，正在创建 ..."
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "(dry-run) ${KUBECTL} create namespace ${NAMESPACE}"
    else
        ${KUBECTL} create namespace "${NAMESPACE}"
    fi
fi

literal_args=()
declare -a present_keys=()
declare -a missing_optional=()
for v in "${REQUIRED_VARS[@]}"; do
    literal_args+=("--from-literal=${v}=${!v}")
    present_keys+=("${v}")
done
for v in "${OPTIONAL_VARS[@]}"; do
    if [[ -n "${!v-}" ]]; then
        literal_args+=("--from-literal=${v}=${!v}")
        present_keys+=("${v}")
    else
        missing_optional+=("${v}")
    fi
done

if [[ "$DRY_RUN" == "true" ]]; then
    # 不展开真实值；只显示 key 名
    echo "(dry-run) 将创建/更新 Secret ${NAMESPACE}/${SECRET_NAME}，键:"
    for k in "${present_keys[@]}"; do
        echo "  - ${k}"
    done
    if (( ${#missing_optional[@]} > 0 )); then
        echo "  （可选键缺失: ${missing_optional[*]}）"
    fi
    exit 0
fi

# Apply：通过 dry-run+apply 做 upsert，避免回显敏感值
${KUBECTL} -n "${NAMESPACE}" create secret generic "${SECRET_NAME}" \
    "${literal_args[@]}" \
    --dry-run=client -o yaml | ${KUBECTL} apply -f - >/dev/null

echo
echo "Secret ${NAMESPACE}/${SECRET_NAME} 已应用。已包含的键:"
for k in "${present_keys[@]}"; do
    echo "  - ${k}"
done
if (( ${#missing_optional[@]} > 0 )); then
    echo "可选键未设置（已跳过）:"
    for k in "${missing_optional[@]}"; do
        echo "  - ${k}"
    done
fi

echo
echo "摘要（kubectl describe；不打印敏感值）:"
${KUBECTL} -n "${NAMESPACE}" describe secret "${SECRET_NAME}" | \
    awk '/^Name:|^Namespace:|^Type:|^Data$/{print; in_data=($0=="Data"); next} in_data && /^[[:space:]]*[A-Za-z0-9_.-]+:/{print}'
