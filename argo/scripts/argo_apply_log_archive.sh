#!/usr/bin/env bash
# argo/scripts/argo_apply_log_archive.sh
#
# 把 archiveLogs + s3 配置 patch 到 argo/workflow-controller-configmap，
# 让 step pod 终态时 stdout/stderr 自动归档到云端 MinIO 的
# robot-dh-artifacts/argo-logs/<ns>/<workflow.name>/<pod.name>/main.log
#
# 用法:
#   ./argo/scripts/argo_apply_log_archive.sh           # 实际 apply
#   ./argo/scripts/argo_apply_log_archive.sh --dry-run # 只输出渲染后的 manifest
#
# 选项:
#   --argo-ns <ns>        argo namespace (默认 argo)
#   --src-ns <ns>         读取 endpoint 的 namespace (默认 robot-dh)
#   --secret <name>       源 secret 名 (默认 robot-dh-v1-6-secrets)
#   --dry-run             不 patch，只打印 manifest 与 patch 命令
#
# 输入：从 robot-dh/robot-dh-v1-6-secrets 中读 ROBOT_DH_S3_ENDPOINT_URL，
# 推断 host:port + insecure（HTTP=true / HTTPS=false），渲染到
# argo/install/workflow-controller-artifact-repository.yaml 的占位符。
# 然后用 kubectl patch --type=strategic 合并到现有 ConfigMap，
# 不破坏 quick-start-minimal 默认的其它字段（如 executor / persistence）。
set -euo pipefail

ARGO_NS="argo"
SRC_NS="robot-dh"
SECRET_NAME="robot-dh-v1-6-secrets"
DRY_RUN="false"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TEMPLATE="${REPO_ROOT}/argo/install/workflow-controller-artifact-repository.yaml"

usage() {
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --argo-ns) ARGO_NS="$2"; shift 2 ;;
    --src-ns) SRC_NS="$2"; shift 2 ;;
    --secret) SECRET_NAME="$2"; shift 2 ;;
    --dry-run) DRY_RUN="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "未知选项: $1" >&2; usage 1 ;;
  esac
done

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[FAIL] kubectl 未安装" >&2
  exit 2
fi
if [[ ! -f "${TEMPLATE}" ]]; then
  echo "[FAIL] 模板缺失: ${TEMPLATE}" >&2
  exit 2
fi

if ! kubectl -n "${ARGO_NS}" get configmap workflow-controller-configmap >/dev/null 2>&1; then
  echo "[FAIL] ${ARGO_NS}/workflow-controller-configmap 不存在；请先 make argo-install" >&2
  exit 2
fi

if ! kubectl -n "${SRC_NS}" get secret "${SECRET_NAME}" >/dev/null 2>&1; then
  echo "[FAIL] secret ${SRC_NS}/${SECRET_NAME} 不存在；先跑 scripts/k8s_create_platform_secret_from_env.sh" >&2
  exit 2
fi

# 读 endpoint URL（base64 解码）
endpoint_b64=$(kubectl -n "${SRC_NS}" get secret "${SECRET_NAME}" \
  -o jsonpath='{.data.ROBOT_DH_S3_ENDPOINT_URL}')
if [[ -z "${endpoint_b64}" ]]; then
  echo "[FAIL] secret ${SRC_NS}/${SECRET_NAME} 缺少 ROBOT_DH_S3_ENDPOINT_URL" >&2
  exit 2
fi
endpoint_url=$(echo "${endpoint_b64}" | base64 -d)

# 解析 scheme + host:port
case "${endpoint_url}" in
  http://*)  insecure="true";  raw="${endpoint_url#http://}"  ;;
  https://*) insecure="false"; raw="${endpoint_url#https://}" ;;
  *) echo "[FAIL] ROBOT_DH_S3_ENDPOINT_URL 不是 http(s):// 开头: ${endpoint_url}" >&2; exit 2 ;;
esac
hostport="${raw%%/*}"
case "${hostport}" in
  *:*) ;;  # 已带端口
  *) [[ "${insecure}" == "true" ]] && hostport="${hostport}:80" || hostport="${hostport}:443" ;;
esac

case "${hostport}" in
  127.0.0.1:*|localhost:*)
    echo "[FAIL] endpoint host 为 ${hostport}，kind controller pod 不可达；请改成云端公网 IP/DNS" >&2
    exit 2
    ;;
esac

# 渲染模板
rendered=$(sed \
  -e "s#__ROBOT_DH_S3_ENDPOINT_HOSTPORT__#${hostport}#g" \
  -e "s#__ROBOT_DH_S3_INSECURE__#${insecure}#g" \
  "${TEMPLATE}")

echo "[INFO] argo namespace: ${ARGO_NS}"
echo "[INFO] endpoint hostport: ${hostport}  insecure=${insecure}"
echo "[INFO] keyFormat: argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log"

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "[DRY-RUN] 渲染后 manifest（敏感字段未含）:"
  printf '%s\n' "${rendered}"
  exit 0
fi

# 用 strategic merge 把 data.artifactRepository 合并进现有 ConfigMap
printf '%s\n' "${rendered}" | kubectl apply -f -
echo "[OK] ${ARGO_NS}/workflow-controller-configmap.data.artifactRepository 已更新"

# 重启 controller 让新 ConfigMap 生效（quick-start-minimal 不会自动 reload）
if kubectl -n "${ARGO_NS}" get deploy/workflow-controller >/dev/null 2>&1; then
  kubectl -n "${ARGO_NS}" rollout restart deploy/workflow-controller
  kubectl -n "${ARGO_NS}" rollout status deploy/workflow-controller --timeout=120s || true
  echo "[OK] workflow-controller 已重启，新 ConfigMap 生效"
else
  echo "[WARN] 未发现 ${ARGO_NS}/deploy/workflow-controller，跳过 rollout restart" >&2
fi
