#!/usr/bin/env bash
# argo/scripts/argo_sync_log_archive_secret.sh
#
# 把 robot-dh/robot-dh-v1-6-secrets 同步到 argo/robot-dh-v1-6-secrets，
# 让 workflow-controller（运行在 argo namespace）能读到 archiveLogs 用的 S3 凭据。
#
# 用法:
#   ./argo/scripts/argo_sync_log_archive_secret.sh
#
# 选项:
#   --src-ns <ns>         源 namespace (默认 robot-dh)
#   --dst-ns <ns>         目标 namespace (默认 argo)
#   --secret <name>       Secret 名 (默认 robot-dh-v1-6-secrets)
#   --dry-run             只展示要执行的 kubectl, 不真正应用
#   -h | --help           帮助
#
# 设计：仅复制 ROBOT_DH_S3_ACCESS_KEY / ROBOT_DH_S3_SECRET_KEY 两个字段，
# 避免把 DB / Redis 等无关凭据带进 controller pod 的环境。
set -euo pipefail

SRC_NS="robot-dh"
DST_NS="argo"
SECRET_NAME="robot-dh-v1-6-secrets"
DRY_RUN="false"

usage() {
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src-ns) SRC_NS="$2"; shift 2 ;;
    --dst-ns) DST_NS="$2"; shift 2 ;;
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

if ! kubectl get namespace "${DST_NS}" >/dev/null 2>&1; then
  echo "[FAIL] namespace ${DST_NS} 不存在；先 make argo-install" >&2
  exit 2
fi

if ! kubectl -n "${SRC_NS}" get secret "${SECRET_NAME}" >/dev/null 2>&1; then
  echo "[FAIL] secret ${SRC_NS}/${SECRET_NAME} 不存在" >&2
  echo "      请先 source client/robot-dh-platform.env 并运行 scripts/k8s_create_platform_secret_from_env.sh" >&2
  exit 2
fi

ACCESS_KEY_B64=$(kubectl -n "${SRC_NS}" get secret "${SECRET_NAME}" \
  -o jsonpath='{.data.ROBOT_DH_S3_ACCESS_KEY}')
SECRET_KEY_B64=$(kubectl -n "${SRC_NS}" get secret "${SECRET_NAME}" \
  -o jsonpath='{.data.ROBOT_DH_S3_SECRET_KEY}')

if [[ -z "${ACCESS_KEY_B64}" || -z "${SECRET_KEY_B64}" ]]; then
  echo "[FAIL] 源 secret 缺少 ROBOT_DH_S3_ACCESS_KEY / ROBOT_DH_S3_SECRET_KEY" >&2
  exit 2
fi

manifest=$(cat <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SECRET_NAME}
  namespace: ${DST_NS}
  labels:
    app: robot-dh
    component: argo-log-archive
  annotations:
    robot-dh.io/synced-from: "${SRC_NS}/${SECRET_NAME}"
    robot-dh.io/synced-fields: "ROBOT_DH_S3_ACCESS_KEY,ROBOT_DH_S3_SECRET_KEY"
type: Opaque
data:
  ROBOT_DH_S3_ACCESS_KEY: ${ACCESS_KEY_B64}
  ROBOT_DH_S3_SECRET_KEY: ${SECRET_KEY_B64}
EOF
)

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "[DRY-RUN] 将向 kubectl apply -f - 写入以下 manifest（敏感字段已隐藏）："
  printf '%s\n' "${manifest}" | sed -E 's/^(  ROBOT_DH_S3_(ACCESS|SECRET)_KEY:).*/\1 <REDACTED>/'
  exit 0
fi

printf '%s\n' "${manifest}" | kubectl apply -f -
echo "[OK] secret ${DST_NS}/${SECRET_NAME} 已同步（仅含 ROBOT_DH_S3_ACCESS_KEY / ROBOT_DH_S3_SECRET_KEY）"
