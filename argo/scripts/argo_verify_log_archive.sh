#!/usr/bin/env bash
# argo/scripts/argo_verify_log_archive.sh
#
# 验证 archiveLogs 是否真正生效。三段验证：
#   1) ConfigMap 端：argo/workflow-controller-configmap 包含 archiveLogs: true。
#   2) Secret 端：argo/robot-dh-v1-6-secrets 至少含 ROBOT_DH_S3_ACCESS_KEY/SECRET_KEY。
#   3) Object 端：可选，需要本机有 mc + 已配置 alias，会列 robot-dh-artifacts/argo-logs/。
#
# 用法:
#   ./argo/scripts/argo_verify_log_archive.sh           # 只做 1+2
#   MC_ALIAS=local ./argo/scripts/argo_verify_log_archive.sh --check-objects
#
# 选项:
#   --argo-ns <ns>        argo namespace (默认 argo)
#   --check-objects       使用 mc 验证 robot-dh-artifacts/argo-logs/ 中是否有对象
#   --workflow <name>     精确到某 workflow 列出对象
set -euo pipefail

ARGO_NS="argo"
CHECK_OBJECTS="false"
WORKFLOW=""
MC_ALIAS="${MC_ALIAS:-local}"

usage() { awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --argo-ns) ARGO_NS="$2"; shift 2 ;;
    --check-objects) CHECK_OBJECTS="true"; shift ;;
    --workflow) WORKFLOW="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "未知选项: $1" >&2; usage 1 ;;
  esac
done

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[FAIL] kubectl 未安装" >&2; exit 2
fi

fail=0

echo "[1/3] 检查 ${ARGO_NS}/workflow-controller-configmap.data.artifactRepository"
ar=$(kubectl -n "${ARGO_NS}" get configmap workflow-controller-configmap \
  -o jsonpath='{.data.artifactRepository}' 2>/dev/null || true)
if [[ -z "${ar}" ]]; then
  echo "  [FAIL] artifactRepository 字段为空"; fail=1
else
  if echo "${ar}" | grep -q '^archiveLogs: true'; then
    echo "  [OK] archiveLogs: true"
  else
    echo "  [FAIL] archiveLogs 未启用："; echo "${ar}" | sed 's/^/    /'; fail=1
  fi
  if echo "${ar}" | grep -q 'bucket: robot-dh-artifacts'; then
    echo "  [OK] bucket=robot-dh-artifacts"
  else
    echo "  [FAIL] bucket 不是 robot-dh-artifacts"; fail=1
  fi
  if echo "${ar}" | grep -q 'argo-logs/{{workflow.namespace}}/{{workflow.name}}/{{pod.name}}/main.log'; then
    echo "  [OK] keyFormat 与 robot-dh-infra 需求一致"
  else
    echo "  [FAIL] keyFormat 与需求不一致"; fail=1
  fi
fi

echo "[2/3] 检查 ${ARGO_NS}/robot-dh-v1-6-secrets"
if kubectl -n "${ARGO_NS}" get secret robot-dh-v1-6-secrets >/dev/null 2>&1; then
  for k in ROBOT_DH_S3_ACCESS_KEY ROBOT_DH_S3_SECRET_KEY; do
    v=$(kubectl -n "${ARGO_NS}" get secret robot-dh-v1-6-secrets -o jsonpath="{.data.${k}}" 2>/dev/null || true)
    if [[ -n "${v}" ]]; then
      echo "  [OK] ${k} 存在"
    else
      echo "  [FAIL] ${k} 缺失"; fail=1
    fi
  done
else
  echo "  [FAIL] secret 不存在；请先 make argo-sync-log-archive-secret"; fail=1
fi

if [[ "${CHECK_OBJECTS}" == "true" ]]; then
  echo "[3/3] 用 mc 列 ${MC_ALIAS}/robot-dh-artifacts/argo-logs/"
  if ! command -v mc >/dev/null 2>&1; then
    echo "  [SKIP] 本机无 mc，跳过 object 端验证"
  elif ! mc alias list "${MC_ALIAS}" >/dev/null 2>&1; then
    echo "  [SKIP] mc alias=${MC_ALIAS} 未配置"
  else
    prefix="argo-logs/"
    [[ -n "${WORKFLOW}" ]] && prefix="argo-logs/robot-dh/${WORKFLOW}/"
    if mc ls -r "${MC_ALIAS}/robot-dh-artifacts/${prefix}" 2>/dev/null | head -5; then
      echo "  [OK] argo-logs/ 下已存在归档对象"
    else
      echo "  [WARN] argo-logs/ 下暂无对象；提交一个 workflow 等终态后再查"
    fi
  fi
else
  echo "[3/3] 跳过 object 端验证（加 --check-objects 启用）"
fi

if [[ "${fail}" -ne 0 ]]; then
  echo "[FAIL] 验证未通过"; exit 1
fi
echo "[OK] 验证通过"
