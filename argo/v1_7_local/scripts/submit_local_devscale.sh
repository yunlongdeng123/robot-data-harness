#!/usr/bin/env bash
# v1.7 Local：提交 devscale 主 DAG（默认入口）。
#
# 用法：
#   ./argo/v1_7_local/scripts/submit_local_devscale.sh
#   ./argo/v1_7_local/scripts/submit_local_devscale.sh --watch       # 提交后跟着 tail
#   ./argo/v1_7_local/scripts/submit_local_devscale.sh --ns robot-dh
#
# 行为：
# 1. 必须在 kind-robot-dh-dev context；不在则提示用户切换并退出 1。
# 2. 必须存在 robot-dh-local-data-pvc；否则提示先 make local-apply-data-pvc。
# 3. 必须 apply 过 v1.7 模板（robot-dh-local-devscale 必须存在）。
# 4. 用 kubectl create -f 提交，把 workflow name 写到 stdout。
# 5. --watch 时调用 watch_local_workflow.sh。

set -euo pipefail

NS="${NS:-robot-dh}"
WATCH=0
CONTEXT_EXPECT="kind-robot-dh-dev"

for arg in "$@"; do
  case "$arg" in
    --watch)        WATCH=1 ;;
    --ns)           shift; NS="$1" ;;
    --skip-context-check) CONTEXT_EXPECT="" ;;
    --help|-h)      sed -n '1,20p' "$0"; exit 0 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

current_ctx="$(kubectl config current-context 2>/dev/null || true)"
if [[ -n "${CONTEXT_EXPECT}" && "${current_ctx}" != "${CONTEXT_EXPECT}" ]]; then
  echo "ERROR: 当前 context=${current_ctx}，期望=${CONTEXT_EXPECT}" >&2
  echo "       请先：kubectl config use-context ${CONTEXT_EXPECT}" >&2
  exit 1
fi

if ! kubectl -n "${NS}" get pvc robot-dh-local-data-pvc >/dev/null 2>&1; then
  echo "ERROR: PVC robot-dh-local-data-pvc 不存在，请先：make local-apply-data-pvc" >&2
  exit 1
fi

if ! kubectl -n "${NS}" get workflowtemplate robot-dh-local-devscale >/dev/null 2>&1; then
  echo "ERROR: WorkflowTemplate robot-dh-local-devscale 不存在，请先：make argo-local-apply" >&2
  exit 1
fi

submit_yaml="${REPO_ROOT}/argo/v1_7_local/workflows/submit-local-devscale.yaml"
echo "[submit] kubectl -n ${NS} create -f ${submit_yaml}"
created="$(kubectl -n "${NS}" create -f "${submit_yaml}" -o jsonpath='{.metadata.name}')"
echo "[submit] Workflow created: ${created}"
echo "${created}"

if [[ "${WATCH}" == "1" ]]; then
  exec "${SCRIPT_DIR}/watch_local_workflow.sh" "${created}" --ns "${NS}"
fi
