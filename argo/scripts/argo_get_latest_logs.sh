#!/usr/bin/env bash
# 打印最近一个 Workflow 各 step pod 的日志（或显式指定 workflow name）。
# 仓库已脱钩 argo 官方 CLI，统一走 kubectl + Argo step pod 的 label selector。
set -euo pipefail

NS="${ROBOT_DH_NS:-robot-dh}"
NAME="${1:-}"
CONTAINER="${LOG_CONTAINER:-main}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[argo-logs] kubectl 未安装" >&2
  exit 2
fi

if [[ -z "${NAME}" ]]; then
  NAME=$(kubectl -n "${NS}" get workflows.argoproj.io \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)
fi
if [[ -z "${NAME}" ]]; then
  echo "[argo-logs] 未在 ns=${NS} 找到 workflow" >&2
  exit 1
fi

echo "[argo-logs] workflow=${NAME} container=${CONTAINER} (LOG_CONTAINER 覆盖；wait/init 用于排查 executor)"
kubectl -n "${NS}" logs -l workflows.argoproj.io/workflow="${NAME}" \
  -c "${CONTAINER}" --tail=-1 --prefix --max-log-requests=20
