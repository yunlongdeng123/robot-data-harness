#!/usr/bin/env bash
# 打印最近一个 Workflow 各 step 的日志（或显式指定 workflow name）。
set -euo pipefail

NS="${ROBOT_DH_NS:-robot-dh}"
NAME="${1:-}"

if [[ -z "${NAME}" ]]; then
  NAME=$(kubectl -n "${NS}" get workflows.argoproj.io \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)
fi
if [[ -z "${NAME}" ]]; then
  echo "[argo-logs] 未找到 workflow" >&2
  exit 1
fi
echo "[argo-logs] workflow=${NAME}"
if command -v argo >/dev/null 2>&1; then
  argo -n "${NS}" logs "${NAME}" --no-color
else
  kubectl -n "${NS}" logs -l workflows.argoproj.io/workflow="${NAME}" --tail=-1 --all-containers=true
fi
