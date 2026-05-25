#!/usr/bin/env bash
# 找 robot-dh ns 中最新的 Workflow，然后调用 robot-dh argo sync 写入 PG。
set -euo pipefail

NAMESPACE="${NAMESPACE:-robot-dh}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[FAIL] kubectl not found on PATH" >&2
  exit 2
fi

latest=$(kubectl -n "${NAMESPACE}" get workflows.argoproj.io \
  --sort-by=.metadata.creationTimestamp \
  -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
if [[ -z "${latest}" ]]; then
  echo "[FAIL] no workflow in namespace ${NAMESPACE}" >&2
  exit 1
fi

echo "[INFO] syncing latest workflow: ${latest}"
PYTHONPATH=src python -m robot_dh.cli argo sync \
  --workflow-name "${latest}" --namespace "${NAMESPACE}"
