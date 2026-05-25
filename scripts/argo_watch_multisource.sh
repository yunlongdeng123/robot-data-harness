#!/usr/bin/env bash
# 监视 robot-dh-multisource-scale30 workflow 的进度。
set -euo pipefail

NAMESPACE="${NAMESPACE:-robot-dh}"
WORKFLOW_PREFIX="${WORKFLOW_PREFIX:-robot-dh-multisource-scale30-}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[FAIL] kubectl not found on PATH" >&2
  exit 2
fi

latest=$(kubectl -n "${NAMESPACE}" get workflows.argoproj.io \
  --sort-by=.metadata.creationTimestamp \
  -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
if [[ -z "${latest}" ]]; then
  echo "no workflow in ${NAMESPACE}" >&2
  exit 1
fi
case "${latest}" in
  ${WORKFLOW_PREFIX}*) ;;
  *)
    echo "[WARN] latest workflow ${latest} doesn't start with ${WORKFLOW_PREFIX}" >&2
    ;;
esac

echo "[INFO] watching ${latest}"
kubectl -n "${NAMESPACE}" get workflow "${latest}" -o wide --watch
