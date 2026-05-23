#!/usr/bin/env bash
set -euo pipefail

NS="${ROBOT_DH_NS:-robot-dh}"
WORKFLOW_FILE="${1:-$(dirname "$0")/../workflows/submit-benchmark.yaml}"

if ! kubectl -n "${NS}" get secret robot-dh-v1-5-secrets >/dev/null 2>&1; then
  echo "[argo-submit-benchmark] WARNING: Secret ${NS}/robot-dh-v1-5-secrets 不存在；benchmark 不依赖远端 secret 也能跑，但 record_to_registry 会被降级。"
fi

NAME=$(kubectl -n "${NS}" create -f "${WORKFLOW_FILE}" -o name)
echo "[argo-submit-benchmark] 已提交 ${NAME}"
