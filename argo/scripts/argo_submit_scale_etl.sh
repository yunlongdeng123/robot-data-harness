#!/usr/bin/env bash
# 提交 scale-etl WorkflowTemplate；submit 前自检 image 与 Secret 是否就位。
set -euo pipefail

NS="${ROBOT_DH_NS:-robot-dh}"
KIND_NODE="${KIND_NODE:-robot-dh-control-plane}"
IMAGE_NAME="${IMAGE_NAME:-robot-data-harness}"
WORKFLOW_FILE="${1:-$(dirname "$0")/../workflows/submit-scale30-etl.yaml}"

if ! kubectl -n "${NS}" get secret robot-dh-v1-5-secrets >/dev/null 2>&1; then
  echo "[argo-submit] ERROR: Secret ${NS}/robot-dh-v1-5-secrets 不存在" >&2
  echo "[argo-submit] 请先执行: source client/robot-dh-v1-5.env && ./scripts/k8s_create_v1_5_secret_from_env.sh" >&2
  exit 2
fi

if command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' | grep -q "^${KIND_NODE}$"; then
  if ! docker exec "${KIND_NODE}" crictl images 2>/dev/null | grep -q "${IMAGE_NAME}"; then
    echo "[argo-submit] WARNING: image ${IMAGE_NAME} 未加载到 kind 节点 ${KIND_NODE}" >&2
    echo "[argo-submit] 请执行: make docker-build && make kind-load" >&2
  fi
fi

echo "[argo-submit] 提交 Workflow: ${WORKFLOW_FILE}"
NAME=$(kubectl -n "${NS}" create -f "${WORKFLOW_FILE}" -o name)
echo "[argo-submit] 已提交 ${NAME}"
echo "[argo-submit] 实时跟踪: argo -n ${NS} logs ${NAME##*/} -f"
