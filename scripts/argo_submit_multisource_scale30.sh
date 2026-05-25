#!/usr/bin/env bash
# 提交 robot-dh-multisource-scale30 WorkflowTemplate；submit 前自检 image / secret 是否就位。
set -euo pipefail

NAMESPACE="${NAMESPACE:-robot-dh}"
WORKFLOW_FILE="${WORKFLOW_FILE:-argo/workflows/submit-multisource-scale30.yaml}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[FAIL] kubectl not found on PATH" >&2
  exit 2
fi

# 1) image 检查
if ! kubectl -n "${NAMESPACE}" get pod -l app=robot-dh-debug --ignore-not-found >/dev/null 2>&1; then
  echo "[INFO] no debug pod present; skipping image-side runtime check"
fi

# 2) secret 检查
if ! kubectl -n "${NAMESPACE}" get secret robot-dh-v1-6-secrets >/dev/null 2>&1; then
  echo "[FAIL] secret ${NAMESPACE}/robot-dh-v1-6-secrets not found" >&2
  echo "      run: ./scripts/k8s_create_platform_secret_from_env.sh" >&2
  exit 2
fi

# 3) WorkflowTemplate 已 apply
if ! kubectl -n "${NAMESPACE}" get workflowtemplate robot-dh-multisource-scale30 >/dev/null 2>&1; then
  echo "[FAIL] workflowtemplate robot-dh-multisource-scale30 not applied" >&2
  echo "      run: kubectl apply -f argo/templates/" >&2
  exit 2
fi

kubectl -n "${NAMESPACE}" create -f "${WORKFLOW_FILE}"
