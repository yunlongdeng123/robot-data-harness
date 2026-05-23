#!/usr/bin/env bash
# Install Argo Workflows in the local kind cluster.
#
# Idempotent：检测已有 argo namespace 时跳过 install。
# 默认版本可通过 ARGO_VERSION 环境变量覆盖；不通过 SSL 暴露公网。

set -euo pipefail

ARGO_VERSION="${ARGO_VERSION:-v3.5.10}"
NS="${ARGO_NS:-argo}"
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-robot-dh}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "[argo-install] kubectl 未安装" >&2
  exit 1
fi

echo "[argo-install] 目标 context=$(kubectl config current-context 2>/dev/null || echo unknown), 命名空间=${NS}, 版本=${ARGO_VERSION}"

if kubectl get namespace "${NS}" >/dev/null 2>&1; then
  echo "[argo-install] 命名空间 ${NS} 已存在；跳过 namespace 创建。"
else
  kubectl apply -f "$(dirname "$0")/../install/namespace.yaml"
fi

if kubectl -n "${NS}" get deploy argo-server >/dev/null 2>&1; then
  echo "[argo-install] Argo Workflows 看起来已安装（deploy/argo-server 存在）；跳过安装。"
  echo "[argo-install] 如需重装请手动执行: kubectl -n ${NS} delete deploy argo-server workflow-controller"
  exit 0
fi

MANIFEST_URL="https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/quick-start-minimal.yaml"
echo "[argo-install] 安装 manifest: ${MANIFEST_URL}"
kubectl -n "${NS}" apply -f "${MANIFEST_URL}"

echo "[argo-install] 等待 argo-server / workflow-controller 就绪..."
kubectl -n "${NS}" rollout status deploy/argo-server --timeout=180s || true
kubectl -n "${NS}" rollout status deploy/workflow-controller --timeout=180s || true

kubectl get pods -n "${NS}"
echo "[argo-install] 完成。访问 UI 请执行: kubectl -n ${NS} port-forward svc/argo-server 2746:2746"
