#!/usr/bin/env bash
# 阻塞等待最近一个 Workflow（或指定名称）结束并打印 status。
set -euo pipefail

NS="${ROBOT_DH_NS:-robot-dh}"
TIMEOUT="${TIMEOUT:-43200}"
NAME="${1:-}"

if [[ -z "${NAME}" ]]; then
  NAME=$(kubectl -n "${NS}" get workflows.argoproj.io \
    --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)
fi
if [[ -z "${NAME}" ]]; then
  echo "[argo-wait] 未找到 workflow" >&2
  exit 1
fi
echo "[argo-wait] 等待 workflow=${NAME} 完成 (timeout=${TIMEOUT}s) ..."
DEADLINE=$(( $(date +%s) + TIMEOUT ))
while (( $(date +%s) < DEADLINE )); do
  PHASE=$(kubectl -n "${NS}" get workflows.argoproj.io "${NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  case "${PHASE}" in
    Succeeded|Failed|Error)
      echo "[argo-wait] workflow=${NAME} 已完成 phase=${PHASE}"
      [[ "${PHASE}" == "Succeeded" ]] && exit 0
      exit 2
      ;;
    *)
      echo "[argo-wait] phase=${PHASE:-?}，5s 后重试..."
      sleep 5
      ;;
  esac
done
echo "[argo-wait] 超时 ${TIMEOUT}s 未完成"
exit 124
