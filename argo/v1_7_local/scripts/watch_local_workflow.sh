#!/usr/bin/env bash
# v1.7 Local：周期 echo 当前 workflow 节点 phase，等终态后退出。
#
# 用法：
#   ./argo/v1_7_local/scripts/watch_local_workflow.sh <workflow-name> [--ns robot-dh] [--interval 5]
#
# 与 tail_live_workflow_logs.sh 的区别：本脚本只看 status / DAG nodes 概览，
# 不打开 kubectl logs。两个脚本可以并行运行。

set -euo pipefail

WF="${1:-}"
NS="robot-dh"
INTERVAL=5
shift || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ns)       NS="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    --help|-h)  sed -n '1,15p' "$0"; exit 0 ;;
    *)          shift ;;
  esac
done

if [[ -z "${WF}" ]]; then
  echo "usage: $0 <workflow-name> [--ns robot-dh] [--interval 5]" >&2
  exit 2
fi

terminal_phases=("Succeeded" "Failed" "Error")

while true; do
  payload="$(kubectl -n "${NS}" get workflow "${WF}" -o json 2>/dev/null || true)"
  if [[ -z "${payload}" ]]; then
    echo "[watch] workflow ${WF} 不存在或未就绪，等待 ${INTERVAL}s..."
    sleep "${INTERVAL}"
    continue
  fi
  phase="$(echo "${payload}" | jq -r '.status.phase // "Pending"')"
  echo "===== $(date -u +%FT%TZ) ${WF} phase=${phase} ====="
  echo "${payload}" | jq -r '
    .status.nodes // {} | to_entries[]
    | select(.value.type == "Pod")
    | "\(.value.displayName // .key)\t\(.value.phase // "-")\t\(.value.message // "")"
  ' | column -t -s $'\t' || true

  for tp in "${terminal_phases[@]}"; do
    if [[ "${phase}" == "${tp}" ]]; then
      echo "[watch] workflow ${WF} 已终态：${phase}"
      exit 0
    fi
  done
  sleep "${INTERVAL}"
done
