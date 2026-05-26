#!/usr/bin/env bash
# v1.7 Local：真·follow workflow log。
#
# 与 v1.6 ``make argo-platform-tail`` 的根本区别：
#   - v1.6 直接 ``kubectl logs -l workflow=<wf> -f`` 只能 stream 调用瞬间
#     已经存在的 pod；DAG 后续新拉起的 pod **不会自动接入**，是 kubectl 已知行为。
#   - 本脚本每 3 秒 ``kubectl get workflow -o json`` 一次，发现新 pod 立即
#     ``kubectl logs -f`` 后台 attach；已 attach 的 pod 不重复 attach。
#   - pod 失败时自动打印 ``kubectl describe pod`` + ``kubectl logs --previous``
#     （如果存在），并把推断出的 archive log URI 一并 echo 出来。
#   - workflow 进入终态后回收所有后台 tail 进程，干净退出。
#
# 用法：
#   ./argo/v1_7_local/scripts/tail_live_workflow_logs.sh <workflow-name> \
#       [--ns robot-dh] [--container main] [--interval 3] [--archive-root file:///...]

set -euo pipefail

WF="${1:-}"
NS="robot-dh"
CONTAINER="main"
INTERVAL=3
ARCHIVE_ROOT="file:///mnt/local-data/robot-dh-local/lake/argo-logs"
shift || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ns)            NS="$2"; shift 2 ;;
    --container)     CONTAINER="$2"; shift 2 ;;
    --interval)      INTERVAL="$2"; shift 2 ;;
    --archive-root)  ARCHIVE_ROOT="$2"; shift 2 ;;
    --help|-h)       sed -n '1,25p' "$0"; exit 0 ;;
    *)               shift ;;
  esac
done

if [[ -z "${WF}" ]]; then
  echo "usage: $0 <workflow-name> [--ns robot-dh] [--container main] [--interval 3]" >&2
  exit 2
fi

attached_file="$(mktemp -t robot-dh-tail-XXXXXX)"
pid_file="$(mktemp -t robot-dh-tail-pids-XXXXXX)"
trap 'cleanup' EXIT INT TERM

cleanup() {
  # kill 后台 tail 进程，不要把整个 process group 都 SIGKILL（脚本本身仍要执行完）
  if [[ -s "${pid_file}" ]]; then
    while IFS= read -r pid; do
      kill "${pid}" 2>/dev/null || true
    done < "${pid_file}"
  fi
  rm -f "${attached_file}" "${pid_file}"
}

is_attached() { grep -Fxq -- "$1" "${attached_file}" 2>/dev/null; }
mark_attached() { echo "$1" >> "${attached_file}"; }

archive_uri_for() {
  local pod="$1"
  echo "${ARCHIVE_ROOT%/}/${NS}/${WF}/${pod}/${CONTAINER}.log"
}

attach_pod() {
  local pod="$1"
  local phase="$2"
  echo ""
  echo ">>>>> pod=${pod} phase=${phase} container=${CONTAINER}"
  echo ">>>>> archive_log_uri (expected): $(archive_uri_for "${pod}")"
  # kubectl logs -f：pod 起来后 stream，已结束的 pod 也能拿到完整日志。
  # --tail=-1 拿全部历史；前缀 [pod] 方便多 pod 并发 attach 时区分。
  (
    set +e
    kubectl -n "${NS}" logs -f --tail=-1 --container "${CONTAINER}" "${pod}" 2>&1 \
      | sed -e "s/^/[${pod}] /"
  ) &
  local sub_pid=$!
  echo "${sub_pid}" >> "${pid_file}"
  mark_attached "${pod}"
}

report_failed_pod() {
  local pod="$1"
  echo ""
  echo "!!!!! pod=${pod} failed; describe + previous-log dump"
  echo "----- kubectl describe pod ${pod} -----"
  kubectl -n "${NS}" describe pod "${pod}" 2>/dev/null || true
  echo "----- kubectl logs --previous ${pod} -----"
  kubectl -n "${NS}" logs --previous --container "${CONTAINER}" "${pod}" 2>/dev/null \
    | sed -e "s/^/[${pod}.prev] /" || true
  echo "!!!!! archive_log_uri (expected): $(archive_uri_for "${pod}")"
}

terminal_phases=("Succeeded" "Failed" "Error")
reported_failed_file="$(mktemp -t robot-dh-tail-failed-XXXXXX)"
trap 'rm -f "${reported_failed_file}"; cleanup' EXIT INT TERM

while true; do
  payload="$(kubectl -n "${NS}" get workflow "${WF}" -o json 2>/dev/null || true)"
  if [[ -z "${payload}" ]]; then
    echo "[tail] workflow ${WF} not found yet, sleeping ${INTERVAL}s..."
    sleep "${INTERVAL}"
    continue
  fi

  # 列所有 Pod 类型 node 的 (pod_name, phase)
  while IFS=$'\t' read -r pod phase; do
    [[ -z "${pod}" || "${pod}" == "null" ]] && continue
    if ! is_attached "${pod}"; then
      attach_pod "${pod}" "${phase}"
    fi
    if [[ "${phase}" == "Failed" || "${phase}" == "Error" ]]; then
      if ! grep -Fxq -- "${pod}" "${reported_failed_file}" 2>/dev/null; then
        report_failed_pod "${pod}"
        echo "${pod}" >> "${reported_failed_file}"
      fi
    fi
  done < <(echo "${payload}" | jq -r '
    .status.nodes // {} | to_entries[]
    | select(.value.type == "Pod")
    | "\(.value.id)\t\(.value.phase // "Pending")"
  ')

  wf_phase="$(echo "${payload}" | jq -r '.status.phase // "Pending"')"
  for tp in "${terminal_phases[@]}"; do
    if [[ "${wf_phase}" == "${tp}" ]]; then
      echo ""
      echo "[tail] workflow ${WF} 终态：${wf_phase}"
      # 给已 attach 的 stream 一点时间把剩余日志冲完
      sleep 2
      echo ""
      echo "===== Archive log index (expected URIs) ====="
      echo "${payload}" | jq -r --arg root "${ARCHIVE_ROOT%/}" --arg ns "${NS}" --arg wf "${WF}" --arg c "${CONTAINER}" '
        .status.nodes // {} | to_entries[]
        | select(.value.type == "Pod")
        | "\(.value.displayName // .key)\t\(.value.phase)\t\($root)/\($ns)/\($wf)/\(.value.id)/\($c).log"
      ' | column -t -s $'\t' || true
      exit 0
    fi
  done
  sleep "${INTERVAL}"
done
