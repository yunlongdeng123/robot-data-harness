#!/usr/bin/env bash
#
# scripts/k8s_wait_lake_job.sh <job-name>
#
# 等待指定 v1.4 lake Job 完成，成功返回 0；失败时打印 describe / pod logs。
#
# 用法:
#   ./scripts/k8s_wait_lake_job.sh robot-dh-lake-etl-scan
#
# 选项 (通过环境变量覆盖):
#   NAMESPACE          (默认 robot-dh)
#   TIMEOUT            (默认 30m，对应 kubectl wait --timeout)
#   KUBECTL            (默认 kubectl)
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "用法: $0 <job-name>" >&2
    exit 2
fi

JOB="$1"
NAMESPACE="${NAMESPACE:-robot-dh}"
TIMEOUT="${TIMEOUT:-30m}"
KUBECTL="${KUBECTL:-kubectl}"

if ! ${KUBECTL} -n "${NAMESPACE}" get job "${JOB}" >/dev/null 2>&1; then
    echo "ERROR: 未找到 Job ${NAMESPACE}/${JOB}" >&2
    exit 3
fi

echo "[wait-job] 最多等待 ${TIMEOUT}，目标 ${NAMESPACE}/${JOB} ..."
set +e
${KUBECTL} -n "${NAMESPACE}" wait --for=condition=complete \
    --timeout="${TIMEOUT}" "job/${JOB}"
complete_status=$?
set -e

if [[ ${complete_status} -eq 0 ]]; then
    echo "[wait-job] ${JOB} 已成功完成。"
    exit 0
fi

# 未完成: 可能 Failed 或超时；走失败路径
set +e
${KUBECTL} -n "${NAMESPACE}" wait --for=condition=failed \
    --timeout=10s "job/${JOB}" >/dev/null 2>&1
failed_status=$?
set -e

echo
echo "[wait-job] ${JOB} 未正常完成（complete_status=${complete_status}, failed_status=${failed_status}）。"
echo "[wait-job] describe:"
${KUBECTL} -n "${NAMESPACE}" describe "job/${JOB}" || true

echo
echo "[wait-job] Pod 日志:"
mapfile -t pods < <(${KUBECTL} -n "${NAMESPACE}" get pods \
    -l "job-name=${JOB}" -o name 2>/dev/null || true)
if (( ${#pods[@]} == 0 )); then
    echo "  （未找到 Job ${JOB} 的 Pod）"
else
    for p in "${pods[@]}"; do
        echo "----- ${p} -----"
        ${KUBECTL} -n "${NAMESPACE}" logs "${p}" --all-containers=true --tail=500 || true
    done
fi
exit 1
