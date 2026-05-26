#!/usr/bin/env bash
# v1.7 Local：workflow 终态后，把 step / archive_log_uri 写回 PG。
# 与 v1.6 ``make argo-sync-latest`` 等价，但默认 namespace 与 archive root
# 指向本地 file URI；当 PG 不可用时回退到 dry-run 不抛。
#
# 用法：
#   ./argo/v1_7_local/scripts/sync_workflow_steps.sh <workflow-name> [--ns robot-dh] \
#       [--archive-root file:///mnt/local-data/robot-dh-local/lake/argo-logs] [--dry-run]

set -euo pipefail

WF="${1:-}"
NS="robot-dh"
ARCHIVE_ROOT="file:///mnt/local-data/robot-dh-local/lake/argo-logs"
DRY_RUN=""
shift || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ns)            NS="$2"; shift 2 ;;
    --archive-root)  ARCHIVE_ROOT="$2"; shift 2 ;;
    --dry-run)       DRY_RUN="--dry-run"; shift ;;
    --help|-h)       sed -n '1,15p' "$0"; exit 0 ;;
    *)               shift ;;
  esac
done

if [[ -z "${WF}" ]]; then
  echo "usage: $0 <workflow-name> [--ns robot-dh] [--archive-root file://...] [--dry-run]" >&2
  exit 2
fi

PYTHON="${PYTHON:-python3}"

echo "[sync] argo sync ${WF}"
${PYTHON} -m robot_dh.cli argo sync \
  --workflow-name "${WF}" \
  --namespace "${NS}" \
  --log-format json || echo "[sync] argo sync 失败（PG 可能未连）"

echo ""
echo "[sync] argo logs index ${WF} (archive_root=${ARCHIVE_ROOT}, dry_run=${DRY_RUN:-no})"
${PYTHON} -m robot_dh.cli argo logs index \
  --workflow-name "${WF}" \
  --namespace "${NS}" \
  --archive-root "${ARCHIVE_ROOT}" \
  ${DRY_RUN} \
  --log-format json
