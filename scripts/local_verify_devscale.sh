#!/usr/bin/env bash
#
# scripts/local_verify_devscale.sh
#
# v1.7 Local-First Data Runtime - 校验本地 devscale 与 devscale_plan.json 一致。
#
# 检查项：
#   1. plan 中每个文件都存在于 dst_path
#   2. 大小匹配
#   3. 每个 dataset 的 _manifest.json 存在
#
# 输出：
#   $ROBOT_DH_LOCAL_DATA_ROOT/manifests/devscale_verify_report.json
#   $ROBOT_DH_LOCAL_DATA_ROOT/manifests/devscale_verify_report.md
#
# 退出码：
#   0 全部 OK
#   1 缺失 / 大小不符 / manifest 缺失 / plan 不存在

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"
ALLOW_NON_D="false"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-non-d-drive) ALLOW_NON_D="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

case "$ROBOT_DH_LOCAL_DATA_ROOT" in
  /mnt/c/*) echo "ERROR: 拒绝在 /mnt/c 下 verify。" >&2; exit 2 ;;
  /mnt/d/*) : ;;
  /mnt/*)
    [[ "$ALLOW_NON_D" == "true" ]] || { echo "ERROR: 非 /mnt/d，请加 --allow-non-d-drive。" >&2; exit 3; }
    ;;
  *) echo "ERROR: ROBOT_DH_LOCAL_DATA_ROOT 必须是 WSL /mnt/<drive>/..." >&2; exit 3 ;;
esac

for c in python3; do
  command -v "$c" >/dev/null 2>&1 || { echo "ERROR: 缺少 $c" >&2; exit 4; }
done

PLAN="${ROBOT_DH_LOCAL_DATA_ROOT}/manifests/devscale_plan.json"
if [[ ! -f "$PLAN" ]]; then
  echo "ERROR: plan 不存在：$PLAN" >&2
  exit 5
fi

REPORT_JSON="${ROBOT_DH_LOCAL_DATA_ROOT}/manifests/devscale_verify_report.json"
REPORT_MD="${ROBOT_DH_LOCAL_DATA_ROOT}/manifests/devscale_verify_report.md"
python3 "${REPO_ROOT}/scripts/_local_devscale_lib.py" verify \
  --plan "$PLAN" \
  --output-json "$REPORT_JSON" \
  --output-md "$REPORT_MD"
