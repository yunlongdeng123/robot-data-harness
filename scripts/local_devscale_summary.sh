#!/usr/bin/env bash
#
# scripts/local_devscale_summary.sh
#
# v1.7 Local-First Data Runtime - 输出本地 devscale 数据集摘要：
#   - dataset_id / family / version / file_count / size_bytes
#   - 每个 dataset 的 _manifest.json 状态
#   - 推荐的 Argo workflow 入参（dataset_uri 走 K8s hostPath 路径）
#
# 用法:
#   ./scripts/local_devscale_summary.sh
#   ./scripts/local_devscale_summary.sh --json /tmp/summary.json

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"
CONFIG="${REPO_ROOT}/configs/devscale_datasets.yaml"
OUTPUT_JSON=""

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG="$2"; shift 2 ;;
    --json) OUTPUT_JSON="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

command -v python3 >/dev/null 2>&1 || { echo "ERROR: 缺少 python3" >&2; exit 2; }

ROOT_ARG=()
if [[ "$ROBOT_DH_LOCAL_DATA_ROOT" != "$DEFAULT_ROOT" ]]; then
  ROOT_ARG=(--root "$ROBOT_DH_LOCAL_DATA_ROOT")
fi
JSON_ARG=()
[[ -n "$OUTPUT_JSON" ]] && JSON_ARG=(--output-json "$OUTPUT_JSON")

python3 "${REPO_ROOT}/scripts/_local_devscale_lib.py" summary \
  --config "$CONFIG" \
  "${ROOT_ARG[@]}" \
  "${JSON_ARG[@]}"
