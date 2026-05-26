#!/usr/bin/env bash
#
# scripts/local_plan_devscale_sync.sh
#
# v1.7 Local-First Data Runtime - 读取 configs/devscale_datasets.yaml，
# 列举远端对象，按 include/exclude/max_files/max_bytes 生成下载计划。
#
# 输出：
#   $ROBOT_DH_LOCAL_DATA_ROOT/manifests/devscale_plan.json
#   $ROBOT_DH_LOCAL_DATA_ROOT/manifests/devscale_plan.md
#
# 用法:
#   source client/robot-dh-v1-6.env
#   ./scripts/local_mc_alias_remote.sh
#   ./scripts/local_plan_devscale_sync.sh
#   ./scripts/local_plan_devscale_sync.sh --allow-over-limit

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"
CONFIG="${REPO_ROOT}/configs/devscale_datasets.yaml"
ALIAS_NAME="${ROBOT_DH_MC_ALIAS_NAME:-robotdh-remote}"
ALLOW_OVER_LIMIT="false"
ALLOW_NON_D="false"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG="$2"; shift 2 ;;
    --alias) ALIAS_NAME="$2"; shift 2 ;;
    --allow-over-limit) ALLOW_OVER_LIMIT="true"; shift ;;
    --allow-non-d-drive) ALLOW_NON_D="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

case "$ROBOT_DH_LOCAL_DATA_ROOT" in
  /mnt/c/*)
    echo "ERROR: 拒绝把 plan 写到 /mnt/c。" >&2
    exit 2
    ;;
  /mnt/d/*) : ;;
  /mnt/*)
    [[ "$ALLOW_NON_D" == "true" ]] || {
      echo "ERROR: ROBOT_DH_LOCAL_DATA_ROOT 不在 /mnt/d/，请加 --allow-non-d-drive。" >&2
      exit 3
    }
    ;;
  *)
    echo "ERROR: ROBOT_DH_LOCAL_DATA_ROOT 必须是 WSL /mnt/<drive>/... 路径" >&2
    exit 3
    ;;
esac

for c in mc yq jq python3; do
  command -v "$c" >/dev/null 2>&1 || { echo "ERROR: 缺少 $c" >&2; exit 4; }
done

if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: 配置文件不存在: $CONFIG" >&2
  exit 5
fi

if ! mc alias list "$ALIAS_NAME" >/dev/null 2>&1; then
  echo "ERROR: mc alias '$ALIAS_NAME' 未注册；请先执行 ./scripts/local_mc_alias_remote.sh" >&2
  exit 6
fi

LISTINGS_DIR="${ROBOT_DH_LOCAL_DATA_ROOT}/manifests/listings"
MANIFEST_DIR="${ROBOT_DH_LOCAL_DATA_ROOT}/manifests"
mkdir -p "$LISTINGS_DIR" "$MANIFEST_DIR"

echo "枚举远端对象（mc ls --json --recursive）..."
mapfile -t DS_IDS < <(yq -r '.datasets[].dataset_id' "$CONFIG")
mapfile -t DS_URIS < <(yq -r '.datasets[].source_uri' "$CONFIG")

if (( ${#DS_IDS[@]} != ${#DS_URIS[@]} )); then
  echo "ERROR: yaml 解析异常：dataset_id / source_uri 数量不一致" >&2
  exit 7
fi

for i in "${!DS_IDS[@]}"; do
  ds_id="${DS_IDS[$i]}"
  src_uri="${DS_URIS[$i]}"
  out_path="${LISTINGS_DIR}/${ds_id}.jsonl"
  echo "  - ${ds_id}: ${src_uri}"
  # 把 s3://bucket/prefix 转成 alias/bucket/prefix
  src_path="${src_uri#s3://}"
  mc ls --json --recursive "${ALIAS_NAME}/${src_path}" > "$out_path" 2> "${out_path}.err" || {
    echo "WARNING: mc ls 失败（${ds_id}），保留 ${out_path}.err 供排查。" >&2
  }
done

echo "生成 plan..."
ROOT_ARG=()
case "$ROBOT_DH_LOCAL_DATA_ROOT" in
  "$DEFAULT_ROOT") : ;;
  *) ROOT_ARG=(--root "$ROBOT_DH_LOCAL_DATA_ROOT") ;;
esac

ALLOW_ARG=()
[[ "$ALLOW_OVER_LIMIT" == "true" ]] && ALLOW_ARG=(--allow-over-limit)

PYTHONPATH="${REPO_ROOT}/src" python3 "${REPO_ROOT}/scripts/_local_devscale_lib.py" plan \
  --config "$CONFIG" \
  --listings-dir "$LISTINGS_DIR" \
  --output-json "${MANIFEST_DIR}/devscale_plan.json" \
  --output-md "${MANIFEST_DIR}/devscale_plan.md" \
  "${ROOT_ARG[@]}" \
  "${ALLOW_ARG[@]}"

echo
echo "计划已生成；下一步:"
echo "  ./scripts/local_sync_devscale.sh"
