#!/usr/bin/env bash
#
# scripts/local_sync_devscale.sh
#
# v1.7 Local-First Data Runtime - 按 devscale_plan.json 把对象从远端 MinIO
# 同步到 $ROBOT_DH_LOCAL_DATA_ROOT/raw/...。
#
# 特性：
#   - 并发：默认 4（--concurrency N）
#   - 已存在且大小一致的文件 -> skip
#   - 每个文件单独最多重试 3 次（--retries N）
#   - 临时目录使用 $ROBOT_DH_LOCAL_DATA_ROOT/tmp（不写 /tmp 或 /mnt/c）
#   - 完成后自动调用 local_verify_devscale.sh（--no-auto-verify 跳过）
#
# 用法:
#   ./scripts/local_sync_devscale.sh
#   ./scripts/local_sync_devscale.sh --concurrency 8 --retries 5
#   ./scripts/local_sync_devscale.sh --no-auto-verify

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"
ALIAS_NAME="${ROBOT_DH_MC_ALIAS_NAME:-robotdh-remote}"
CONCURRENCY=4
RETRIES=3
ALLOW_NON_D="false"
AUTO_VERIFY="true"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --alias) ALIAS_NAME="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --retries) RETRIES="$2"; shift 2 ;;
    --no-auto-verify) AUTO_VERIFY="false"; shift ;;
    --allow-non-d-drive) ALLOW_NON_D="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

case "$ROBOT_DH_LOCAL_DATA_ROOT" in
  /mnt/c/*)
    echo "ERROR: 拒绝把数据下载到 /mnt/c。" >&2
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

PLAN="${ROBOT_DH_LOCAL_DATA_ROOT}/manifests/devscale_plan.json"
if [[ ! -f "$PLAN" ]]; then
  echo "ERROR: plan 不存在：$PLAN；请先执行 ./scripts/local_plan_devscale_sync.sh" >&2
  exit 5
fi

if ! mc alias list "$ALIAS_NAME" >/dev/null 2>&1; then
  echo "ERROR: mc alias '$ALIAS_NAME' 未注册" >&2
  exit 6
fi

WORK_DIR="${ROBOT_DH_LOCAL_DATA_ROOT}/tmp/sync_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$WORK_DIR"
export TMPDIR="$WORK_DIR"

# 生成 src/dst 列表
SYNC_LIST="${WORK_DIR}/sync_list.tsv"
python3 "${REPO_ROOT}/scripts/_local_devscale_lib.py" sync-pre \
  --plan "$PLAN" \
  --output "$SYNC_LIST"

TOTAL=$(wc -l < "$SYNC_LIST" | tr -d ' ')
if [[ "$TOTAL" -eq 0 ]]; then
  echo "WARNING: plan 中没有任何文件，跳过下载。"
  exit 0
fi
echo "准备同步 ${TOTAL} 个文件（concurrency=${CONCURRENCY} retries=${RETRIES}）"

RESULTS="${WORK_DIR}/results.tsv"
: > "$RESULTS"

# 单文件下载函数：src=s3://bucket/key dst=/abs/path
download_one() {
  local ds_id="$1" src="$2" dst="$3" expected_size="$4"
  local alias="$5" retries="$6" results_file="$7"

  # 已存在且大小一致 -> skip
  if [[ -f "$dst" ]]; then
    local actual
    actual="$(stat -c%s "$dst" 2>/dev/null || echo 0)"
    if [[ "$actual" == "$expected_size" ]]; then
      printf '%s\t%s\t%s\t%s\tok\n' "$ds_id" "$src" "$dst" "$expected_size" >> "$results_file"
      return 0
    fi
  fi

  mkdir -p "$(dirname "$dst")"
  local src_path="${src#s3://}"
  local attempt=0
  while (( attempt < retries )); do
    attempt=$(( attempt + 1 ))
    if mc cp --quiet "${alias}/${src_path}" "$dst" >/dev/null 2>&1; then
      local actual
      actual="$(stat -c%s "$dst" 2>/dev/null || echo 0)"
      if [[ "$actual" == "$expected_size" ]]; then
        printf '%s\t%s\t%s\t%s\tok\n' "$ds_id" "$src" "$dst" "$expected_size" >> "$results_file"
        return 0
      fi
      # 大小不一致也算失败，下一轮重试前先删
      rm -f "$dst"
    fi
    sleep $(( attempt * 2 ))
  done
  printf '%s\t%s\t%s\t%s\tfail\n' "$ds_id" "$src" "$dst" "$expected_size" >> "$results_file"
  return 1
}
export -f download_one

# xargs 并发：每行一个任务，按 NUL 分割
# 为了避免 export 函数的兼容性问题，把每行写成 shell 命令再 xargs。
CMD_LIST="${WORK_DIR}/cmd_list.sh"
: > "$CMD_LIST"
while IFS=$'\t' read -r ds_id src dst size; do
  printf 'download_one %q %q %q %q %q %q %q\n' \
    "$ds_id" "$src" "$dst" "$size" "$ALIAS_NAME" "$RETRIES" "$RESULTS" >> "$CMD_LIST"
done < "$SYNC_LIST"

set +e
# 用 xargs -P 控制并发；每行单独走 bash -c 加载 export 的函数。
# 注意：先 export -f download_one；xargs 会继承 env。
xargs -a "$CMD_LIST" -d '\n' -P "$CONCURRENCY" -I {} bash -c '{}'
XARGS_EXIT=$?
set -e

OK_COUNT=$(awk -F'\t' '$5=="ok"' "$RESULTS" | wc -l | tr -d ' ')
FAIL_COUNT=$(awk -F'\t' '$5=="fail"' "$RESULTS" | wc -l | tr -d ' ')
echo
echo "同步完成: ok=${OK_COUNT}  fail=${FAIL_COUNT}  (xargs exit=${XARGS_EXIT})"

# 写 manifest + sync_report
SYNC_REPORT="${ROBOT_DH_LOCAL_DATA_ROOT}/manifests/devscale_sync_report.json"
python3 "${REPO_ROOT}/scripts/_local_devscale_lib.py" sync-post \
  --plan "$PLAN" \
  --results "$RESULTS" \
  --output "$SYNC_REPORT" || true

echo "sync report: $SYNC_REPORT"

if [[ "$AUTO_VERIFY" == "true" ]]; then
  echo "自动调用 verify..."
  "${REPO_ROOT}/scripts/local_verify_devscale.sh" || {
    echo "ERROR: verify 失败" >&2
    exit 7
  }
fi

if (( FAIL_COUNT > 0 )); then
  echo "ERROR: 仍有 ${FAIL_COUNT} 个文件失败，请检查 ${RESULTS}" >&2
  exit 8
fi
