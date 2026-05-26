#!/usr/bin/env bash
#
# scripts/local_preflight_d_drive.sh
#
# v1.7 Local-First Data Runtime - 本机 D 盘 / 工具链 preflight。
#
# 检查项：
#   1. ROBOT_DH_LOCAL_DATA_ROOT 是否落在 /mnt/d 或 /mnt/<drive> 等
#      非 C 盘的 WSL 挂载点（非 /mnt/d 必须显式 --allow-non-d-drive）。
#   2. 该目录可用空间 >= 10 GB（默认；--min-free-gb 覆盖）。
#   3. docker / kind / kubectl / mc / jq / yq 是否安装。
#   4. 当前 shell 工作目录不是 /mnt/c（避免误把仓库放到 C 盘）。
#
# 用法:
#   ./scripts/local_preflight_d_drive.sh
#   ROBOT_DH_LOCAL_DATA_ROOT=/mnt/e/robot-dh-local ./scripts/local_preflight_d_drive.sh --allow-non-d-drive
#
# 输出：
#   $ROBOT_DH_LOCAL_DATA_ROOT/logs/preflight_YYYYmmdd_HHMMSS.json
#
# 退出码：
#   0 全部 OK
#   2 缺少必要工具
#   3 路径不在允许范围且未传 --allow-non-d-drive
#   4 可用空间不足

set -euo pipefail

DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"

ALLOW_NON_D="false"
MIN_FREE_GB=10
JSON_ONLY="false"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-non-d-drive) ALLOW_NON_D="true"; shift ;;
    --min-free-gb) MIN_FREE_GB="$2"; shift 2 ;;
    --json) JSON_ONLY="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

# 工具用 ts 简写，避免大段 printf。
ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log() { [[ "$JSON_ONLY" == "true" ]] || echo "[$(date -u +%H:%M:%SZ)] $*"; }
err() { echo "ERROR: $*" >&2; }

# 各工具版本输出语法不一致：kubectl 不支持 --version；额外 || true 兜底，
# 防止任意工具 stderr/exit 干扰 set -euo pipefail 把整脚本拖死。
tool_version() {
  local bin="$1"
  case "$bin" in
    kubectl) "$bin" version --client 2>&1 | head -n1 || true ;;
    *)       "$bin" --version 2>&1 | head -n1 || true ;;
  esac
}

# 1) ROOT 是否在 /mnt/d 或允许的非 D 盘
case "$ROBOT_DH_LOCAL_DATA_ROOT" in
  /mnt/d/*) ROOT_DRIVE_OK="true"; ROOT_DRIVE="d" ;;
  /mnt/c/*) ROOT_DRIVE_OK="false"; ROOT_DRIVE="c" ;;
  /mnt/*)
    ROOT_DRIVE="$(echo "$ROBOT_DH_LOCAL_DATA_ROOT" | awk -F/ '{print $3}')"
    if [[ "$ALLOW_NON_D" == "true" ]]; then
      ROOT_DRIVE_OK="true"
    else
      ROOT_DRIVE_OK="false"
    fi
    ;;
  *)
    ROOT_DRIVE="unknown"
    ROOT_DRIVE_OK="false"
    ;;
esac

# 2) 当前 cwd 不能落在 /mnt/c
CWD="$(pwd -P)"
case "$CWD" in
  /mnt/c/*) CWD_OK="false" ;;
  *) CWD_OK="true" ;;
esac

# 3) 工具检测
declare -A TOOLS=(
  [docker]=docker
  [kind]=kind
  [kubectl]=kubectl
  [mc]=mc
  [jq]=jq
  [yq]=yq
)
TOOLS_OK="true"
TOOLS_JSON=""
for name in "${!TOOLS[@]}"; do
  if command -v "${TOOLS[$name]}" >/dev/null 2>&1; then
    ver="$(tool_version "${TOOLS[$name]}" | tr -d '"' | tr -d "'" )"
    TOOLS_JSON+="\"${name}\":{\"installed\":true,\"version\":\"${ver}\"},"
  else
    TOOLS_JSON+="\"${name}\":{\"installed\":false,\"version\":null},"
    TOOLS_OK="false"
  fi
done
TOOLS_JSON="${TOOLS_JSON%,}"

# 4) df -h 与可用空间（用 df --output=avail -B1）
FREE_BYTES=0
DF_MOUNT="-"
TARGET_FOR_DF="$ROBOT_DH_LOCAL_DATA_ROOT"
# 如果 ROOT 还没建，用其父目录探测。
while [[ ! -d "$TARGET_FOR_DF" && "$TARGET_FOR_DF" != "/" ]]; do
  TARGET_FOR_DF="$(dirname "$TARGET_FOR_DF")"
done
if [[ -d "$TARGET_FOR_DF" ]]; then
  FREE_BYTES="$(df -B1 --output=avail "$TARGET_FOR_DF" 2>/dev/null | tail -n1 | tr -d ' ' || echo 0)"
  DF_MOUNT="$(df --output=target "$TARGET_FOR_DF" 2>/dev/null | tail -n1 || echo -)"
fi
FREE_GB=$(( FREE_BYTES / 1024 / 1024 / 1024 ))
if (( FREE_GB >= MIN_FREE_GB )); then
  FREE_OK="true"
else
  FREE_OK="false"
fi

# 5) /mnt/d 是否存在
if [[ -d "/mnt/d" ]]; then
  MNT_D_EXISTS="true"
else
  MNT_D_EXISTS="false"
fi

# 综合判定
OVERALL="ok"
EXIT_CODE=0
if [[ "$TOOLS_OK" != "true" ]]; then
  OVERALL="missing_tools"
  EXIT_CODE=2
elif [[ "$ROOT_DRIVE_OK" != "true" ]]; then
  OVERALL="root_not_on_allowed_drive"
  EXIT_CODE=3
elif [[ "$FREE_OK" != "true" ]]; then
  OVERALL="insufficient_disk"
  EXIT_CODE=4
fi

# 输出 JSON 到日志文件
LOG_DIR="${ROBOT_DH_LOCAL_DATA_ROOT}/logs"
mkdir -p "$LOG_DIR"
TS_LOCAL="$(date +%Y%m%d_%H%M%S)"
REPORT_PATH="${LOG_DIR}/preflight_${TS_LOCAL}.json"

cat > "$REPORT_PATH" <<EOF
{
  "generated_at": "$(ts)",
  "robot_dh_local_data_root": "${ROBOT_DH_LOCAL_DATA_ROOT}",
  "root_drive": "${ROOT_DRIVE}",
  "root_drive_ok": ${ROOT_DRIVE_OK},
  "allow_non_d_drive": ${ALLOW_NON_D},
  "mnt_d_exists": ${MNT_D_EXISTS},
  "cwd": "${CWD}",
  "cwd_on_c_drive_ok": ${CWD_OK},
  "df_mount": "${DF_MOUNT}",
  "free_bytes": ${FREE_BYTES},
  "free_gb": ${FREE_GB},
  "min_free_gb": ${MIN_FREE_GB},
  "free_ok": ${FREE_OK},
  "tools": {${TOOLS_JSON}},
  "tools_ok": ${TOOLS_OK},
  "overall": "${OVERALL}",
  "exit_code": ${EXIT_CODE}
}
EOF

if [[ "$JSON_ONLY" == "true" ]]; then
  cat "$REPORT_PATH"
else
  log "ROBOT_DH_LOCAL_DATA_ROOT = ${ROBOT_DH_LOCAL_DATA_ROOT}  (drive=${ROOT_DRIVE} ok=${ROOT_DRIVE_OK})"
  log "/mnt/d exists           = ${MNT_D_EXISTS}"
  log "df mount                = ${DF_MOUNT}"
  log "free space              = ${FREE_GB} GB (min ${MIN_FREE_GB} GB) ok=${FREE_OK}"
  log "current working dir     = ${CWD}  (on /mnt/c? $( [[ $CWD_OK == true ]] && echo no || echo YES ))"
  log "tools_ok                = ${TOOLS_OK}"
  for name in "${!TOOLS[@]}"; do
    if command -v "${TOOLS[$name]}" >/dev/null 2>&1; then
      log "  - ${name}: $(tool_version "${TOOLS[$name]}")"
    else
      log "  - ${name}: MISSING"
    fi
  done
  log "overall                 = ${OVERALL}"
  log "report                  = ${REPORT_PATH}"
fi

if (( EXIT_CODE != 0 )); then
  case "$OVERALL" in
    missing_tools)
      err "缺少工具，请安装后重试（docker/kind/kubectl/mc/jq/yq）。"
      ;;
    root_not_on_allowed_drive)
      err "ROBOT_DH_LOCAL_DATA_ROOT=${ROBOT_DH_LOCAL_DATA_ROOT} 不在 /mnt/d/ 下；"
      err "如确需用其它 WSL 挂载点，请追加 --allow-non-d-drive 重试。"
      err "**不要**使用 /mnt/c：会塞满 C 盘并占用 WSL VHDX。"
      ;;
    insufficient_disk)
      err "可用空间不足：${FREE_GB} GB < ${MIN_FREE_GB} GB"
      ;;
  esac
fi

exit "$EXIT_CODE"
