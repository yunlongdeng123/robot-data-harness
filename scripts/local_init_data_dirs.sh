#!/usr/bin/env bash
#
# scripts/local_init_data_dirs.sh
#
# v1.7 Local-First Data Runtime - 在 ROBOT_DH_LOCAL_DATA_ROOT 下创建
# 固定目录结构。可重复执行，不删除已有数据（除非显式 --force-clean）。
#
# 目录结构：
#   $ROBOT_DH_LOCAL_DATA_ROOT/
#     raw/
#       droid_lerobot_dev1g/v1/
#       robomimic_dev1g/v1/
#       bridgedata_v2_dev/v1/
#     lake/{ods,dwd,ads,qc,ml-ready,tmp}
#     cache/{input-cache,argo-workdir}
#     manifests/
#     logs/
#     tmp/
#
# 用法:
#   ./scripts/local_init_data_dirs.sh
#   ./scripts/local_init_data_dirs.sh --allow-non-d-drive
#   ROBOT_DH_LOCAL_DATA_ROOT=/mnt/e/robot-dh-local ./scripts/local_init_data_dirs.sh --allow-non-d-drive

set -euo pipefail

DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"
ALLOW_NON_D="false"
FORCE_CLEAN="false"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-non-d-drive) ALLOW_NON_D="true"; shift ;;
    --force-clean) FORCE_CLEAN="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

case "$ROBOT_DH_LOCAL_DATA_ROOT" in
  /mnt/c/*)
    echo "ERROR: 拒绝在 /mnt/c 下创建 robot-dh-local；会塞满 C 盘 / WSL VHDX。" >&2
    exit 2
    ;;
  /mnt/d/*)
    : ;;
  /mnt/*)
    if [[ "$ALLOW_NON_D" != "true" ]]; then
      echo "WARNING: ROBOT_DH_LOCAL_DATA_ROOT=${ROBOT_DH_LOCAL_DATA_ROOT} 不在 /mnt/d/ 下。" >&2
      echo "如确需用该路径，请追加 --allow-non-d-drive。" >&2
      exit 3
    fi
    ;;
  *)
    echo "ERROR: ROBOT_DH_LOCAL_DATA_ROOT 必须是 WSL /mnt/<drive>/... 路径；当前=${ROBOT_DH_LOCAL_DATA_ROOT}" >&2
    exit 3
    ;;
esac

if [[ "$FORCE_CLEAN" == "true" ]]; then
  echo "WARNING: 你传了 --force-clean，将清空 ${ROBOT_DH_LOCAL_DATA_ROOT}。"
  echo "请输入 YES 二次确认（其它任何输入将取消）："
  read -r confirm
  if [[ "$confirm" != "YES" ]]; then
    echo "已取消。"
    exit 4
  fi
  rm -rf -- "${ROBOT_DH_LOCAL_DATA_ROOT:?}/raw" \
            "${ROBOT_DH_LOCAL_DATA_ROOT:?}/lake" \
            "${ROBOT_DH_LOCAL_DATA_ROOT:?}/cache" \
            "${ROBOT_DH_LOCAL_DATA_ROOT:?}/manifests" \
            "${ROBOT_DH_LOCAL_DATA_ROOT:?}/tmp" \
            "${ROBOT_DH_LOCAL_DATA_ROOT:?}/logs"
fi

DIRS=(
  "raw/droid_lerobot_dev1g/v1"
  "raw/robomimic_dev1g/v1"
  "raw/bridgedata_v2_dev/v1"
  "lake/ods"
  "lake/dwd"
  "lake/ads"
  "lake/qc"
  "lake/ml-ready"
  "lake/tmp"
  "cache/input-cache"
  "cache/argo-workdir"
  "manifests"
  "logs"
  "tmp"
)

for d in "${DIRS[@]}"; do
  mkdir -p "${ROBOT_DH_LOCAL_DATA_ROOT}/${d}"
done

# 写一个 README（仅首次创建），帮 Windows 资源管理器里的人看清这是干啥的。
README="${ROBOT_DH_LOCAL_DATA_ROOT}/README.txt"
if [[ ! -f "$README" ]]; then
  cat > "$README" <<EOF
robot-dh-local (v1.7 Local-First Data Runtime)
==============================================

本目录是 robot-data-harness v1.7 的本地数据根目录。

约定：
- WSL 路径:   ${ROBOT_DH_LOCAL_DATA_ROOT}
- Windows:    通常对应 D:\\robot-dh-local
- kind node:  /mnt/local-data/robot-dh-local
- Pod 内:     /mnt/local-data/robot-dh-local

子目录：
  raw/            devscale 镜像数据（droid_lerobot_dev1g / robomimic_dev1g / bridgedata_v2_dev）
  lake/           本地 ods / dwd / ads / qc / ml-ready 产物
  cache/          input-cache / argo-workdir，用于跨 pod-retry 复用
  manifests/      devscale_plan.json / devscale_sync_report.json / devscale_verify_report.json
  logs/           preflight / sync / verify 日志
  tmp/            临时目录（不入 git，可随时清空）

警告：
- **不要**把整 scale30 数据集塞到本目录；scale30 走远端 Argo workflow。
- **不要**把这个目录链接到 /mnt/c 或 WSL ext4 vhdx。
EOF
fi

# 写一个 .robot-dh-local-marker，让其它脚本能识别这是 v1.7 根目录。
MARKER="${ROBOT_DH_LOCAL_DATA_ROOT}/.robot-dh-local-marker"
echo "{\"created_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"version\":\"v1.7\"}" > "$MARKER"

echo "已就绪：${ROBOT_DH_LOCAL_DATA_ROOT}"
echo "下一步："
echo "  ./scripts/local_mc_alias_remote.sh"
echo "  ./scripts/local_plan_devscale_sync.sh"
