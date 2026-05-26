#!/usr/bin/env bash
#
# scripts/local_destroy_kind_dev.sh
#
# v1.7 Local-First Data Runtime - 删除专用 kind cluster `robot-dh-dev`。
#
# 安全约束：
#   1. 仅删除名为 robot-dh-dev 的集群；
#      想删别的（含默认 robot-dh），请用 kind delete cluster --name <name>。
#   2. 必须二次确认（输入 DELETE_DEV_KIND）。
#   3. 不会删除 D 盘上的数据；只删 kind container。
#
# 用法:
#   ./scripts/local_destroy_kind_dev.sh
#   ./scripts/local_destroy_kind_dev.sh --name robot-dh-dev

set -euo pipefail

CLUSTER_NAME="robot-dh-dev"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) CLUSTER_NAME="$2"; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

if [[ "$CLUSTER_NAME" == "robot-dh" ]]; then
  echo "ERROR: 拒绝通过本脚本删除生产/默认 kind 'robot-dh'。" >&2
  echo "如确需，请用 kind delete cluster --name robot-dh。" >&2
  exit 2
fi

for c in kind; do
  command -v "$c" >/dev/null 2>&1 || { echo "ERROR: 缺少 $c" >&2; exit 3; }
done

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "kind cluster '${CLUSTER_NAME}' 不存在，无需删除。"
  exit 0
fi

echo "WARNING: 即将删除 kind cluster '${CLUSTER_NAME}'。"
echo "  - D 盘数据 (ROBOT_DH_LOCAL_DATA_ROOT) 不会被删除。"
echo "  - 已部署到该 cluster 的 K8s 资源（PV/PVC/Pod/Workflow）会随集群一起销毁。"
echo "请输入 DELETE_DEV_KIND 二次确认（其它任何输入将取消）："
read -r confirm
if [[ "$confirm" != "DELETE_DEV_KIND" ]]; then
  echo "已取消。"
  exit 4
fi

kind delete cluster --name "$CLUSTER_NAME"
echo "kind cluster '${CLUSTER_NAME}' 已删除。"
