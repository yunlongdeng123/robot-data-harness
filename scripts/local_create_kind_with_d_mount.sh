#!/usr/bin/env bash
#
# scripts/local_create_kind_with_d_mount.sh
#
# v1.7 Local-First Data Runtime - 创建专用 kind cluster `robot-dh-dev`，
# 通过 extraMounts 把 D 盘 robot-dh-local 挂到 node /mnt/local-data/。
#
# 默认 config：configs/kind-robot-dh-dev-local.yaml
#
# 行为：
#   - 已存在 cluster 默认不重建
#   - 重建需要 --recreate + 二次确认（输入 RECREATE_KIND）
#   - 创建完成后 verify：kubectl get nodes & docker exec ls 挂载点
#
# 用法:
#   ./scripts/local_create_kind_with_d_mount.sh
#   ./scripts/local_create_kind_with_d_mount.sh --recreate
#   ./scripts/local_create_kind_with_d_mount.sh --config configs/kind-robot-dh-dev-local.yaml

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd -P)"
CLUSTER_NAME="robot-dh-dev"
CONFIG="${REPO_ROOT}/configs/kind-robot-dh-dev-local.yaml"
RECREATE="false"
DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"

usage() {
  grep -E '^#( |$)' "$0" | sed -E 's/^# ?//'
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) CLUSTER_NAME="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    --recreate) RECREATE="true"; shift ;;
    -h|--help) usage 0 ;;
    *) echo "ERROR: 未知选项: $1" >&2; usage 1 ;;
  esac
done

for c in docker kind kubectl; do
  command -v "$c" >/dev/null 2>&1 || { echo "ERROR: 缺少 $c" >&2; exit 2; }
done

if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: kind config 不存在: $CONFIG" >&2
  exit 3
fi

if [[ ! -d "$ROBOT_DH_LOCAL_DATA_ROOT" ]]; then
  echo "ERROR: hostPath 不存在: $ROBOT_DH_LOCAL_DATA_ROOT" >&2
  echo "请先运行 ./scripts/local_init_data_dirs.sh" >&2
  exit 4
fi

# 校验 config 里的 hostPath 与当前 ROBOT_DH_LOCAL_DATA_ROOT 一致；
# 不一致直接 fail（避免一段时间后误以为 hostPath 已生效）。
# 注意：yaml 里 hostPath 写在 list item 内（"- hostPath: /xxx"），前缀含 "- "
# 而非纯空白，所以不能用 "^[[:space:]]*hostPath:" 这种行首正则；
# 改为跳过注释行后用 token 匹配，兼容两种 yaml 写法。
_yaml_extract() {
  # $1=key（不含冒号）, $2=yaml 文件
  awk -v key="$1:" '
    /^[[:space:]]*#/ { next }
    {
      for (i = 1; i <= NF; i++) {
        if ($i == key) { print $(i + 1); exit }
      }
    }
  ' "$2"
}

HOST_PATH_IN_CFG="$(_yaml_extract hostPath "$CONFIG")"
if [[ "$HOST_PATH_IN_CFG" != "$ROBOT_DH_LOCAL_DATA_ROOT" ]]; then
  echo "ERROR: kind config 中 hostPath=${HOST_PATH_IN_CFG}，" >&2
  echo "       与 ROBOT_DH_LOCAL_DATA_ROOT=${ROBOT_DH_LOCAL_DATA_ROOT} 不一致。" >&2
  echo "       请同步修改 ${CONFIG}，或 export ROBOT_DH_LOCAL_DATA_ROOT 到一致的值。" >&2
  exit 5
fi

CONTAINER_PATH_IN_CFG="$(_yaml_extract containerPath "$CONFIG")"
if [[ -z "$CONTAINER_PATH_IN_CFG" ]]; then
  echo "ERROR: kind config 中没有 containerPath" >&2
  exit 5
fi

EXISTS="false"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  EXISTS="true"
fi

if [[ "$EXISTS" == "true" ]]; then
  if [[ "$RECREATE" != "true" ]]; then
    echo "kind cluster '${CLUSTER_NAME}' 已存在，跳过创建。"
    echo "如确需重建，请加 --recreate。"
    echo
    echo "验证当前 cluster:"
    kubectl --context "kind-${CLUSTER_NAME}" get nodes -o wide
    exit 0
  fi
  echo "WARNING: 将删除并重建 kind cluster '${CLUSTER_NAME}'。"
  echo "请输入 RECREATE_KIND 二次确认（其它任何输入将取消）："
  read -r confirm
  if [[ "$confirm" != "RECREATE_KIND" ]]; then
    echo "已取消。"
    exit 6
  fi
  kind delete cluster --name "$CLUSTER_NAME"
fi

echo "创建 kind cluster '${CLUSTER_NAME}'（config=${CONFIG}）..."
kind create cluster --name "$CLUSTER_NAME" --config "$CONFIG"

CTX="kind-${CLUSTER_NAME}"
echo "等待 control-plane Ready..."
kubectl --context "$CTX" wait --for=condition=Ready node --all --timeout=120s

echo
echo "=== 节点信息 ==="
kubectl --context "$CTX" get nodes -o wide

NODE_NAME="${CLUSTER_NAME}-control-plane"
echo
echo "=== docker exec ls ${CONTAINER_PATH_IN_CFG} on ${NODE_NAME} ==="
if docker exec "$NODE_NAME" ls -la "$CONTAINER_PATH_IN_CFG" 2>/dev/null; then
  echo
  echo "OK：hostPath ${ROBOT_DH_LOCAL_DATA_ROOT} 已映射到 node ${CONTAINER_PATH_IN_CFG}。"
else
  echo "WARNING: 在 node 内未列到 ${CONTAINER_PATH_IN_CFG}；请检查 Docker Desktop 是否共享了 D 盘。" >&2
  exit 7
fi

echo
echo "下一步："
echo "  kubectl config use-context ${CTX}"
echo "  make local-apply-data-pvc"
echo "  make local-data-debug"
