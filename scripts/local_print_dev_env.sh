#!/usr/bin/env bash
#
# scripts/local_print_dev_env.sh
#
# v1.7 Local-First Data Runtime - 打印一段 shell 可 `source` 的 env，
# 让本地 Argo workflow / robot-dh CLI 直接读 D 盘镜像数据。
#
# 不会改写当前 shell 的环境；只是把建议的 export 打到 stdout。
#
# 用法:
#   ./scripts/local_print_dev_env.sh                           # 打印
#   eval "$(./scripts/local_print_dev_env.sh)"                  # 直接 export
#   ./scripts/local_print_dev_env.sh > client/robot-dh-dev.env  # 落盘

set -euo pipefail

DEFAULT_ROOT="/mnt/d/robot-dh-local"
ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT:-${DEFAULT_ROOT}}"

# 默认 kind container 内挂载点；只有当用户在 kind config 里改过 containerPath
# 才需要覆盖此处。
K8S_LOCAL_DATA_ROOT="${ROBOT_DH_K8S_LOCAL_DATA_ROOT:-/mnt/local-data/robot-dh-local}"

cat <<EOF
# v1.7 Local-First Data Runtime - dev env
# 生成时间: $(date -u +%Y-%m-%dT%H:%M:%SZ)
# WSL 路径: ${ROBOT_DH_LOCAL_DATA_ROOT}
# kind/pod 路径: ${K8S_LOCAL_DATA_ROOT}
#
# 注意：
#   - 本片段**不会**覆盖远端 ROBOT_DH_DB_URI / ROBOT_DH_S3_* 等业务 env，
#     远端 secret 与本地 dev 数据是两件事。
#   - Argo workflow 用 dataset_uri=file://... 时不再走公网，速度提升来源
#     于 hostPath 挂载（kind extraMounts）。

export ROBOT_DH_LOCAL_DATA_ROOT="${ROBOT_DH_LOCAL_DATA_ROOT}"
export ROBOT_DH_K8S_LOCAL_DATA_ROOT="${K8S_LOCAL_DATA_ROOT}"

export ROBOT_DH_DEV_DATA_ROOT="file://${K8S_LOCAL_DATA_ROOT}/raw"
export ROBOT_DH_DEV_LAKE_ROOT="file://${K8S_LOCAL_DATA_ROOT}/lake"
export ROBOT_DH_INPUT_CACHE_DIR="${K8S_LOCAL_DATA_ROOT}/cache/input-cache"

# Argo workflow 默认 dataset_uri（推荐覆盖到具体 dataset）：
#   robot-dh-multisource-scale30 / contract-qc / ml-ready
#   --parameter dataset_uri=file://${K8S_LOCAL_DATA_ROOT}/raw/<dataset_id>/<version>
EOF
