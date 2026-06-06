#!/usr/bin/env bash
# 启动 AutoDL 推理 worker。
#
# 前置：
#   1) pip install -e /path/to/robot-data-harness   # 复用主项目 robot_dh
#   2) cp config.example.env config.env && 填真实值
#   3) set -a; source config.env; set +a
#
# 用法：
#   ./run_worker.sh --dry-run --max-jobs 1
#   ./run_worker.sh --model-id openai-compatible-chat-v1 --max-jobs 4
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z "${ROBOT_DH_DB_URI:-}" ]]; then
  echo "ERROR: 未设置 ROBOT_DH_DB_URI；先 set -a; source config.env; set +a" >&2
  exit 2
fi

exec python "${SCRIPT_DIR}/worker.py" "$@"
