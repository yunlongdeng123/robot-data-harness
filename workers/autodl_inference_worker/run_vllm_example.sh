#!/usr/bin/env bash
# 在 AutoDL GPU 容器内启动 vLLM OpenAI-compatible endpoint（示例，仅供参考）。
#
# 注意：
#   - 这是 AutoDL GPU 容器内运行示例，无 GPU 环境不保证可运行。
#   - 不要在普通 AutoDL 容器里再跑 Docker（Docker-in-Docker）。
#   - 不要把核心数据放 AutoDL 临时盘；重要输出必须写回 MinIO。
#   - endpoint 可用 SSH tunnel 或安全组白名单访问；不要把 9000/5432 等管理端口暴露公网。
set -euo pipefail

MODEL="${ROBOT_DH_OPENAI_COMPATIBLE_MODEL:-Qwen/Qwen2.5-0.5B-Instruct}"
HOST="${VLLM_HOST:-0.0.0.0}"
PORT="${VLLM_PORT:-8000}"

echo "[vllm] starting OpenAI-compatible server: model=${MODEL} host=${HOST} port=${PORT}"
exec python -m vllm.entrypoints.openai.api_server \
  --model "${MODEL}" \
  --host "${HOST}" \
  --port "${PORT}"
