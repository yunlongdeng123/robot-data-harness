#!/usr/bin/env bash
set -euo pipefail

SSH_HOST="${SSH_HOST:-robot-dh-tencent}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_PORT="${SSH_PORT:-22}"
SSH_IDENTITY_FILE="${SSH_IDENTITY_FILE:-}"

ssh_args=(
  -NT
  -o ExitOnForwardFailure=yes
  -o ServerAliveInterval=30
  -o ServerAliveCountMax=3
  -p "$SSH_PORT"
  -L 15432:127.0.0.1:5432
  -L 19000:127.0.0.1:9000
  -L 19001:127.0.0.1:9001
  -L 16379:127.0.0.1:6379
)

if [[ -n "$SSH_IDENTITY_FILE" ]]; then
  ssh_args+=( -i "$SSH_IDENTITY_FILE" )
fi

exec ssh "${ssh_args[@]}" "${SSH_USER}@${SSH_HOST}"
