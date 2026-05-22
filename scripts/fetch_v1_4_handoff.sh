#!/usr/bin/env bash
#
# fetch_v1_4_handoff.sh
#
# 一次性脚本：从云端服务器（robot-dh-infra @ /opt/robot-dh-infra）
# 拉取 v1.4 基础设施交接物到本 WSL 主工程。
#
# 目录布局与交接清单一致：
#   client/{robot-dh-lake.env.example,wsl-open-tunnels.sh,wsl-remote-doctor.sh,
#           wsl-access-checklist.md,wsl-public-access-checklist.md,
#           k8s-lake-secret.example.yaml,k8s-create-lake-secret.example.sh}
#   docs/{lake_layout.md,v1_4_infra_runbook.md}
#   postgres/migrations/001_lake_metadata.sql
#   minio/policies/robot_dh_lake_readwrite.json
#   docs/remote_assets_<ts>.json   （最近一次清单快照）
#   ~/.config/robot-dh/robot-dh-lake.env  （真实密码 env，0600，在仓库外）
#
# 前置条件:
#   - 本 WSL shell 可 SSH 到云端（测试:
#     `ssh "$SSH_TARGET" 'whoami && hostname'`）。
#   - 有权在服务器上执行带 --show-secrets 的 24 脚本。
#
# 用法:
#   scripts/fetch_v1_4_handoff.sh                  # 完整交接
#   scripts/fetch_v1_4_handoff.sh --skip-secrets   # 不重新生成真实密码 env
#   SSH_TARGET=ubuntu@82.156.129.81 scripts/fetch_v1_4_handoff.sh
#
set -euo pipefail

SSH_TARGET="${SSH_TARGET:-ubuntu@82.156.129.81}"
REMOTE_ROOT="${REMOTE_ROOT:-/opt/robot-dh-infra}"
REMOTE_LOGS_GLOB="${REMOTE_LOGS_GLOB:-/data/robot-dh/logs/remote_assets_*.json}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_ENV_DIR="${LOCAL_ENV_DIR:-$HOME/.config/robot-dh}"
LOCAL_ENV_PATH="${LOCAL_ENV_PATH:-$LOCAL_ENV_DIR/robot-dh-lake.env}"

SKIP_SECRETS=0
for arg in "$@"; do
  case "$arg" in
    --skip-secrets) SKIP_SECRETS=1 ;;
    -h|--help)
      sed -n '2,30p' "$0"
      exit 0
      ;;
    *) echo "未知参数: $arg" >&2; exit 2 ;;
  esac
done

log()  { printf '[fetch] %s\n' "$*" >&2; }
warn() { printf '[fetch][warn] %s\n' "$*" >&2; }
die()  { printf '[fetch][err]  %s\n' "$*" >&2; exit 1; }

cd "$REPO_ROOT"

log "SSH 探测 ($SSH_TARGET)"
if ! ssh -o BatchMode=yes -o ConnectTimeout=15 "$SSH_TARGET" 'whoami && hostname && uname -s' >/dev/null 2>&1; then
  die "SSH 连接 $SSH_TARGET 失败。请先修复 WSL 出站（常见为代理劫持），再重试。"
fi
log "SSH 正常"

mkdir -p \
  "$REPO_ROOT/client" \
  "$REPO_ROOT/docs" \
  "$REPO_ROOT/postgres/migrations" \
  "$REPO_ROOT/minio/policies"

mkdir -p "$LOCAL_ENV_DIR" && chmod 700 "$LOCAL_ENV_DIR"

scp_quiet() {
  local src="$1" dst="$2" label="$3" optional="${4:-required}"
  if scp -q -o ConnectTimeout=15 "$SSH_TARGET":"$src" "$dst" 2>/dev/null; then
    log "  成功 $label"
    return 0
  fi
  if [[ "$optional" == "optional" ]]; then
    warn "  缺失 $label  （可选，源: $src）"
    return 0
  fi
  die "拉取失败: $label  （源: $src）"
}

log "1/5  client/  （必需）"
scp_quiet "$REMOTE_ROOT/client/robot-dh-lake.env.example"   client/                                   "client/robot-dh-lake.env.example"
scp_quiet "$REMOTE_ROOT/client/wsl-open-tunnels.sh"         client/                                   "client/wsl-open-tunnels.sh"
scp_quiet "$REMOTE_ROOT/client/wsl-remote-doctor.sh"        client/                                   "client/wsl-remote-doctor.sh"
scp_quiet "$REMOTE_ROOT/client/wsl-access-checklist.md"     client/                                   "client/wsl-access-checklist.md"

log "2/5  client/  （K8s 直连模式，推荐）"
scp_quiet "$REMOTE_ROOT/client/k8s-lake-secret.example.yaml"      client/                            "client/k8s-lake-secret.example.yaml"      optional
scp_quiet "$REMOTE_ROOT/client/k8s-create-lake-secret.example.sh" client/                            "client/k8s-create-lake-secret.example.sh" optional
scp_quiet "$REMOTE_ROOT/client/wsl-public-access-checklist.md"    client/                            "client/wsl-public-access-checklist.md"    optional

log "3/5  docs/"
scp_quiet "$REMOTE_ROOT/docs/lake_layout.md"                docs/                                    "docs/lake_layout.md"
scp_quiet "$REMOTE_ROOT/docs/v1_4_infra_runbook.md"         docs/                                    "docs/v1_4_infra_runbook.md"

log "4/5  postgres + minio 参考资产"
scp_quiet "$REMOTE_ROOT/postgres/migrations/001_lake_metadata.sql"    postgres/migrations/           "postgres/migrations/001_lake_metadata.sql"
scp_quiet "$REMOTE_ROOT/minio/policies/robot_dh_lake_readwrite.json"  minio/policies/                "minio/policies/robot_dh_lake_readwrite.json"

log "5/5  最新远程资产清单"
REMOTE_LATEST=$(ssh -o ConnectTimeout=15 "$SSH_TARGET" "ls -1t $REMOTE_LOGS_GLOB 2>/dev/null | head -n1" || true)
if [[ -n "$REMOTE_LATEST" ]]; then
  base=$(basename "$REMOTE_LATEST")
  scp_quiet "$REMOTE_LATEST" "docs/$base"                                                            "docs/$base"
else
  warn "远端未找到 $REMOTE_LOGS_GLOB；ETL 清单需另行获取"
fi

chmod +x client/wsl-open-tunnels.sh client/wsl-remote-doctor.sh 2>/dev/null || true
if [[ -f client/k8s-create-lake-secret.example.sh ]]; then
  chmod +x client/k8s-create-lake-secret.example.sh
fi

if [[ "$SKIP_SECRETS" == "1" ]]; then
  warn "跳过真实密码 env 生成（--skip-secrets）"
else
  log "真实密码 lake env（经 $REMOTE_ROOT/scripts/24_export_lake_client_env.sh --show-secrets）"
  if ssh -o ConnectTimeout=20 "$SSH_TARGET" \
       "cd $REMOTE_ROOT && test -x ./scripts/24_export_lake_client_env.sh"; then
    ssh -o ConnectTimeout=30 "$SSH_TARGET" \
      "cd $REMOTE_ROOT && ./scripts/24_export_lake_client_env.sh --show-secrets" >/dev/null
    scp_quiet "$REMOTE_ROOT/client/robot-dh-lake.env" "$LOCAL_ENV_PATH" "$LOCAL_ENV_PATH"
    chmod 600 "$LOCAL_ENV_PATH"
    log "  成功 真实密码 env -> $LOCAL_ENV_PATH （权限 0600）"
  else
    warn "远端未找到 scripts/24_export_lake_client_env.sh；跳过真实密码 env"
  fi
fi

log "完成。请运行 scripts/verify_v1_4_handoff.sh 进行校验。"
