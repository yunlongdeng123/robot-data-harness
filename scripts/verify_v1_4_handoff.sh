#!/usr/bin/env bash
#
# verify_v1_4_handoff.sh
#
# 在 WSL 接收端对 v1.4 交接物做静态校验。
# 不访问网络或远端基础设施；连通性部分在 SSH 就绪后
# 交由 client/wsl-remote-doctor.sh。
#
# 检查项:
#   1. 必需文件是否齐全
#   2. 真实密码 lake env 是否存在于 ~/.config/robot-dh/ 且为 0600
#   3. 该 env 中是否定义 9 个约定环境变量
#   4. .gitignore 是否覆盖所有含密钥的文件
#   5. （可选）对比示例 env 与真实 env 的键差异
#
# 用法:
#   scripts/verify_v1_4_handoff.sh
#   LOCAL_ENV_PATH=/custom/path scripts/verify_v1_4_handoff.sh
#
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_ENV_PATH="${LOCAL_ENV_PATH:-$HOME/.config/robot-dh/robot-dh-lake.env}"
EXAMPLE_ENV_PATH="$REPO_ROOT/client/robot-dh-lake.env.example"

CONTRACT_VARS=(
  ROBOT_DH_DB_URI
  ROBOT_DH_ARTIFACT_STORE
  ROBOT_DH_S3_ENDPOINT_URL
  ROBOT_DH_S3_ACCESS_KEY
  ROBOT_DH_S3_SECRET_KEY
  ROBOT_DH_S3_DATA_BUCKET
  ROBOT_DH_S3_ARTIFACT_BUCKET
  ROBOT_DH_S3_LAKE_BUCKET
  ROBOT_DH_REDIS_URL
)

REQUIRED_FILES=(
  "client/robot-dh-lake.env.example"
  "client/wsl-open-tunnels.sh"
  "client/wsl-remote-doctor.sh"
)

# 应通过 SSH 从云端获取的文件。SSH 不通时，
# 接收端重建文件（postgres/migrations/*.reconstructed.sql、
# minio/policies/*.reconstructed.json 等）作为兜底并产生 WARN。
PREFERRED_THEN_FALLBACK=(
  "postgres/migrations/001_lake_metadata.sql|postgres/migrations/001_lake_metadata.reconstructed.sql"
  "minio/policies/robot_dh_lake_readwrite.json|minio/policies/robot_dh_lake_readwrite.reconstructed.json"
)

# 纯设计/运维文档 — SSH 不通时无法在接收端合理重建。缺失时一律 WARN。
SSH_ONLY_FILES=(
  "docs/lake_layout.md"
  "docs/v1_4_infra_runbook.md"
  "client/wsl-access-checklist.md"
)

OPTIONAL_FILES=(
  "client/k8s-lake-secret.example.yaml"
  "client/k8s-create-lake-secret.example.sh"
  "client/wsl-public-access-checklist.md"
)

GITIGNORE_PATTERNS=(
  "client/*.env"
  "client/wsl-export-*.sh"
  "client/k8s-lake-secret.yaml"
  "k8s/secret.yaml"
)

c_red()   { printf '\033[31m%s\033[0m' "$1"; }
c_green() { printf '\033[32m%s\033[0m' "$1"; }
c_yellow(){ printf '\033[33m%s\033[0m' "$1"; }

PASS=0; FAIL=0; WARN=0
ok()   { printf '  [%s] %s\n' "$(c_green OK)"   "$1"; PASS=$((PASS+1)); }
bad()  { printf '  [%s] %s\n' "$(c_red FAIL)"   "$1"; FAIL=$((FAIL+1)); }
warn() { printf '  [%s] %s\n' "$(c_yellow WARN)" "$1"; WARN=$((WARN+1)); }

section() { printf '\n== %s ==\n' "$1"; }

cd "$REPO_ROOT"

section "1. 必需文件（v1.4 必备）"
for f in "${REQUIRED_FILES[@]}"; do
  if [[ -e "$f" ]]; then ok "$f"; else bad "$f  （缺失）"; fi
done

section "1b. 优先从 SSH 获取（兜底: 接收端重建）"
for entry in "${PREFERRED_THEN_FALLBACK[@]}"; do
  preferred="${entry%%|*}"
  fallback="${entry##*|}"
  if [[ -e "$preferred" ]]; then
    ok "$preferred  （权威版本）"
  elif [[ -e "$fallback" ]]; then
    warn "$preferred 缺失；使用 $fallback （重建版，SSH 恢复后请替换）"
  else
    bad "$preferred 与 $fallback 均缺失"
  fi
done

section "1c. 仅 SSH 可获取的设计文档（无法重建）"
for f in "${SSH_ONLY_FILES[@]}"; do
  if [[ -e "$f" ]]; then
    ok "$f"
  else
    warn "$f  （仅 SSH；云端连通后通过 scp 获取）"
  fi
done

section "2. 可选文件（K8s 直连模式）"
for f in "${OPTIONAL_FILES[@]}"; do
  if [[ -e "$f" ]]; then ok "$f"; else warn "$f  （缺失，仅在使用 K8s 直连时需要）"; fi
done

section "3. 远程资产清单快照"
inv_count=$(ls -1 docs/remote_assets_*.json 2>/dev/null | wc -l)
if [[ "$inv_count" -ge 1 ]]; then
  latest_inv=$(ls -1t docs/remote_assets_*.json | head -n1)
  ok "找到 $inv_count 个快照；最新 = $latest_inv"
else
  warn "无 docs/remote_assets_*.json 快照（ETL 清单）"
fi

section "4. 真实密码 lake env（仓库外）"
if [[ -f "$LOCAL_ENV_PATH" ]]; then
  mode=$(stat -c '%a' "$LOCAL_ENV_PATH" 2>/dev/null || stat -f '%Lp' "$LOCAL_ENV_PATH" 2>/dev/null)
  if [[ "$mode" == "600" ]]; then
    ok "$LOCAL_ENV_PATH  （权限 0600）"
  else
    bad "$LOCAL_ENV_PATH  （权限 $mode，须为 0600）。修复: chmod 600 $LOCAL_ENV_PATH"
  fi
else
  bad "$LOCAL_ENV_PATH  （缺失）。请运行 scripts/fetch_v1_4_handoff.sh 生成。"
fi

section "5. 真实密码 env 中的 9 个约定变量"
if [[ -f "$LOCAL_ENV_PATH" ]]; then
  missing=()
  for v in "${CONTRACT_VARS[@]}"; do
    if grep -qE "^[[:space:]]*(export[[:space:]]+)?$v=" "$LOCAL_ENV_PATH"; then
      ok "$v"
    else
      bad "$v  （在 $LOCAL_ENV_PATH 中缺失）"
      missing+=("$v")
    fi
  done
  if [[ "${#missing[@]}" -gt 0 ]]; then
    echo
    echo "  提示: 主工程要求 ROBOT_DH_ARTIFACT_STORE，但导出脚本可能未包含。"
    echo "        若缺失请手动添加:"
    echo "          echo 'ROBOT_DH_ARTIFACT_STORE=s3' >> $LOCAL_ENV_PATH"
  fi
else
  warn "跳过（env 文件缺失）"
fi

section "6. 示例 env 与真实 env（键集合差异）"
if [[ -f "$EXAMPLE_ENV_PATH" && -f "$LOCAL_ENV_PATH" ]]; then
  ex_keys=$(grep -oE '^[A-Z_][A-Z0-9_]*=' "$EXAMPLE_ENV_PATH" | sort -u)
  rl_keys=$(grep -oE '^[A-Z_][A-Z0-9_]*=' "$LOCAL_ENV_PATH"   | sort -u)
  diff_only_example=$(comm -23 <(echo "$ex_keys") <(echo "$rl_keys"))
  diff_only_real=$(comm -13 <(echo "$ex_keys") <(echo "$rl_keys"))
  if [[ -z "$diff_only_example" && -z "$diff_only_real" ]]; then
    ok "示例与真实密码 env 的键集合一致"
  else
    [[ -n "$diff_only_example" ]] && warn "仅在示例中存在: $(echo "$diff_only_example" | tr '\n' ' ')"
    [[ -n "$diff_only_real" ]]    && warn "仅在真实 env 中存在: $(echo "$diff_only_real" | tr '\n' ' ')"
  fi
else
  warn "跳过（需要 $EXAMPLE_ENV_PATH 与 $LOCAL_ENV_PATH）"
fi

section "7. .gitignore 保护所有密钥文件"
if [[ -f .gitignore ]]; then
  for p in "${GITIGNORE_PATTERNS[@]}"; do
    if grep -qxF "$p" .gitignore; then
      ok ".gitignore 已覆盖 '$p'"
    else
      bad ".gitignore 缺少模式 '$p'"
    fi
  done
else
  bad ".gitignore 不存在"
fi

section "汇总"
printf '  通过=%s  警告=%s  失败=%s\n' "$(c_green "$PASS")" "$(c_yellow "$WARN")" "$(c_red "$FAIL")"
echo
if [[ "$FAIL" -eq 0 ]]; then
  echo "  下一步: source $LOCAL_ENV_PATH && ./client/wsl-remote-doctor.sh"
  exit 0
else
  exit 1
fi
