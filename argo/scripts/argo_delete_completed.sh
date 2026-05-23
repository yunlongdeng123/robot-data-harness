#!/usr/bin/env bash
# 清理 robot-dh namespace 中已完成的 Workflow / WorkflowTaskResult。
set -euo pipefail

NS="${ROBOT_DH_NS:-robot-dh}"

if ! kubectl -n "${NS}" get workflows.argoproj.io >/dev/null 2>&1; then
  echo "[argo-delete] ${NS} 下没有 workflows 资源"
  exit 0
fi

DEL_PHASES=("Succeeded" "Failed" "Error")
for phase in "${DEL_PHASES[@]}"; do
  names=$(kubectl -n "${NS}" get workflows.argoproj.io -o json \
    | python3 -c "import sys, json; data=json.load(sys.stdin);
items=[i for i in data.get('items', []) if i.get('status',{}).get('phase') == '${phase}'];
print('\n'.join(i['metadata']['name'] for i in items))")
  if [[ -n "${names}" ]]; then
    echo "[argo-delete] phase=${phase} 中待清理: $(echo "${names}" | wc -l) 条"
    while IFS= read -r name; do
      [[ -z "${name}" ]] && continue
      kubectl -n "${NS}" delete workflows.argoproj.io "${name}"
    done <<<"${names}"
  fi
done
echo "[argo-delete] 完成。"
