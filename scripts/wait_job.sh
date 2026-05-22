#!/usr/bin/env bash
set -euo pipefail

if kubectl wait --for=condition=complete job/robot-dh-validator -n robot-dh --timeout=300s; then
  exit 0
fi

echo "Job 未成功完成。正在 describe Job 与 Pod ..." >&2
kubectl -n robot-dh describe job robot-dh-validator >&2 || true
kubectl -n robot-dh get pods -l job-name=robot-dh-validator >&2 || true
kubectl -n robot-dh describe pods -l job-name=robot-dh-validator >&2 || true
kubectl -n robot-dh logs job/robot-dh-validator --all-containers=true >&2 || true
exit 1
