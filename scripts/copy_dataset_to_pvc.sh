#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="robot-dh"
DEBUG_POD="robot-dh-debug"

if [[ $# -ne 2 ]]; then
  echo "用法: $0 <local_dataset_dir> <dataset_name_in_pvc>" >&2
  exit 1
fi

SOURCE_DIR="$1"
DATASET_NAME="$2"

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "本地数据集目录不存在: $SOURCE_DIR" >&2
  exit 1
fi

kubectl get namespace "$NAMESPACE" >/dev/null
kubectl -n "$NAMESPACE" get pod "$DEBUG_POD" >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$DEBUG_POD" --timeout=120s >/dev/null
kubectl -n "$NAMESPACE" exec "$DEBUG_POD" -- mkdir -p "/data/$DATASET_NAME"
kubectl cp "$SOURCE_DIR/." "$NAMESPACE/$DEBUG_POD:/data/$DATASET_NAME"
kubectl -n "$NAMESPACE" exec "$DEBUG_POD" -- ls -lah "/data/$DATASET_NAME"
