#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="robot-dh"
DEBUG_POD="robot-dh-debug"

if [[ $# -ne 2 ]]; then
  echo "用法: $0 <dataset_name_in_pvc> <local_output_dir>" >&2
  exit 1
fi

DATASET_NAME="$1"
OUTPUT_DIR="$2"

mkdir -p "$OUTPUT_DIR"
kubectl -n "$NAMESPACE" get pod "$DEBUG_POD" >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/"$DEBUG_POD" --timeout=120s >/dev/null
kubectl cp "$NAMESPACE/$DEBUG_POD:/artifacts/$DATASET_NAME/." "$OUTPUT_DIR"

if [[ ! -f "$OUTPUT_DIR/report.json" ]]; then
  echo "已复制的产物中未找到 report.json: $OUTPUT_DIR" >&2
  exit 1
fi

if [[ ! -f "$OUTPUT_DIR/report.html" ]]; then
  echo "已复制的产物中未找到 report.html: $OUTPUT_DIR" >&2
  exit 1
fi

ls -lah "$OUTPUT_DIR"
