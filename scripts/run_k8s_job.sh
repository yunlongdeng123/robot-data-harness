#!/usr/bin/env bash
set -euo pipefail

kubectl delete job robot-dh-validator -n robot-dh --ignore-not-found
kubectl apply -f k8s/validator-job.yaml
