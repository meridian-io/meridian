#!/usr/bin/env bash
# Patches the local test cluster CR to Idle with the Docker Compose Trino URL.
set -euo pipefail

NS=${MERIDIAN_NAMESPACE:-meridian}

kubectl apply -f hack/cluster-local.yaml

kubectl patch cluster trino-local -n "$NS" --subresource=status --type=merge -p '{
  "status": {
    "phase": "Idle",
    "ready": true,
    "coordinatorURL": "http://localhost:8080",
    "idleAt": "2026-02-28T10:00:00Z"
  }
}'

echo "trino-local cluster CR ready:"
kubectl get cluster trino-local -n "$NS" -o jsonpath='{.status}' | python3 -m json.tool
