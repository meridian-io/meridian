#!/usr/bin/env bash
# Patches status subresources on test Cluster fixtures.
# kubectl does not set status on apply — it must be patched separately.
set -euo pipefail

NS=${MERIDIAN_NAMESPACE:-meridian}

echo "Patching cluster status in namespace: $NS"

kubectl patch cluster trino-idle-01 -n "$NS" --subresource=status --type=merge -p '{
  "status": {
    "phase": "Idle",
    "ready": true,
    "coordinatorURL": "http://trino-idle-01.meridian.svc.cluster.local:8080",
    "idleAt": "2026-02-28T08:00:00Z"
  }
}'

kubectl patch cluster trino-idle-02 -n "$NS" --subresource=status --type=merge -p '{
  "status": {
    "phase": "Idle",
    "ready": true,
    "coordinatorURL": "http://trino-idle-02.meridian.svc.cluster.local:8080",
    "idleAt": "2026-02-28T09:00:00Z"
  }
}'

kubectl patch cluster trino-reserved-01 -n "$NS" --subresource=status --type=merge -p '{
  "status": {
    "phase": "Reserved",
    "ready": true,
    "coordinatorURL": "http://trino-reserved-01.meridian.svc.cluster.local:8080",
    "idleAt": "2026-02-28T07:00:00Z",
    "reservedAt": "2026-02-28T10:00:00Z"
  }
}'

kubectl patch cluster trino-pending-01 -n "$NS" --subresource=status --type=merge -p '{
  "status": {
    "phase": "Pending",
    "ready": false,
    "message": "Waiting for coordinator pod to become ready"
  }
}'

echo "Status patches applied."
kubectl get clusters -n "$NS"
