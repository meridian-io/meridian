#!/usr/bin/env bash
# Test: Degraded Phase Detection
# Verifies that when a coordinator pod becomes unavailable while a cluster
# is Idle, the operator detects it and the pool automatically replaces the cluster.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
POOL="qe-deg-pool-$SUFFIX"

cleanup() {
  # Undo any deployment scaling we did (in case test failed mid-way)
  for c in $(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
      -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl scale deployment "${c}-coordinator" -n "$NS" --replicas=1 &>/dev/null || true
  done
  kubectl delete clusterpool "$POOL" -n "$NS" --ignore-not-found &>/dev/null || true
  kubectl delete clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "05 · Degraded Phase Detection"

kubectl apply -f - &>/dev/null <<EOF
apiVersion: meridian.io/v1alpha1
kind: ClusterPool
metadata:
  name: $POOL
  namespace: $NS
spec:
  replicas: 1
  template:
    profile: default
    image: $TEST_IMAGE
    workers: 1
EOF

info "Waiting for 1 Idle cluster..."
wait_for_pool_count "$POOL" "Idle" 1 120

CLUSTER=$(pool_cluster_with_phase "$POOL" "Idle")
if [[ -z "$CLUSTER" ]]; then
  fail "No Idle cluster found to test with"
  suite_end; exit 1
fi

pass "Cluster $CLUSTER is Idle (baseline confirmed)"

# 1. Simulate coordinator failure: scale deployment to 0 ready replicas
kubectl scale deployment "${CLUSTER}-coordinator" -n "$NS" --replicas=0 &>/dev/null
pass "Coordinator scaled to 0 replicas (simulating pod crash)"

# 2. Operator's periodic health check detects the failure → Degraded
# reconcileIdle runs every 30s, so allow up to 40s
if wait_for_phase "$CLUSTER" "Degraded" 45; then
  pass "Cluster transitioned to Degraded (unhealthy coordinator detected)"
else
  PHASE=$(kubectl get cluster "$CLUSTER" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
  fail "Cluster did not reach Degraded (current phase: $PHASE)"
fi

# 3. Pool controller deletes the Degraded cluster (runs every 30s)
if wait_for_deletion "$CLUSTER" 60; then
  pass "Pool deleted Degraded cluster automatically"
else
  fail "Pool did not delete Degraded cluster within 60s"
fi

# 4. Pool creates a replacement cluster
if wait_for_pool_count "$POOL" "Pending" 1 60; then
  pass "Pool created a replacement cluster"
else
  # It may have already gone Idle
  IDLE=$(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$IDLE" -ge 1 ]]; then
    pass "Replacement cluster already progressed past Pending"
  else
    fail "Pool did not create a replacement cluster"
  fi
fi

suite_end
