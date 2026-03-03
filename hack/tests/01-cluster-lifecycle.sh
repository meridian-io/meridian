#!/usr/bin/env bash
# Test: Single Cluster Lifecycle
# Verifies that the operator moves a cluster through all phases:
# Created → Pending → Idle → Reserved → Idle → Deleted
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
CLUSTER="qe-cluster-$SUFFIX"

cleanup() {
  kubectl delete cluster "$CLUSTER" -n "$NS" --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "01 · Cluster Lifecycle"

# ── Create ────────────────────────────────────────────────────────────────────
kubectl apply -f - &>/dev/null <<EOF
apiVersion: meridian.io/v1alpha1
kind: Cluster
metadata:
  name: $CLUSTER
  namespace: $NS
spec:
  profile: default
  image: $TEST_IMAGE
  workers: 1
EOF

# 1. Cluster appears and operator picks it up (Pending or already Idle — nginx
#    starts fast enough that Pending can be skipped in a single poll interval).
INITIAL_PHASE=""
for _ in $(seq 1 10); do
  INITIAL_PHASE=$(kubectl get cluster "$CLUSTER" -n "$NS" \
    -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  [[ -n "$INITIAL_PHASE" ]] && break
  sleep 2
done
if [[ "$INITIAL_PHASE" == "Pending" || "$INITIAL_PHASE" == "Idle" ]]; then
  pass "Cluster created and picked up by operator (phase: $INITIAL_PHASE)"
else
  fail "Cluster did not reach Pending or Idle within 20s (phase: $INITIAL_PHASE)"
fi

# 2. Operator created coordinator and worker Deployments
if kubectl get deployment "${CLUSTER}-coordinator" -n "$NS" &>/dev/null && \
   kubectl get deployment "${CLUSTER}-worker"      -n "$NS" &>/dev/null; then
  pass "Coordinator and worker Deployments created"
else
  fail "Deployments not created by operator"
fi

# 3. Operator created coordinator Service
if kubectl get service "${CLUSTER}-coordinator" -n "$NS" &>/dev/null; then
  pass "Coordinator Service created"
else
  fail "Service not created by operator"
fi

# 4. Cluster reaches Idle once coordinator pod is ready (nginx starts fast)
if wait_for_phase "$CLUSTER" "Idle" 90; then
  pass "Cluster transitioned to Idle (coordinator healthy)"
else
  fail "Cluster did not reach Idle within 90s"
fi

# 5. Reserve the cluster
kubectl patch cluster "$CLUSTER" -n "$NS" --type=merge \
  -p "{\"spec\":{\"clientId\":\"qe-client\",\"reservationId\":\"res-$SUFFIX\"}}" &>/dev/null

if wait_for_phase "$CLUSTER" "Reserved" 30; then
  pass "Cluster reserved (phase = Reserved)"
else
  fail "Cluster did not reach Reserved"
fi

# 6. Release the cluster
kubectl patch cluster "$CLUSTER" -n "$NS" --type=merge \
  -p '{"spec":{"clientId":"","reservationId":""}}' &>/dev/null

if wait_for_phase "$CLUSTER" "Idle" 30; then
  pass "Cluster released back to Idle"
else
  fail "Cluster did not return to Idle after release"
fi

# 7. Delete and verify owned resources are cleaned up
kubectl delete cluster "$CLUSTER" -n "$NS" &>/dev/null
if wait_for_deletion "$CLUSTER" 30; then
  pass "Cluster deleted cleanly"
else
  fail "Cluster deletion timed out"
fi

sleep 5  # allow GC
if ! kubectl get deployment "${CLUSTER}-coordinator" -n "$NS" &>/dev/null && \
   ! kubectl get service    "${CLUSTER}-coordinator" -n "$NS" &>/dev/null; then
  pass "Owned Deployments and Service garbage-collected"
else
  fail "Owned resources not cleaned up after cluster deletion"
fi

suite_end
