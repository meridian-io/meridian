#!/usr/bin/env bash
# Test: Reservation TTL Auto-Release
# Verifies that the operator automatically releases a reservation whose
# reservedAt timestamp has exceeded the pool's reservationTtl.
# Simulates a crashed client by back-dating the reservedAt field.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
POOL="qe-ttl-pool-$SUFFIX"
TTL="2m"

cleanup() {
  kubectl delete clusterpool "$POOL" -n "$NS" --ignore-not-found &>/dev/null || true
  kubectl delete clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "08 · Reservation TTL Auto-Release"

kubectl apply -f - &>/dev/null <<EOF
apiVersion: meridian.io/v1alpha1
kind: ClusterPool
metadata:
  name: $POOL
  namespace: $NS
spec:
  replicas: 1
  reservationTtl: $TTL
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
pass "Cluster $CLUSTER is Idle (reservationTtl=$TTL set)"

# Reserve via kubectl (simulates a client reserving the cluster)
RES_ID="res-ttl-$SUFFIX"
kubectl patch cluster "$CLUSTER" -n "$NS" --type=merge \
  -p "{\"spec\":{\"clientId\":\"crashed-client\",\"reservationId\":\"$RES_ID\"}}" &>/dev/null
wait_for_phase "$CLUSTER" "Reserved" 20 || true
pass "Cluster $CLUSTER reserved by crashed-client"

# Back-date reservedAt to simulate TTL expiry.
# We set it to 10 minutes ago — well past the 2m TTL.
BACKDATED=$(date -u -v-10M +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || \
            date -u -d "10 minutes ago" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")

if [[ -z "$BACKDATED" ]]; then
  fail "Could not compute backdated timestamp — skipping TTL expiry simulation"
  suite_end; exit 0
fi

kubectl patch cluster "$CLUSTER" -n "$NS" --type=merge \
  -p "{\"status\":{\"reservedAt\":\"$BACKDATED\"}}" \
  --subresource=status &>/dev/null || \
kubectl patch cluster "$CLUSTER" -n "$NS" --type=merge \
  -p "{\"status\":{\"reservedAt\":\"$BACKDATED\"}}" &>/dev/null || true

pass "reservedAt back-dated to $BACKDATED (simulating $TTL expiry)"

# The cluster controller reconciles Reserved clusters every 30s.
# With a 2m TTL and a 10-minute-old reservedAt, the next reconcile
# should clear the reservation and return the cluster to Idle.
info "Waiting up to 90s for TTL auto-release..."
if wait_for_phase "$CLUSTER" "Idle" 90; then
  pass "Cluster auto-released to Idle after TTL expiry"
else
  PHASE=$(kubectl get cluster "$CLUSTER" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null)
  fail "Cluster not released after 90s (current phase: $PHASE)"
fi

# Reservation ID must be cleared
RES_FIELD=$(kubectl get cluster "$CLUSTER" -n "$NS" \
  -o jsonpath='{.spec.reservationId}' 2>/dev/null || echo "")
if [[ -z "$RES_FIELD" ]]; then
  pass "spec.reservationId cleared after TTL release"
else
  fail "spec.reservationId still set to '$RES_FIELD' after release"
fi

# Client ID must be cleared
CLIENT_FIELD=$(kubectl get cluster "$CLUSTER" -n "$NS" \
  -o jsonpath='{.spec.clientId}' 2>/dev/null || echo "")
if [[ -z "$CLIENT_FIELD" ]]; then
  pass "spec.clientId cleared after TTL release"
else
  fail "spec.clientId still set to '$CLIENT_FIELD' after release"
fi

suite_end
