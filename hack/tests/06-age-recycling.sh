#!/usr/bin/env bash
# Test: Cluster Age Recycling
# Verifies that the pool replaces clusters older than maxClusterAge.
# Uses a 1-minute max age — test waits 90 seconds.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
POOL="qe-age-pool-$SUFFIX"

cleanup() {
  kubectl delete clusterpool "$POOL" -n "$NS" --ignore-not-found &>/dev/null || true
  kubectl delete clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "06 · Cluster Age Recycling"

kubectl apply -f - &>/dev/null <<EOF
apiVersion: meridian.io/v1alpha1
kind: ClusterPool
metadata:
  name: $POOL
  namespace: $NS
spec:
  replicas: 1
  maxClusterAge: 1m
  template:
    profile: default
    image: $TEST_IMAGE
    workers: 1
EOF

info "Waiting for 1 Idle cluster..."
wait_for_pool_count "$POOL" "Idle" 1 120

ORIGINAL=$(pool_cluster_with_phase "$POOL" "Idle")
if [[ -z "$ORIGINAL" ]]; then
  fail "No Idle cluster found to test with"
  suite_end; exit 1
fi

pass "Cluster $ORIGINAL is Idle (maxClusterAge=1m set)"

# Wait for the cluster to exceed maxClusterAge (1m) and be recycled.
# Pool reconciles every 30s, so allow up to 120s total.
info "Waiting 90 seconds for age recycling to trigger..."
ELAPSED=0
RECYCLED=false
while [[ $ELAPSED -lt 120 ]]; do
  sleep 5; ((ELAPSED+=5)) || true
  printf "."
  # Recycling happened if the original cluster is gone
  if ! kubectl get cluster "$ORIGINAL" -n "$NS" &>/dev/null; then
    RECYCLED=true
    printf " ✓\n"
    break
  fi
done
[[ "$RECYCLED" == "false" ]] && printf "\n"

if [[ "$RECYCLED" == "true" ]]; then
  pass "Original cluster was recycled after exceeding maxClusterAge"
else
  fail "Original cluster was not recycled after 120s (maxClusterAge=1m)"
fi

# Pool maintains desired count — a replacement should be created
if wait_for_pool_total "$POOL" 1 60; then
  pass "Pool maintained desired replica count (replacement created)"
else
  fail "Pool did not create replacement after recycling"
fi

suite_end
