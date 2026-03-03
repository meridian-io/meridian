#!/usr/bin/env bash
# Test: Workload-Labeled Pools
# Verifies that clusters in a workload pool carry the workload label,
# and that the REST API can filter clusters by workload.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
POOL="qe-wl-pool-$SUFFIX"
WORKLOAD="analytics-$SUFFIX"

cleanup() {
  kubectl delete clusterpool "$POOL" -n "$NS" --ignore-not-found &>/dev/null || true
  kubectl delete clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "03 · Workload-Labeled Pools"

kubectl apply -f - &>/dev/null <<EOF
apiVersion: meridian.io/v1alpha1
kind: ClusterPool
metadata:
  name: $POOL
  namespace: $NS
spec:
  replicas: 1
  workload: $WORKLOAD
  template:
    profile: default
    image: $TEST_IMAGE
    workers: 1
EOF

# Wait for the cluster to be created
if wait_for_pool_total "$POOL" 1 60; then
  pass "Pool created 1 cluster"
else
  fail "Pool did not create a cluster within 60s"
fi

# 1. Cluster carries the meridian.io/workload label
CLUSTER=$(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

WL=$(kubectl get cluster "$CLUSTER" -n "$NS" \
  -o jsonpath="{.metadata.labels['meridian\\.io/workload']}" 2>/dev/null || echo "")
if [[ "$WL" == "$WORKLOAD" ]]; then
  pass "Cluster carries meridian.io/workload=$WORKLOAD label"
else
  fail "Cluster workload label is '$WL' (expected '$WORKLOAD')"
fi

# 2. Pool label is also present
PL=$(kubectl get cluster "$CLUSTER" -n "$NS" \
  -o jsonpath="{.metadata.labels['meridian\\.io/cluster-pool']}" 2>/dev/null || echo "")
if [[ "$PL" == "$POOL" ]]; then
  pass "Cluster carries meridian.io/cluster-pool=$POOL label"
else
  fail "Cluster pool label is '$PL' (expected '$POOL')"
fi

# 3. kubectl can filter by workload label
COUNT=$(kubectl get clusters -n "$NS" -l "meridian.io/workload=$WORKLOAD" \
  --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [[ "$COUNT" -ge 1 ]]; then
  pass "kubectl label filter returns $COUNT cluster(s) for workload=$WORKLOAD"
else
  fail "kubectl label filter returned no clusters for workload=$WORKLOAD"
fi

# 4. REST API: GET /clusters?workload= returns only this pool's clusters
wait_for_pool_count "$POOL" "Idle" 1 120  # wait for Idle so REST returns it

FOUND=$(rest_get "/api/v1/clusters?workload=$WORKLOAD" 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "0")
if [[ "$FOUND" -ge 1 ]]; then
  pass "REST GET /clusters?workload=$WORKLOAD returns $FOUND cluster(s)"
else
  fail "REST GET /clusters?workload=$WORKLOAD returned no clusters"
fi

suite_end
