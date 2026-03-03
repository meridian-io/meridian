#!/usr/bin/env bash
# Test: ClusterPool Scaling
# Verifies that the pool controller creates, scales up, and scales down clusters.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
POOL="qe-pool-$SUFFIX"

cleanup() {
  kubectl delete clusterpool "$POOL" -n "$NS" --ignore-not-found &>/dev/null || true
  kubectl delete clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "02 · ClusterPool Scaling"

# ── Create pool with 2 replicas ───────────────────────────────────────────────
kubectl apply -f - &>/dev/null <<EOF
apiVersion: meridian.io/v1alpha1
kind: ClusterPool
metadata:
  name: $POOL
  namespace: $NS
spec:
  replicas: 2
  template:
    profile: default
    image: $TEST_IMAGE
    workers: 1
EOF

# 1. Pool controller creates 2 clusters
if wait_for_pool_total "$POOL" 2 60; then
  pass "Pool created 2 clusters"
else
  fail "Pool did not create 2 clusters within 60s"
fi

# 2. Both clusters reach Idle (nginx starts fast)
if wait_for_pool_count "$POOL" "Idle" 2 120; then
  pass "Both clusters reached Idle"
else
  fail "Clusters did not reach Idle within 120s"
fi

# 3. Pool status reflects Ready=2
READY=$(kubectl get clusterpool "$POOL" -n "$NS" \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
if [[ "$READY" -eq 2 ]]; then
  pass "Pool status shows readyReplicas=2"
else
  fail "Pool status shows readyReplicas=$READY (expected 2)"
fi

# 4. Scale up to 3
kubectl patch clusterpool "$POOL" -n "$NS" --type=merge \
  -p '{"spec":{"replicas":3}}' &>/dev/null

if wait_for_pool_total "$POOL" 3 60; then
  pass "Pool scaled up: 3 clusters exist"
else
  fail "Pool did not scale up to 3 clusters"
fi

# 5. Scale down to 1 — pool deletes one idle cluster per 30s cycle
kubectl patch clusterpool "$POOL" -n "$NS" --type=merge \
  -p '{"spec":{"replicas":1}}' &>/dev/null

# Allow two reconcile cycles (gradual: one delete per cycle)
if wait_for_pool_total "$POOL" 1 120; then
  pass "Pool scaled down: 1 cluster remains"
else
  COUNT=$(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --no-headers 2>/dev/null | wc -l | tr -d ' ')
  fail "Pool did not scale down to 1 (current: $COUNT)"
fi

suite_end
