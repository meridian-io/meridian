#!/usr/bin/env bash
# Test: REST API — all endpoints, error cases, security
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
POOL="qe-rest-pool-$SUFFIX"

cleanup() {
  kubectl delete clusterpool "$POOL" -n "$NS" --ignore-not-found &>/dev/null || true
  kubectl delete clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "04 · REST API"

# Seed: create a pool and wait for an Idle cluster to test against
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

info "Waiting for 2 Idle clusters (takes ~60-90s)..."
wait_for_pool_count "$POOL" "Idle" 2 120

# ── Health check ──────────────────────────────────────────────────────────────
STATUS=$(rest_status GET "/healthz")
if [[ "$STATUS" == "200" ]]; then
  pass "GET /healthz → 200 OK"
else
  fail "GET /healthz → $STATUS (expected 200)"
fi

# ── List clusters ─────────────────────────────────────────────────────────────
RESULT=$(rest_get "/api/v1/clusters" 2>/dev/null || echo "[]")
COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$COUNT" -ge 2 ]]; then
  pass "GET /api/v1/clusters → $COUNT clusters"
else
  fail "GET /api/v1/clusters → only $COUNT clusters"
fi

# 2. Filter by phase=Idle
IDLE_COUNT=$(rest_get "/api/v1/clusters?phase=Idle" 2>/dev/null | \
  python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$IDLE_COUNT" -ge 2 ]]; then
  pass "GET /api/v1/clusters?phase=Idle → $IDLE_COUNT Idle clusters"
else
  fail "GET /api/v1/clusters?phase=Idle → $IDLE_COUNT (expected ≥2)"
fi

# 3. Filter by profile
PROFILE_COUNT=$(rest_get "/api/v1/clusters?profile=default" 2>/dev/null | \
  python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$PROFILE_COUNT" -ge 2 ]]; then
  pass "GET /api/v1/clusters?profile=default → $PROFILE_COUNT clusters"
else
  fail "GET /api/v1/clusters?profile=default → $PROFILE_COUNT (expected ≥2)"
fi

# ── Get single cluster ────────────────────────────────────────────────────────
CLUSTER=$(pool_cluster_with_phase "$POOL" "Idle")

PHASE=$(rest_get "/api/v1/clusters/$CLUSTER" 2>/dev/null | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['phase'])" 2>/dev/null || echo "")
if [[ "$PHASE" == "Idle" ]]; then
  pass "GET /api/v1/clusters/$CLUSTER → phase=Idle"
else
  fail "GET /api/v1/clusters/$CLUSTER → phase=$PHASE (expected Idle)"
fi

STATUS=$(rest_status GET "/api/v1/clusters/does-not-exist-12345")
if [[ "$STATUS" == "404" ]]; then
  pass "GET /api/v1/clusters/nonexistent → 404"
else
  fail "GET /api/v1/clusters/nonexistent → $STATUS (expected 404)"
fi

# ── Reserve ───────────────────────────────────────────────────────────────────
RES_ID="res-$SUFFIX"
RESULT=$(rest_post "/api/v1/clusters/reservations" \
  "{\"reservationId\":\"$RES_ID\",\"profile\":\"default\"}" 2>/dev/null || echo "{}")

RESERVED_NAME=$(echo "$RESULT" | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('clusterName',''))" 2>/dev/null || echo "")
COORD_URL=$(echo "$RESULT" | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('coordinatorUrl',''))" 2>/dev/null || echo "")

if [[ -n "$RESERVED_NAME" && -n "$COORD_URL" ]]; then
  pass "POST /api/v1/clusters/reservations → reserved $RESERVED_NAME"
else
  fail "POST /api/v1/clusters/reservations → no cluster returned"
fi

# Verify cluster is Reserved in Kubernetes
PHASE=$(kubectl get cluster "$RESERVED_NAME" -n "$NS" \
  -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
if [[ "$PHASE" == "Reserved" ]]; then
  pass "Reserved cluster shows phase=Reserved in Kubernetes"
else
  fail "Reserved cluster phase=$PHASE (expected Reserved)"
fi

# ── Idempotent reservation ────────────────────────────────────────────────────
RESULT2=$(rest_post "/api/v1/clusters/reservations" \
  "{\"reservationId\":\"$RES_ID\",\"profile\":\"default\"}" 2>/dev/null || echo "{}")
RESERVED_NAME2=$(echo "$RESULT2" | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('clusterName',''))" 2>/dev/null || echo "")

if [[ "$RESERVED_NAME2" == "$RESERVED_NAME" ]]; then
  pass "Repeat reservation (same ID) returns same cluster (idempotent)"
else
  fail "Repeat reservation returned different cluster: $RESERVED_NAME2 vs $RESERVED_NAME"
fi

# ── Security: cross-client release attempt ────────────────────────────────────
STATUS=$(rest_status DELETE "/api/v1/clusters/reservations/$RES_ID" "" \
  "$TLS_DIR/other.crt" "$TLS_DIR/other.key")
if [[ "$STATUS" == "403" ]]; then
  pass "DELETE by wrong client → 403 Forbidden (security check)"
else
  fail "DELETE by wrong client → $STATUS (expected 403)"
fi

# ── Release ───────────────────────────────────────────────────────────────────
STATUS=$(rest_delete "/api/v1/clusters/reservations/$RES_ID")
if [[ "$STATUS" == "204" ]]; then
  pass "DELETE /api/v1/clusters/reservations/$RES_ID → 204 No Content"
else
  fail "DELETE reservation → $STATUS (expected 204)"
fi

# Cluster returns to Idle
if wait_for_phase "$RESERVED_NAME" "Idle" 30; then
  pass "Cluster returned to Idle after release"
else
  fail "Cluster did not return to Idle after release"
fi

# Release non-existent reservation → 404
STATUS=$(rest_delete "/api/v1/clusters/reservations/does-not-exist-99")
if [[ "$STATUS" == "404" ]]; then
  pass "DELETE nonexistent reservation → 404"
else
  fail "DELETE nonexistent reservation → $STATUS (expected 404)"
fi

# ── List pools ────────────────────────────────────────────────────────────────
POOLS=$(rest_get "/api/v1/pools" 2>/dev/null | \
  python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [[ "$POOLS" -ge 1 ]]; then
  pass "GET /api/v1/pools → $POOLS pool(s)"
else
  fail "GET /api/v1/pools → no pools"
fi

# ── Scale pool ────────────────────────────────────────────────────────────────
RESULT=$(rest_patch "/api/v1/pools/$POOL/replicas" '{"replicas":3}' 2>/dev/null || echo "{}")
NEW_REPLICAS=$(echo "$RESULT" | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('replicas',0))" 2>/dev/null || echo "0")
if [[ "$NEW_REPLICAS" -eq 3 ]]; then
  pass "PATCH /api/v1/pools/$POOL/replicas → replicas=3"
else
  fail "PATCH replicas → $NEW_REPLICAS (expected 3)"
fi

# Verify in Kubernetes
K8S_REPLICAS=$(kubectl get clusterpool "$POOL" -n "$NS" \
  -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
if [[ "$K8S_REPLICAS" -eq 3 ]]; then
  pass "Pool spec.replicas=3 confirmed in Kubernetes"
else
  fail "Pool spec.replicas=$K8S_REPLICAS in Kubernetes (expected 3)"
fi

# Invalid replicas → 400
STATUS=$(rest_status PATCH "/api/v1/pools/$POOL/replicas" '{"replicas":0}')
if [[ "$STATUS" == "400" ]]; then
  pass "PATCH replicas=0 → 400 Bad Request"
else
  fail "PATCH replicas=0 → $STATUS (expected 400)"
fi

# Non-existent pool → 404
STATUS=$(rest_status PATCH "/api/v1/pools/ghost-pool/replicas" '{"replicas":2}')
if [[ "$STATUS" == "404" ]]; then
  pass "PATCH nonexistent pool → 404"
else
  fail "PATCH nonexistent pool → $STATUS (expected 404)"
fi

suite_end
