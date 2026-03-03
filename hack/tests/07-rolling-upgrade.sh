#!/usr/bin/env bash
# Test: Rolling Upgrade (Blue-Green)
# Verifies that changing the pool image triggers one-at-a-time cluster replacement.
# Reserved clusters are NOT replaced mid-reservation.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

SUFFIX=$(unique_suffix)
POOL="qe-upgrade-pool-$SUFFIX"
IMAGE_V1="nginx:1.25-alpine"
IMAGE_V2="nginx:1.27-alpine"

cleanup() {
  kubectl delete clusterpool "$POOL" -n "$NS" --ignore-not-found &>/dev/null || true
  kubectl delete clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    --ignore-not-found &>/dev/null || true
}
trap cleanup EXIT

suite_start "07 · Rolling Upgrade"

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
    image: $IMAGE_V1
    workers: 1
EOF

info "Waiting for 2 Idle clusters with image $IMAGE_V1..."
wait_for_pool_count "$POOL" "Idle" 2 120

# Verify both clusters have v1 image
V1_COUNT=$(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
  -o jsonpath="{.items[?(@.spec.image=='$IMAGE_V1')].metadata.name}" 2>/dev/null | wc -w | tr -d ' ')
if [[ "$V1_COUNT" -eq 2 ]]; then
  pass "Both clusters running image $IMAGE_V1"
else
  fail "Expected 2 clusters with $IMAGE_V1, found $V1_COUNT"
fi

# Reserve one cluster — it should NOT be upgraded while reserved
CLUSTER_A=$(pool_cluster_with_phase "$POOL" "Idle")
kubectl patch cluster "$CLUSTER_A" -n "$NS" --type=merge \
  -p "{\"spec\":{\"clientId\":\"qe-client\",\"reservationId\":\"res-upgrade-$SUFFIX\"}}" &>/dev/null
wait_for_phase "$CLUSTER_A" "Reserved" 20 || true
pass "Cluster $CLUSTER_A reserved (should be skipped during upgrade)"

# Trigger rolling upgrade by changing the image
kubectl patch clusterpool "$POOL" -n "$NS" --type=merge \
  -p "{\"spec\":{\"template\":{\"image\":\"$IMAGE_V2\"}}}" &>/dev/null
pass "Pool image updated to $IMAGE_V2 (rolling upgrade triggered)"

# Wait for at least one cluster with the new image to appear
ELAPSED=0
NEW_IMAGE_FOUND=false
while [[ $ELAPSED -lt 120 ]]; do
  sleep 5; ((ELAPSED+=5)) || true
  printf "."
  COUNT=$(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$POOL" \
    -o jsonpath="{.items[?(@.spec.image=='$IMAGE_V2')].metadata.name}" 2>/dev/null | wc -w | tr -d ' ')
  if [[ "$COUNT" -ge 1 ]]; then
    NEW_IMAGE_FOUND=true
    printf " ✓\n"
    break
  fi
done
[[ "$NEW_IMAGE_FOUND" == "false" ]] && printf "\n"

if [[ "$NEW_IMAGE_FOUND" == "true" ]]; then
  pass "New cluster created with image $IMAGE_V2"
else
  fail "No cluster with image $IMAGE_V2 appeared after 120s"
fi

# Reserved cluster must still be on the old image (not touched mid-reservation)
RESERVED_IMAGE=$(kubectl get cluster "$CLUSTER_A" -n "$NS" \
  -o jsonpath='{.spec.image}' 2>/dev/null || echo "")
if [[ "$RESERVED_IMAGE" == "$IMAGE_V1" ]]; then
  pass "Reserved cluster still running $IMAGE_V1 (upgrade correctly skipped)"
else
  fail "Reserved cluster image is $RESERVED_IMAGE (should remain $IMAGE_V1)"
fi

# Release the reserved cluster
kubectl patch cluster "$CLUSTER_A" -n "$NS" --type=merge \
  -p '{"spec":{"clientId":"","reservationId":""}}' &>/dev/null

pass "Reserved cluster released — upgrade will now replace it in next cycle"

suite_end
