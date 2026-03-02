#!/usr/bin/env bash
# End-to-end test for Phase 4 credential rotation via the Kubernetes provider.
#
# Prerequisites:
#   1. kind cluster running:        ./hack/setup-local.sh
#   2. Trino + MySQL running:       cd hack && docker compose up -d trino mysql
#   3. Operator running:            ./bin/meridian-operator --namespace meridian
#   4. Credentials secret applied:  kubectl apply -f config/samples/test-k8s-secret.yaml -n meridian
#
# Usage: ./hack/e2e-rotation-test.sh
set -euo pipefail

NS=meridian
CLUSTER=trino-local
ANNOTATION_KEY="meridian.io/rotate-credentials"
ROTATION_VALUE="kubernetes/mysql_prod/mysql-catalog-creds"
TIMEOUT=60   # seconds to wait for annotation to be cleared

green()  { printf '\033[32m%s\033[0m\n' "$*"; }
red()    { printf '\033[31m%s\033[0m\n' "$*"; }
yellow() { printf '\033[33m%s\033[0m\n' "$*"; }
info()   { printf '  %s\n' "$*"; }

# ── preflight ──────────────────────────────────────────────────────────────────
green "==> Preflight checks"

if ! kubectl get cluster "$CLUSTER" -n "$NS" &>/dev/null; then
  red "Cluster CR '$CLUSTER' not found in namespace '$NS'."
  info "Run: kubectl apply -f hack/cluster-local.yaml && hack/patch-cluster-local.sh"
  exit 1
fi

if ! kubectl get secret mysql-catalog-creds -n "$NS" &>/dev/null; then
  red "Secret 'mysql-catalog-creds' not found in namespace '$NS'."
  info "Run: kubectl apply -f config/samples/test-k8s-secret.yaml -n $NS"
  exit 1
fi

# ── annotate the cluster ───────────────────────────────────────────────────────
green "==> Triggering credential rotation"
info "cluster: $CLUSTER"
info "annotation: $ANNOTATION_KEY=$ROTATION_VALUE"

kubectl annotate cluster "$CLUSTER" -n "$NS" \
  "${ANNOTATION_KEY}=${ROTATION_VALUE}" \
  --overwrite

green "==> Waiting for operator to process rotation (timeout=${TIMEOUT}s)"

elapsed=0
while true; do
  ann=$(kubectl get cluster "$CLUSTER" -n "$NS" \
    -o jsonpath="{.metadata.annotations.meridian\\.io/rotate-credentials}" 2>/dev/null || true)

  if [[ -z "$ann" ]]; then
    green "==> Annotation cleared — rotation processed by operator"
    break
  fi

  if [[ $elapsed -ge $TIMEOUT ]]; then
    red "Timed out after ${TIMEOUT}s — annotation not cleared."
    echo ""
    yellow "Current cluster status:"
    kubectl get cluster "$CLUSTER" -n "$NS" -o yaml | grep -A20 "status:"
    exit 1
  fi

  printf '\r  Waiting... %ds elapsed' "$elapsed"
  sleep 2
  elapsed=$((elapsed + 2))
done

echo ""

# ── check outcome ──────────────────────────────────────────────────────────────
green "==> Checking rotation result"

PHASE=$(kubectl get cluster "$CLUSTER" -n "$NS" -o jsonpath='{.status.phase}')
LAST_ROTATED=$(kubectl get cluster "$CLUSTER" -n "$NS" -o jsonpath='{.status.lastRotatedAt}')
CONDITION=$(kubectl get cluster "$CLUSTER" -n "$NS" \
  -o jsonpath='{.status.conditions[?(@.type=="CredentialRotation")].status}' 2>/dev/null || echo "")

info "phase         : $PHASE"
info "lastRotatedAt : ${LAST_ROTATED:-<not set>}"
info "CredentialRotation condition: ${CONDITION:-<not found>}"

if [[ "$CONDITION" == "True" ]]; then
  green "==> PASS — credential rotation succeeded"
else
  # Rotation may have failed — check failure count.
  FAILURES=$(kubectl get cluster "$CLUSTER" -n "$NS" \
    -o jsonpath='{.status.rotationFailures}' 2>/dev/null || echo "0")

  if [[ "${FAILURES:-0}" -gt 0 ]]; then
    red "==> FAIL — rotation failed (failures=$FAILURES)"
    yellow "Operator logs (last 30 lines):"
    # Adjust label selector to match your operator deployment.
    kubectl logs -l app=meridian-operator -n "$NS" --tail=30 2>/dev/null || \
      echo "  (no operator pod found — check you ran ./bin/meridian-operator locally)"
    exit 1
  else
    yellow "==> Rotation processed but condition not set to True — check operator logs"
  fi
fi

echo ""
green "==> Verify catalog in Trino (requires Trino CLI or curl):"
echo "  curl -s -X POST http://localhost:8080/v1/statement \\"
echo "    -H 'X-Trino-User: admin' \\"
echo "    -d 'SHOW CATALOGS' | jq '.data'"
