#!/usr/bin/env bash
# Sets up a local kind cluster with Meridian CRDs and test fixtures.
# Prerequisites: Docker Desktop running, kind, kubectl, go installed.
#
# Usage:
#   ./hack/setup-local.sh           # full setup
#   ./hack/setup-local.sh --reset   # tear down and rebuild
set -euo pipefail

CLUSTER_NAME=meridian-dev
NS=meridian
KUBECONFIG_PATH="$HOME/.kube/config"

# ── colours ──────────────────────────────────────────────────────────────────
green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
info()  { printf '  %s\n' "$*"; }

# ── prereq checks ─────────────────────────────────────────────────────────────
check_prereqs() {
  local missing=0
  for cmd in docker kind kubectl go; do
    if ! command -v "$cmd" &>/dev/null; then
      red "Missing: $cmd"
      missing=1
    fi
  done
  if [[ $missing -eq 1 ]]; then
    echo ""
    echo "Install missing tools:"
    echo "  Docker Desktop: https://www.docker.com/products/docker-desktop/"
    echo "  kind + kubectl: brew install kind kubectl"
    exit 1
  fi

  if ! docker info &>/dev/null; then
    red "Docker daemon is not running — start Docker Desktop first."
    exit 1
  fi
}

# ── optional reset ─────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--reset" ]]; then
  echo "Tearing down existing cluster..."
  kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
fi

# ── main ──────────────────────────────────────────────────────────────────────
green "==> Checking prerequisites"
check_prereqs

green "==> Creating kind cluster: $CLUSTER_NAME"
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  info "Cluster already exists — skipping creation"
else
  kind create cluster --name "$CLUSTER_NAME" --wait 60s
fi

green "==> Creating namespace: $NS"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

green "==> Applying CRD definitions"
kubectl apply -f config/crd/bases/meridian.io_clusters.yaml
kubectl apply -f config/crd/bases/meridian.io_clusterpools.yaml
kubectl apply -f config/crd/bases/meridian.io_clusterpoolautoscalers.yaml

# Wait for CRDs to be established.
kubectl wait --for=condition=Established crd/clusters.meridian.io --timeout=30s
kubectl wait --for=condition=Established crd/clusterpools.meridian.io --timeout=30s
kubectl wait --for=condition=Established crd/clusterpoolautoscalers.meridian.io --timeout=30s
info "CRDs established"

green "==> Applying test fixtures"
kubectl apply -f config/samples/test-clusters.yaml

green "==> Patching fixture status"
bash hack/patch-status.sh

green "==> Building meridian-mcp binary"
(cd mcp && go build -o ../bin/meridian-mcp ./cmd/meridian-mcp)

green "==> Building meridian-operator binary"
(cd operator && go build -o ../bin/meridian-operator .)

green "==> Applying credential rotation test fixtures (Phase 4)"
kubectl apply -f operator/config/samples/test-k8s-secret.yaml

green "==> Creating trino-local Cluster CR (points to Docker Compose Trino)"
kubectl apply -f hack/cluster-local.yaml
kubectl patch cluster trino-local -n "$NS" --subresource=status --type=merge -p '{
  "status": {
    "phase": "Idle",
    "ready": true,
    "coordinatorURL": "http://localhost:8080",
    "idleAt": "2026-02-28T10:00:00Z"
  }
}'

green "==> Setup complete"
echo ""
echo "  kind cluster : $CLUSTER_NAME"
echo "  namespace    : $NS"
echo "  binaries     : ./bin/meridian-mcp  ./bin/meridian-operator"
echo ""
echo "Next — start Trino + MySQL (+ optional Vault):"
echo "  cd hack && docker compose up -d trino mysql"
echo "  cd hack && docker compose up -d vault   # optional: for vault provider testing"
echo "  ./hack/seed-vault.sh                    # optional: seed vault with test secrets"
echo ""
echo "Run the operator locally (in a separate terminal):"
echo "  ./bin/meridian-operator --namespace meridian --credential-provider kubernetes"
echo ""
echo "Run credential rotation E2E test:"
echo "  ./hack/e2e-rotation-test.sh"
echo ""
echo "Run MCP unit/protocol tests (no Docker needed):"
echo "  ./hack/test-mcp.sh"
echo ""
echo "Run full E2E catalog test (needs docker compose up):"
echo "  python3 hack/e2e-catalog-test.py"
echo ""
echo "Connect to Claude Desktop:"
echo "  See hack/claude-desktop-config.json"
