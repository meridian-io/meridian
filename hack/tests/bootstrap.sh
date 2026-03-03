#!/usr/bin/env bash
# Meridian QE Bootstrap
# Sets up a local kind cluster and deploys the operator so that
# run-all.sh can execute the full test suite.
#
# Run this ONCE before running ./hack/tests/run-all.sh.
# You do NOT need to know Kubernetes to use this script.
#
# Requirements:
#   Docker Desktop  (must be running)
#   Go 1.23+        (to build the operator)
#
# Everything else (kind, kubectl) is downloaded automatically.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLUSTER_NAME="meridian-qe"
NS="meridian"
KIND_VERSION="v0.22.0"
BIN_DIR="$REPO_ROOT/bin"
KIND_BIN="$BIN_DIR/kind"
KUBECTL_BIN=$(command -v kubectl 2>/dev/null || echo "$BIN_DIR/kubectl")

# ── Colour helpers ─────────────────────────────────────────────────────────────
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail() { printf "  \033[31m✗\033[0m %s\n" "$*"; exit 1; }
step() { printf "\n\033[1m▶ %s\033[0m\n" "$*"; }

# ── Detect OS/arch ────────────────────────────────────────────────────────────
OS=$(uname -s | tr '[:upper:]' '[:lower:]')    # darwin | linux
ARCH=$(uname -m)
case "$ARCH" in
  x86_64)  ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) fail "Unsupported architecture: $ARCH" ;;
esac

mkdir -p "$BIN_DIR"

# ── Step 1: Docker ────────────────────────────────────────────────────────────
step "Checking Docker"
if ! docker info &>/dev/null; then
  fail "Docker is not running. Open Docker Desktop and wait for it to start, then re-run this script."
fi
ok "Docker is running"

# ── Step 2: kind ─────────────────────────────────────────────────────────────
step "Installing kind (Kubernetes in Docker)"
if [[ ! -x "$KIND_BIN" ]]; then
  printf "  Downloading kind %s...\n" "$KIND_VERSION"
  curl -sLo "$KIND_BIN" \
    "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-${OS}-${ARCH}"
  chmod +x "$KIND_BIN"
fi
ok "kind $(\"$KIND_BIN\" version 2>/dev/null | head -1)"

# ── Step 3: kubectl ───────────────────────────────────────────────────────────
step "Checking kubectl"
if ! command -v kubectl &>/dev/null; then
  printf "  kubectl not found — downloading...\n"
  KUBE_VERSION=$(curl -sL https://dl.k8s.io/release/stable.txt)
  curl -sLo "$BIN_DIR/kubectl" \
    "https://dl.k8s.io/release/${KUBE_VERSION}/bin/${OS}/${ARCH}/kubectl"
  chmod +x "$BIN_DIR/kubectl"
  KUBECTL_BIN="$BIN_DIR/kubectl"
  export PATH="$BIN_DIR:$PATH"
fi
ok "kubectl $(kubectl version --client --short 2>/dev/null | head -1)"

# ── Step 4: kind cluster ──────────────────────────────────────────────────────
step "Creating local Kubernetes cluster '$CLUSTER_NAME'"
if "$KIND_BIN" get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  ok "Cluster '$CLUSTER_NAME' already exists — skipping create"
else
  printf "  Creating cluster (this takes ~60s)...\n"
  "$KIND_BIN" create cluster --name "$CLUSTER_NAME" --wait 120s \
    --config - <<'KIND_CFG'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
KIND_CFG
  ok "Cluster created"
fi

# Point kubectl at the new cluster
"$KIND_BIN" export kubeconfig --name "$CLUSTER_NAME" &>/dev/null
ok "KUBECONFIG updated to use '$CLUSTER_NAME'"

# ── Step 5: Namespace ─────────────────────────────────────────────────────────
step "Creating namespace '$NS'"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - &>/dev/null
ok "Namespace '$NS' ready"

# ── Step 6: CRDs ─────────────────────────────────────────────────────────────
step "Installing Meridian CRDs"
kubectl apply -f "$REPO_ROOT/operator/config/crd/bases/" &>/dev/null
ok "CRDs installed"

# ── Step 7: RBAC ─────────────────────────────────────────────────────────────
step "Applying RBAC (ClusterRole + binding)"
# Create a service account and grant it cluster-admin for local testing.
kubectl create serviceaccount meridian-operator -n "$NS" \
  --dry-run=client -o yaml | kubectl apply -f - &>/dev/null
kubectl create clusterrolebinding meridian-operator-admin \
  --clusterrole=cluster-admin \
  --serviceaccount="${NS}:meridian-operator" \
  --dry-run=client -o yaml | kubectl apply -f - &>/dev/null
ok "RBAC applied"

# ── Step 8: Build operator ────────────────────────────────────────────────────
step "Building operator binary"
if [[ ! -x "$REPO_ROOT/bin/meridian-operator" ]]; then
  printf "  Building (requires Go 1.23+)...\n"
  cd "$REPO_ROOT" && make build
  ok "Binary built at bin/meridian-operator"
else
  ok "Binary already exists at bin/meridian-operator"
fi

# ── Step 9: TLS certs ─────────────────────────────────────────────────────────
step "Generating test TLS certificates"
source "$SCRIPT_DIR/lib.sh"
ensure_tls_certs
ok "TLS certs ready at $TLS_DIR"

# ── Done ──────────────────────────────────────────────────────────────────────
printf "\n\033[1m\033[32m✓ Bootstrap complete!\033[0m\n\n"
printf "  Cluster:  %s\n" "$CLUSTER_NAME"
printf "  TLS:      %s\n" "$TLS_DIR"
printf "\n"
printf "  Next step — run the full QE test suite:\n\n"
printf "    \033[1m./hack/tests/run-all.sh --skip-operator\033[0m\n\n"
printf "  The --skip-operator flag is used when you want to run the operator\n"
printf "  yourself in a separate terminal. To let the test runner start and\n"
printf "  stop it automatically, omit the flag:\n\n"
printf "    \033[1m./hack/tests/run-all.sh\033[0m\n\n"
