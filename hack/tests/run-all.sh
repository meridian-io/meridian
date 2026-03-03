#!/usr/bin/env bash
# Meridian QE Master Test Runner
# Runs all test suites in order and prints a final PASS/FAIL summary.
#
# Prerequisites (installed and on PATH):
#   kubectl  — connected to a cluster with the Meridian operator running
#   curl     — for REST API tests
#   openssl  — for TLS cert generation
#   python3  — for JSON parsing in REST tests
#
# Usage:
#   ./hack/tests/run-all.sh [--skip-operator]
#
#   --skip-operator   Assume the operator is already running on :8443.
#                     Without this flag the script starts the operator
#                     binary from ./bin/manager (built by `make build`).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/lib.sh"

# ── Argument parsing ───────────────────────────────────────────────────────────
SKIP_OPERATOR=false
for arg in "$@"; do
  [[ "$arg" == "--skip-operator" ]] && SKIP_OPERATOR=true
done

OPERATOR_PID=""

# ── Prerequisites check ────────────────────────────────────────────────────────
check_prereqs() {
  local missing=()
  for cmd in kubectl curl openssl python3; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    printf "\033[31mERROR\033[0m Missing required tools: %s\n" "${missing[*]}"
    printf "Install them and re-run.\n"
    exit 1
  fi

  if ! kubectl cluster-info &>/dev/null; then
    printf "\033[31mERROR\033[0m No Kubernetes cluster found.\n\n"
    printf "  Run the bootstrap script first to create a local cluster:\n\n"
    printf "    \033[1m./hack/tests/bootstrap.sh\033[0m\n\n"
    exit 1
  fi

  if ! kubectl get namespace "$NS" &>/dev/null; then
    printf "\033[31mERROR\033[0m Namespace '%s' not found.\n\n" "$NS"
    printf "  Run the bootstrap script to set up the environment:\n\n"
    printf "    \033[1m./hack/tests/bootstrap.sh\033[0m\n\n"
    exit 1
  fi
}

# ── TLS setup ─────────────────────────────────────────────────────────────────
setup_tls() {
  ensure_tls_certs
  info "TLS certs ready at $TLS_DIR"
}

# ── Operator lifecycle ─────────────────────────────────────────────────────────
start_operator() {
  if [[ "$SKIP_OPERATOR" == "true" ]]; then
    info "Skipping operator start (--skip-operator passed)"
    return 0
  fi

  local binary="$REPO_ROOT/bin/manager"
  if [[ ! -x "$binary" ]]; then
    printf "\033[31mERROR\033[0m Operator binary not found at %s\n" "$binary"
    printf "Build it first: cd operator && make build\n"
    exit 1
  fi

  info "Starting operator..."
  "$binary" \
    --tls-cert-file "$TLS_DIR/server.crt" \
    --tls-key-file  "$TLS_DIR/server.key" \
    --tls-ca-file   "$TLS_DIR/ca.crt" \
    --namespace     "$NS" \
    &>/tmp/meridian-operator.log &
  OPERATOR_PID=$!

  # Wait until /healthz responds
  local elapsed=0
  printf "      waiting for operator "
  while [[ $elapsed -lt 30 ]]; do
    if curl -sk -o /dev/null -w "%{http_code}" \
        --cert "$TLS_DIR/client.crt" --key "$TLS_DIR/client.key" \
        --cacert "$TLS_DIR/ca.crt" \
        https://localhost:8443/healthz 2>/dev/null | grep -q "200"; then
      printf "✓\n"
      return 0
    fi
    sleep 2; ((elapsed+=2)) || true; printf "."
  done
  printf " timed out\n"
  printf "\033[31mERROR\033[0m Operator did not start within 30s. Log:\n"
  tail -20 /tmp/meridian-operator.log || true
  exit 1
}

stop_operator() {
  if [[ -n "$OPERATOR_PID" ]]; then
    kill "$OPERATOR_PID" &>/dev/null || true
    wait "$OPERATOR_PID" &>/dev/null || true
    OPERATOR_PID=""
  fi
}
trap stop_operator EXIT

# ── Test suites ───────────────────────────────────────────────────────────────
SUITES=(
  "01-cluster-lifecycle.sh"
  "02-pool-scaling.sh"
  "03-workload-routing.sh"
  "04-rest-api.sh"
  "05-degraded-phase.sh"
  "06-age-recycling.sh"
  "07-rolling-upgrade.sh"
  "08-reservation-ttl.sh"
)

run_suites() {
  local suite_pass=0 suite_fail=0
  for suite in "${SUITES[@]}"; do
    local script="$SCRIPT_DIR/$suite"
    if [[ ! -f "$script" ]]; then
      printf "\n\033[33mSKIP\033[0m %s (file not found)\n" "$suite"
      continue
    fi
    if bash "$script"; then
      ((suite_pass++)) || true
    else
      ((suite_fail++)) || true
    fi
  done
  echo ""
  echo "─────────────────────────────────────────────────────"
  echo "  Suites:  $suite_pass passed, $suite_fail failed"
  echo "  Checks:  $TOTAL_PASS passed, $TOTAL_FAIL failed"
  echo "─────────────────────────────────────────────────────"
  if [[ $suite_fail -eq 0 && $TOTAL_FAIL -eq 0 ]]; then
    printf "  \033[32m✓ ALL TESTS PASSED\033[0m\n"
    return 0
  else
    printf "  \033[31m✗ SOME TESTS FAILED\033[0m\n"
    return 1
  fi
}

# ── Main ──────────────────────────────────────────────────────────────────────
printf "\n\033[1mMeridian QE Test Suite\033[0m\n"
printf "(first time? run \033[1m./hack/tests/bootstrap.sh\033[0m first)\n"
printf "Cluster: %s\n" "$(kubectl config current-context 2>/dev/null || echo unknown)"
printf "Date:    %s\n" "$(date)"
echo "═════════════════════════════════════════════════════"

check_prereqs
setup_tls
start_operator
run_suites
