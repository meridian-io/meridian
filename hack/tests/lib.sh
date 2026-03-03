#!/usr/bin/env bash
# Shared utilities for Meridian QE tests.
# Source this file — do not run directly.

NS=meridian
TLS_DIR=/tmp/meridian-tls
OPERATOR_URL=https://localhost:8443
TEST_IMAGE=nginx:alpine   # Small, fast-starting image for testing

# ── Counters ──────────────────────────────────────────────────────────────────
TOTAL_PASS=0
TOTAL_FAIL=0
SUITE_PASS=0
SUITE_FAIL=0

# ── Output ────────────────────────────────────────────────────────────────────
pass() { printf "    \033[32m✓\033[0m %s\n" "$1"; ((TOTAL_PASS++)) || true; ((SUITE_PASS++)) || true; }
fail() { printf "    \033[31m✗\033[0m %s\n" "$1"; ((TOTAL_FAIL++)) || true; ((SUITE_FAIL++)) || true; }
info() { printf "    %s\n" "$1"; }

suite_start() {
  SUITE_PASS=0; SUITE_FAIL=0
  printf "\n\033[1m%s\033[0m\n" "$1"
}

suite_end() {
  # Write counts to shared temp file so run-all.sh can aggregate across subprocesses.
  echo "${SUITE_PASS} ${SUITE_FAIL}" >> /tmp/meridian-qe-counts
  if [[ $SUITE_FAIL -eq 0 ]]; then
    printf "  \033[32mPASS\033[0m (%d checks)\n" "$SUITE_PASS"
    return 0
  else
    printf "  \033[31mFAIL\033[0m (%d failed, %d passed)\n" "$SUITE_FAIL" "$SUITE_PASS"
    return 1
  fi
}

# ── Waiting helpers ───────────────────────────────────────────────────────────

# Wait for a cluster to reach the expected phase. Prints dots while waiting.
wait_for_phase() {
  local name=$1 phase=$2 timeout=${3:-90}
  local elapsed=0
  printf "      waiting for %s → %s " "$name" "$phase"
  while [[ $elapsed -lt $timeout ]]; do
    local cur
    cur=$(kubectl get cluster "$name" -n "$NS" \
      -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [[ "$cur" == "$phase" ]]; then printf "✓\n"; return 0; fi
    sleep 3; ((elapsed+=3)) || true; printf "."
  done
  printf " timed out (current: %s)\n" "$cur"
  return 1
}

# Wait for at least N clusters in a pool to reach a given phase.
wait_for_pool_count() {
  local pool=$1 phase=$2 count=$3 timeout=${4:-120}
  local elapsed=0
  printf "      waiting for %d %s clusters in %s " "$count" "$phase" "$pool"
  while [[ $elapsed -lt $timeout ]]; do
    local cur
    cur=$(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$pool" \
      --no-headers 2>/dev/null | \
      awk -v p="$phase" '$2==p' | wc -l | tr -d ' ')
    if [[ "$cur" -ge "$count" ]]; then printf "✓\n"; return 0; fi
    sleep 3; ((elapsed+=3)) || true; printf "."
  done
  printf " timed out (current: %s/%s)\n" "$cur" "$count"
  return 1
}

# Wait for pool total cluster count to equal N (any phase).
wait_for_pool_total() {
  local pool=$1 count=$2 timeout=${3:-60}
  local elapsed=0
  printf "      waiting for %d total clusters in %s " "$count" "$pool"
  while [[ $elapsed -lt $timeout ]]; do
    local cur
    cur=$(kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$pool" \
      --no-headers 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$cur" -eq "$count" ]]; then printf "✓\n"; return 0; fi
    sleep 3; ((elapsed+=3)) || true; printf "."
  done
  printf " timed out (current: %s)\n" "$cur"
  return 1
}

# Wait until a cluster no longer exists.
wait_for_deletion() {
  local name=$1 timeout=${2:-60}
  local elapsed=0
  printf "      waiting for %s to be deleted " "$name"
  while [[ $elapsed -lt $timeout ]]; do
    if ! kubectl get cluster "$name" -n "$NS" &>/dev/null; then
      printf "✓\n"; return 0
    fi
    sleep 3; ((elapsed+=3)) || true; printf "."
  done
  printf " timed out\n"; return 1
}

# ── TLS helpers ───────────────────────────────────────────────────────────────

ensure_tls_certs() {
  [[ -f "$TLS_DIR/client.crt" ]] && return 0
  info "Generating test TLS certificates..."
  mkdir -p "$TLS_DIR"
  openssl genrsa -out "$TLS_DIR/ca.key" 2048 2>/dev/null
  openssl req -x509 -new -nodes -key "$TLS_DIR/ca.key" -sha256 -days 365 \
    -out "$TLS_DIR/ca.crt" -subj "/CN=meridian-ca" 2>/dev/null
  openssl genrsa -out "$TLS_DIR/server.key" 2048 2>/dev/null
  openssl req -new -key "$TLS_DIR/server.key" -out "$TLS_DIR/server.csr" \
    -subj "/CN=localhost" 2>/dev/null
  openssl x509 -req -in "$TLS_DIR/server.csr" -CA "$TLS_DIR/ca.crt" \
    -CAkey "$TLS_DIR/ca.key" -CAcreateserial -out "$TLS_DIR/server.crt" \
    -days 365 -sha256 2>/dev/null
  openssl genrsa -out "$TLS_DIR/client.key" 2048 2>/dev/null
  openssl req -new -key "$TLS_DIR/client.key" -out "$TLS_DIR/client.csr" \
    -subj "/CN=qe-tester" 2>/dev/null
  openssl x509 -req -in "$TLS_DIR/client.csr" -CA "$TLS_DIR/ca.crt" \
    -CAkey "$TLS_DIR/ca.key" -CAcreateserial -out "$TLS_DIR/client.crt" \
    -days 365 -sha256 2>/dev/null
  # Second client cert with different identity (for security tests)
  openssl genrsa -out "$TLS_DIR/other.key" 2048 2>/dev/null
  openssl req -new -key "$TLS_DIR/other.key" -out "$TLS_DIR/other.csr" \
    -subj "/CN=other-client" 2>/dev/null
  openssl x509 -req -in "$TLS_DIR/other.csr" -CA "$TLS_DIR/ca.crt" \
    -CAkey "$TLS_DIR/ca.key" -CAcreateserial -out "$TLS_DIR/other.crt" \
    -days 365 -sha256 2>/dev/null
}

# ── REST helpers ──────────────────────────────────────────────────────────────

_curl() {
  curl -sf "$@" \
    --cert "$TLS_DIR/client.crt" --key "$TLS_DIR/client.key" \
    --cacert "$TLS_DIR/ca.crt"
}

rest_get()    { _curl "$OPERATOR_URL$1"; }
rest_post()   { _curl -X POST  "$OPERATOR_URL$1" -H "Content-Type: application/json" -d "$2"; }
rest_patch()  { _curl -X PATCH "$OPERATOR_URL$1" -H "Content-Type: application/json" -d "$2"; }
# rest_delete captures the HTTP status code without failing on 4xx/5xx (no -f flag).
rest_delete() {
  curl -s -X DELETE "$OPERATOR_URL$1" -o /dev/null -w "%{http_code}" \
    --cert "$TLS_DIR/client.crt" --key "$TLS_DIR/client.key" \
    --cacert "$TLS_DIR/ca.crt"
}

# Returns the HTTP status code for a request (uses first client cert by default).
rest_status() {
  local method=${1:-GET} path=$2 body=${3:-} cert=${4:-$TLS_DIR/client.crt} key=${5:-$TLS_DIR/client.key}
  local args=(-s -o /dev/null -w "%{http_code}" -X "$method" "$OPERATOR_URL$path" \
    --cert "$cert" --key "$key" --cacert "$TLS_DIR/ca.crt")
  [[ -n "$body" ]] && args+=(-H "Content-Type: application/json" -d "$body")
  curl "${args[@]}"
}

# ── Misc ──────────────────────────────────────────────────────────────────────

# Generate a unique suffix so test resources don't collide across runs.
unique_suffix() { date +%s | tail -c 5; }

# Get the name of the first cluster in a pool with a given phase.
pool_cluster_with_phase() {
  local pool=$1 phase=$2
  kubectl get clusters -n "$NS" -l "meridian.io/cluster-pool=$pool" \
    --no-headers 2>/dev/null | awk -v p="$phase" '$2==p {print $1; exit}'
}
