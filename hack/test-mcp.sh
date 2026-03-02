#!/usr/bin/env bash
# Protocol-level MCP test suite.
# Sends JSON-RPC messages directly to meridian-mcp over stdio and validates responses.
# No LLM required — tests the server contract directly.
#
# Prerequisites: ./hack/setup-local.sh must have been run.
# Usage: ./hack/test-mcp.sh [--verbose]
set -euo pipefail

BINARY=${BINARY:-./bin/meridian-mcp}
NS=${MERIDIAN_NAMESPACE:-meridian}
VERBOSE=${1:-}

PASS=0
FAIL=0

# ── colours ──────────────────────────────────────────────────────────────────
green() { printf '\033[32m  PASS\033[0m  %s\n' "$*"; }
red()   { printf '\033[31m  FAIL\033[0m  %s\n' "$*"; }
head()  { printf '\n\033[1m%s\033[0m\n' "$*"; }

# ── helpers ───────────────────────────────────────────────────────────────────

# mcp_call: send a sequence of JSON-RPC messages and return the last response.
# The server reads from stdin and writes to stdout. We use a temp FIFO.
mcp_call() {
  local messages="$1"
  local tmpdir
  tmpdir=$(mktemp -d)
  local fifo="$tmpdir/mcp.fifo"
  mkfifo "$fifo"

  # Start the server reading from the FIFO.
  "$BINARY" --transport stdio --namespace "$NS" <"$fifo" 2>/dev/null &
  local pid=$!

  # Write all messages to the FIFO and capture stdout.
  local response
  response=$(printf '%s\n' "$messages" >"$fifo" && wait "$pid" 2>/dev/null; true) || true

  rm -rf "$tmpdir"
  echo "$response"
}

# send_and_check: run a single session with init + one tool call, check the response.
send_and_check() {
  local test_name="$1"
  local tool_call="$2"     # full JSON-RPC tools/call message
  local expect_key="$3"    # string that must appear in response

  # Build the session: initialize → initialized notification → tool call.
  local init='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"qe-test","version":"1.0"}}}'
  local notif='{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'

  local response
  response=$(printf '%s\n%s\n%s\n' "$init" "$notif" "$tool_call" | \
    "$BINARY" --transport stdio --namespace "$NS" 2>/dev/null || true)

  if [[ "$VERBOSE" == "--verbose" ]]; then
    echo "  Response: $response"
  fi

  if echo "$response" | grep -q "$expect_key"; then
    green "$test_name"
    PASS=$((PASS + 1))
  else
    red "$test_name (expected '$expect_key' in response)"
    if [[ "$VERBOSE" != "--verbose" ]]; then
      echo "    Response: $response"
    fi
    FAIL=$((FAIL + 1))
  fi
}

# ── test binary exists ────────────────────────────────────────────────────────
if [[ ! -f "$BINARY" ]]; then
  echo "Binary not found: $BINARY"
  echo "Run ./hack/setup-local.sh first."
  exit 1
fi

# ── test: version flag ────────────────────────────────────────────────────────
head "Binary sanity"

if "$BINARY" --version 2>/dev/null | grep -q "meridian-mcp"; then
  green "--version flag works"
  PASS=$((PASS + 1))
else
  red "--version flag"
  FAIL=$((FAIL + 1))
fi

# ── test: MCP initialization ──────────────────────────────────────────────────
head "MCP protocol handshake"

INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"qe-test","version":"1.0"}}}'
RESP=$(printf '%s\n' "$INIT" | "$BINARY" --transport stdio --namespace "$NS" 2>/dev/null || true)

if echo "$RESP" | grep -q '"protocolVersion"'; then
  green "initialize returns protocolVersion"
  PASS=$((PASS + 1))
else
  red "initialize response missing protocolVersion"
  FAIL=$((FAIL + 1))
fi

if echo "$RESP" | grep -q '"meridian-mcp"'; then
  green "initialize returns server name meridian-mcp"
  PASS=$((PASS + 1))
else
  red "initialize response missing server name"
  FAIL=$((FAIL + 1))
fi

# ── test: tools/list ──────────────────────────────────────────────────────────
head "Tool registration (tools/list)"

INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"qe-test","version":"1.0"}}}'
NOTIF='{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'
LIST='{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'

RESP=$(printf '%s\n%s\n%s\n' "$INIT" "$NOTIF" "$LIST" | \
  "$BINARY" --transport stdio --namespace "$NS" 2>/dev/null || true)

for tool in list_clusters get_cluster create_cluster delete_cluster \
            reserve_cluster release_cluster scale_pool list_pools \
            add_catalog remove_catalog rotate_credentials \
            get_audit_log promote_environment; do
  if echo "$RESP" | grep -q "\"$tool\""; then
    green "tool registered: $tool"
    PASS=$((PASS + 1))
  else
    red "tool missing: $tool"
    FAIL=$((FAIL + 1))
  fi
done

# ── test: tool calls ──────────────────────────────────────────────────────────
head "Tool calls (requires kind cluster from setup-local.sh)"

# Check if kubeconfig exists — skip K8s-dependent tests if not.
if [[ ! -f "$HOME/.kube/config" ]]; then
  echo "  No kubeconfig found — skipping K8s-dependent tool call tests."
  echo "  Run ./hack/setup-local.sh to create a local cluster."
else

  send_and_check \
    "list_clusters returns JSON array" \
    '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_clusters","arguments":{}}}' \
    '"namespace"'

  send_and_check \
    "list_clusters includes trino-idle-01" \
    '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_clusters","arguments":{}}}' \
    'trino-idle-01'

  send_and_check \
    "get_cluster returns phase field" \
    '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_cluster","arguments":{"name":"trino-idle-01"}}}' \
    '"phase"'

  send_and_check \
    "get_cluster trino-idle-01 phase is Idle" \
    '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_cluster","arguments":{"name":"trino-idle-01"}}}' \
    'Idle'

  send_and_check \
    "list_pools returns pool data" \
    '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_pools","arguments":{}}}' \
    'standard-pool'

  send_and_check \
    "get_audit_log returns events array" \
    '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_audit_log","arguments":{"limit":10}}}' \
    'time'

  # Negative: reserve a Reserved cluster — must fail with correct error message.
  INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"qe-test","version":"1.0"}}}'
  NOTIF='{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'
  CALL='{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"reserve_cluster","arguments":{"cluster_name":"trino-reserved-01","client_id":"test","reservation_id":"test-001"}}}'
  RESP=$(printf '%s\n%s\n%s\n' "$INIT" "$NOTIF" "$CALL" | \
    "$BINARY" --transport stdio --namespace "$NS" 2>/dev/null || true)
  if echo "$RESP" | grep -qE '"error"|Reserved'; then
    green "reserve non-Idle cluster returns error"
    PASS=$((PASS + 1))
  else
    red "reserve non-Idle cluster should have failed"
    FAIL=$((FAIL + 1))
  fi

fi

# ── summary ────────────────────────────────────────────────────────────────────
echo ""
echo "────────────────────────────────"
printf '\033[32mPASS: %d\033[0m  \033[31mFAIL: %d\033[0m\n' "$PASS" "$FAIL"
echo "────────────────────────────────"

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
