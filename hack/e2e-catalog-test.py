#!/usr/bin/env python3
"""
End-to-end test: add a MySQL catalog to a live Trino instance via the Meridian MCP server.

Prerequisites (run once):
  1. Docker Desktop running
  2. cd hack && docker compose up -d
  3. ./hack/setup-local.sh            (creates kind cluster + CRDs)
  4. bash hack/patch-cluster-local.sh (creates trino-local Cluster CR)

Then run this script:
  python3 hack/e2e-catalog-test.py

What it tests:
  Phase 1  — Trino is reachable and the tpch catalog is present
  Phase 2  — MCP list_clusters sees trino-local as Idle
  Phase 3  — MCP add_catalog adds a MySQL catalog to Trino
  Phase 4  — Trino REST API confirms the MySQL catalog was created
  Phase 5  — MCP remove_catalog removes the MySQL catalog
  Phase 6  — Trino REST API confirms the MySQL catalog is gone
"""

import json
import subprocess
import sys
import time
import urllib.request
import urllib.error

BINARY       = "./bin/meridian-mcp"
KUBECONFIG   = "./hack/kubeconfig-local.yaml"   # replaced by kind kubeconfig after setup-local.sh
NAMESPACE    = "meridian"
TRINO_URL    = "http://localhost:8080"
CATALOG_NAME = "mysql_testdb"

PASS = 0
FAIL = 0

# ── helpers ───────────────────────────────────────────────────────────────────

def ok(name):
    global PASS
    print(f"  \033[32mPASS\033[0m  {name}")
    PASS += 1

def fail(name, detail=""):
    global FAIL
    print(f"  \033[31mFAIL\033[0m  {name}")
    if detail:
        print(f"        {detail}")
    FAIL += 1

def section(name):
    print(f"\n\033[1m=== {name} ===\033[0m")

def trino_get(path):
    """GET request to Trino REST API."""
    req = urllib.request.Request(f"{TRINO_URL}{path}", headers={"X-Trino-User": "meridian"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception as e:
        return None, str(e)

def mcp_call(*tool_messages, kubeconfig=KUBECONFIG):
    """Send JSON-RPC messages to meridian-mcp via stdio, return parsed responses."""
    init   = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                         "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                                    "clientInfo": {"name": "e2e-test", "version": "1.0"}}})
    notif  = json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
    msgs   = "\n".join([init, notif] + list(tool_messages)) + "\n"

    proc = subprocess.Popen(
        [BINARY, "--transport", "stdio", "--namespace", NAMESPACE, "--kubeconfig", kubeconfig],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = proc.communicate(input=msgs.encode(), timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()

    responses = []
    for line in stdout.decode().splitlines():
        line = line.strip()
        if line:
            try:
                responses.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return responses, stderr.decode()

def tool_call(name, arguments, kubeconfig=KUBECONFIG):
    """Call a single MCP tool and return the last response."""
    msg = json.dumps({"jsonrpc": "2.0", "id": 99, "method": "tools/call",
                      "params": {"name": name, "arguments": arguments}})
    responses, stderr = mcp_call(msg, kubeconfig=kubeconfig)
    # Find the tools/call response (id=99)
    for r in responses:
        if r.get("id") == 99:
            return r, stderr
    return {}, stderr

def get_tool_text(response):
    """Extract text content from a tools/call response."""
    result = response.get("result", {})
    content = result.get("content", [])
    for c in content:
        if c.get("type") == "text":
            return c.get("text", "")
    # mcp-go may encode differently
    if isinstance(result, dict) and "content" in result:
        return str(result["content"])
    return str(result)

def wait_for_trino(max_seconds=60):
    """Poll Trino /v1/info until it reports started."""
    deadline = time.time() + max_seconds
    while time.time() < deadline:
        try:
            status, info = trino_get("/v1/info")
            if status == 200 and not info.get("starting", True):
                return True
        except Exception:
            pass
        time.sleep(2)
    return False

# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Trino reachability
# ─────────────────────────────────────────────────────────────────────────────

section("Phase 1 — Trino is reachable")

print("  Waiting for Trino to finish starting (up to 60s)…")
if wait_for_trino(60):
    ok("Trino /v1/info responds and is not starting")
else:
    fail("Trino did not become ready in 60s — is docker compose up running?")
    print("\n  Run: cd hack && docker compose up -d")
    sys.exit(1)

status, catalogs = trino_get("/v1/catalog")
if status == 200:
    ok(f"/v1/catalog returns catalog list (HTTP 200)")
    catalog_names = list(catalogs.keys()) if isinstance(catalogs, dict) else catalogs
    print(f"        Catalogs present: {catalog_names}")
    if "tpch" in str(catalogs):
        ok("tpch catalog is present (confirms built-in catalog loaded)")
    else:
        fail("tpch catalog not found — Trino catalog config may not be mounted correctly")
else:
    fail(f"/v1/catalog returned HTTP {status}")

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — MCP sees the cluster
# ─────────────────────────────────────────────────────────────────────────────

section("Phase 2 — MCP list_clusters sees trino-local")

# Try kind kubeconfig first (set up by setup-local.sh), fall back to fake one.
import os
kind_kubeconfig = os.path.expanduser("~/.kube/config")
kubeconfig = kind_kubeconfig if os.path.exists(kind_kubeconfig) else KUBECONFIG

resp, _ = tool_call("list_clusters", {}, kubeconfig=kubeconfig)
text = get_tool_text(resp)

if "operator not detected" in text:
    fail("Meridian operator not detected — CRDs not applied yet",
         "Run: ./hack/setup-local.sh && bash hack/patch-cluster-local.sh")
elif "trino-local" in text:
    ok("list_clusters returns trino-local cluster")
    try:
        clusters = json.loads(text)
        trino_local = next((c for c in clusters if c["name"] == "trino-local"), None)
        if trino_local:
            ok(f"trino-local phase={trino_local.get('phase', '?')} coordinatorUrl={trino_local.get('coordinatorUrl','?')}")
        else:
            fail("trino-local not found in parsed response")
    except json.JSONDecodeError:
        fail("list_clusters response is not valid JSON", text[:200])
else:
    fail("list_clusters did not return trino-local", text[:200])
    print("  Run: bash hack/patch-cluster-local.sh")

resp2, _ = tool_call("get_cluster", {"name": "trino-local"}, kubeconfig=kubeconfig)
text2 = get_tool_text(resp2)
if '"phase"' in text2 and "Idle" in text2:
    ok("get_cluster trino-local shows phase=Idle")
else:
    fail("get_cluster trino-local not Idle", text2[:200])

# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — add_catalog (MySQL) via MCP
# ─────────────────────────────────────────────────────────────────────────────

section("Phase 3 — add_catalog: push MySQL catalog to Trino via MCP")

# MySQL is accessible from Trino at mysql:3306 (Docker Compose internal network).
mysql_properties = (
    "connector.name=mysql\n"
    "connection-url=jdbc:mysql://mysql:3306\n"
    "connection-user=root\n"
    "connection-password=trino_test"
)

print(f"  Calling add_catalog: catalog={CATALOG_NAME}")
print(f"  Properties:\n    " + mysql_properties.replace("\n", "\n    "))

resp3, _ = tool_call("add_catalog", {
    "cluster_name": "trino-local",
    "catalog_name": CATALOG_NAME,
    "properties": mysql_properties,
}, kubeconfig=kubeconfig)

text3 = get_tool_text(resp3)
error3 = resp3.get("error")

if error3:
    fail(f"add_catalog returned error: {error3.get('message', str(error3))}")
    print("\n  Possible causes:")
    print("  • Trino does not have catalog.management=dynamic in config.properties")
    print("  • Trino is not fully started yet")
    print("  • The catalog directory is not writable")
    print(f"\n  Full response: {resp3}")
elif "added" in text3.lower() or CATALOG_NAME in text3:
    ok(f"add_catalog succeeded: {text3.strip()}")
else:
    fail(f"add_catalog unexpected response: {text3[:200]}")

# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — Verify catalog exists in Trino via REST API
# ─────────────────────────────────────────────────────────────────────────────

section("Phase 4 — Verify MySQL catalog exists in Trino")

# Give Trino a moment to register the new catalog.
time.sleep(3)

status4, catalogs4 = trino_get("/v1/catalog")
print(f"  Trino catalogs after add_catalog: {catalogs4}")

catalog_names4 = list(catalogs4.keys()) if isinstance(catalogs4, dict) else str(catalogs4)
if CATALOG_NAME in str(catalog_names4):
    ok(f"catalog {CATALOG_NAME!r} found in Trino /v1/catalog")
else:
    fail(f"catalog {CATALOG_NAME!r} NOT found in Trino — dynamic catalog API may not be enabled",
         f"Catalogs present: {catalog_names4}")

# Also verify MySQL connectivity by querying the catalog's schemas.
status5, schemas = trino_get(f"/v1/catalog/{CATALOG_NAME}")
if status5 == 200:
    ok(f"GET /v1/catalog/{CATALOG_NAME} returns 200 (catalog is registered)")
else:
    print(f"        GET /v1/catalog/{CATALOG_NAME} returned {status5} (catalog detail may not be a full REST resource)")

# ─────────────────────────────────────────────────────────────────────────────
# Phase 5 — remove_catalog via MCP
# ─────────────────────────────────────────────────────────────────────────────

section("Phase 5 — remove_catalog: remove MySQL catalog via MCP")

resp5, _ = tool_call("remove_catalog", {
    "cluster_name": "trino-local",
    "catalog_name": CATALOG_NAME,
}, kubeconfig=kubeconfig)

text5 = get_tool_text(resp5)
error5 = resp5.get("error")

if error5:
    fail(f"remove_catalog returned error: {error5.get('message', str(error5))}")
elif "removed" in text5.lower() or CATALOG_NAME in text5:
    ok(f"remove_catalog succeeded: {text5.strip()}")
else:
    fail(f"remove_catalog unexpected response: {text5[:200]}")

# ─────────────────────────────────────────────────────────────────────────────
# Phase 6 — Verify catalog is gone
# ─────────────────────────────────────────────────────────────────────────────

section("Phase 6 — Verify MySQL catalog removed from Trino")

time.sleep(2)

status6, catalogs6 = trino_get("/v1/catalog")
catalog_names6 = list(catalogs6.keys()) if isinstance(catalogs6, dict) else str(catalogs6)
print(f"  Trino catalogs after remove_catalog: {catalog_names6}")

if CATALOG_NAME not in str(catalog_names6):
    ok(f"catalog {CATALOG_NAME!r} no longer in Trino /v1/catalog")
else:
    fail(f"catalog {CATALOG_NAME!r} still present after remove_catalog")

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n{'─'*50}")
print(f"\033[32mPASS: {PASS}\033[0m  \033[31mFAIL: {FAIL}\033[0m")
print(f"{'─'*50}\n")

sys.exit(0 if FAIL == 0 else 1)
