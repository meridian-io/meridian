# Meridian

**The open-source Trino control plane and MCP server for Kubernetes.**

**[getmeridian.dev](https://getmeridian.dev)** · **Latest release: [v1.2](https://github.com/meridian-io/meridian/releases/tag/v1.2)**

Trino clusters take 30–90 seconds to provision. Meridian eliminates that wait: a hot standby pool keeps pre-provisioned clusters idle and ready, so a reservation completes in zero seconds. No cold starts for CI/CD jobs, batch workloads, or multi-tenant platforms.

Meridian has two parts:

- A **Kubernetes operator** that manages the full cluster lifecycle (`Empty → Pending → Idle → Reserved`) and keeps a warm pool of clusters.
- An **MCP server** that lets Claude, Cursor, or any MCP client explore catalogs, run queries, and reserve clusters in natural language. It is secure by default: read-only queries, a token required for remote access, and config-changing tools off unless you enable them.

---

## MCP Server

The `meridian` binary is an MCP server for Trino clusters managed by Meridian. It needs the Meridian CRDs in your Kubernetes cluster. To use an existing Trino deployment, register it as a `Cluster` resource. [Local Development](#local-development) shows how.

### Install

**Binary** (macOS arm64 shown; Linux and Windows builds are on the [releases page](https://github.com/meridian-io/meridian/releases)):
```bash
curl -Lo meridian.tar.gz https://github.com/meridian-io/meridian/releases/download/v1.2/trino-mcp-server_1.2_darwin_arm64.tar.gz
tar xzf meridian.tar.gz && sudo mv meridian /usr/local/bin/
```

**Docker:**
```bash
docker pull ghcr.io/meridian-io/trino-mcp-server:v1.2
```

### Run

```bash
# stdio: for Claude Desktop and other local MCP clients
meridian --transport stdio --namespace meridian

# SSE: for remote or team use. A bearer token is required.
export MERIDIAN_MCP_TOKEN=$(openssl rand -hex 32)
meridian --transport sse --addr :8080 --namespace meridian

# In-cluster: uses the pod's service account; token from a mounted Secret
meridian --transport sse --addr :8080 --auth-token-file /etc/meridian/token
```

### Connect to Claude Desktop

Add to `claude_desktop_config.json`. On macOS it's in `~/Library/Application Support/Claude/`, and on Windows in `%APPDATA%\Claude\`:

```json
{
  "mcpServers": {
    "meridian": {
      "command": "meridian",
      "args": ["--transport", "stdio", "--namespace", "meridian"]
    }
  }
}
```

### Security Defaults

Each default can be relaxed with a flag:

| Default | Why | To change |
|---|---|---|
| SSE requires a bearer token (`Authorization: Bearer <token>`, min 16 chars) | Without it, anyone who can reach the port can run queries | `MERIDIAN_MCP_TOKEN` or `--auth-token-file`. `--insecure-no-auth` is for local testing only |
| `execute_query` is **read-only** (SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, VALUES, TABLE) | An AI client shouldn't be able to `DROP` or `DELETE` by accident | `--allow-writes` |
| 12 core tools are on. The 7 tools that change cluster or catalog config are **off** | Smaller blast radius | `--enable-tools=scale_pool,add_catalog` or `--enable-tools=all` |
| `output_file` writes only into `--output-dir` (default: a per-user cache dir, e.g. `~/.cache/meridian/exports`) and is disabled in SSE mode | Remote callers must not write files onto the server | `--output-dir` |

The read-only check is a guardrail for AI clients. Trino access control remains the real security boundary. For example, connector table functions such as `system.query(...)` send SQL to the underlying database and are not inspected by Meridian.

Other flags: `--base-url` (public URL behind a proxy), `--cache-max-entries` (default 1000), and `--log-format json|text`. In SSE mode, `/healthz` reports the process is alive and `/readyz` reports the Kubernetes API is reachable. Neither requires a token.

### Tools

**Core tools (on by default)**

| Tool | Description | Cache TTL |
|---|---|---|
| `list_pools` | List all ClusterPools with current state | — |
| `list_clusters` | List all clusters with phase, health, coordinator URL | — |
| `get_cluster` | Get full details of a specific cluster | — |
| `reserve_cluster` | Claim an idle cluster by name or workload. Idempotent and safe under concurrency: two clients never get the same cluster | — |
| `release_cluster` | Return a cluster to the idle pool. Pass `client_id` to release only your own reservation | — |
| `list_catalogs` | List all catalogs registered in a cluster | 5 min |
| `list_schemas` | List schemas in a catalog | 5 min |
| `list_tables` | List tables in a catalog schema | 5 min |
| `get_table_schema` | Get column names and data types for a table | 5 min |
| `execute_query` | Run SQL (read-only by default) and return results as a table. Max 10,000 rows | none (opt-in via `ttl`) |
| `explain_query` | Return the Trino execution plan without running the query | 5 min |
| `get_audit_log` | Fetch cluster lifecycle and management events | — |

**Held tools (enable with `--enable-tools`)**

| Tool | Description |
|---|---|
| `create_cluster` | Provision a cluster outside a pool |
| `delete_cluster` | Tear down a cluster |
| `scale_pool` | Set desired replica count on a ClusterPool |
| `add_catalog` | Add a catalog to a running cluster, no restart |
| `remove_catalog` | Remove a catalog from a running cluster, no restart |
| `rotate_credentials` | Trigger a credential refresh from Kubernetes Secrets, Vault, or AWS Secrets Manager |
| `promote_environment` | Copy a pool config between namespaces (dev → staging → prod) |

### Query Result Caching

Results are cached in memory to avoid repeated round-trips to Trino:

- **Metadata tools** (`list_catalogs`, `list_schemas`, `list_tables`, `get_table_schema`, `explain_query`) cache for 5 minutes.
- **`execute_query`** is not cached by default, so data changes are visible immediately. Pass `ttl=N` to cache static or reference data for N seconds.
- **Cache hits** are marked `[cache hit]`.
- **`add_catalog` / `remove_catalog`** clear that cluster's cache, so `list_catalogs` reflects the change right away.
- **Identical concurrent queries** run once (singleflight). The second caller gets the first caller's result.
- **Cache key:** the cluster, `max_rows`, and the SQL. A result cut off at `max_rows` is never served to a request that asked for more rows.
- **Size:** at most `--cache-max-entries` results (default 1000).

### Query Tagging

Every statement sent to Trino is tagged with a unique `mcp_query_id`, as a SQL comment:

```sql
SELECT * FROM mysql_testdb.testdb.orders
-- mcp_query_id=3f2a1b4c-e8d7-4a9b-b1c2-0f5e6d7a8b9c
```

The `query_id` is shown at the top of every result. Use it to find the exact execution in the Trino UI (`http://<coordinator>:8080/ui`) or query history. Cache hits don't reach Trino, so they aren't tagged.

### CSV Export

For large results, pass `output_file` with a plain file name. The rows are written as CSV into `--output-dir`, and only a summary goes back to the AI client:

```
Run SELECT * FROM mysql_testdb.testdb.orders on trino-local with output_file=orders.csv
```
```
Results written to /home/you/.cache/meridian/exports/orders.csv (1042 rows, 8 columns)
query_id: 3f2a1b4c-...
```

Directories and `..` are rejected, and CSV export is disabled in SSE mode.

---

## Kubernetes Operator

The operator runs three controllers in one binary:

| Controller | Responsibility |
|---|---|
| `ClusterController` | Lifecycle per cluster. Creates the coordinator and worker Deployments and the coordinator Service, health-gates the move to `Idle`, marks a cluster `Degraded` if its coordinator dies while idle, and recovers evicted coordinators. |
| `ClusterPoolController` | Keeps the warm pool at `spec.replicas`. Creates up to 3 clusters per cycle and deletes the oldest idle cluster when over-provisioned (one per cycle). Replaces `Failed` and `Degraded` clusters, releases reservations older than `reservationTtl`, rolls clusters onto a new image, and recycles clusters older than `maxClusterAge`. |
| `ClusterPoolAutoscalerController` | Adjusts `ClusterPool.spec.replicas` from utilization (`reserved / total`). Scales up at ≥ threshold (default 70%) and down below threshold × 0.75 (hysteresis). |

### Cluster Lifecycle

```
Empty → Pending → Idle ⇄ Reserved
           │        │
           ▼        ▼
        Failed   Degraded   (the pool replaces both)
```

### Architecture

```
┌──────────────────────────────────────────────────────┐
│                       Meridian                       │
│                                                      │
│      ┌─────────────┐          ┌───────────────┐      │
│      │  REST API   │          │  MCP Server   │      │
│      │  Go / mTLS  │          │  Go           │      │
│      └──────┬──────┘          └──────┬────────┘      │
│             │                        │               │
│      ┌──────▼────────────────────────▼──────┐        │
│      │        Kubernetes API Server         │        │
│      └──────┬───────────────────────────────┘        │
│       ┌─────┼─────────────────┐                      │
│       ▼     ▼                 ▼                      │
│  Cluster    ClusterPool    ClusterPool               │
│  Controller Controller     Autoscaler                │
└──────────────────────────────────────────────────────┘
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
      Cluster(Idle)  Cluster(Reserved)  Cluster(Pending)
```

### CRDs (`meridian.io/v1alpha1`)

| Kind | Purpose |
|---|---|
| `Cluster` | A single Trino cluster (coordinator + workers) |
| `ClusterPool` | A hot standby pool of N clusters |
| `ClusterPoolAutoscaler` | Scales a pool by reservation utilization |

### Pod Sizing (Profiles)

Every Trino pod gets CPU and memory from its cluster's profile. The memory request equals the limit, so pods aren't OOM-killed by overcommit. CPU has a request but no limit, because CPU limits throttle the JVM.

| Profile | JVM heap | Pod memory | CPU request |
|---|---|---|---|
| `default` | 4G | 5Gi | 1 |
| `medium` | 8G | 10Gi | 2 |
| `large` | 16G | 20Gi | 4 |

Custom profiles live in a ConfigMap named `meridian-profile-<name>`. All keys are optional:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: meridian-profile-analytics
  namespace: meridian
data:
  jvmHeap: "24G"            # pod memory defaults to 125% of the heap if not set
  queryMaxMemory: "48GB"
  queryMaxMemoryPerNode: "6GB"
  memory: "30Gi"            # both roles
  cpu: "6"
  coordinatorMemory: "16Gi" # role-specific overrides
  workerCPU: "8"
```

Sizes apply to newly created clusters. Existing clusters pick them up when the pool replaces them (image upgrade or `maxClusterAge`).

### Operator Flags

| Flag | Default | Purpose |
|---|---|---|
| `--namespace` | `meridian` | Namespace for leader election, the REST API, and credential Secrets |
| `--cluster-concurrency` | `5` | Clusters reconciled in parallel |
| `--pool-max-creates-per-cycle` | `3` | Clusters a pool creates per reconcile cycle |
| `--health-probe-bind-address` | `:8082` | `/healthz` (process alive) and `/readyz` (caches synced) |
| `--metrics-bind-address` | `:8081` | Prometheus metrics |
| `--rest-addr`, `--tls-cert`, `--tls-key`, `--tls-ca` | — | mTLS reservation REST API. Shuts down gracefully with the operator |
| `--credential-provider` | `kubernetes` | Secret backend for credential rotation: `kubernetes`, `vault`, or `aws-secrets-manager` |

### Run the Operator

```bash
docker pull ghcr.io/meridian-io/trino-operator:v1.2

# or build and run from source against your current kubeconfig
make install   # apply CRDs
make build     # → bin/meridian-operator
./bin/meridian-operator --namespace meridian

# with the mTLS REST API
./bin/meridian-operator --namespace meridian \
  --rest-addr :8443 --tls-cert cert.pem --tls-key key.pem --tls-ca ca.pem
```

### Reserve a Cluster (REST API)

The client identity comes from the mTLS client certificate's CN.

```bash
curl -X POST https://meridian:8443/api/v1/clusters/reservations \
  --cert client.crt --key client.key \
  -H "Content-Type: application/json" \
  -d '{"reservationId": "job-123", "profile": "default"}'
```
```json
{"clusterName": "pool-abc123", "coordinatorUrl": "http://pool-abc123-coordinator.meridian.svc.cluster.local:8080", "reservedAt": "2026-03-01T10:00:00Z"}
```

---

## Local Development

This setup runs Trino and MySQL in Docker and a local kind Kubernetes cluster, so you can test every tool end to end without cloud infrastructure.

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop), then `brew install kind kubectl`.

### 1. Start Trino and MySQL

```bash
cd hack && docker compose up -d
```

| Container | Port | What it is |
|---|---|---|
| `meridian-trino` | `8080` | Trino 435 with `catalog.management=dynamic` |
| `meridian-mysql` | `3306` | MySQL 8.0 with a `testdb` database (user `root`, password `trino_test`) |
| `meridian-adminer` | `8081` | Adminer UI for browsing MySQL |

Wait until Trino is ready:

```bash
curl -sf http://localhost:8080/v1/info | python3 -c "import sys,json; print('ready' if not json.load(sys.stdin).get('starting') else 'starting')"
```

### 2. Create a Local Kubernetes Cluster

```bash
./hack/setup-local.sh
```

This creates a kind cluster named `meridian-dev`, applies the CRDs, seeds test clusters, and registers the Docker Trino as a `Cluster` named `trino-local` (coordinator `http://localhost:8080`).

### 3. Build and Connect the MCP Server

```bash
make build-mcp   # → bin/meridian
```

Point Claude Desktop at the local build. The catalog tools are enabled here because the walkthrough below adds a catalog:

```json
{
  "mcpServers": {
    "meridian": {
      "command": "/path/to/meridian/bin/meridian",
      "args": ["--transport", "stdio", "--namespace", "meridian",
               "--kubeconfig", "/Users/you/.kube/config",
               "--enable-tools", "add_catalog,remove_catalog"]
    }
  }
}
```

Restart Claude Desktop to load the tools.

> **Note:** When adding the MySQL catalog, use `mysql:3306` in the connection URL, not `localhost:3306`. Trino runs inside Docker and reaches MySQL by its Docker network hostname.

### 4. Build and Test

```bash
make build       # operator → bin/meridian-operator
make build-mcp   # MCP server → bin/meridian
make test-all    # all tests (operator + MCP)
make manifests   # regenerate CRDs and RBAC after changing API types
```

## Manual Test Cases

Run these in Claude Desktop with the local environment running.

| # | What to say to Claude | Expected result |
|---|---|---|
| 1 | `List all clusters in the meridian namespace` | Shows `trino-local` and the seeded test clusters with phase and health |
| 2 | `Get details for the cluster named trino-local` | Coordinator URL `http://localhost:8080`, phase, health |
| 3 | `Add a catalog called mysql_testdb to trino-local with connector.name=mysql, connection-url=jdbc:mysql://mysql:3306, connection-user=root, connection-password=trino_test` | `Catalog "mysql_testdb" added to cluster "trino-local"` |
| 4 | `List all catalogs on trino-local` | Shows `mysql_testdb`, `system`, `tpch`. Fresh result, because `add_catalog` cleared the cache |
| 5 | `List all catalogs on trino-local again` | Same result marked `[cache hit]` |
| 6 | `List schemas in the mysql_testdb catalog on trino-local` | `information_schema`, `testdb` |
| 7 | `List tables in mysql_testdb.testdb on trino-local` | The tables in `testdb` |
| 8 | `Show me the schema for the test table in mysql_testdb.testdb on trino-local` | Column `name` of type `char(11)` |
| 9 | `Run SELECT * FROM mysql_testdb.testdb.test on trino-local` | 2 rows with a `query_id` line, no `[cache hit]` |
| 10 | `Run that same query again with ttl=60`, then once more | The third run is marked `[cache hit]` |
| 11 | `Run DROP TABLE mysql_testdb.testdb.test on trino-local` | Blocked: the server is in read-only mode |
| 12 | `Run SELECT * FROM mysql_testdb.testdb.test on trino-local and save it to test.csv` | `Results written to …/meridian/exports/test.csv (2 rows, 1 columns)` |
| 13 | `Remove the mysql_testdb catalog from trino-local` | `Catalog "mysql_testdb" removed from cluster "trino-local"` |

Cache results (5, 10) only hold within one MCP server process. Restarting Claude Desktop clears them.

### Operator Test Cases

Run the operator locally against the kind cluster:

```bash
make install && make build
./bin/meridian-operator --namespace meridian --kubeconfig ~/.kube/config
```

| # | Action | Expected result |
|---|---|---|
| 14 | `kubectl apply -f operator/config/samples/test-cluster.yaml` | Cluster `test-cluster` created with an empty phase |
| 15 | `kubectl get cluster test-cluster -n meridian -w` | Phase moves to `Pending` within seconds |
| 16 | `kubectl get deployments -n meridian` | `test-cluster-coordinator` (1 replica) and `test-cluster-worker` (2 replicas) |
| 17 | `kubectl get deploy test-cluster-worker -n meridian -o jsonpath='{.spec.template.spec.containers[0].resources}'` | Memory request and limit `5Gi`, CPU request `1` (default profile) |
| 18 | Wait for the coordinator pod to be Ready | Phase `Pending` → `Idle`, `ready: true`, `idleAt` set |
| 19 | `kubectl patch cluster test-cluster -n meridian --type=merge -p '{"spec":{"clientId":"client-abc","reservationId":"res-001"}}'` | Phase `Idle` → `Reserved`, `reservedAt` set |
| 20 | `kubectl patch cluster test-cluster -n meridian --type=merge -p '{"spec":{"clientId":"","reservationId":""}}'` | Phase `Reserved` → `Idle` |
| 21 | `kubectl apply -f operator/config/samples/test-clusterpool.yaml` | ClusterPool `test-pool` with `replicas: 2`. Two clusters `test-pool-<suffix>` are created |
| 22 | `kubectl patch clusterpool test-pool -n meridian --type=merge -p '{"spec":{"replicas":1}}'` | Within 30s the oldest idle cluster is deleted |
| 23 | `kubectl patch clusterpool test-pool -n meridian --type=merge -p '{"spec":{"replicas":3}}'` | Two new clusters are created |
| 24 | `kubectl delete clusterpool test-pool -n meridian` | All pool clusters are deleted (owner references) |
| 25 | `kubectl delete cluster test-cluster -n meridian` | Its Deployments and Service are deleted |

---

## Beyond AI Agents

The MCP server is a standard RPC interface. Any system that automates Trino cluster lifecycle can use it. For example, a CI job can reserve a fresh cluster for integration tests and release it afterwards (pseudo-code, using any MCP client):

```yaml
- name: Reserve test cluster
  run: |
    RESULT=$(mcp-client call reserve_cluster \
      --workload ci --client-id github-actions --reservation-id ${{ github.run_id }})
    echo "CLUSTER_NAME=$(echo $RESULT | jq -r .clusterName)" >> $GITHUB_ENV
    echo "COORDINATOR_URL=$(echo $RESULT | jq -r .coordinatorUrl)" >> $GITHUB_ENV

- name: Run integration tests
  run: ./test.sh $COORDINATOR_URL

- name: Release cluster
  if: always()
  run: mcp-client call release_cluster --cluster-name $CLUSTER_NAME --client-id github-actions
```

The same pattern works as tasks in Airflow, Dagster, or Prefect.

## Why Meridian

- **There is no official open-source Trino operator.** [trinodb/trino#396](https://github.com/trinodb/trino/issues/396) has been open since 2019.
- **There is no official Trino MCP server.** [trinodb/trino#26239](https://github.com/trinodb/trino/issues/26239) is open. Meridian covers cluster management, not just query execution.
- **One interface for everyone.** CI/CD, orchestrators, internal tools, and AI agents all use the same binary.

## Roadmap

| Area | Status | Description |
|---|---|---|
| MCP Server | ✅ Released | 19 tools (12 on by default), stdio + SSE, query cache with singleflight, query tagging, CSV export |
| Kubernetes Operator | ✅ Released | Cluster lifecycle with health gating and `Degraded` recovery, hot standby pools, autoscaler, rolling image upgrades, age recycling, Trino Gateway integration, workload-labeled pools |
| REST API | ✅ Released | Reserve, release, list, get, and scale over mTLS with per-client ownership checks |
| Credential Rotation | ✅ Released | Rotation without cluster restart from Kubernetes Secrets, Vault, or AWS Secrets Manager |
| Profile System | ✅ Released | Trino config and pod CPU/memory per profile, built-in and custom |
| Production Hardening (v1.3) | 🔨 In progress | Secure-by-default MCP server, operator hardening, Helm chart, CI checks |
| Metrics & E2E Tests | 📋 Planned | Prometheus metrics, Grafana dashboards, kind-based end-to-end tests against real Trino |
| Web UI | 📋 Planned | Pool visualization, catalog management, audit trail |

---

## License

Apache 2.0. See [LICENSE](LICENSE).
