# Meridian

**The open-source Trino control plane and MCP server for Kubernetes.**

**[getmeridian.dev](https://getmeridian.dev)** · **v1.1**

Trino clusters take 30–90 seconds to provision. Meridian eliminates that wait — a hot standby pool keeps pre-provisioned clusters idle and ready, reserved in zero seconds. No cold starts for CI/CD jobs, batch workloads, or multi-tenant platforms.

Meridian is two things in one: a **Kubernetes operator** that manages the full cluster lifecycle (`Empty → Pending → Idle → Reserved`), and an **MCP server** with 19 tools so Claude, Cursor, or any MCP client can provision clusters, add catalogs, rotate credentials, and run queries through natural language.

The platform large data teams built internally. Now open source.

---

## MCP Server — Start Here

The `meridian` binary is a standalone MCP server that works against **any Trino deployment on Kubernetes**. Install the operator for the full hot standby pool experience; the MCP server is useful on day one without it.

### Install

**Binary (all platforms):**
```bash
# macOS arm64
curl -Lo meridian.tar.gz https://github.com/meridian-io/meridian/releases/download/v1.1/trino-mcp-server_v1.1_darwin_arm64.tar.gz
tar xzf meridian.tar.gz && sudo mv meridian /usr/local/bin/
```

**Docker:**
```bash
docker pull ghcr.io/meridian-io/trino-mcp-server:v1.1
```

### Run

```bash
# stdio — for Claude Desktop / local MCP clients
meridian --transport stdio --namespace meridian

# SSE — for remote / team use (Cursor, Claude API)
meridian --transport sse --addr :8080 --namespace meridian

# In-cluster (reads service account token automatically)
meridian --transport sse --addr :8080
```

### Connect to Claude Desktop

Add to `~/.config/claude/claude_desktop_config.json`:

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

### Available MCP Tools

| Tool | Description | Cache TTL |
|---|---|---|
| `list_clusters` | List all clusters with phase, health, coordinator URL | — |
| `get_cluster` | Get full details of a specific cluster | — |
| `create_cluster` | Provision a new Trino cluster from a profile | — |
| `delete_cluster` | Tear down a cluster | — |
| `reserve_cluster` | Assign an idle cluster to a client (idempotent) | — |
| `release_cluster` | Return a cluster to the idle pool | — |
| `scale_pool` | Set desired replica count on a ClusterPool | — |
| `list_pools` | List all ClusterPools with current state | — |
| `add_catalog` | Push a new catalog to a cluster — no restart | invalidates cache |
| `remove_catalog` | Remove a catalog — no restart | invalidates cache |
| `list_catalogs` | List all catalogs registered in a cluster | 5 min |
| `list_schemas` | List schemas in a catalog | 5 min |
| `list_tables` | List tables in a catalog schema | 5 min |
| `get_table_schema` | Get column names and data types for a table | 5 min |
| `execute_query` | Execute SQL and return results as a table. Tagged with `mcp_query_id` for audit correlation. Use `output_file` to write large results as CSV. | none (opt-in via `ttl` param) |
| `explain_query` | Return the Trino execution plan for a SQL statement without executing it | 5 min |
| `rotate_credentials` | Trigger Vault/ASM credential refresh | — |
| `get_audit_log` | Fetch cluster lifecycle and management events | — |
| `promote_environment` | Promote config from dev → staging → prod | — |

### Query Result Caching

Meridian caches Trino query results in memory to avoid redundant round-trips for repeated calls within the same session.

| Tool | Default TTL | Rationale |
|---|---|---|
| `execute_query` | **none** | Data changes must be visible immediately — opt in with `ttl=N` for static data |
| `list_catalogs` | 5 minutes | Catalog list rarely changes |
| `list_schemas` | 5 minutes | Schema list rarely changes |
| `list_tables` | 5 minutes | Table list rarely changes |
| `get_table_schema` | 5 minutes | Column metadata rarely changes |

**How it works:**

1. First call → cache miss → query hits Trino → result stored with expiry timestamp
2. Same call within TTL → cache hit → result returned instantly, marked `[cache hit]`
3. After TTL expires → next call hits Trino again and refreshes the cache
4. `add_catalog` / `remove_catalog` immediately invalidate the catalog cache so `list_catalogs` always reflects the current state

**Singleflight:** If two identical queries arrive simultaneously (e.g. Claude calling the same tool twice in one turn), only one hits Trino. The second waits and is served from cache once the first completes.

**Opt-in caching for `execute_query`:** Pass `ttl=N` when querying static or reference data:

```
Run SELECT * FROM mysql_testdb.testdb.orders on trino-local with ttl=60
```

**Cache key:** SHA-256 hash of `(coordinatorURL + sql)`. Different SQL or different cluster = distinct cache entry, no collisions.

**Edge case — `add_catalog`:** When you add or remove a catalog via MCP, the cache for that coordinator is immediately invalidated. `list_catalogs` called right after will always hit Trino and return the current state — no stale data.

### Query Tagging

Every SQL statement sent to Trino is automatically tagged with a unique `mcp_query_id`:

```sql
SELECT * FROM mysql_testdb.testdb.orders
-- mcp_query_id=3f2a1b4c-e8d7-4a9b-b1c2-0f5e6d7a8b9c
```

The tag appears as a SQL comment, so Trino executes it normally. The `query_id` is shown at the top of every result:

```
query_id: 3f2a1b4c-e8d7-4a9b-b1c2-0f5e6d7a8b9c
| name        |
|-------------|
| 55555577    |
| 76y56       |

(2 rows)
```

Use it to look up the exact execution in the Trino UI (`http://<coordinator>:8080/ui`) or query history — useful when debugging slow queries or unexpected results.

Note: tags are applied only on fresh Trino calls. Cache hits (`[cache hit]`) skip tagging since the query never reaches Trino again.

### CSV File Export

For large result sets that would overflow the context window, use `output_file` to write results as CSV instead of returning them inline:

```
Run SELECT * FROM mysql_testdb.testdb.orders on trino-local with output_file=/tmp/orders.csv
```

Returns:
```
Results written to /tmp/orders.csv (1042 rows, 8 columns)
query_id: 3f2a1b4c-...
```

The file includes a header row followed by all data rows. The full table is never sent to the LLM — only the summary.

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                   Project Meridian                    │
│                                                      │
│  ┌────────────┐  ┌─────────────┐  ┌───────────────┐  │
│  │  Web UI    │  │  REST API   │  │  MCP Server   │  │
│  │  Next.js   │  │  Go/mTLS    │  │  Go · 19 tools│  │
│  └────────────┘  └──────┬──────┘  └──────┬────────┘  │
│                         │                │           │
│                ┌────────▼────────────────▼──────┐    │
│                │       Kubernetes API Server     │    │
│                └────────┬────────────────────────┘    │
│       ┌─────────────────┼──────────────┐             │
│       ▼                 ▼              ▼             │
│  ClusterController  ClusterPool   ClusterPool        │
│                     Controller    Autoscaler         │
└──────────────────────────────────────────────────────┘
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
      Cluster(Idle)  Cluster(Reserved)  Cluster(Pending)
```

## Cluster Lifecycle

```
Empty → Pending → Idle → Reserved
                    ↓
                  Failed
```

## Components

| Directory | Language | Description |
|---|---|---|
| `mcp/` | Go | MCP server — **ships first**, standalone binary |
| `operator/` | Go | Kubernetes operator — controllers, CRDs, REST API |
| `ui/` | Next.js | Web management dashboard |
| `charts/` | Helm | One-command install |
| `config/` | YAML | CRD manifests |
| `docs/` | HTML | Architecture and API documentation |

## Kubernetes Operator

The operator manages the full Trino cluster lifecycle on Kubernetes. Three controllers run in a single binary:

| Controller | Responsibility |
|---|---|
| `ClusterController` | Lifecycle per cluster: `Empty → Pending → Idle → Reserved → Failed`. Creates coordinator Deployment, worker Deployment, and coordinator Service. Health-gates transition to Idle. Handles coordinator eviction recovery. |
| `ClusterPoolController` | Maintains hot standby pool: creates clusters to reach `spec.replicas`, deletes oldest idle cluster when over-provisioned (one per cycle), purges failed clusters immediately. |
| `ClusterPoolAutoscalerController` | Adjusts `ClusterPool.spec.replicas` based on utilization (`reserved / total`). Scales up at ≥ threshold (default 70%), scales down at < threshold × 0.75 (hysteresis). |

### Docker Images

```bash
# MCP server — for Claude Desktop, CI/CD, AI agents
docker pull ghcr.io/meridian-io/trino-mcp-server:v1.1

# Kubernetes operator — control plane
docker pull ghcr.io/meridian-io/trino-operator:v1.1
```

### Build and Run Locally

```bash
# Build
cd operator && go build -o ../bin/meridian-operator .

# Apply CRDs
kubectl apply -f operator/config/crd/bases/

# Run (reads ~/.kube/config by default)
./bin/meridian-operator --namespace meridian

# With REST API enabled
./bin/meridian-operator --namespace meridian \
  --rest-addr :8443 \
  --tls-cert /path/to/cert.pem \
  --tls-key /path/to/key.pem
```

### Run Tests

```bash
cd operator && go test ./internal/...
```

### Reserve a Cluster (REST API)

```bash
curl -X POST https://meridian:8443/api/v1/clusters/reservations \
  --cert client.crt --key client.key \
  -H "Content-Type: application/json" \
  -d '{"reservationId": "job-123", "profile": "default"}'
```

Returns:
```json
{"clusterName": "pool-abc123", "coordinatorUrl": "http://pool-abc123-coordinator.meridian.svc.cluster.local:8080", "reservedAt": "2026-03-01T10:00:00Z"}
```

### Install via Helm (Phase 6)

```bash
helm install meridian charts/meridian \
  --namespace meridian \
  --create-namespace
```

## Local Development & E2E Testing

This setup runs a real Trino cluster and MySQL database locally using Docker, then connects the MCP server to it via a kind Kubernetes cluster. You can test every catalog and query tool end-to-end without any cloud infrastructure.

### Prerequisites

```bash
# Docker Desktop — https://www.docker.com/products/docker-desktop
# Then install kind and kubectl
brew install kind kubectl
```

### 1. Start Trino and MySQL

```bash
cd hack
docker compose up -d
```

This starts three containers:

| Container | Port | What it is |
|---|---|---|
| `meridian-trino` | `8080` | Trino 435 with `catalog.management=dynamic` enabled |
| `meridian-mysql` | `3306` | MySQL 8.0 with a `testdb` database |
| `meridian-adminer` | `8081` | Adminer UI for browsing MySQL |

Wait for Trino to be ready:

```bash
curl -sf http://localhost:8080/v1/info | python3 -c "import sys,json; print('ready' if not json.load(sys.stdin).get('starting') else 'starting')"
```

MySQL credentials: host `localhost`, port `3306`, user `root`, password `trino_test`, database `testdb`.

### 2. Create a Local Kubernetes Cluster

```bash
./hack/setup-local.sh
```

This creates a kind cluster named `meridian-dev`, applies the Meridian CRDs, seeds test Cluster fixtures, and creates a `trino-local` Cluster CR that points to the Docker Compose Trino at `http://localhost:8080`.

### 3. Build and Run the MCP Server

```bash
cd mcp && go build -o ../bin/meridian ./cmd/meridian-mcp/

# Run locally
../bin/meridian --transport stdio \
  --namespace meridian \
  --kubeconfig ~/.kube/config
```

### 4. Connect to Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

```json
{
  "mcpServers": {
    "meridian": {
      "command": "/path/to/meridian",
      "args": ["--transport", "stdio", "--namespace", "meridian", "--kubeconfig", "/Users/you/.kube/config"]
    }
  }
}
```

Restart Claude Desktop. You should see the Meridian tools available.

### 5. E2E Test — Add a MySQL Catalog and Query It

With Claude Desktop connected, test the full flow:

**Step 1 — Add MySQL as a catalog:**
> Add a catalog called `mysql_testdb` to the cluster named `trino-local` with these properties:
> ```
> connector.name=mysql
> connection-url=jdbc:mysql://mysql:3306
> connection-user=root
> connection-password=trino_test
> ```

**Step 2 — Browse the catalog:**
> List the schemas in the `mysql_testdb` catalog on `trino-local`

**Step 3 — Query the data:**
> Run `SHOW TABLES FROM mysql_testdb.testdb` on `trino-local`

**Verify directly in Trino:**
```bash
docker exec meridian-trino trino --execute "SHOW CATALOGS"
docker exec meridian-trino trino --execute "SHOW SCHEMAS FROM mysql_testdb"
```

> **Note:** When adding a MySQL catalog, use `mysql:3306` as the connection URL (not `localhost:3306`). Trino runs inside Docker and reaches MySQL via the internal Docker network hostname `mysql`.

---

```bash
# MCP server
cd mcp && go build -o ../bin/meridian ./cmd/meridian-mcp

# Kubernetes operator
cd operator && go build -o ../bin/meridian-operator .

# Or use Make
make build       # operator
make build-mcp   # MCP server
make test        # all tests
```

## Manual Testing Use Cases

The following test cases cover all working tools end-to-end. Run them in Claude Desktop with the local dev environment running (`docker compose up -d` + `./hack/setup-local.sh`).

**Reference table used in tests:**

| Database | Table | Rows | Column |
|---|---|---|---|
| MySQL `testdb` | `test` | 2 | `name` (char 11) |

---

| # | What to say to Claude | Expected result |
|---|---|---|
| 1 | `List all clusters in the meridian namespace` | Shows `trino-local`, `trino-idle-01`, `trino-idle-02`, `trino-pending-01`, `trino-reserved-01` with phase and health |
| 2 | `Get details for the cluster named trino-local in the meridian namespace` | Shows coordinator URL `http://localhost:8080`, phase, health status |
| 3 | `Add a catalog called mysql_testdb to trino-local with connector.name=mysql, connection-url=jdbc:mysql://mysql:3306, connection-user=root, connection-password=trino_test` | Returns `Catalog "mysql_testdb" added to cluster "trino-local"` |
| 4 | `List all catalogs on trino-local` | Shows `mysql_testdb`, `system`, `tpch` — result is fresh (cache was busted by add_catalog) |
| 5 | `List all catalogs on trino-local again` (same conversation) | Same result with `[cache hit]` — served from 5-minute cache |
| 6 | `List schemas in the mysql_testdb catalog on trino-local` | Shows `information_schema`, `testdb` |
| 7 | `List tables in mysql_testdb.testdb on trino-local` | Shows `orders`, `test` (or whatever tables exist) |
| 8 | `Show me the schema for the test table in mysql_testdb.testdb on trino-local` | Shows column `name` with type `char(11)` |
| 9 | `Run SELECT * FROM mysql_testdb.testdb.test on trino-local` | Returns 2 rows: `55555577`, `76y56` — no `[cache hit]` (execute_query has no cache by default) |
| 10 | `Run that same query again` (same conversation) | Returns 2 rows again — still no `[cache hit]` (correct — data may have changed) |
| 11 | `Run SELECT * FROM mysql_testdb.testdb.test on trino-local with ttl=60` | Returns 2 rows, no `[cache hit]` on first call |
| 12 | `Run that same query again` (same conversation, within 60s) | Returns 2 rows with `[cache hit]` — opt-in cache is working |
| 13 | `Run SELECT * FROM mysql_testdb.testdb.test on trino-local` | Result includes `query_id: <uuid>` on the first line — query tagging is working |
| 14 | Open `http://localhost:8080/ui` in a browser, find the query by the `mcp_query_id` in the query text | Query appears in Trino UI with the tag comment visible |
| 15 | `Run SELECT * FROM mysql_testdb.testdb.test on trino-local and save the results to /tmp/test.csv` | Returns `Results written to /tmp/test.csv (2 rows, 1 columns)` — no table inline |
| 16 | `cat /tmp/test.csv` (in terminal) | CSV file contains header `name` and 2 data rows |
| 17 (cleanup) | `Remove the mysql_testdb catalog from trino-local` | Returns `Catalog "mysql_testdb" removed from cluster "trino-local"` |

> **Note:** Tests 5, 12 require running the follow-up in the **same Claude Desktop session** — the cache is in-memory and resets when the MCP process restarts.

---

### Phase 2 — Operator Manual Tests

Prerequisites: kind cluster running, CRDs applied, operator running locally.

```bash
kubectl apply -f operator/config/crd/bases/
./bin/meridian-operator --namespace meridian --kubeconfig ~/.kube/config
```

#### Cluster Lifecycle (Empty → Pending → Idle → Reserved → Idle)

| # | Action | Expected result |
|---|---|---|
| 18 | `kubectl apply -f operator/config/samples/test-cluster.yaml` | Cluster `test-cluster` created with phase `""` (Empty) |
| 19 | `kubectl get cluster test-cluster -n meridian -w` | Phase transitions: `""` → `Pending` within seconds as operator creates Deployments and Service |
| 20 | `kubectl get deployments -n meridian` | `test-cluster-coordinator` (1 replica) and `test-cluster-worker` (2 replicas) exist |
| 21 | `kubectl get svc -n meridian` | `test-cluster-coordinator` Service exists on port 8080 |
| 22 | Wait for coordinator pod to be Ready, then watch cluster | Phase transitions `Pending` → `Idle`, `ready: true`, `idleAt` timestamp set |
| 23 | `kubectl patch cluster test-cluster -n meridian --type=merge -p '{"spec":{"clientId":"client-abc","reservationId":"res-001"}}'` | Cluster transitions `Idle` → `Reserved`, `reservedAt` timestamp set |
| 24 | `kubectl get cluster test-cluster -n meridian -o jsonpath='{.status.phase}'` | Returns `Reserved` |
| 25 | `kubectl patch cluster test-cluster -n meridian --type=merge -p '{"spec":{"clientId":"","reservationId":""}}'` | Cluster transitions `Reserved` → `Idle`, `idleAt` refreshed |

#### Hot Standby Pool (ClusterPool)

| # | Action | Expected result |
|---|---|---|
| 26 | `kubectl apply -f operator/config/samples/test-clusterpool.yaml` | ClusterPool `test-pool` created with `spec.replicas: 2` |
| 27 | `kubectl get clusters -n meridian -l meridian.io/cluster-pool=test-pool -w` | 2 clusters created automatically (`test-pool-<suffix>`), each transitioning Empty → Pending → Idle |
| 28 | `kubectl patch clusterpool test-pool -n meridian --type=merge -p '{"spec":{"replicas":1}}'` | After next reconcile (≤30s), oldest idle cluster deleted — 1 cluster remains |
| 29 | `kubectl patch clusterpool test-pool -n meridian --type=merge -p '{"spec":{"replicas":3}}'` | 2 new clusters created to reach desired count of 3 |
| 30 | `kubectl get clusterpool test-pool -n meridian -o jsonpath='{.status}'` | Shows `readyReplicas`, `pendingReplicas`, `reservedReplicas` counts |

#### Cleanup

| # | Action | Expected result |
|---|---|---|
| 31 | `kubectl delete clusterpool test-pool -n meridian` | Owner references cascade — all pool clusters deleted automatically |
| 32 | `kubectl delete cluster test-cluster -n meridian` | Coordinator Deployment, worker Deployment, and Service deleted via owner references |

---

## CRDs

```yaml
# meridian.io/v1alpha1
Cluster              — single Trino cluster lifecycle
ClusterPool          — hot standby pool of N clusters
ClusterPoolAutoscaler — scale pool by reservation utilization
```

## Beyond AI Agents

The MCP server is a standard RPC interface — any system that automates Trino cluster lifecycle can use it, not just LLMs.

### CI/CD Pipelines

Reserve a fresh cluster for integration tests, run them, release when done:

```yaml
# GitHub Actions
- name: Reserve test cluster
  run: |
    RESULT=$(mcp-client call reserve_cluster \
      --profile ci --reservation-id ${{ github.run_id }})
    echo "COORDINATOR_URL=$(echo $RESULT | jq -r .coordinatorUrl)" >> $GITHUB_ENV

- name: Run integration tests
  run: ./test.sh $COORDINATOR_URL

- name: Release cluster
  if: always()
  run: mcp-client call release_cluster --cluster-name ${{ env.CLUSTER_NAME }}
```

### Workflow Orchestrators

Manage cluster lifecycle as part of an Airflow / Dagster / Prefect DAG:

```python
@task
def reserve_trino_cluster(dag_run):
    return mcp.call("reserve_cluster", profile="batch", reservation_id=dag_run.run_id)

@task
def run_transformation(cluster):
    run_dbt(target=cluster["coordinatorUrl"])

@task
def release_trino_cluster(cluster):
    mcp.call("release_cluster", cluster_name=cluster["clusterName"])
```

### Other MCP Clients

| Client | Use case |
|---|---|
| Slack / PagerDuty bot | On-call engineer scales pool via button click |
| Backstage plugin | App developers provision clusters via form, no kubectl access |
| Terraform / Pulumi provider | Declare cluster pools as infrastructure-as-code |

## Why Meridian

- **No open-source Trino control plane exists** — [trinodb/trino #396](https://github.com/trinodb/trino/issues/396) open since 2019; Meridian is the answer
- **No official Trino MCP server exists** — [trinodb/trino #26239](https://github.com/trinodb/trino/issues/26239) open; Meridian ships 19 management tools, not just query execution
- **Works as a universal operations interface** — CI/CD, orchestrators, internal tooling, and AI agents all use the same binary

## Roadmap

| Phase | Status | Description |
|---|---|---|
| **Phase 1 — MCP Server** | ✅ Complete | 19 MCP tools, Go binary, stdio + SSE transport, local dev setup, TTL query result cache (5min for schema metadata, opt-in for queries, singleflight deduplication, auto-invalidation on catalog changes), query tagging (`mcp_query_id` for audit correlation), CSV file export for large result sets, query execution plan via `explain_query` |
| **Phase 2 — Kubernetes Operator** | ✅ Complete | ClusterController (Empty→Pending→Idle→Reserved→Idle→Degraded), ClusterPoolController (hot standby pool, gradual scale-down, oldest-first selection, age recycling via `maxClusterAge`, rolling image upgrades), ClusterPoolAutoscalerController (utilization-based with hysteresis), Trino Gateway integration, workload-labeled pools |
| **Phase 3 — REST API** | ✅ Complete | Full REST surface: reserve, release, list clusters (phase/profile/workload filters), get cluster, list pools, scale pool — all with mTLS and cross-client ownership validation |
| **Phase 4 — Catalog & Credential Layer** | ✅ Complete | Annotation-driven credential rotation without cluster restart. Supports Kubernetes Secrets, HashiCorp Vault (K8s auth, KV v2), and AWS Secrets Manager (IRSA). TTL cache with proactive refresh, exponential backoff, and `CredentialRotation` condition on the Cluster object. |
| **Phase 5 — Profile System** | 🔨 In Progress | ClusterPool profile templates — inject Trino `config.properties`, `jvm.config`, and catalog definitions into coordinator and worker pods at provision time |
| **Phase 6 — Web UI** | 📋 Planned | Next.js dashboard — cluster pool visualization, catalog management UI, audit trail viewer |
| **Phase 7 — Helm Chart & Docs** | 📋 Planned | One-command install, quickstart guide, full architecture documentation |

---

## License

Apache 2.0 — see [LICENSE](LICENSE)
