# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [v1.4.1] — 2026-10-26

Fixes two operator bugs found by the new end-to-end tests on a real cluster.

### Fixed
- **Clusters were marked Idle before Trino could run queries.**
  - What happened: a cluster became Idle as soon as its coordinator container started. Queries on a freshly Idle cluster failed for about 30 seconds in testing ("connection refused", then "Trino server is still initializing").
  - The fix: Trino pods now have a readiness probe that runs the image's own `/usr/lib/trino/bin/health-check`. A cluster becomes Idle only when its coordinator and at least one worker have finished starting. Images without that script keep the old behaviour.
  - Scope: applies to clusters created after the upgrade. Existing clusters are unchanged; delete idle ones to have the pool replace them.
- **Retrying a REST reservation dropped the connection.**
  - What happened: repeating `POST /api/v1/clusters/reservations` before the operator recorded the first one (usually under a second) crashed the request. The client got no response, while the cluster stayed reserved.
  - The fix: a repeated request now returns the same cluster, as documented.

### Added
- **End-to-end tests in CI.** Every pull request runs Meridian on a kind cluster with real Trino:
  - It installs the last published chart, upgrades it to the PR's build, and checks pools, MCP queries, `create_cluster` / `delete_cluster`, reservations over REST and MCP, scaling, coordinator crash recovery, credential rotation, and an MCP rolling restart with no failed calls.
  - Run it locally with `make e2e`.
- **Helm:** optional `nodePort` for the MCP and REST Services (`mcp.service.nodePort`, `operator.rest.service.nodePort`) when their type is `NodePort`.

---

## [v1.4.0] — 2026-10-22

The MCP server now follows the current MCP specification, revision 2026-07-28.

### Upgrade notes
- **The remote endpoint is now `/mcp` (Streamable HTTP).** Point clients at `http(s)://<host>/mcp`. For example, Claude Code: `claude mcp add --transport http meridian https://…/mcp --header "Authorization: Bearer …"`.
- **`--transport http` replaces `--transport sse`.** `sse` still works as a deprecated alias.
- **The old HTTP+SSE transport (`/sse`, `/message`) is deprecated.** It is still served by default (`--legacy-sse`, Helm `mcp.legacySSE`) and will be removed in v1.5.
- **Requests with an `Origin` header are rejected (403)** unless listed in `--allowed-origins`, which is meant for a web UI behind the same proxy. This is not CORS. Non-browser clients send no Origin and are unaffected.
- **Tool failures are now tool results with `isError: true`** instead of JSON-RPC errors, as the spec requires. Clients that parsed the old error message should read the result content instead.
- `make run-mcp-sse` is now `make run-mcp-http`.

### Added
- **MCP 2026-07-28 support.**
  - Stateless requests with per-request metadata, plus `server/discover`.
  - Strict checks on the `MCP-Protocol-Version`, `Mcp-Method` and `Mcp-Name` headers.
  - Clients from older revisions (2025-03-26 through 2025-11-25) work on the same endpoint.
- **Multiple MCP replicas.** `/mcp` keeps no session state, so any replica serves any request (Helm `mcp.replicas`; requires `mcp.legacySSE: false`).
- **Display titles** for every tool.
- **New flags:**
  - `--allowed-origins`
  - `--disable-localhost-protection`
  - `--legacy-sse`
- **Request body limit:** 1 MiB, rejected with 413 (also for chunked bodies).
- **Conformance tests** cover each applicable spec rule over HTTP and stdio.

### Fixed
- **Query and catalog tools failed on clusters created by the operator.** The MCP server read `status.coordinatorURL`, but the CRD field is `status.coordinatorUrl`. This broke every query, catalog and reservation tool against operator-managed clusters with "has no coordinator URL". A new test pins the fields the MCP server reads to the generated CRDs.
- **`create_cluster` always failed** with "spec.profile: Required value". It set the profile only as a label. The local-dev fixtures (`hack/cluster-local.yaml`, `config/samples/test-clusters.yaml`) had the same problem, and also used the wrong `coordinatorURL` key, so `hack/setup-local.sh` failed. All are fixed.
- `scale_pool` returned an always-empty `status`. It now returns the ready and reserved counts.
- **`list_pools` now returns current counts** (ready, reserved, pending, failed, degraded) as its description promised.
- **Rolling restarts of the MCP server no longer drop requests.** On SIGTERM it keeps serving for `--shutdown-delay` (default 5s) while reporting not ready, then shuts down. Before this fix, about 3% of calls failed during a restart in testing; after it, none did.

### Changed
- Upgraded `github.com/mark3labs/mcp-go` from v0.44.1 to v1.3.0.

---

## [v1.3.0] — 2026-10-22 (withdrawn)

> **Withdrawn.** In this release the MCP query, catalog and reservation tools fail on clusters created by the operator (`coordinatorUrl` field mismatch). Use v1.4.0.

### Upgrade notes (breaking defaults)
- **SSE needs a token.** Set `MERIDIAN_MCP_TOKEN` or `--auth-token-file`; clients send `Authorization: Bearer <token>`. `--insecure-no-auth` restores the old behavior for local testing.
- **`execute_query` is read-only by default.** Start with `--allow-writes` to run INSERT/DROP/etc.
- **7 tools are off by default:** `create_cluster`, `delete_cluster`, `scale_pool`, `add_catalog`, `remove_catalog`, `rotate_credentials`, `promote_environment`. Enable with `--enable-tools=<names>` or `--enable-tools=all`.
- **`output_file` takes a file name**, written to `--output-dir` (default: a per-user cache dir, e.g. `~/.cache/meridian/exports`). Absolute paths outside that directory (like `/tmp/results.csv`) are rejected. It is disabled in SSE mode.
- **`execute_query` returns at most 10,000 rows**, including CSV exports. `max_rows=0` used to mean "no limit" and now means 10,000. Results over the limit are marked truncated, and the query is cancelled in Trino.
- **`reserve_cluster` requires non-empty `client_id` and `reservation_id`.**
- **A bad `--tls-ca` stops the operator.** If the REST API's CA file is missing or holds no certificates, the operator now refuses to start instead of silently accepting any client.
- **Building from source needs Go 1.26.**

### Security
- Fixed SQL injection in `list_schemas`, `list_tables`, `get_table_schema` (identifiers are now quoted) and in `add_catalog` / `remove_catalog` (names validated).
- Fixed arbitrary file write through `execute_query.output_file`.
- `EXPLAIN ANALYZE` is blocked in read-only mode because it executes the statement.
- CSV export no longer follows symlinks or hard links, and refuses an output directory that other users can write to.
- `/readyz` no longer returns internal error details to unauthenticated callers.
- A statement is never re-submitted after a 502/504 from a proxy, so it can't run twice.
- Corrected tool safety labels (`execute_query`, `scale_pool`, `promote_environment`).

### Fixed
- `reserve_cluster`: two clients could reserve the same cluster. Reservations now use optimistic locking and retry the next idle cluster on conflict. Repeating the same `(client_id, reservation_id)` returns the same cluster, as documented.
- SSE told remote clients to post to `http://localhost:<port>`. The message endpoint is now relative, or set with `--base-url`.
- Catalog cache invalidation after `add_catalog` / `remove_catalog` never matched any entry. A query in flight during invalidation no longer re-caches stale results.
- A cancelled request no longer fails other callers waiting on the same query.
- Cached results ignored `max_rows`.
- Queries cut off at `max_rows` kept running in Trino; they are now cancelled.
- The MCP server didn't notice an operator installed after it started.

### Operator
- **Pod resources from profiles.** Trino pods get memory requests/limits and CPU requests (default 5Gi/1, medium 10Gi/2, large 20Gi/4). Custom profiles can set `memory`, `cpu`, and role-specific overrides. Previously pods had none and could starve their node. Applies to newly created clusters.
- **Faster pool fill.** Pools create up to 3 clusters per cycle (`--pool-max-creates-per-cycle`), and up to 5 clusters reconcile in parallel (`--cluster-concurrency`).
- **Readiness probe.** `/readyz` passes only after the operator's caches have synced.
- **REST API lifecycle.** The mTLS REST API now starts and stops with the operator and drains in-flight requests on shutdown. A REST startup failure (e.g. a bad certificate) now stops the operator instead of being logged and ignored.
- **Fixed:** RBAC was missing `get` on Secrets, so credential rotation with the Kubernetes provider was forbidden in a real cluster. Secrets are now read directly instead of through a cluster-wide cache.
- **Fixed:** `make install` and `hack/setup-local.sh` applied stale CRDs that silently dropped `workload`, `gateway`, `maxClusterAge`, and `reservationTtl`. There is now one generated copy in `operator/config/crd/bases`, plus `make verify-manifests` to catch drift.
- **Fixed:** cluster names could collide when several were created at once. Names now use a random 8-character suffix.
- Init containers have explicit resources. No container sets a CPU limit (it throttles the JVM), so a namespace quota on `limits.cpu` needs a LimitRange with a default CPU limit.
- RBAC now includes `update` on `clusters/finalizers` and `clusterpools/finalizers`, required where owner-reference permissions are enforced (e.g. OpenShift).
- A custom profile ConfigMap named after a built-in tier (e.g. `meridian-profile-large`) now starts from that tier instead of `default`.
- Pools recount clusters against the API server before creating more, so a lagging cache can't make them over-create.

### Helm chart and CI
- **Helm chart** (`charts/meridian`, published as `oci://ghcr.io/meridian-io/charts/meridian`). Installs the CRDs, the operator (cluster-wide, one install per cluster) with RBAC and leader election, and optionally the mTLS REST API and the MCP server. The MCP server needs a token Secret, and its permissions match the tools you enable. Pods run as non-root with a read-only root filesystem and no capabilities.
- **REST API fails closed** on a bad `--tls-ca` (see Upgrade notes). Without `--tls-ca`, the operator logs a warning. Added a 10s header-read timeout.
- **Go 1.26.** Both modules now require Go 1.26, and releases build with the latest 1.26 patch. Go 1.23 no longer receives security fixes, and the patched `golang.org/x/net` needs 1.26. This clears all 30+ known vulnerabilities govulncheck reported.
- **CI** runs build, vet, race tests, golangci-lint, and govulncheck for both modules, plus gofmt, a generated-manifest drift check (operator and chart), and `helm lint` + kubeconform.
- Fixed error messages containing `%` being garbled in operator logs.

### Added
- `/healthz` and `/readyz` endpoints, graceful shutdown on SIGTERM, keep-alive on SSE streams.
- Structured logging (`--log-format json|text`), bounded query cache (`--cache-max-entries`), retries when Trino returns 502/503/504, shared HTTP connection pool, 30s Kubernetes API timeout.
- `release_cluster` takes an optional `client_id` ownership check.
- Unit tests for the MCP server (previously none).

---

## [v1.0] – [v1.2] — 2026-03

Added the Kubernetes operator (Cluster, ClusterPool and ClusterPoolAutoscaler controllers), the mTLS REST API, credential rotation, and the profile system. See the [GitHub releases](https://github.com/meridian-io/meridian/releases) for details.

---

## [v0.1.0] — 2026-03-01

First release of Project Meridian — the open-source Trino control plane.

### MCP Server (Phase 1)

19 MCP tools covering the full Trino cluster lifecycle, catalog management, and query execution. Works against any Trino deployment on Kubernetes without the operator.

#### Cluster Management
- `list_clusters` — list all clusters with phase, health, and coordinator URL
- `get_cluster` — get full details of a specific cluster
- `create_cluster` — provision a new Trino cluster from a profile
- `delete_cluster` — tear down a cluster
- `reserve_cluster` — assign an idle cluster to a client (idempotent)
- `release_cluster` — return a cluster to the idle pool
- `scale_pool` — set desired replica count on a ClusterPool
- `list_pools` — list all ClusterPools with current state

#### Catalog Management
- `add_catalog` — push a new catalog to a running cluster, no restart required
- `remove_catalog` — remove a catalog, no restart required

#### Query & Schema
- `execute_query` — execute SQL and return results as a table
- `explain_query` — return the Trino execution plan without executing the query
- `list_catalogs` — list all catalogs registered in a cluster
- `list_schemas` — list schemas in a catalog
- `list_tables` — list tables in a catalog schema
- `get_table_schema` — get column names and data types for a table

#### Operations
- `rotate_credentials` — trigger Vault/ASM credential refresh
- `get_audit_log` — fetch cluster lifecycle and management events
- `promote_environment` — promote config from dev → staging → prod

#### Query Intelligence
- **TTL caching** — schema metadata cached for 5 minutes; `execute_query` opt-in via `ttl=N`
- **Singleflight** — duplicate parallel queries deduplicated; only one hits Trino
- **Cache invalidation** — `add_catalog` / `remove_catalog` immediately bust the catalog cache
- **Query tagging** — every query sent to Trino is tagged with `-- mcp_query_id=<uuid>` for audit trail correlation
- **CSV file export** — `output_file` parameter writes large result sets to CSV instead of returning inline

#### Transport
- `stdio` — for Claude Desktop and local MCP clients
- `SSE` — for remote and team use (Cursor, Claude API)

### Distribution
- Binaries for macOS (arm64, amd64), Linux (arm64, amd64), Windows (amd64)
- Docker image: `ghcr.io/meridian-io/meridian-mcp:v0.1.0`
