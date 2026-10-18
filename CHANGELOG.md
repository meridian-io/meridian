# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased] — v1.3

### Upgrade notes (breaking defaults)
- **SSE needs a token.** Set `MERIDIAN_MCP_TOKEN` or `--auth-token-file`; clients send `Authorization: Bearer <token>`. `--insecure-no-auth` restores the old behavior for local testing.
- **`execute_query` is read-only by default.** Start with `--allow-writes` to run INSERT/DROP/etc.
- **7 tools are off by default:** `create_cluster`, `delete_cluster`, `scale_pool`, `add_catalog`, `remove_catalog`, `rotate_credentials`, `promote_environment`. Enable with `--enable-tools=<names>` or `--enable-tools=all`.
- **`output_file` takes a file name**, written to `--output-dir` (default: a per-user cache dir, e.g. `~/.cache/meridian/exports`). Absolute paths outside that directory (like `/tmp/results.csv`) are rejected. It is disabled in SSE mode.
- **`execute_query` returns at most 10,000 rows**, including CSV exports. `max_rows=0` used to mean "no limit" and now means 10,000. Results over the limit are marked truncated, and the query is cancelled in Trino.
- **`reserve_cluster` requires non-empty `client_id` and `reservation_id`.**

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

### Added
- `/healthz` and `/readyz` endpoints, graceful shutdown on SIGTERM, keep-alive on SSE streams.
- Structured logging (`--log-format json|text`), bounded query cache (`--cache-max-entries`), retries when Trino returns 502/503/504, shared HTTP connection pool, 30s Kubernetes API timeout.
- `release_cluster` takes an optional `client_id` ownership check.
- Unit tests for the MCP server (previously none).

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

---

## [Unreleased]

### Phase 2 — Kubernetes Operator
- ClusterController — cluster lifecycle: Empty → Pending → Idle → Reserved → Failed
- ClusterPoolController — hot standby pool reconciliation (scale up/down, gradual deletion)
- ClusterPoolAutoscalerController — utilization-based autoscaling with hysteresis
