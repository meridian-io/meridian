# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
