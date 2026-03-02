package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

const (
	defaultQueryTTL = 0               // execute_query: no cache by default — data changes must be visible immediately
	schemaCacheTTL  = 5 * time.Minute // list_catalogs / list_schemas / list_tables — metadata rarely changes
)

// ── execute_query ─────────────────────────────────────────────────────────────

func ExecuteQueryTool() mcp.Tool {
	return mcp.NewTool("execute_query",
		mcp.WithDescription("Execute a SQL query against a Trino cluster and return results as a table. Results are NOT cached by default — set ttl=N to cache for N seconds if the underlying data is static. Each query is tagged with a unique mcp_query_id for audit trail correlation in Trino's query history. For large result sets, set output_file to a file path to write results as CSV instead of returning them inline."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("sql", mcp.Required(), mcp.Description("SQL statement to execute.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithNumber("max_rows", mcp.Description("Maximum rows to return. Defaults to 100.")),
		mcp.WithNumber("ttl", mcp.Description("Cache TTL in seconds. Defaults to 0 (no cache). Set to a positive value to cache results for static data.")),
		mcp.WithString("output_file", mcp.Description("File path to write results as CSV (e.g. /tmp/results.csv). When set, results are written to the file and a summary is returned instead of the full table — useful for large result sets that would overflow the context window.")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(false),
	)
}

func ExecuteQueryHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		sql := mcp.ParseString(req, "sql", "")
		ns := mcp.ParseString(req, "namespace", "")
		maxRows := int(mcp.ParseFloat64(req, "max_rows", 100))
		ttlSecs := mcp.ParseFloat64(req, "ttl", 0)
		outputFile := mcp.ParseString(req, "output_file", "")

		if sql == "" {
			return nil, fmt.Errorf("sql is required")
		}

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		result, err := executeTrinoQueryCached(ctx, coordinatorURL, sql, maxRows,
			time.Duration(ttlSecs)*time.Second)
		if err != nil {
			return nil, fmt.Errorf("execute query: %w", err)
		}

		if outputFile != "" {
			msg, err := writeResultToCSV(result, outputFile)
			if err != nil {
				return nil, err
			}
			return mcp.NewToolResultText(msg), nil
		}

		return mcp.NewToolResultText(formatTrinoResult(result)), nil
	}
}

// ── explain_query ─────────────────────────────────────────────────────────────

func ExplainQueryTool() mcp.Tool {
	return mcp.NewTool("explain_query",
		mcp.WithDescription("Return the Trino query execution plan for a SQL statement without executing it. Useful for understanding how Trino will execute a query and diagnosing performance issues. Results cached for 5 minutes."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("sql", mcp.Required(), mcp.Description("SQL statement to explain.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ExplainQueryHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		sql := mcp.ParseString(req, "sql", "")
		ns := mcp.ParseString(req, "namespace", "")

		if sql == "" {
			return nil, fmt.Errorf("sql is required")
		}

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		result, err := executeTrinoQueryCached(ctx, coordinatorURL,
			fmt.Sprintf("EXPLAIN %s", sql), 0, schemaCacheTTL)
		if err != nil {
			return nil, fmt.Errorf("explain query: %w", err)
		}

		return mcp.NewToolResultText(formatTrinoResult(result)), nil
	}
}

// ── list_catalogs ─────────────────────────────────────────────────────────────

func ListCatalogsTool() mcp.Tool {
	return mcp.NewTool("list_catalogs",
		mcp.WithDescription("List all catalogs (data sources) registered in a Trino cluster. Results cached for 5 minutes."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ListCatalogsHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		ns := mcp.ParseString(req, "namespace", "")

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		result, err := executeTrinoQueryCached(ctx, coordinatorURL, "SHOW CATALOGS", 0, schemaCacheTTL)
		if err != nil {
			return nil, fmt.Errorf("list catalogs: %w", err)
		}

		return mcp.NewToolResultText(formatTrinoResult(result)), nil
	}
}

// ── list_schemas ──────────────────────────────────────────────────────────────

func ListSchemasTool() mcp.Tool {
	return mcp.NewTool("list_schemas",
		mcp.WithDescription("List all schemas in a Trino catalog. Results cached for 5 minutes."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("catalog", mcp.Required(), mcp.Description("Catalog name (e.g. mysql_testdb).")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ListSchemasHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		catalog := mcp.ParseString(req, "catalog", "")
		ns := mcp.ParseString(req, "namespace", "")

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		result, err := executeTrinoQueryCached(ctx, coordinatorURL,
			fmt.Sprintf("SHOW SCHEMAS FROM %s", catalog), 0, schemaCacheTTL)
		if err != nil {
			return nil, fmt.Errorf("list schemas: %w", err)
		}

		return mcp.NewToolResultText(formatTrinoResult(result)), nil
	}
}

// ── get_table_schema ──────────────────────────────────────────────────────────

func GetTableSchemaTool() mcp.Tool {
	return mcp.NewTool("get_table_schema",
		mcp.WithDescription("Get the schema (column names and data types) for a table in a Trino catalog. The table parameter can be fully qualified (catalog.schema.table) or bare when catalog and schema are provided separately. Results cached for 5 minutes."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("table", mcp.Required(), mcp.Description("Table name. Can be fully qualified (catalog.schema.table) or bare if catalog and schema are also provided.")),
		mcp.WithString("catalog", mcp.Description("Catalog name. Not required if table is fully qualified.")),
		mcp.WithString("schema", mcp.Description("Schema name. Not required if table is fully qualified.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func GetTableSchemaHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		table := mcp.ParseString(req, "table", "")
		catalog := mcp.ParseString(req, "catalog", "")
		schema := mcp.ParseString(req, "schema", "")
		ns := mcp.ParseString(req, "namespace", "")

		if table == "" {
			return nil, fmt.Errorf("table is required")
		}

		// Build fully qualified reference when catalog/schema are passed separately.
		ref := table
		if catalog != "" && schema != "" && !strings.Contains(table, ".") {
			ref = fmt.Sprintf("%s.%s.%s", catalog, schema, table)
		} else if schema != "" && !strings.Contains(table, ".") {
			ref = fmt.Sprintf("%s.%s", schema, table)
		}

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		result, err := executeTrinoQueryCached(ctx, coordinatorURL,
			fmt.Sprintf("DESCRIBE %s", ref), 0, schemaCacheTTL)
		if err != nil {
			return nil, fmt.Errorf("get table schema: %w", err)
		}

		return mcp.NewToolResultText(formatTrinoResult(result)), nil
	}
}

// ── list_tables ───────────────────────────────────────────────────────────────

func ListTablesTool() mcp.Tool {
	return mcp.NewTool("list_tables",
		mcp.WithDescription("List all tables in a Trino catalog schema. Results cached for 5 minutes."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("catalog", mcp.Required(), mcp.Description("Catalog name (e.g. mysql_testdb).")),
		mcp.WithString("schema", mcp.Required(), mcp.Description("Schema name (e.g. testdb).")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ListTablesHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		catalog := mcp.ParseString(req, "catalog", "")
		schema := mcp.ParseString(req, "schema", "")
		ns := mcp.ParseString(req, "namespace", "")

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		result, err := executeTrinoQueryCached(ctx, coordinatorURL,
			fmt.Sprintf("SHOW TABLES FROM %s.%s", catalog, schema), 0, schemaCacheTTL)
		if err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}

		return mcp.NewToolResultText(formatTrinoResult(result)), nil
	}
}
