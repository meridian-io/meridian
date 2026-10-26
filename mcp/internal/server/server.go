package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
	"github.com/meridian-io/meridian/mcp/internal/metrics"
	"github.com/meridian-io/meridian/mcp/internal/tools"
)

// Options configures which tools are registered and how they behave.
type Options struct {
	// ExtraTools lists held tools to register in addition to the core set.
	ExtraTools map[string]bool
	// Query controls execute_query / explain_query behavior.
	Query tools.QueryOptions
}

type handlerFunc = func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)

type toolEntry struct {
	tool    mcp.Tool
	handler handlerFunc
}

// CoreTools are registered by default. They read state, run queries, and
// reserve/release pool clusters.
var CoreTools = []string{
	"list_pools", "list_clusters", "get_cluster",
	"reserve_cluster", "release_cluster",
	"list_catalogs", "list_schemas", "list_tables", "get_table_schema",
	"explain_query", "execute_query",
	"get_audit_log",
}

// HeldTools are off unless enabled with --enable-tools. They change cluster
// or catalog configuration and are not yet hardened for default use.
var HeldTools = []string{
	"create_cluster", "delete_cluster", "scale_pool",
	"add_catalog", "remove_catalog",
	"rotate_credentials", "promote_environment",
}

func allTools(c *k8s.Client, q tools.QueryOptions) map[string]toolEntry {
	return map[string]toolEntry{
		// Core.
		"list_pools":       {tools.ListPoolsTool(), tools.ListPoolsHandler(c)},
		"list_clusters":    {tools.ListClustersTool(), tools.ListClustersHandler(c)},
		"get_cluster":      {tools.GetClusterTool(), tools.GetClusterHandler(c)},
		"reserve_cluster":  {tools.ReserveClusterTool(), tools.ReserveClusterHandler(c)},
		"release_cluster":  {tools.ReleaseClusterTool(), tools.ReleaseClusterHandler(c)},
		"list_catalogs":    {tools.ListCatalogsTool(), tools.ListCatalogsHandler(c)},
		"list_schemas":     {tools.ListSchemasTool(), tools.ListSchemasHandler(c)},
		"list_tables":      {tools.ListTablesTool(), tools.ListTablesHandler(c)},
		"get_table_schema": {tools.GetTableSchemaTool(), tools.GetTableSchemaHandler(c)},
		"explain_query":    {tools.ExplainQueryTool(), tools.ExplainQueryHandler(c, q)},
		"execute_query":    {tools.ExecuteQueryTool(q), tools.ExecuteQueryHandler(c, q)},
		"get_audit_log":    {tools.GetAuditLogTool(), tools.GetAuditLogHandler(c)},

		// Held.
		"create_cluster":      {tools.CreateClusterTool(), tools.CreateClusterHandler(c)},
		"delete_cluster":      {tools.DeleteClusterTool(), tools.DeleteClusterHandler(c)},
		"scale_pool":          {tools.ScalePoolTool(), tools.ScalePoolHandler(c)},
		"add_catalog":         {tools.AddCatalogTool(), tools.AddCatalogHandler(c)},
		"remove_catalog":      {tools.RemoveCatalogTool(), tools.RemoveCatalogHandler(c)},
		"rotate_credentials":  {tools.RotateCredentialsTool(), tools.RotateCredentialsHandler(c)},
		"promote_environment": {tools.PromoteEnvironmentTool(), tools.PromoteEnvironmentHandler(c)},
	}
}

// ParseEnableTools turns the --enable-tools value into a set of held tool
// names. "all" enables every held tool. Unknown names are an error so typos
// don't silently leave a tool off.
func ParseEnableTools(spec string) (map[string]bool, error) {
	out := map[string]bool{}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return out, nil
	}
	held := map[string]bool{}
	for _, n := range HeldTools {
		held[n] = true
	}
	core := map[string]bool{}
	for _, n := range CoreTools {
		core[n] = true
	}

	for _, name := range strings.Split(spec, ",") {
		name = strings.TrimSpace(name)
		switch {
		case name == "":
			continue
		case name == "all":
			for n := range held {
				out[n] = true
			}
		case held[name]:
			out[name] = true
		case core[name]:
			// Already on; accept it so configs can list every tool they use.
		default:
			valid := append([]string(nil), HeldTools...)
			sort.Strings(valid)
			return nil, fmt.Errorf("unknown tool %q in --enable-tools (valid: all, %s)", name, strings.Join(valid, ", "))
		}
	}
	return out, nil
}

// New builds the MCP server with the core tools plus any enabled held tools.
// It returns the server and the names of the registered tools.
func New(c *k8s.Client, version string, opts Options) (*server.MCPServer, []string) {
	s := server.NewMCPServer(
		"meridian",
		version,
		// The tool set is fixed for the life of the process.
		server.WithToolCapabilities(false),
		server.WithRecovery(),
	)

	entries := allTools(c, opts.Query)
	var names []string
	register := func(name string) {
		e := entries[name]
		if e.tool.Title == "" {
			e.tool.Title = toolTitles[name]
		}
		s.AddTool(e.tool, instrument(name, toolErrorsAsResults(e.handler)))
		// Create the series at zero so rates and alerts work before the first call.
		metrics.ToolCalls.WithLabelValues(name, metrics.ResultOK)
		metrics.ToolCalls.WithLabelValues(name, metrics.ResultError)
		names = append(names, name)
	}

	for _, name := range CoreTools {
		register(name)
	}
	for _, name := range HeldTools {
		if opts.ExtraTools[name] {
			register(name)
		}
	}
	return s, names
}

// toolTitles are the display names clients show for each tool.
var toolTitles = map[string]string{
	"list_pools":          "List pools",
	"list_clusters":       "List clusters",
	"get_cluster":         "Get cluster",
	"reserve_cluster":     "Reserve cluster",
	"release_cluster":     "Release cluster",
	"list_catalogs":       "List catalogs",
	"list_schemas":        "List schemas",
	"list_tables":         "List tables",
	"get_table_schema":    "Get table schema",
	"explain_query":       "Explain query",
	"execute_query":       "Run query",
	"get_audit_log":       "Get audit log",
	"create_cluster":      "Create cluster",
	"delete_cluster":      "Delete cluster",
	"scale_pool":          "Scale pool",
	"add_catalog":         "Add catalog",
	"remove_catalog":      "Remove catalog",
	"rotate_credentials":  "Rotate credentials",
	"promote_environment": "Promote environment",
}

// instrument counts each call by result and records its duration. A panic is
// counted as an error and then re-raised for the server's recovery handler.
func instrument(name string, h handlerFunc) handlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (res *mcp.CallToolResult, err error) {
		start := time.Now()
		result := metrics.ResultError
		defer func() {
			metrics.ToolCalls.WithLabelValues(name, result).Inc()
			metrics.ToolDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
		}()
		res, err = h(ctx, req)
		if err == nil && res != nil && !res.IsError {
			result = metrics.ResultOK
		}
		return res, err
	}
}

// toolErrorsAsResults turns a handler error into a tool result with
// isError set. The MCP spec asks for tool failures (bad arguments, API
// errors) to come back this way so the model can see them and correct
// itself; protocol errors are for unknown tools and malformed requests.
func toolErrorsAsResults(h handlerFunc) handlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		res, err := h(ctx, req)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return res, nil
	}
}
