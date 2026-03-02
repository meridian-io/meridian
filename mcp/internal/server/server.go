package server

import (
	"github.com/mark3labs/mcp-go/server"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
	"github.com/meridian-io/meridian/mcp/internal/tools"
)

// New builds and returns the configured MCP server with all Meridian tools registered.
func New(c *k8s.Client, version string) *server.MCPServer {
	s := server.NewMCPServer(
		"meridian",
		version,
		server.WithToolCapabilities(true),
	)

	// Cluster lifecycle.
	s.AddTool(tools.ListClustersTool(), tools.ListClustersHandler(c))
	s.AddTool(tools.GetClusterTool(), tools.GetClusterHandler(c))
	s.AddTool(tools.CreateClusterTool(), tools.CreateClusterHandler(c))
	s.AddTool(tools.DeleteClusterTool(), tools.DeleteClusterHandler(c))

	// Reservation.
	s.AddTool(tools.ReserveClusterTool(), tools.ReserveClusterHandler(c))
	s.AddTool(tools.ReleaseClusterTool(), tools.ReleaseClusterHandler(c))

	// Pool management.
	s.AddTool(tools.ScalePoolTool(), tools.ScalePoolHandler(c))
	s.AddTool(tools.ListPoolsTool(), tools.ListPoolsHandler(c))

	// Catalog management.
	s.AddTool(tools.AddCatalogTool(), tools.AddCatalogHandler(c))
	s.AddTool(tools.RemoveCatalogTool(), tools.RemoveCatalogHandler(c))

	// Query + introspection.
	s.AddTool(tools.ExecuteQueryTool(), tools.ExecuteQueryHandler(c))
	s.AddTool(tools.ExplainQueryTool(), tools.ExplainQueryHandler(c))
	s.AddTool(tools.ListCatalogsTool(), tools.ListCatalogsHandler(c))
	s.AddTool(tools.ListSchemasTool(), tools.ListSchemasHandler(c))
	s.AddTool(tools.ListTablesTool(), tools.ListTablesHandler(c))
	s.AddTool(tools.GetTableSchemaTool(), tools.GetTableSchemaHandler(c))

	// Credentials.
	s.AddTool(tools.RotateCredentialsTool(), tools.RotateCredentialsHandler(c))

	// Audit + promotion.
	s.AddTool(tools.GetAuditLogTool(), tools.GetAuditLogHandler(c))
	s.AddTool(tools.PromoteEnvironmentTool(), tools.PromoteEnvironmentHandler(c))

	return s
}
