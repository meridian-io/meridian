package tools

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

// ── add_catalog ───────────────────────────────────────────────────────────────

func AddCatalogTool() mcp.Tool {
	return mcp.NewTool("add_catalog",
		mcp.WithDescription("Add a new catalog to a running Trino cluster without restart. Properties are in Java properties format (key=value, one per line). Requires catalog.management=dynamic in Trino config."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("catalog_name", mcp.Required(), mcp.Description("Name for the new catalog (e.g. my_mysql).")),
		mcp.WithString("properties", mcp.Required(), mcp.Description("Catalog properties in Java properties format. Must include connector.name. Example:\nconnector.name=mysql\nconnection-url=jdbc:mysql://host:3306\nconnection-user=root\nconnection-password=pass")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(false),
	)
}

func AddCatalogHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		catalogName := mcp.ParseString(req, "catalog_name", "")
		properties := mcp.ParseString(req, "properties", "")
		ns := mcp.ParseString(req, "namespace", "")

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		props := parseProperties(properties)
		connectorName, ok := props["connector.name"]
		if !ok {
			return nil, fmt.Errorf("properties must include connector.name")
		}
		delete(props, "connector.name")

		sql, err := buildCreateCatalogSQL(catalogName, connectorName, props)
		if err != nil {
			return nil, err
		}
		if err := executeTrinoSQL(ctx, coordinatorURL, sql); err != nil {
			return nil, fmt.Errorf("add catalog %q: %w", catalogName, err)
		}

		// Bust SHOW CATALOGS cache for this coordinator.
		globalCache.invalidatePrefix(coordinatorPrefix(coordinatorURL))

		return mcp.NewToolResultText(fmt.Sprintf("Catalog %q added to cluster %q.", catalogName, clusterName)), nil
	}
}

// ── remove_catalog ────────────────────────────────────────────────────────────

func RemoveCatalogTool() mcp.Tool {
	return mcp.NewTool("remove_catalog",
		mcp.WithDescription("Remove a catalog from a running Trino cluster without restart. Requires catalog.management=dynamic in Trino config."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("catalog_name", mcp.Required(), mcp.Description("Name of the catalog to remove.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func RemoveCatalogHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		catalogName := mcp.ParseString(req, "catalog_name", "")
		ns := mcp.ParseString(req, "namespace", "")

		coordinatorURL, err := getCoordinatorURL(ctx, c, clusterName, ns)
		if err != nil {
			return nil, err
		}

		if !simpleIdentRE.MatchString(catalogName) {
			return nil, fmt.Errorf("invalid catalog name %q: use letters, digits, and underscores", catalogName)
		}
		sql := fmt.Sprintf("DROP CATALOG %s", catalogName)
		if err := executeTrinoSQL(ctx, coordinatorURL, sql); err != nil {
			return nil, fmt.Errorf("remove catalog %q: %w", catalogName, err)
		}

		// Bust SHOW CATALOGS cache for this coordinator.
		globalCache.invalidatePrefix(coordinatorPrefix(coordinatorURL))

		return mcp.NewToolResultText(fmt.Sprintf("Catalog %q removed from cluster %q.", catalogName, clusterName)), nil
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func getCoordinatorURL(ctx context.Context, c *k8s.Client, clusterName, ns string) (string, error) {
	cluster, err := c.GetCluster(ctx, clusterName, ns)
	if err != nil {
		return "", fmt.Errorf("get cluster %q: %w", clusterName, err)
	}
	url := strFromObj(cluster.Object, "status", "coordinatorURL")
	if url == "" {
		return "", fmt.Errorf("cluster %q has no coordinatorURL — is it Ready?", clusterName)
	}
	return url, nil
}

// parseProperties parses Java properties format (key=value, one per line).
func parseProperties(raw string) map[string]string {
	props := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.IndexByte(line, '=')
		if idx < 0 {
			continue
		}
		props[strings.TrimSpace(line[:idx])] = strings.TrimSpace(line[idx+1:])
	}
	return props
}

var (
	// simpleIdentRE matches names that are safe to use unquoted in SQL.
	simpleIdentRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// propertyKeyRE matches Trino catalog property keys (e.g. connection-url).
	propertyKeyRE = regexp.MustCompile(`^[A-Za-z0-9_.\-]+$`)
)

// buildCreateCatalogSQL builds a CREATE CATALOG SQL statement.
// Trino 392+ supports: CREATE CATALOG name USING connector WITH ("key"='value', ...)
//
// Rules:
//   - catalog name and connector name are unquoted identifiers (quoting embeds the
//     quote characters into the stored name, making the catalog unreachable), so
//     they are validated against simpleIdentRE instead
//   - property keys are double-quoted because they contain hyphens (e.g. connection-url)
//     and validated against propertyKeyRE
//   - property values are single-quoted strings with quotes escaped
func buildCreateCatalogSQL(catalogName, connectorName string, props map[string]string) (string, error) {
	if !simpleIdentRE.MatchString(catalogName) {
		return "", fmt.Errorf("invalid catalog name %q: use letters, digits, and underscores", catalogName)
	}
	if !simpleIdentRE.MatchString(connectorName) {
		return "", fmt.Errorf("invalid connector.name %q", connectorName)
	}
	if len(props) == 0 {
		return fmt.Sprintf("CREATE CATALOG %s USING %s", catalogName, connectorName), nil
	}
	parts := make([]string, 0, len(props))
	for k, v := range props {
		if !propertyKeyRE.MatchString(k) {
			return "", fmt.Errorf("invalid property key %q", k)
		}
		parts = append(parts, fmt.Sprintf(`"%s"='%s'`, k, strings.ReplaceAll(v, "'", "''")))
	}
	return fmt.Sprintf("CREATE CATALOG %s USING %s WITH (%s)",
		catalogName, connectorName, strings.Join(parts, ", ")), nil
}
