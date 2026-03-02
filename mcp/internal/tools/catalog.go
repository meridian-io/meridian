package tools

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
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

		sql := buildCreateCatalogSQL(catalogName, connectorName, props)
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

// buildCreateCatalogSQL builds a CREATE CATALOG SQL statement.
// Trino 392+ supports: CREATE CATALOG name USING connector WITH ("key"='value', ...)
//
// Rules:
//   - catalog name and connector name are unquoted identifiers (quoting embeds the
//     quote characters into the stored name, making the catalog unreachable)
//   - property keys are double-quoted because they contain hyphens (e.g. connection-url)
//   - property values are single-quoted strings
func buildCreateCatalogSQL(catalogName, connectorName string, props map[string]string) string {
	if len(props) == 0 {
		return fmt.Sprintf("CREATE CATALOG %s USING %s", catalogName, connectorName)
	}
	parts := make([]string, 0, len(props))
	for k, v := range props {
		parts = append(parts, fmt.Sprintf(`"%s"='%s'`, k, strings.ReplaceAll(v, "'", "''")))
	}
	return fmt.Sprintf("CREATE CATALOG %s USING %s WITH (%s)",
		catalogName, connectorName, strings.Join(parts, ", "))
}

// trinoStatementResponse mirrors the Trino statement API shape we care about.
type trinoStatementResponse struct {
	ID      string `json:"id"`
	NextURI string `json:"nextUri"`
	Columns []struct {
		Name string `json:"name"`
	} `json:"columns"`
	Data  [][]interface{} `json:"data"`
	Stats struct {
		State string `json:"state"`
	} `json:"stats"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// trinoQueryResult holds the accumulated rows from a SELECT-style query.
type trinoQueryResult struct {
	Columns   []string
	Rows      [][]string
	Truncated bool   // true if max_rows limit was hit
	FromCache bool   // true if result was served from cache
	QueryID   string // mcp_query_id tag appended to the SQL sent to Trino
}

func appendTrinoRows(result *trinoQueryResult, sr *trinoStatementResponse, maxRows int) {
	if len(result.Columns) == 0 {
		for _, col := range sr.Columns {
			result.Columns = append(result.Columns, col.Name)
		}
	}
	for _, row := range sr.Data {
		if maxRows > 0 && len(result.Rows) >= maxRows {
			result.Truncated = true
			break
		}
		strRow := make([]string, len(row))
		for i, v := range row {
			if v == nil {
				strRow[i] = "NULL"
			} else {
				strRow[i] = fmt.Sprintf("%v", v)
			}
		}
		result.Rows = append(result.Rows, strRow)
	}
}

// executeTrinoQuery submits SQL and returns structured results (columns + rows).
// maxRows=0 means no limit. Each call appends a unique mcp_query_id tag for
// audit trail correlation in Trino's query history.
func executeTrinoQuery(ctx context.Context, coordinatorURL, sql string, maxRows int) (*trinoQueryResult, error) {
	queryID := uuid.New().String()
	tagged := sql + "\n-- mcp_query_id=" + queryID

	client := &http.Client{Timeout: 30 * time.Second}
	base := strings.TrimRight(coordinatorURL, "/")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/statement", strings.NewReader(tagged))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Trino-User", "meridian")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("submit statement: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Trino returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var sr trinoStatementResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return nil, fmt.Errorf("parse statement response: %w", err)
	}

	result := &trinoQueryResult{QueryID: queryID}
	appendTrinoRows(result, &sr, maxRows)

	for sr.NextURI != "" {
		switch sr.Stats.State {
		case "FINISHED":
			return result, nil
		case "FAILED":
			if sr.Error != nil {
				return nil, fmt.Errorf("Trino error: %s", sr.Error.Message)
			}
			return nil, fmt.Errorf("query failed with no error detail")
		}
		if maxRows > 0 && len(result.Rows) >= maxRows {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}

		pollReq, err := http.NewRequestWithContext(ctx, http.MethodGet, sr.NextURI, nil)
		if err != nil {
			return nil, err
		}
		pollReq.Header.Set("X-Trino-User", "meridian")

		pollResp, err := client.Do(pollReq)
		if err != nil {
			return nil, fmt.Errorf("poll statement: %w", err)
		}
		pollBody, _ := io.ReadAll(pollResp.Body)
		pollResp.Body.Close()

		sr = trinoStatementResponse{}
		if err := json.Unmarshal(pollBody, &sr); err != nil {
			return nil, fmt.Errorf("parse poll response: %w", err)
		}
		appendTrinoRows(result, &sr, maxRows)
	}

	if sr.Error != nil {
		return nil, fmt.Errorf("Trino error: %s", sr.Error.Message)
	}
	return result, nil
}

// formatTrinoResult renders query results as a markdown table.
func formatTrinoResult(r *trinoQueryResult) string {
	if len(r.Columns) == 0 && len(r.Rows) == 0 {
		return "(no results)"
	}

	widths := make([]int, len(r.Columns))
	for i, col := range r.Columns {
		widths[i] = len(col)
	}
	for _, row := range r.Rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	var sb strings.Builder
	if r.FromCache {
		sb.WriteString("[cache hit]\n")
	}
	if r.QueryID != "" {
		sb.WriteString(fmt.Sprintf("query_id: %s\n", r.QueryID))
	}
	sb.WriteString("|")
	for i, col := range r.Columns {
		sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], col))
	}
	sb.WriteString("\n|")
	for _, w := range widths {
		sb.WriteString(strings.Repeat("-", w+2) + "|")
	}
	sb.WriteString("\n")
	for _, row := range r.Rows {
		sb.WriteString("|")
		for i := range r.Columns {
			cell := ""
			if i < len(row) {
				cell = row[i]
			}
			sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], cell))
		}
		sb.WriteString("\n")
	}
	suffix := fmt.Sprintf("\n(%d rows)", len(r.Rows))
	if r.Truncated {
		suffix += " — results truncated, use max_rows to fetch more"
	}
	sb.WriteString(suffix)
	return sb.String()
}

// writeResultToCSV writes query results as a CSV file to path and returns a
// summary message. Use this instead of formatTrinoResult when the result set
// is large and would overflow the LLM context window.
func writeResultToCSV(r *trinoQueryResult, path string) (string, error) {
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if err := w.Write(r.Columns); err != nil {
		return "", fmt.Errorf("write CSV header: %w", err)
	}
	for _, row := range r.Rows {
		if err := w.Write(row); err != nil {
			return "", fmt.Errorf("write CSV row: %w", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return "", fmt.Errorf("flush CSV: %w", err)
	}

	msg := fmt.Sprintf("Results written to %s (%d rows, %d columns)", path, len(r.Rows), len(r.Columns))
	if r.QueryID != "" {
		msg += fmt.Sprintf("\nquery_id: %s", r.QueryID)
	}
	if r.Truncated {
		msg += "\n(results truncated — increase max_rows to fetch more)"
	}
	return msg, nil
}

// executeTrinoSQL submits a SQL statement to the Trino coordinator and polls until
// it finishes. Returns an error if the query fails.
func executeTrinoSQL(ctx context.Context, coordinatorURL, sql string) error {
	client := &http.Client{Timeout: 30 * time.Second}
	base := strings.TrimRight(coordinatorURL, "/")

	// Submit the statement.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/statement",
		strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Trino-User", "meridian")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("submit statement: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Trino returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var sr trinoStatementResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return fmt.Errorf("parse statement response: %w", err)
	}

	// Poll nextUri until finished.
	for sr.NextURI != "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}

		pollReq, err := http.NewRequestWithContext(ctx, http.MethodGet, sr.NextURI, nil)
		if err != nil {
			return err
		}
		pollReq.Header.Set("X-Trino-User", "meridian")

		pollResp, err := client.Do(pollReq)
		if err != nil {
			return fmt.Errorf("poll statement: %w", err)
		}
		pollBody, _ := io.ReadAll(pollResp.Body)
		pollResp.Body.Close()

		sr = trinoStatementResponse{}
		if err := json.Unmarshal(pollBody, &sr); err != nil {
			return fmt.Errorf("parse poll response: %w", err)
		}

		switch sr.Stats.State {
		case "FINISHED":
			return nil
		case "FAILED":
			if sr.Error != nil {
				return fmt.Errorf("Trino error: %s", sr.Error.Message)
			}
			return fmt.Errorf("query failed with no error detail")
		}
	}

	return nil
}

// trinoRequest is kept for potential future use (health checks, etc.)
func trinoRequest(ctx context.Context, method, url, body string) (*http.Response, error) {
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "text/plain")
	}
	req.Header.Set("X-Trino-User", "meridian")

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}
