package tools

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	trinoUser         = "meridian"
	trinoMaxRetries   = 5
	trinoEmptyPollGap = 100 * time.Millisecond
	maxResponseBytes  = 64 << 20 // one Trino page; guards against a runaway response
)

// trinoDialTimeout bounds opening a connection to a coordinator. Inside a
// cluster that takes milliseconds; a coordinator pod that was just killed can
// leave its address silent, and without a bound each request would wait for
// the default 30s before failing.
var trinoDialTimeout = 5 * time.Second

// trinoDial opens connections; tests replace it.
var trinoDial = (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext

// trinoHTTP is shared by every Trino call so connections are reused.
var trinoHTTP = &http.Client{
	Timeout: 30 * time.Second,
	Transport: func() http.RoundTripper {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConnsPerHost = 16
		t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			ctx, cancel := context.WithTimeout(ctx, trinoDialTimeout)
			defer cancel()
			return trinoDial(ctx, network, addr)
		}
		return t
	}(),
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

// trinoCall sends one request to the statement API and decodes the response.
// Trino answers 503 when it is busy and hasn't accepted the request, so 503 is
// always retried. 502/504 come from a proxy and the query may already have
// been submitted, so they are retried only for GET (polling), never for POST.
func trinoCall(ctx context.Context, method, url, body string) (*trinoStatementResponse, error) {
	var lastErr error
	for attempt := 0; attempt < trinoMaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			}
		}

		var reader io.Reader
		if body != "" {
			reader = strings.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, reader)
		if err != nil {
			return nil, err
		}
		if body != "" {
			req.Header.Set("Content-Type", "text/plain")
		}
		req.Header.Set("X-Trino-User", trinoUser)

		resp, err := trinoHTTP.Do(req)
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read response: %w", err)
		}

		switch resp.StatusCode {
		case http.StatusOK:
			var sr trinoStatementResponse
			if err := json.Unmarshal(data, &sr); err != nil {
				return nil, fmt.Errorf("parse Trino response: %w", err)
			}
			return &sr, nil
		case http.StatusServiceUnavailable:
			lastErr = fmt.Errorf("Trino returned HTTP %d", resp.StatusCode)
			continue
		case http.StatusBadGateway, http.StatusGatewayTimeout:
			lastErr = fmt.Errorf("Trino returned HTTP %d", resp.StatusCode)
			if method == http.MethodGet {
				continue
			}
			return nil, fmt.Errorf("%w (not retried: the statement may already have been submitted)", lastErr)
		default:
			return nil, fmt.Errorf("Trino returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		}
	}
	return nil, fmt.Errorf("Trino unavailable after %d attempts: %w", trinoMaxRetries, lastErr)
}

// cancelTrinoQuery asks Trino to stop a query we no longer need. Best effort.
func cancelTrinoQuery(nextURI string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, nextURI, nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Trino-User", trinoUser)
	if resp, err := trinoHTTP.Do(req); err == nil {
		resp.Body.Close()
	}
}

// runTrino submits sql and follows nextUri until the query finishes.
// maxRows=0 means no limit. When the limit is hit, the query is cancelled in
// Trino so it doesn't keep running in the background.
func runTrino(ctx context.Context, coordinatorURL, sql string, maxRows int) (result *trinoQueryResult, err error) {
	base := strings.TrimRight(coordinatorURL, "/")
	result = &trinoQueryResult{}

	sr, err := trinoCall(ctx, http.MethodPost, base+"/v1/statement", sql)
	if err != nil {
		return nil, fmt.Errorf("submit statement: %w", err)
	}

	// Cancel the query in Trino if we stop early for any reason.
	defer func() {
		if sr != nil && sr.NextURI != "" && (err != nil || result.Truncated) {
			cancelTrinoQuery(sr.NextURI)
		}
	}()

	for {
		if sr.Error != nil {
			return nil, fmt.Errorf("Trino error: %s", sr.Error.Message)
		}
		appendTrinoRows(result, sr, maxRows)
		if result.Truncated || sr.NextURI == "" {
			return result, nil
		}
		if len(sr.Data) == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(trinoEmptyPollGap):
			}
		}
		next, err := trinoCall(ctx, http.MethodGet, sr.NextURI, "")
		if err != nil {
			return nil, fmt.Errorf("poll statement: %w", err)
		}
		sr = next
	}
}

// executeTrinoQuery runs a query and returns its rows. Each call is tagged with a
// unique mcp_query_id comment so it can be found in Trino's query history.
func executeTrinoQuery(ctx context.Context, coordinatorURL, sql string, maxRows int) (*trinoQueryResult, error) {
	queryID := uuid.New().String()
	result, err := runTrino(ctx, coordinatorURL, sql+"\n-- mcp_query_id="+queryID, maxRows)
	if err != nil {
		return nil, err
	}
	result.QueryID = queryID
	return result, nil
}

// executeTrinoSQL runs a statement whose rows we don't need (DDL).
func executeTrinoSQL(ctx context.Context, coordinatorURL, sql string) error {
	_, err := runTrino(ctx, coordinatorURL, sql, 0)
	return err
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

// writeResultToCSV writes query results to path (already validated by
// resolveOutputPath) and returns a summary message. It writes a new temp file
// and renames it into place, so an existing symlink or hard link at path is
// replaced rather than followed.
func writeResultToCSV(r *trinoQueryResult, path string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".meridian-*.csv.tmp")
	if err != nil {
		return "", fmt.Errorf("create output file: %w", err)
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(tmp)
		}
	}()

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
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close CSV: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("move CSV into place: %w", err)
	}
	ok = true

	msg := fmt.Sprintf("Results written to %s (%d rows, %d columns)", path, len(r.Rows), len(r.Columns))
	if r.QueryID != "" {
		msg += fmt.Sprintf("\nquery_id: %s", r.QueryID)
	}
	if r.Truncated {
		msg += "\n(results truncated — increase max_rows to fetch more)"
	}
	return msg, nil
}
