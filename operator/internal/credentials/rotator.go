package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Rotator executes an in-place catalog credential rotation on a live Trino cluster.
//
// Rotation is performed using Trino's dynamic catalog API (requires
// catalog.management=dynamic in Trino config):
//  1. DROP CATALOG IF EXISTS <name>
//  2. CREATE CATALOG <name> USING <connector> WITH (<properties>)
//
// No cluster restart is needed. In-flight queries on other catalogs are unaffected.
// Rotator is stateless and safe for concurrent use.
type Rotator struct {
	httpClient *http.Client
}

// NewRotator creates a Rotator with a 30-second HTTP timeout.
func NewRotator() *Rotator {
	return &Rotator{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Rotate drops and recreates a catalog on the live Trino cluster with credentials
// from secret. coordinatorURL is taken from cluster.Status.CoordinatorURL.
//
// Returns nil on success. The caller is responsible for clearing the rotation
// annotation and updating status.LastRotatedAt only after a nil return.
func (r *Rotator) Rotate(ctx context.Context, coordinatorURL, catalogName string, secret *Secret) error {
	if coordinatorURL == "" {
		return fmt.Errorf("cluster has no coordinatorURL — is it Ready?")
	}

	// Extract connector.name — required for CREATE CATALOG ... USING <connector>.
	props := make(map[string]string, len(secret.Properties))
	for k, v := range secret.Properties {
		props[k] = v
	}
	connectorName, ok := props["connector.name"]
	if !ok {
		return fmt.Errorf("secret is missing required property connector.name")
	}
	delete(props, "connector.name")

	base := strings.TrimRight(coordinatorURL, "/")

	// Step 1: drop the existing catalog (idempotent — IF EXISTS handles missing catalog).
	dropSQL := fmt.Sprintf("DROP CATALOG IF EXISTS %s", catalogName)
	if err := r.execSQL(ctx, base, dropSQL); err != nil {
		return fmt.Errorf("drop catalog %q: %w", catalogName, err)
	}

	// Step 2: recreate the catalog with fresh credentials.
	createSQL := buildCatalogSQL(catalogName, connectorName, props)
	if err := r.execSQL(ctx, base, createSQL); err != nil {
		return fmt.Errorf("create catalog %q: %w", catalogName, err)
	}

	return nil
}

// IsCatalogNotFound returns true when the Trino error message indicates that
// a catalog does not exist. Used by the ClusterController to distinguish
// a missing catalog (reactive path — create fresh) from other errors (retry).
func IsCatalogNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "catalog not found") ||
		strings.Contains(msg, "no such catalog") ||
		strings.Contains(msg, "catalog does not exist")
}

// ── Trino statement API ───────────────────────────────────────────────────────

// trinoResponse mirrors the Trino /v1/statement response shape.
type trinoResponse struct {
	NextURI string `json:"nextUri"`
	Stats   struct {
		State string `json:"state"`
	} `json:"stats"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// execSQL submits sql to the Trino coordinator at base and polls until finished.
func (r *Rotator) execSQL(ctx context.Context, base, sql string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/statement", strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Trino-User", "meridian")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("submit statement: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("trino returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var tr trinoResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return fmt.Errorf("parse statement response: %w", err)
	}
	if tr.Error != nil {
		return fmt.Errorf("trino error: %s", tr.Error.Message)
	}

	return r.poll(ctx, &tr)
}

// poll follows nextUri until the query reaches a terminal state.
func (r *Rotator) poll(ctx context.Context, tr *trinoResponse) error {
	for tr.NextURI != "" {
		switch tr.Stats.State {
		case "FINISHED":
			return nil
		case "FAILED":
			if tr.Error != nil {
				return fmt.Errorf("trino error: %s", tr.Error.Message)
			}
			return fmt.Errorf("query failed")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, tr.NextURI, nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Trino-User", "meridian")

		resp, err := r.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("poll statement: %w", err)
		}
		pollBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		*tr = trinoResponse{}
		if err := json.Unmarshal(pollBody, tr); err != nil {
			return fmt.Errorf("parse poll response: %w", err)
		}
		if tr.Error != nil {
			return fmt.Errorf("trino error: %s", tr.Error.Message)
		}
	}

	// No nextUri and no terminal state — query finished inline (common for DDL).
	if tr.Stats.State == "FAILED" {
		if tr.Error != nil {
			return fmt.Errorf("trino error: %s", tr.Error.Message)
		}
		return fmt.Errorf("query failed")
	}
	return nil
}

// ── SQL helpers ───────────────────────────────────────────────────────────────

// buildCatalogSQL builds a CREATE CATALOG SQL statement.
// Trino 392+ syntax: CREATE CATALOG name USING connector WITH ("key"='value', ...)
//
// Rules:
//   - catalog name and connector name are unquoted identifiers
//   - property keys are double-quoted (they contain hyphens, e.g. connection-url)
//   - property values are single-quoted strings; single quotes inside values are escaped
func buildCatalogSQL(catalogName, connectorName string, props map[string]string) string {
	if len(props) == 0 {
		return fmt.Sprintf("CREATE CATALOG %s USING %s", catalogName, connectorName)
	}
	parts := make([]string, 0, len(props))
	for k, v := range props {
		escaped := strings.ReplaceAll(v, "'", "''")
		parts = append(parts, fmt.Sprintf(`"%s"='%s'`, k, escaped))
	}
	return fmt.Sprintf("CREATE CATALOG %s USING %s WITH (%s)",
		catalogName, connectorName, strings.Join(parts, ", "))
}
