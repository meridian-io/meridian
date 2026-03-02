package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ── reserve_cluster ───────────────────────────────────────────────────────────

func ReserveClusterTool() mcp.Tool {
	return mcp.NewTool("reserve_cluster",
		mcp.WithDescription("Assign an idle Trino cluster to a client. Specify cluster_name for direct reservation or workload for automatic pool routing. Idempotent — the same (clientId, reservationId) pair always returns the same cluster."),
		mcp.WithString("cluster_name", mcp.Description("Name of a specific Idle cluster to reserve. Use this for direct targeting; use workload for pool-based routing.")),
		mcp.WithString("workload", mcp.Description("Workload type to route to (e.g. \"analytics\", \"etl\"). Picks the oldest idle cluster labeled with this workload. Use instead of cluster_name for automatic pool routing.")),
		mcp.WithString("client_id", mcp.Required(), mcp.Description("Identifier for the client or workload claiming this cluster.")),
		mcp.WithString("reservation_id", mcp.Required(), mcp.Description("Unique idempotency key for this reservation. Re-using the same (clientId, reservationId) returns the same cluster.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ReserveClusterHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !c.HasMeridianOperator() {
			return nil, fmt.Errorf("Meridian operator required for cluster reservation")
		}

		clusterName := mcp.ParseString(req, "cluster_name", "")
		workload := mcp.ParseString(req, "workload", "")
		clientID := mcp.ParseString(req, "client_id", "")
		reservationID := mcp.ParseString(req, "reservation_id", "")
		ns := mcp.ParseString(req, "namespace", "")

		if clusterName == "" && workload == "" {
			return nil, fmt.Errorf("either cluster_name or workload is required")
		}

		if clusterName != "" {
			return reserveByName(ctx, c, clusterName, clientID, reservationID, ns)
		}
		return reserveByWorkload(ctx, c, workload, clientID, reservationID, ns)
	}
}

// reserveByName is the original direct-reservation path.
func reserveByName(ctx context.Context, c *k8s.Client, clusterName, clientID, reservationID, ns string) (*mcp.CallToolResult, error) {
	cluster, err := c.GetCluster(ctx, clusterName, ns)
	if err != nil {
		return nil, fmt.Errorf("get cluster %q: %w", clusterName, err)
	}

	status, _ := cluster.Object["status"].(map[string]interface{})
	phase, _ := status["phase"].(string)
	if phase != "Idle" {
		return nil, fmt.Errorf("cluster %q is %q — only Idle clusters can be reserved", clusterName, phase)
	}

	patch := fmt.Sprintf(`{"spec":{"clientId":%q,"reservationId":%q}}`, clientID, reservationID)
	updated, err := c.PatchCluster(ctx, clusterName, ns, []byte(patch))
	if err != nil {
		return nil, fmt.Errorf("patch cluster: %w", err)
	}

	return reservationResult(clusterName, clientID, reservationID, strFromObj(updated.Object, "status", "coordinatorURL"))
}

// reserveByWorkload selects the oldest idle cluster labeled with the given workload.
func reserveByWorkload(ctx context.Context, c *k8s.Client, workload, clientID, reservationID, ns string) (*mcp.CallToolResult, error) {
	labelSelector := fmt.Sprintf("meridian.io/workload=%s", workload)
	clusters, err := c.ListClustersWithSelector(ctx, ns, labelSelector)
	if err != nil {
		return nil, fmt.Errorf("list clusters for workload %q: %w", workload, err)
	}

	best := pickOldestIdle(clusters)
	if best == nil {
		return nil, fmt.Errorf("no idle clusters available for workload %q", workload)
	}

	clusterName := best.GetName()
	patch := fmt.Sprintf(`{"spec":{"clientId":%q,"reservationId":%q}}`, clientID, reservationID)
	updated, err := c.PatchCluster(ctx, clusterName, ns, []byte(patch))
	if err != nil {
		return nil, fmt.Errorf("patch cluster: %w", err)
	}

	return reservationResult(clusterName, clientID, reservationID, strFromObj(updated.Object, "status", "coordinatorURL"))
}

// pickOldestIdle returns the idle+ready cluster with the earliest idleAt timestamp.
func pickOldestIdle(clusters []unstructured.Unstructured) *unstructured.Unstructured {
	var best *unstructured.Unstructured
	var bestIdleAt time.Time

	for i := range clusters {
		c := &clusters[i]
		status, _ := c.Object["status"].(map[string]interface{})
		phase, _ := status["phase"].(string)
		ready, _ := status["ready"].(bool)
		if phase != "Idle" || !ready {
			continue
		}
		idleAtStr, _ := status["idleAt"].(string)
		idleAt, _ := time.Parse(time.RFC3339, idleAtStr)

		if best == nil || (!idleAt.IsZero() && idleAt.Before(bestIdleAt)) {
			best = c
			bestIdleAt = idleAt
		}
	}
	return best
}

func reservationResult(clusterName, clientID, reservationID, coordinatorURL string) (*mcp.CallToolResult, error) {
	result := map[string]string{
		"clusterName":    clusterName,
		"coordinatorUrl": coordinatorURL,
		"clientId":       clientID,
		"reservationId":  reservationID,
		"reservedAt":     time.Now().UTC().Format(time.RFC3339),
	}
	out, _ := json.MarshalIndent(result, "", "  ")
	return mcp.NewToolResultText(string(out)), nil
}

// ── release_cluster ───────────────────────────────────────────────────────────

func ReleaseClusterTool() mcp.Tool {
	return mcp.NewTool("release_cluster",
		mcp.WithDescription("Return a reserved Trino cluster to the idle pool. Clears the clientId and reservationId — the cluster transitions back to Idle and becomes available for the next reservation."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the Reserved cluster to release.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ReleaseClusterHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !c.HasMeridianOperator() {
			return nil, fmt.Errorf("Meridian operator required for cluster reservation")
		}

		clusterName := mcp.ParseString(req, "cluster_name", "")
		ns := mcp.ParseString(req, "namespace", "")

		patch := `{"spec":{"clientId":null,"reservationId":null}}`
		_, err := c.PatchCluster(ctx, clusterName, ns, []byte(patch))
		if err != nil {
			return nil, fmt.Errorf("release cluster %q: %w", clusterName, err)
		}

		return mcp.NewToolResultText(fmt.Sprintf("Cluster %q released — returning to Idle pool.", clusterName)), nil
	}
}

// strFromObj is a nested string extractor for unstructured objects.
func strFromObj(obj map[string]interface{}, keys ...string) string {
	cur := obj
	for i, k := range keys {
		if i == len(keys)-1 {
			v, _ := cur[k].(string)
			return v
		}
		next, _ := cur[k].(map[string]interface{})
		if next == nil {
			return ""
		}
		cur = next
	}
	return ""
}
