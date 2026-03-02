package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

// ── reserve_cluster ───────────────────────────────────────────────────────────

func ReserveClusterTool() mcp.Tool {
	return mcp.NewTool("reserve_cluster",
		mcp.WithDescription("Assign an idle Trino cluster to a client. Idempotent — the same (clientId, reservationId) pair always returns the same cluster. The cluster transitions from Idle → Reserved."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of an Idle cluster to reserve.")),
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
		clientID := mcp.ParseString(req, "client_id", "")
		reservationID := mcp.ParseString(req, "reservation_id", "")
		ns := mcp.ParseString(req, "namespace", "")

		// Verify the cluster exists and is Idle before patching.
		cluster, err := c.GetCluster(ctx, clusterName, ns)
		if err != nil {
			return nil, fmt.Errorf("get cluster %q: %w", clusterName, err)
		}

		status, _ := cluster.Object["status"].(map[string]interface{})
		phase, _ := status["phase"].(string)
		if phase != "Idle" {
			return nil, fmt.Errorf("cluster %q is %q — only Idle clusters can be reserved", clusterName, phase)
		}

		// Patch spec.clientId + spec.reservationId — ClusterController detects and
		// transitions the cluster to Reserved.
		patch := fmt.Sprintf(
			`{"spec":{"clientId":%q,"reservationId":%q}}`,
			clientID, reservationID,
		)
		updated, err := c.PatchCluster(ctx, clusterName, ns, []byte(patch))
		if err != nil {
			return nil, fmt.Errorf("patch cluster: %w", err)
		}

		result := map[string]string{
			"clusterName":    clusterName,
			"coordinatorUrl": strFromObj(updated.Object, "status", "coordinatorURL"),
			"clientId":       clientID,
			"reservationId":  reservationID,
			"reservedAt":     time.Now().UTC().Format(time.RFC3339),
		}
		out, _ := json.MarshalIndent(result, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	}
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
