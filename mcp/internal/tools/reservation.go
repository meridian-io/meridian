package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		if clientID == "" || reservationID == "" {
			return nil, fmt.Errorf("client_id and reservation_id are required")
		}

		if clusterName != "" {
			return reserveByName(ctx, c, clusterName, clientID, reservationID, ns)
		}
		return reserveByWorkload(ctx, c, workload, clientID, reservationID, ns)
	}
}

// maxReserveAttempts bounds retries when another client claims a candidate first.
const maxReserveAttempts = 5

// reserveByName reserves one specific cluster. A conflict caused by an
// unrelated update (e.g. a status change) is retried with a fresh read.
func reserveByName(ctx context.Context, c *k8s.Client, clusterName, clientID, reservationID, ns string) (*mcp.CallToolResult, error) {
	for attempt := 0; attempt < maxReserveAttempts; attempt++ {
		cluster, err := c.GetCluster(ctx, clusterName, ns)
		if err != nil {
			return nil, fmt.Errorf("get cluster %q: %w", clusterName, err)
		}

		// Idempotency: the same (clientId, reservationId) gets the same answer.
		if ownedBy(cluster, clientID, reservationID) {
			return reservationResult(clusterName, clientID, reservationID, strFromObj(cluster.Object, "status", "coordinatorUrl"))
		}
		if !isReservable(cluster) {
			phase := strFromObj(cluster.Object, "status", "phase")
			return nil, fmt.Errorf("cluster %q is not available (phase %q) — only healthy, unreserved Idle clusters can be reserved", clusterName, phase)
		}

		updated, err := claimCluster(ctx, c, cluster, clientID, reservationID, ns)
		if apierrors.IsConflict(err) {
			continue // re-read: if someone else claimed it, isReservable fails next time
		}
		if err != nil {
			return nil, fmt.Errorf("patch cluster: %w", err)
		}
		return reservationResult(clusterName, clientID, reservationID, strFromObj(updated.Object, "status", "coordinatorUrl"))
	}
	return nil, fmt.Errorf("cluster %q kept changing while reserving — retry shortly", clusterName)
}

// reserveByWorkload reserves the oldest idle cluster labeled with the workload.
// If another client claims a candidate first, it moves on to the next one.
func reserveByWorkload(ctx context.Context, c *k8s.Client, workload, clientID, reservationID, ns string) (*mcp.CallToolResult, error) {
	if !workloadRE.MatchString(workload) {
		return nil, fmt.Errorf("invalid workload %q", workload)
	}
	labelSelector := "meridian.io/workload=" + workload

	for attempt := 0; attempt < maxReserveAttempts; attempt++ {
		clusters, err := c.ListClustersWithSelector(ctx, ns, labelSelector)
		if err != nil {
			return nil, fmt.Errorf("list clusters for workload %q: %w", workload, err)
		}

		for i := range clusters {
			if ownedBy(&clusters[i], clientID, reservationID) {
				cl := &clusters[i]
				return reservationResult(cl.GetName(), clientID, reservationID, strFromObj(cl.Object, "status", "coordinatorUrl"))
			}
		}

		best := pickOldestIdle(clusters)
		if best == nil {
			return nil, fmt.Errorf("no idle clusters available for workload %q", workload)
		}

		updated, err := claimCluster(ctx, c, best, clientID, reservationID, ns)
		if apierrors.IsConflict(err) {
			continue // re-list: a claimed cluster is no longer reservable
		}
		if err != nil {
			return nil, fmt.Errorf("patch cluster: %w", err)
		}
		return reservationResult(best.GetName(), clientID, reservationID, strFromObj(updated.Object, "status", "coordinatorUrl"))
	}
	return nil, fmt.Errorf("could not reserve a cluster for workload %q: too many concurrent reservations, retry shortly", workload)
}

// workloadRE matches valid Kubernetes label values.
var workloadRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9_.\-]{0,61}[A-Za-z0-9])?$`)

// claimCluster writes the reservation only if the cluster hasn't changed since
// we read it. The resourceVersion in the patch makes the API server reject the
// write with a Conflict if another client got there first.
func claimCluster(ctx context.Context, c *k8s.Client, cluster *unstructured.Unstructured, clientID, reservationID, ns string) (*unstructured.Unstructured, error) {
	patch, err := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{"resourceVersion": cluster.GetResourceVersion()},
		"spec":     map[string]interface{}{"clientId": clientID, "reservationId": reservationID},
	})
	if err != nil {
		return nil, err
	}
	return c.PatchCluster(ctx, cluster.GetName(), ns, patch)
}

// ownedBy reports whether the cluster is already reserved for this request.
func ownedBy(cluster *unstructured.Unstructured, clientID, reservationID string) bool {
	if clientID == "" || reservationID == "" {
		return false
	}
	return strFromObj(cluster.Object, "spec", "clientId") == clientID &&
		strFromObj(cluster.Object, "spec", "reservationId") == reservationID
}

// isReservable reports whether the cluster is healthy, Idle, and unclaimed.
func isReservable(cluster *unstructured.Unstructured) bool {
	status, _ := cluster.Object["status"].(map[string]interface{})
	phase, _ := status["phase"].(string)
	ready, _ := status["ready"].(bool)
	return phase == "Idle" && ready && strFromObj(cluster.Object, "spec", "clientId") == ""
}

// pickOldestIdle returns the reservable cluster with the earliest idleAt.
func pickOldestIdle(clusters []unstructured.Unstructured) *unstructured.Unstructured {
	var best *unstructured.Unstructured
	var bestIdleAt time.Time

	for i := range clusters {
		c := &clusters[i]
		if !isReservable(c) {
			continue
		}
		idleAt, _ := time.Parse(time.RFC3339, strFromObj(c.Object, "status", "idleAt"))

		switch {
		case best == nil:
			best, bestIdleAt = c, idleAt
		case bestIdleAt.IsZero() && !idleAt.IsZero():
			best, bestIdleAt = c, idleAt
		case !idleAt.IsZero() && idleAt.Before(bestIdleAt):
			best, bestIdleAt = c, idleAt
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
		mcp.WithString("client_id", mcp.Description("If set, the release only succeeds when the cluster is reserved by this client.")),
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
		clientID := mcp.ParseString(req, "client_id", "")
		ns := mcp.ParseString(req, "namespace", "")

		cluster, err := c.GetCluster(ctx, clusterName, ns)
		if err != nil {
			return nil, fmt.Errorf("get cluster %q: %w", clusterName, err)
		}
		owner := strFromObj(cluster.Object, "spec", "clientId")
		if owner == "" {
			return mcp.NewToolResultText(fmt.Sprintf("Cluster %q is not reserved — nothing to release.", clusterName)), nil
		}
		if clientID != "" && owner != clientID {
			return nil, fmt.Errorf("cluster %q is reserved by a different client", clusterName)
		}

		patch, _ := json.Marshal(map[string]interface{}{
			"metadata": map[string]interface{}{"resourceVersion": cluster.GetResourceVersion()},
			"spec":     map[string]interface{}{"clientId": nil, "reservationId": nil},
		})
		_, err = c.PatchCluster(ctx, clusterName, ns, patch)
		if apierrors.IsConflict(err) {
			return nil, fmt.Errorf("cluster %q changed while releasing — retry", clusterName)
		}
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
