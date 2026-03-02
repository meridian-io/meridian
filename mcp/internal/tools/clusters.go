package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

// ClusterSummary is the JSON shape returned by list_clusters / get_cluster.
type ClusterSummary struct {
	Name           string `json:"name"`
	Namespace      string `json:"namespace"`
	Phase          string `json:"phase"`
	Ready          bool   `json:"ready"`
	CoordinatorURL string `json:"coordinatorUrl,omitempty"`
	Profile        string `json:"profile,omitempty"`
	Workers        int64  `json:"workers,omitempty"`
	IdleAt         string `json:"idleAt,omitempty"`
	ReservedAt     string `json:"reservedAt,omitempty"`
	ClientID       string `json:"clientId,omitempty"`
	ReservationID  string `json:"reservationId,omitempty"`
}

// ── list_clusters ─────────────────────────────────────────────────────────────

func ListClustersTool() mcp.Tool {
	return mcp.NewTool("list_clusters",
		mcp.WithDescription("List all Trino clusters managed by Meridian with their phase (Idle/Reserved/Pending/Failed), health, coordinator URL, and reservation info."),
		mcp.WithString("namespace",
			mcp.Description("Kubernetes namespace to search. Defaults to the configured namespace."),
		),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ListClustersHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !c.HasMeridianOperator() {
			return mcp.NewToolResultText("Meridian operator not detected. Install the operator to enable cluster pool management."), nil
		}
		ns := mcp.ParseString(req, "namespace", "")
		items, err := c.ListClusters(ctx, ns)
		if err != nil {
			return nil, fmt.Errorf("list clusters: %w", err)
		}
		summaries := make([]ClusterSummary, 0, len(items))
		for _, item := range items {
			summaries = append(summaries, clusterSummary(item.Object))
		}
		out, _ := json.MarshalIndent(summaries, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	}
}

// ── get_cluster ───────────────────────────────────────────────────────────────

func GetClusterTool() mcp.Tool {
	return mcp.NewTool("get_cluster",
		mcp.WithDescription("Get full details of a specific Trino cluster: phase, health, coordinator URL, current reservation, and timestamps."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Cluster resource name.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func GetClusterHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		name := mcp.ParseString(req, "name", "")
		ns := mcp.ParseString(req, "namespace", "")
		cluster, err := c.GetCluster(ctx, name, ns)
		if err != nil {
			return nil, fmt.Errorf("get cluster %q: %w", name, err)
		}
		out, _ := json.MarshalIndent(clusterSummary(cluster.Object), "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	}
}

// ── create_cluster ────────────────────────────────────────────────────────────

func CreateClusterTool() mcp.Tool {
	return mcp.NewTool("create_cluster",
		mcp.WithDescription("Provision a new Trino cluster. The Meridian operator manages lifecycle — the cluster becomes Idle once the coordinator passes health checks."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Name for the new Cluster resource.")),
		mcp.WithString("profile", mcp.Required(), mcp.Description("Profile name (e.g. standard-trino). Must match a profile used in a ClusterPool.")),
		mcp.WithString("image", mcp.Description("Trino Docker image. Defaults to trinodb/trino:latest.")),
		mcp.WithNumber("workers", mcp.Description("Number of worker nodes. Defaults to 2.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(false),
	)
}

func CreateClusterHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !c.HasMeridianOperator() {
			return nil, fmt.Errorf("Meridian operator required — install it first")
		}
		name := mcp.ParseString(req, "name", "")
		profile := mcp.ParseString(req, "profile", "")
		image := mcp.ParseString(req, "image", "")
		ns := mcp.ParseString(req, "namespace", "")
		workers := mcp.ParseFloat64(req, "workers", 0)

		if image == "" {
			image = "trinodb/trino:latest"
		}
		if workers == 0 {
			workers = 2
		}
		if ns == "" {
			ns = c.Namespace
		}

		obj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "meridian.io/v1alpha1",
				"kind":       "Cluster",
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": ns,
					"labels":    map[string]interface{}{"meridian.io/profile": profile},
				},
				"spec": map[string]interface{}{
					"image":   image,
					"workers": int64(workers),
				},
			},
		}

		created, err := c.CreateCluster(ctx, obj)
		if err != nil {
			return nil, fmt.Errorf("create cluster: %w", err)
		}
		out, _ := json.MarshalIndent(clusterSummary(created.Object), "", "  ")
		return mcp.NewToolResultText(fmt.Sprintf("Cluster %q created — operator will provision it.\n\n%s", name, out)), nil
	}
}

// ── delete_cluster ────────────────────────────────────────────────────────────

func DeleteClusterTool() mcp.Tool {
	return mcp.NewTool("delete_cluster",
		mcp.WithDescription("Tear down a Trino cluster and remove all associated Kubernetes resources. Release the reservation first if the cluster is Reserved."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Cluster resource name.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(true),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func DeleteClusterHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		name := mcp.ParseString(req, "name", "")
		ns := mcp.ParseString(req, "namespace", "")
		if err := c.DeleteCluster(ctx, name, ns); err != nil {
			return nil, fmt.Errorf("delete cluster %q: %w", name, err)
		}
		return mcp.NewToolResultText(fmt.Sprintf("Cluster %q deleted.", name)), nil
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func clusterSummary(obj map[string]interface{}) ClusterSummary {
	meta, _ := obj["metadata"].(map[string]interface{})
	spec, _ := obj["spec"].(map[string]interface{})
	status, _ := obj["status"].(map[string]interface{})
	labels, _ := meta["labels"].(map[string]interface{})

	s := ClusterSummary{
		Name:      str(meta, "name"),
		Namespace: str(meta, "namespace"),
	}
	if labels != nil {
		s.Profile, _ = labels["meridian.io/profile"].(string)
	}
	if spec != nil {
		switch v := spec["workers"].(type) {
		case int64:
			s.Workers = v
		case float64:
			s.Workers = int64(v)
		}
		s.CoordinatorURL, _ = spec["coordinatorURL"].(string)
	}
	if status != nil {
		s.Phase, _ = status["phase"].(string)
		s.Ready, _ = status["ready"].(bool)
		s.CoordinatorURL, _ = status["coordinatorURL"].(string)
		s.IdleAt, _ = status["idleAt"].(string)
		s.ReservedAt, _ = status["reservedAt"].(string)
	}
	if spec != nil {
		s.ClientID, _ = spec["clientId"].(string)
		s.ReservationID, _ = spec["reservationId"].(string)
	}
	return s
}

func str(m map[string]interface{}, key string) string {
	v, _ := m[key].(string)
	return v
}
