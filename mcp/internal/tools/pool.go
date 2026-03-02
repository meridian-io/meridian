package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

// ── scale_pool ────────────────────────────────────────────────────────────────

func ScalePoolTool() mcp.Tool {
	return mcp.NewTool("scale_pool",
		mcp.WithDescription("Set the desired replica count on a ClusterPool. The ClusterPoolController reconciles to this target — creating or deleting clusters as needed. Respects minReplicas/maxReplicas bounds from any attached ClusterPoolAutoscaler."),
		mcp.WithString("pool_name", mcp.Required(), mcp.Description("Name of the ClusterPool to scale.")),
		mcp.WithNumber("replicas", mcp.Required(), mcp.Description("Desired number of clusters in the pool.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ScalePoolHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !c.HasMeridianOperator() {
			return nil, fmt.Errorf("Meridian operator required to manage cluster pools")
		}

		poolName := mcp.ParseString(req, "pool_name", "")
		replicas := mcp.ParseFloat64(req, "replicas", 0)
		ns := mcp.ParseString(req, "namespace", "")

		if replicas < 0 {
			return nil, fmt.Errorf("replicas must be >= 0")
		}

		// Fetch current state for context.
		pool, err := c.GetClusterPool(ctx, poolName, ns)
		if err != nil {
			return nil, fmt.Errorf("get pool %q: %w", poolName, err)
		}

		currentReplicas := int64Field(pool.Object, "spec", "replicas")

		patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, int64(replicas))
		updated, err := c.PatchClusterPool(ctx, poolName, ns, []byte(patch))
		if err != nil {
			return nil, fmt.Errorf("patch pool: %w", err)
		}

		result := map[string]interface{}{
			"poolName":         poolName,
			"previousReplicas": currentReplicas,
			"desiredReplicas":  int64(replicas),
			"status":           strFromObj(updated.Object, "status", "phase"),
		}
		out, _ := json.MarshalIndent(result, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	}
}

// ── list_pools ────────────────────────────────────────────────────────────────

func ListPoolsTool() mcp.Tool {
	return mcp.NewTool("list_pools",
		mcp.WithDescription("List all ClusterPools with their current replica count, desired replicas, and autoscaler settings."),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func ListPoolsHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !c.HasMeridianOperator() {
			return nil, fmt.Errorf("Meridian operator required")
		}
		ns := mcp.ParseString(req, "namespace", "")
		pools, err := c.ListClusterPools(ctx, ns)
		if err != nil {
			return nil, fmt.Errorf("list pools: %w", err)
		}

		type poolSummary struct {
			Name            string `json:"name"`
			Namespace       string `json:"namespace"`
			DesiredReplicas int64  `json:"desiredReplicas"`
			Profile         string `json:"profile,omitempty"`
		}

		summaries := make([]poolSummary, 0, len(pools))
		for _, p := range pools {
			meta, _ := p.Object["metadata"].(map[string]interface{})
			spec, _ := p.Object["spec"].(map[string]interface{})
			ps := poolSummary{
				Name:      str(meta, "name"),
				Namespace: str(meta, "namespace"),
			}
			ps.DesiredReplicas = int64Field(p.Object, "spec", "replicas")
			if spec != nil {
				tmpl, _ := spec["template"].(map[string]interface{})
				if tmpl != nil {
					ps.Profile, _ = tmpl["profile"].(string)
				}
			}
			summaries = append(summaries, ps)
		}

		out, _ := json.MarshalIndent(summaries, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	}
}

func int64Field(obj map[string]interface{}, keys ...string) int64 {
	cur := obj
	for i, k := range keys {
		if i == len(keys)-1 {
			switch v := cur[k].(type) {
			case int64:
				return v
			case float64:
				return int64(v)
			}
			return 0
		}
		next, _ := cur[k].(map[string]interface{})
		if next == nil {
			return 0
		}
		cur = next
	}
	return 0
}
