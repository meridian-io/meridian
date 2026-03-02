package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

// ── promote_environment ───────────────────────────────────────────────────────

func PromoteEnvironmentTool() mcp.Tool {
	return mcp.NewTool("promote_environment",
		mcp.WithDescription("Copy a ClusterPool configuration from one environment namespace to another (e.g. dev → staging → prod). Returns a diff of what will change and applies it when confirm=true."),
		mcp.WithString("pool_name", mcp.Required(), mcp.Description("Name of the ClusterPool to promote.")),
		mcp.WithString("source_namespace", mcp.Required(), mcp.Description("Source namespace (e.g. meridian-dev).")),
		mcp.WithString("target_namespace", mcp.Required(), mcp.Description("Target namespace (e.g. meridian-staging).")),
		mcp.WithBoolean("confirm", mcp.Description("Set to true to apply the promotion. Defaults to false (dry-run — shows diff only).")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func PromoteEnvironmentHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if !c.HasMeridianOperator() {
			return nil, fmt.Errorf("Meridian operator required for environment promotion")
		}

		poolName := mcp.ParseString(req, "pool_name", "")
		sourceNS := mcp.ParseString(req, "source_namespace", "")
		targetNS := mcp.ParseString(req, "target_namespace", "")
		confirm := mcp.ParseBoolean(req, "confirm", false)

		// Fetch source pool.
		source, err := c.GetClusterPool(ctx, poolName, sourceNS)
		if err != nil {
			return nil, fmt.Errorf("get source pool %q in %q: %w", poolName, sourceNS, err)
		}

		sourceSpec, _ := source.Object["spec"].(map[string]interface{})

		// Try to fetch existing target pool for diff.
		existingTarget, _ := c.GetClusterPool(ctx, poolName, targetNS)

		diff := buildPromotionDiff(poolName, sourceNS, targetNS, sourceSpec, existingTarget)

		if !confirm {
			return mcp.NewToolResultText(fmt.Sprintf(
				"DRY RUN — promotion not applied.\nSet confirm=true to apply.\n\n%s", diff,
			)), nil
		}

		// Build the new ClusterPool object for the target namespace.
		newPool := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "meridian.io/v1alpha1",
				"kind":       "ClusterPool",
				"metadata": map[string]interface{}{
					"name":      poolName,
					"namespace": targetNS,
					"annotations": map[string]interface{}{
						"meridian.io/promoted-from": sourceNS,
					},
				},
				"spec": deepCopySpec(sourceSpec),
			},
		}

		if existingTarget != nil {
			// Update existing pool via patch.
			specBytes, _ := json.Marshal(map[string]interface{}{"spec": deepCopySpec(sourceSpec)})
			_, err = c.PatchClusterPool(ctx, poolName, targetNS, specBytes)
		} else {
			// Create the pool in the target namespace.
			_, err = c.Dynamic.Resource(k8s.ClusterPoolGVR).Namespace(targetNS).
				Create(ctx, newPool, metav1.CreateOptions{})
		}
		if err != nil {
			return nil, fmt.Errorf("apply promotion: %w", err)
		}

		return mcp.NewToolResultText(fmt.Sprintf(
			"Promotion applied: %s/%s → %s/%s\n\n%s",
			sourceNS, poolName, targetNS, poolName, diff,
		)), nil
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func buildPromotionDiff(poolName, sourceNS, targetNS string, sourceSpec map[string]interface{}, target *unstructured.Unstructured) string {
	sourceJSON, _ := json.MarshalIndent(sourceSpec, "  ", "  ")

	if target == nil {
		return fmt.Sprintf("NEW ClusterPool %s/%s will be created with spec:\n  %s", targetNS, poolName, string(sourceJSON))
	}

	targetSpec, _ := target.Object["spec"].(map[string]interface{})
	targetJSON, _ := json.MarshalIndent(targetSpec, "  ", "  ")

	return fmt.Sprintf(
		"ClusterPool %s/%s will be UPDATED.\n\nSource (%s) spec:\n  %s\n\nTarget (%s) current spec:\n  %s",
		targetNS, poolName, sourceNS, string(sourceJSON), targetNS, string(targetJSON),
	)
}

// deepCopySpec strips status and metadata that shouldn't be copied.
func deepCopySpec(spec map[string]interface{}) map[string]interface{} {
	if spec == nil {
		return nil
	}
	b, _ := json.Marshal(spec)
	var copy map[string]interface{}
	json.Unmarshal(b, &copy)
	return copy
}
