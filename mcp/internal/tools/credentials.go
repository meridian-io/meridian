package tools

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

// ── rotate_credentials ────────────────────────────────────────────────────────

func RotateCredentialsTool() mcp.Tool {
	return mcp.NewTool("rotate_credentials",
		mcp.WithDescription("Trigger a credential rotation for a catalog on a running cluster. Fetches the latest secret from the configured provider (Vault or AWS Secrets Manager) and pushes an updated catalog config via the Trino dynamic catalog API — no cluster restart required."),
		mcp.WithString("cluster_name", mcp.Required(), mcp.Description("Name of the target Cluster resource.")),
		mcp.WithString("catalog_name", mcp.Required(), mcp.Description("Name of the catalog whose credentials should be rotated.")),
		mcp.WithString("secret_path", mcp.Required(), mcp.Description("Path to the secret in Vault (e.g. secret/data/trino/postgres) or AWS Secrets Manager ARN.")),
		mcp.WithString("provider", mcp.Description("Secret provider: vault or aws-secrets-manager. Defaults to vault.")),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace. Defaults to the configured namespace.")),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func RotateCredentialsHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		clusterName := mcp.ParseString(req, "cluster_name", "")
		catalogName := mcp.ParseString(req, "catalog_name", "")
		secretPath := mcp.ParseString(req, "secret_path", "")
		provider := mcp.ParseString(req, "provider", "")
		ns := mcp.ParseString(req, "namespace", "")

		if provider == "" {
			provider = "vault"
		}

		// Verify the cluster is reachable.
		_, err := c.GetCluster(ctx, clusterName, ns)
		if err != nil {
			return nil, fmt.Errorf("get cluster %q: %w", clusterName, err)
		}

		// Phase 1: annotation-based rotation trigger.
		// The operator watches for this annotation and handles the Vault/ASM fetch
		// + dynamic catalog push. Full Vault/ASM client is implemented in Phase 4.
		patch := fmt.Sprintf(
			`{"metadata":{"annotations":{"meridian.io/rotate-credentials":"%s/%s/%s"}}}`,
			provider, catalogName, secretPath,
		)
		_, err = c.PatchCluster(ctx, clusterName, ns, []byte(patch))
		if err != nil {
			return nil, fmt.Errorf("trigger rotation: %w", err)
		}

		return mcp.NewToolResultText(fmt.Sprintf(
			"Credential rotation triggered for catalog %q on cluster %q.\nProvider: %s\nSecret path: %s\n\nThe operator will fetch the latest secret and push the updated catalog config.",
			catalogName, clusterName, provider, secretPath,
		)), nil
	}
}
