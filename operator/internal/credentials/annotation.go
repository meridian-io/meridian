package credentials

import (
	"fmt"
	"strings"
	"time"
)

const (
	// AnnotationRotateCredentials is patched by the MCP rotate_credentials tool
	// to trigger a credential rotation on the target cluster.
	// Format: "<provider>/<catalog-name>/<secret-path>"
	// The secret-path may itself contain slashes (Vault paths, ASM ARNs).
	AnnotationRotateCredentials = "meridian.io/rotate-credentials"
)

// ParseRotationAnnotation splits the annotation value into its three components.
//
// Format: "<provider>/<catalog-name>/<secret-path>"
//
// Only the first two slashes are used as split points so that secret paths
// containing slashes (e.g. Vault KV paths, AWS ARNs) are preserved intact.
//
// Examples:
//
//	"kubernetes/mysql_catalog/mysql-credentials"
//	"vault/pg_catalog/secret/data/trino/pg-prod"
//	"aws-secrets-manager/hive_catalog/arn:aws:secretsmanager:us-east-1:123:secret:trino/hive"
func ParseRotationAnnotation(value string) (*RotationRequest, error) {
	parts := strings.SplitN(strings.TrimSpace(value), "/", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("expected format <provider>/<catalog>/<path>, got %q", value)
	}

	provider := strings.TrimSpace(parts[0])
	catalog := strings.TrimSpace(parts[1])
	path := strings.TrimSpace(parts[2])

	if provider == "" || catalog == "" || path == "" {
		return nil, fmt.Errorf("provider, catalog, and path must all be non-empty in %q", value)
	}

	switch provider {
	case "kubernetes", "vault", "aws-secrets-manager":
	default:
		return nil, fmt.Errorf("unknown provider %q: must be kubernetes, vault, or aws-secrets-manager", provider)
	}

	return &RotationRequest{
		Provider:    provider,
		CatalogName: catalog,
		SecretPath:  path,
	}, nil
}

// BackoffDuration returns the wait duration before the next rotation retry
// given the number of consecutive failures.
//
// Formula: min(base × 2^(failures-1), maxBackoff)
// base = 5s, max = 20 min
//
// Table:
//
//	failures=1  →   5s
//	failures=2  →  10s
//	failures=3  →  20s
//	failures=4  →  40s
//	failures=5  →  80s
//	failures=10 → 1200s (capped at 20 min)
func BackoffDuration(failures int32) time.Duration {
	const (
		base       = 5 * time.Second
		maxBackoff = 20 * time.Minute
	)
	if failures <= 0 {
		return base
	}
	d := base
	for i := int32(1); i < failures; i++ {
		d *= 2
		if d >= maxBackoff {
			return maxBackoff
		}
	}
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}
