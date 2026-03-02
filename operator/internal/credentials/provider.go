package credentials

import (
	"context"
	"time"
)

// Secret holds catalog properties fetched from a secret backend.
// Keys and values are in Trino Java properties format (e.g. "connection-url").
// The caller owns the Properties map — the provider must not retain references after returning.
type Secret struct {
	Properties map[string]string
	FetchedAt  time.Time
}

// RotationRequest is the parsed form of the meridian.io/rotate-credentials annotation.
type RotationRequest struct {
	Provider    string // "kubernetes" | "vault" | "aws-secrets-manager"
	CatalogName string
	SecretPath  string // semantics depend on provider
}

// SecretProvider reads credentials from an external secret store.
// All implementations must be safe for concurrent use.
type SecretProvider interface {
	// Fetch retrieves the secret at path and returns it as a Secret.
	// Context cancellation must be respected.
	Fetch(ctx context.Context, path string) (*Secret, error)

	// Name returns the canonical provider identifier used in annotations and logs.
	// Must match the provider field in the rotation annotation.
	Name() string
}
