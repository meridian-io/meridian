package credentials

import (
	"testing"
	"time"
)

// ── ParseRotationAnnotation ───────────────────────────────────────────────────

func TestParseRotationAnnotation_Kubernetes(t *testing.T) {
	req, err := ParseRotationAnnotation("kubernetes/mysql_catalog/mysql-credentials")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Provider != "kubernetes" {
		t.Errorf("expected provider kubernetes, got %q", req.Provider)
	}
	if req.CatalogName != "mysql_catalog" {
		t.Errorf("expected catalog mysql_catalog, got %q", req.CatalogName)
	}
	if req.SecretPath != "mysql-credentials" {
		t.Errorf("expected path mysql-credentials, got %q", req.SecretPath)
	}
}

func TestParseRotationAnnotation_VaultPathWithSlashes(t *testing.T) {
	// Vault paths contain slashes — only the first two slashes split the annotation.
	req, err := ParseRotationAnnotation("vault/pg_catalog/secret/data/trino/pg-prod")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Provider != "vault" {
		t.Errorf("expected provider vault, got %q", req.Provider)
	}
	if req.CatalogName != "pg_catalog" {
		t.Errorf("expected catalog pg_catalog, got %q", req.CatalogName)
	}
	if req.SecretPath != "secret/data/trino/pg-prod" {
		t.Errorf("expected path secret/data/trino/pg-prod, got %q", req.SecretPath)
	}
}

func TestParseRotationAnnotation_AWSARNWithSlashes(t *testing.T) {
	// ASM ARNs contain colons and slashes after the third token.
	arn := "arn:aws:secretsmanager:us-east-1:123456789012:secret:trino/hive"
	req, err := ParseRotationAnnotation("aws-secrets-manager/hive_catalog/" + arn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Provider != "aws-secrets-manager" {
		t.Errorf("expected provider aws-secrets-manager, got %q", req.Provider)
	}
	if req.SecretPath != arn {
		t.Errorf("expected path %q, got %q", arn, req.SecretPath)
	}
}

func TestParseRotationAnnotation_UnknownProvider(t *testing.T) {
	_, err := ParseRotationAnnotation("gcp-secret-manager/my_catalog/projects/123/secrets/trino")
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}

func TestParseRotationAnnotation_MissingComponent(t *testing.T) {
	cases := []string{
		"kubernetes/mysql_catalog",   // no path
		"kubernetes",                 // only provider
		"",                           // empty
		"kubernetes//mysql-secret",   // empty catalog
		"kubernetes/mysql_catalog/",  // empty path
	}
	for _, c := range cases {
		_, err := ParseRotationAnnotation(c)
		if err == nil {
			t.Errorf("expected error for %q, got nil", c)
		}
	}
}

func TestParseRotationAnnotation_Whitespace(t *testing.T) {
	// Leading/trailing whitespace in the annotation value is trimmed.
	req, err := ParseRotationAnnotation("  vault/my_catalog/secret/data/trino/pg  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Provider != "vault" {
		t.Errorf("expected provider vault, got %q", req.Provider)
	}
}

// ── BackoffDuration ───────────────────────────────────────────────────────────

func TestBackoffDuration_Table(t *testing.T) {
	cases := []struct {
		failures int32
		want     time.Duration
	}{
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{4, 40 * time.Second},
		{5, 80 * time.Second},
	}
	for _, c := range cases {
		got := BackoffDuration(c.failures)
		if got != c.want {
			t.Errorf("BackoffDuration(%d) = %v, want %v", c.failures, got, c.want)
		}
	}
}

func TestBackoffDuration_Cap(t *testing.T) {
	// Any large failure count must not exceed 20 minutes.
	max := 20 * time.Minute
	for _, n := range []int32{10, 50, 100, 1000} {
		got := BackoffDuration(n)
		if got > max {
			t.Errorf("BackoffDuration(%d) = %v, exceeds cap of %v", n, got, max)
		}
		if got != max {
			t.Errorf("BackoffDuration(%d) = %v, want cap %v", n, got, max)
		}
	}
}

func TestBackoffDuration_ZeroOrNegative(t *testing.T) {
	// Zero or negative failures return the base duration.
	for _, n := range []int32{0, -1, -100} {
		got := BackoffDuration(n)
		if got != 5*time.Second {
			t.Errorf("BackoffDuration(%d) = %v, want 5s", n, got)
		}
	}
}
