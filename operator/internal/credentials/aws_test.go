package credentials

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	smithy "github.com/aws/smithy-go"
)

// ── mock ──────────────────────────────────────────────────────────────────────

type mockSM struct {
	secretString *string
	secretBinary []byte
	err          error
}

func (m *mockSM) GetSecretValue(_ context.Context, _ *secretsmanager.GetSecretValueInput,
	_ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	if m.err != nil {
		return nil, m.err
	}
	out := &secretsmanager.GetSecretValueOutput{}
	if m.secretString != nil {
		out.SecretString = m.secretString
	}
	if m.secretBinary != nil {
		out.SecretBinary = m.secretBinary
	}
	return out, nil
}

func newAWSProvider(mock *mockSM) *AWSProvider {
	return &AWSProvider{sm: mock}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestAWSProvider_FetchSuccess verifies a full success path with a JSON secret string.
func TestAWSProvider_FetchSuccess(t *testing.T) {
	payload := `{
		"connector.name":      "mysql",
		"connection-url":      "jdbc:mysql://mysql:3306",
		"connection-user":     "root",
		"connection-password": "s3cr3t"
	}`
	p := newAWSProvider(&mockSM{secretString: aws.String(payload)})

	secret, err := p.Fetch(context.Background(), "trino/mysql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if secret.Properties["connector.name"] != "mysql" {
		t.Errorf("connector.name=%q, want mysql", secret.Properties["connector.name"])
	}
	if secret.Properties["connection-password"] != "s3cr3t" {
		t.Errorf("connection-password=%q, want s3cr3t", secret.Properties["connection-password"])
	}
	if secret.FetchedAt.IsZero() {
		t.Error("FetchedAt should not be zero")
	}
}

// TestAWSProvider_BinarySecret verifies that binary secrets (JSON bytes) are handled.
func TestAWSProvider_BinarySecret(t *testing.T) {
	payload := []byte(`{"connector.name":"mysql","connection-password":"bin-secret"}`)
	p := newAWSProvider(&mockSM{secretBinary: payload})

	secret, err := p.Fetch(context.Background(), "trino/mysql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if secret.Properties["connection-password"] != "bin-secret" {
		t.Errorf("got %q, want bin-secret", secret.Properties["connection-password"])
	}
}

// TestAWSProvider_NotFound verifies ResourceNotFoundException is surfaced cleanly.
func TestAWSProvider_NotFound(t *testing.T) {
	p := newAWSProvider(&mockSM{
		err: &smtypes.ResourceNotFoundException{Message: aws.String("secret not found")},
	})
	_, err := p.Fetch(context.Background(), "trino/missing")
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error %q does not mention 'not found'", err.Error())
	}
}

// TestAWSProvider_AccessDenied verifies AccessDeniedException is surfaced with IRSA hint.
// AccessDeniedException arrives as a smithy generic API error, not a typed struct.
func TestAWSProvider_AccessDenied(t *testing.T) {
	p := newAWSProvider(&mockSM{
		err: &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "access denied"},
	})
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error for access denied")
	}
	if !strings.Contains(err.Error(), "IRSA") {
		t.Errorf("error %q should mention IRSA", err.Error())
	}
}

// TestAWSProvider_OtherError verifies that generic errors are wrapped.
func TestAWSProvider_OtherError(t *testing.T) {
	p := newAWSProvider(&mockSM{err: errors.New("network timeout")})
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "network timeout") {
		t.Errorf("error %q should contain original message", err.Error())
	}
}

// TestAWSProvider_NoValue verifies an error when the API returns no string or binary.
func TestAWSProvider_NoValue(t *testing.T) {
	p := newAWSProvider(&mockSM{})
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error for empty output")
	}
}

// TestAWSProvider_NotJSON verifies an error when the secret value is not valid JSON.
func TestAWSProvider_NotJSON(t *testing.T) {
	p := newAWSProvider(&mockSM{secretString: aws.String("not-json-at-all")})
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error for non-JSON secret")
	}
}

// TestAWSProvider_EmptyJSON verifies an error when the secret is an empty object.
func TestAWSProvider_EmptyJSON(t *testing.T) {
	p := newAWSProvider(&mockSM{secretString: aws.String("{}")})
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error for empty JSON object")
	}
}

// TestAWSProvider_EmptyString verifies an error when the secret string is blank.
func TestAWSProvider_EmptyString(t *testing.T) {
	p := newAWSProvider(&mockSM{secretString: aws.String("   ")})
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error for blank secret string")
	}
}

// TestAWSProvider_NonStringValues verifies that numeric/bool JSON values are coerced to strings.
func TestAWSProvider_NonStringValues(t *testing.T) {
	payload := `{"connector.name":"mysql","port":3306,"ssl":true}`
	p := newAWSProvider(&mockSM{secretString: aws.String(payload)})

	secret, err := p.Fetch(context.Background(), "trino/mysql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if secret.Properties["port"] == "" {
		t.Error("port should be coerced to a non-empty string")
	}
	if secret.Properties["ssl"] == "" {
		t.Error("ssl should be coerced to a non-empty string")
	}
}

// TestAWSProvider_ARNPath verifies that a full ARN is passed through to GetSecretValue.
func TestAWSProvider_ARNPath(t *testing.T) {
	captured := ""
	sm := &capturingMockSM{
		secretString: aws.String(`{"connector.name":"mysql"}`),
		onCall: func(id string) { captured = id },
	}
	p := &AWSProvider{sm: sm}

	arn := "arn:aws:secretsmanager:us-east-1:123456789012:secret:trino/mysql-abc123"
	if _, err := p.Fetch(context.Background(), arn); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured != arn {
		t.Errorf("SecretId passed as %q, want %q", captured, arn)
	}
}

// TestAWSProvider_Name verifies the canonical provider name.
func TestAWSProvider_Name(t *testing.T) {
	p := &AWSProvider{}
	if p.Name() != "aws-secrets-manager" {
		t.Errorf("Name()=%q, want aws-secrets-manager", p.Name())
	}
}

// TestAWSProvider_ImplementsInterface ensures AWSProvider satisfies SecretProvider.
func TestAWSProvider_ImplementsInterface(t *testing.T) {
	var _ SecretProvider = (*AWSProvider)(nil)
}

// ── helpers ───────────────────────────────────────────────────────────────────

type capturingMockSM struct {
	secretString *string
	onCall       func(id string)
}

func (m *capturingMockSM) GetSecretValue(_ context.Context, in *secretsmanager.GetSecretValueInput,
	_ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	if m.onCall != nil && in.SecretId != nil {
		m.onCall(*in.SecretId)
	}
	return &secretsmanager.GetSecretValueOutput{SecretString: m.secretString}, nil
}
