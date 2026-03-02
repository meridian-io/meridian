package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	smithy "github.com/aws/smithy-go"
)

// secretsManagerAPI is the subset of the Secrets Manager client used by
// AWSProvider. Defined as an interface so tests can inject a mock.
type secretsManagerAPI interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput,
		optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// AWSProvider reads credentials from AWS Secrets Manager using IRSA
// (IAM Roles for Service Accounts). The pod must have its service account
// annotated with an IAM role ARN that grants secretsmanager:GetSecretValue.
//
// Secret values must be stored as a JSON object whose keys are Trino catalog
// property names:
//
//	{
//	  "connector.name": "mysql",
//	  "connection-url": "jdbc:mysql://mysql:3306",
//	  "connection-user": "root",
//	  "connection-password": "secret"
//	}
//
// Annotation usage:
//
//	meridian.io/rotate-credentials: "aws-secrets-manager/mysql_catalog/arn:aws:secretsmanager:us-east-1:123456789012:secret:trino/mysql-abc123"
//	                                                     ^catalog  ^full ARN or friendly name
type AWSProvider struct {
	sm secretsManagerAPI
}

// NewAWSProvider creates a provider that fetches secrets from AWS Secrets Manager.
// Uses the ambient IRSA credentials — no explicit credentials are required.
func NewAWSProvider(ctx context.Context, region string) (*AWSProvider, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return &AWSProvider{sm: secretsmanager.NewFromConfig(cfg)}, nil
}

// Fetch retrieves the secret at path from AWS Secrets Manager and returns it
// as a Secret. path is the secret name or full ARN
// (e.g. "trino/mysql" or "arn:aws:secretsmanager:us-east-1:123:secret:trino/mysql-abc123").
//
// The secret value must be a JSON object; all values are coerced to strings.
func (p *AWSProvider) Fetch(ctx context.Context, path string) (*Secret, error) {
	out, err := p.sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(path),
	})
	if err != nil {
		return nil, mapAWSError(err, path)
	}

	var raw string
	switch {
	case out.SecretString != nil:
		raw = *out.SecretString
	case out.SecretBinary != nil:
		raw = string(out.SecretBinary)
	default:
		return nil, fmt.Errorf("aws secret %q has no value", path)
	}

	props, err := parseSecretJSON(raw, path)
	if err != nil {
		return nil, err
	}

	return &Secret{Properties: props, FetchedAt: time.Now()}, nil
}

func (p *AWSProvider) Name() string { return "aws-secrets-manager" }

// mapAWSError translates AWS SDK errors to descriptive messages.
func mapAWSError(err error, path string) error {
	var notFound *smtypes.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return fmt.Errorf("aws secret not found: %q", path)
	}

	// AccessDeniedException surfaces as a generic smithy API error, not a typed struct.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDeniedException" {
		return fmt.Errorf("aws secret access denied: %q (check IRSA role policy)", path)
	}

	return fmt.Errorf("aws get secret %q: %w", path, err)
}

// parseSecretJSON decodes a JSON object into a string map.
// All non-string values are coerced to string via fmt.Sprintf.
func parseSecretJSON(raw, path string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("aws secret %q is empty", path)
	}

	var data map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil, fmt.Errorf("aws secret %q is not a JSON object: %w", path, err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("aws secret %q has no keys", path)
	}

	props := make(map[string]string, len(data))
	for k, v := range data {
		switch s := v.(type) {
		case string:
			props[k] = s
		default:
			props[k] = fmt.Sprintf("%v", v)
		}
	}
	return props, nil
}
