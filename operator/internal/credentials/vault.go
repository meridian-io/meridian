package credentials

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	// serviceAccountTokenPath is the standard pod service account JWT path.
	serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

	// tokenRenewBuffer is how early before expiry we proactively re-login.
	tokenRenewBuffer = 5 * time.Minute
)

// VaultProvider reads credentials from HashiCorp Vault using the Kubernetes
// auth method (ServiceAccount JWT). Authenticates once and caches the Vault
// client token, re-authenticating proactively before expiry or on 403.
//
// No Vault SDK is used — all API calls are plain HTTPS requests so there are
// no additional binary dependencies.
//
// Annotation usage:
//
//	meridian.io/rotate-credentials: "vault/mysql_catalog/trino/mysql"
//	                                         ^catalog  ^KV v2 path under mount
type VaultProvider struct {
	addr      string
	role      string
	mountPath string

	client    *http.Client
	tokenPath string // override in tests; defaults to serviceAccountTokenPath

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
}

// NewVaultProvider creates a provider that authenticates to Vault via the
// Kubernetes auth method and reads KV v2 secrets.
//
//	addr:      Vault server address (e.g. "https://vault.example.com")
//	role:      Vault Kubernetes auth role name
//	mountPath: KV v2 mount path (e.g. "secret")
func NewVaultProvider(addr, role, mountPath string) *VaultProvider {
	return &VaultProvider{
		addr:      addr,
		role:      role,
		mountPath: mountPath,
		client:    &http.Client{Timeout: 10 * time.Second},
		tokenPath: serviceAccountTokenPath,
	}
}

// Fetch retrieves the secret at path from Vault KV v2 and returns it as a
// Secret. path is the key path under the mount (e.g. "trino/mysql").
func (p *VaultProvider) Fetch(ctx context.Context, path string) (*Secret, error) {
	token, err := p.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("vault auth: %w", err)
	}

	props, err := p.kvGet(ctx, token, path)
	if err != nil {
		return nil, err
	}

	return &Secret{Properties: props, FetchedAt: time.Now()}, nil
}

func (p *VaultProvider) Name() string { return "vault" }

// getToken returns the cached client token, re-authenticating if it is
// missing or within tokenRenewBuffer of expiry.
func (p *VaultProvider) getToken(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.token != "" && time.Now().Before(p.tokenExpiry.Add(-tokenRenewBuffer)) {
		return p.token, nil
	}

	return p.login(ctx)
}

// login authenticates to Vault with the pod's service account JWT.
// Must be called with p.mu held.
func (p *VaultProvider) login(ctx context.Context) (string, error) {
	jwt, err := os.ReadFile(p.tokenPath)
	if err != nil {
		return "", fmt.Errorf("read service account token: %w", err)
	}

	body, _ := json.Marshal(map[string]string{
		"jwt":  string(jwt),
		"role": p.role,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.addr+"/v1/auth/kubernetes/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("vault login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("vault login returned HTTP %d", resp.StatusCode)
	}

	var loginResp struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int    `json:"lease_duration"`
		} `json:"auth"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return "", fmt.Errorf("decode vault login response: %w", err)
	}
	if loginResp.Auth.ClientToken == "" {
		return "", fmt.Errorf("vault login: empty client_token in response")
	}

	p.token = loginResp.Auth.ClientToken
	p.tokenExpiry = time.Now().Add(time.Duration(loginResp.Auth.LeaseDuration) * time.Second)
	return p.token, nil
}

// kvGet reads a KV v2 secret from Vault. On 401/403 it clears the cached
// token so the next call triggers a fresh login.
func (p *VaultProvider) kvGet(ctx context.Context, token, path string) (map[string]string, error) {
	url := fmt.Sprintf("%s/v1/%s/data/%s", p.addr, p.mountPath, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", token)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vault kv get: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// handled below
	case http.StatusNotFound:
		return nil, fmt.Errorf("vault secret not found at path %q", path)
	case http.StatusForbidden, http.StatusUnauthorized:
		// Token may have been revoked — force re-login on next Fetch.
		p.mu.Lock()
		p.token = ""
		p.mu.Unlock()
		return nil, fmt.Errorf("vault kv get: permission denied (path=%q)", path)
	default:
		return nil, fmt.Errorf("vault kv get returned HTTP %d (path=%q)", resp.StatusCode, path)
	}

	// KV v2 response shape: {"data": {"data": {<secret k/v>}, "metadata": {...}}}
	var kvResp struct {
		Data struct {
			Data map[string]interface{} `json:"data"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&kvResp); err != nil {
		return nil, fmt.Errorf("decode vault kv response: %w", err)
	}

	if len(kvResp.Data.Data) == 0 {
		return nil, fmt.Errorf("vault secret is empty at path %q", path)
	}

	props := make(map[string]string, len(kvResp.Data.Data))
	for k, v := range kvResp.Data.Data {
		switch s := v.(type) {
		case string:
			props[k] = s
		default:
			props[k] = fmt.Sprintf("%v", v)
		}
	}
	return props, nil
}
