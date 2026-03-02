package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// vaultServer is a test double for the Vault HTTP API.
type vaultServer struct {
	loginCalls atomic.Int32

	// Configurable responses.
	loginStatus  int
	loginToken   string
	loginTTL     int // lease_duration seconds
	kvStatus     int
	kvData       map[string]interface{}
}

func newVaultServer(token string, ttl int, kvData map[string]interface{}) *vaultServer {
	return &vaultServer{
		loginStatus: http.StatusOK,
		loginToken:  token,
		loginTTL:    ttl,
		kvStatus:    http.StatusOK,
		kvData:      kvData,
	}
}

func (vs *vaultServer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/auth/kubernetes/login", func(w http.ResponseWriter, r *http.Request) {
		vs.loginCalls.Add(1)
		if vs.loginStatus != http.StatusOK {
			http.Error(w, "login error", vs.loginStatus)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"auth": map[string]interface{}{
				"client_token":   vs.loginToken,
				"lease_duration": vs.loginTTL,
			},
		})
	})

	mux.HandleFunc("/v1/secret/data/", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Vault-Token"); got != vs.loginToken {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if vs.kvStatus != http.StatusOK {
			http.Error(w, "kv error", vs.kvStatus)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{
				"data":     vs.kvData,
				"metadata": map[string]interface{}{"version": 1},
			},
		})
	})

	return mux
}

// newTestVaultProvider creates a VaultProvider pointed at srv with a temp JWT file.
func newTestVaultProvider(t *testing.T, srv *httptest.Server) *VaultProvider {
	t.Helper()
	jwtPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(jwtPath, []byte("test-jwt"), 0600); err != nil {
		t.Fatal(err)
	}
	p := NewVaultProvider(srv.URL, "meridian-operator", "secret")
	p.tokenPath = jwtPath
	return p
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestVaultProvider_FetchSuccess verifies a successful login + KV read.
func TestVaultProvider_FetchSuccess(t *testing.T) {
	kvData := map[string]interface{}{
		"connector.name":      "mysql",
		"connection-url":      "jdbc:mysql://mysql:3306",
		"connection-user":     "root",
		"connection-password": "secret",
	}
	vs := newVaultServer("tok-abc", 3600, kvData)
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := newTestVaultProvider(t, srv)
	secret, err := p.Fetch(context.Background(), "trino/mysql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if secret.Properties["connector.name"] != "mysql" {
		t.Errorf("got connector.name=%q, want mysql", secret.Properties["connector.name"])
	}
	if secret.Properties["connection-password"] != "secret" {
		t.Errorf("got connection-password=%q, want secret", secret.Properties["connection-password"])
	}
	if secret.FetchedAt.IsZero() {
		t.Error("FetchedAt should not be zero")
	}
}

// TestVaultProvider_TokenCached verifies that login is called only once across
// multiple Fetch calls when the token has not expired.
func TestVaultProvider_TokenCached(t *testing.T) {
	vs := newVaultServer("tok-xyz", 3600, map[string]interface{}{
		"connector.name": "tpch",
	})
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := newTestVaultProvider(t, srv)

	for i := 0; i < 5; i++ {
		if _, err := p.Fetch(context.Background(), "trino/tpch"); err != nil {
			t.Fatalf("Fetch #%d: %v", i+1, err)
		}
	}

	if n := vs.loginCalls.Load(); n != 1 {
		t.Errorf("expected 1 login call, got %d", n)
	}
}

// TestVaultProvider_TokenExpiry verifies re-login when the token has expired.
func TestVaultProvider_TokenExpiry(t *testing.T) {
	vs := newVaultServer("tok-exp", 3600, map[string]interface{}{
		"connector.name": "mysql",
	})
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := newTestVaultProvider(t, srv)

	// First fetch — logs in.
	if _, err := p.Fetch(context.Background(), "trino/mysql"); err != nil {
		t.Fatalf("first fetch: %v", err)
	}

	// Simulate an already-expired token by backdating expiry past the buffer.
	p.mu.Lock()
	p.tokenExpiry = time.Now().Add(-(tokenRenewBuffer + time.Second))
	p.mu.Unlock()

	// Second fetch — should re-login.
	if _, err := p.Fetch(context.Background(), "trino/mysql"); err != nil {
		t.Fatalf("second fetch: %v", err)
	}

	if n := vs.loginCalls.Load(); n != 2 {
		t.Errorf("expected 2 login calls after expiry, got %d", n)
	}
}

// TestVaultProvider_LoginFailure verifies error propagation when Vault rejects auth.
func TestVaultProvider_LoginFailure(t *testing.T) {
	vs := newVaultServer("", 0, nil)
	vs.loginStatus = http.StatusForbidden
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := newTestVaultProvider(t, srv)
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error on login failure")
	}
}

// TestVaultProvider_SecretNotFound verifies the 404 path.
func TestVaultProvider_SecretNotFound(t *testing.T) {
	vs := newVaultServer("tok-ok", 3600, nil)
	vs.kvStatus = http.StatusNotFound
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := newTestVaultProvider(t, srv)
	_, err := p.Fetch(context.Background(), "trino/missing")
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
}

// TestVaultProvider_PermissionDenied verifies that a 403 on KV read clears
// the cached token so the next call triggers a fresh login.
func TestVaultProvider_PermissionDenied(t *testing.T) {
	// Serve two different tokens: first login → "bad-tok", second → "good-tok".
	// The handler returns 403 for "bad-tok" and 200 for "good-tok".
	callCount := 0
	tokens := []string{"bad-tok", "good-tok"}

	kvData := map[string]interface{}{"connector.name": "mysql"}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/auth/kubernetes/login" {
			tok := tokens[callCount%len(tokens)]
			callCount++
			json.NewEncoder(w).Encode(map[string]interface{}{
				"auth": map[string]interface{}{
					"client_token":   tok,
					"lease_duration": 3600,
				},
			})
			return
		}
		// KV read
		if r.Header.Get("X-Vault-Token") != "good-tok" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]interface{}{"data": kvData},
		})
	}))
	defer srv.Close()

	jwtPath := filepath.Join(t.TempDir(), "token")
	os.WriteFile(jwtPath, []byte("test-jwt"), 0600)
	p := NewVaultProvider(srv.URL, "meridian-operator", "secret")
	p.tokenPath = jwtPath

	// First call gets bad-tok, 403 on KV → clears token, returns error.
	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected permission denied error on first fetch")
	}

	// Token should be cleared now — next call should re-login with good-tok.
	secret, err := p.Fetch(context.Background(), "trino/mysql")
	if err != nil {
		t.Fatalf("expected success after re-login, got: %v", err)
	}
	if secret.Properties["connector.name"] != "mysql" {
		t.Errorf("unexpected properties: %v", secret.Properties)
	}
}

// TestVaultProvider_MissingJWTFile verifies an error when the SA token is absent.
func TestVaultProvider_MissingJWTFile(t *testing.T) {
	vs := newVaultServer("tok", 3600, nil)
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := NewVaultProvider(srv.URL, "meridian-operator", "secret")
	p.tokenPath = "/nonexistent/path/token"

	_, err := p.Fetch(context.Background(), "trino/mysql")
	if err == nil {
		t.Fatal("expected error when JWT file is missing")
	}
}

// TestVaultProvider_EmptySecret verifies an error when the KV data is empty.
func TestVaultProvider_EmptySecret(t *testing.T) {
	vs := newVaultServer("tok", 3600, map[string]interface{}{})
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := newTestVaultProvider(t, srv)
	_, err := p.Fetch(context.Background(), "trino/empty")
	if err == nil {
		t.Fatal("expected error for empty secret")
	}
}

// TestVaultProvider_ContextCancelled verifies that a cancelled context stops the fetch.
func TestVaultProvider_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	jwtPath := filepath.Join(t.TempDir(), "token")
	os.WriteFile(jwtPath, []byte("test-jwt"), 0600)
	p := NewVaultProvider(srv.URL, "meridian-operator", "secret")
	p.tokenPath = jwtPath

	_, err := p.Fetch(ctx, "trino/mysql")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// TestVaultProvider_NonStringValues verifies that non-string JSON values are
// coerced to strings rather than panicking.
func TestVaultProvider_NonStringValues(t *testing.T) {
	kvData := map[string]interface{}{
		"connector.name": "mysql",
		"port":           float64(3306), // JSON numbers decode as float64
	}
	vs := newVaultServer("tok", 3600, kvData)
	srv := httptest.NewServer(vs.handler())
	defer srv.Close()

	p := newTestVaultProvider(t, srv)
	secret, err := p.Fetch(context.Background(), "trino/mysql")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if secret.Properties["port"] != fmt.Sprintf("%v", float64(3306)) {
		t.Errorf("port not coerced to string: %q", secret.Properties["port"])
	}
}

// TestVaultProvider_Name verifies the canonical provider name.
func TestVaultProvider_Name(t *testing.T) {
	p := NewVaultProvider("https://vault.example.com", "role", "secret")
	if p.Name() != "vault" {
		t.Errorf("expected Name()=vault, got %q", p.Name())
	}
}

// TestVaultProvider_ImplementsInterface ensures VaultProvider satisfies SecretProvider.
func TestVaultProvider_ImplementsInterface(t *testing.T) {
	var _ SecretProvider = (*VaultProvider)(nil)
}
