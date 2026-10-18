package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// MinTokenLength is the shortest bearer token accepted for SSE mode.
const MinTokenLength = 16

// LoadToken reads the bearer token from a file (preferred) or an env var.
// Returns "" when neither is set.
func LoadToken(tokenFile, envValue string) (string, error) {
	token := strings.TrimSpace(envValue)
	if tokenFile != "" {
		b, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("read auth token file: %w", err)
		}
		token = strings.TrimSpace(string(b))
	}
	if token != "" && len(token) < MinTokenLength {
		return "", fmt.Errorf("auth token must be at least %d characters", MinTokenLength)
	}
	return token, nil
}

// RequireBearer wraps next so every request must carry "Authorization: Bearer <token>".
// Tokens are compared by hash in constant time so neither content nor length leaks.
func RequireBearer(token string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, got, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		ok = ok && strings.EqualFold(scheme, "Bearer")
		gotSum := sha256.Sum256([]byte(strings.TrimSpace(got)))
		if !ok || subtle.ConstantTimeCompare(want[:], gotSum[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="meridian"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ReadyFunc reports whether the server can do useful work.
type ReadyFunc func(ctx context.Context) error

// NewMux routes health checks (no auth) and MCP traffic (behind auth when a
// token is set).
func NewMux(mcpHandler http.Handler, token string, ready ReadyFunc) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			// Details go to the log, not to unauthenticated callers.
			slog.Warn("readiness check failed", "error", err)
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	h := mcpHandler
	if token != "" {
		h = RequireBearer(token, mcpHandler)
	}
	mux.Handle("/", h)
	return mux
}
