package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/server"
)

// MinTokenLength is the shortest bearer token accepted in http mode.
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

// DefaultMaxBodyBytes caps the size of one MCP request body.
const DefaultMaxBodyBytes = 1 << 20 // 1 MiB

// MuxOptions configures the HTTP routes for remote mode.
type MuxOptions struct {
	// MCP serves the Streamable HTTP endpoint (/mcp).
	MCP http.Handler
	// LegacySSE and LegacyMessage serve the deprecated HTTP+SSE transport
	// (/sse and /message). Both nil disables it.
	LegacySSE     http.Handler
	LegacyMessage http.Handler
	// Token is the bearer token required on every MCP route. Empty disables auth.
	Token string
	// AllowedOrigins lists origins (e.g. https://app.example.com) whose
	// requests are accepted when they carry an Origin header, such as a web UI
	// served behind the same proxy. Any other Origin gets 403. Requests
	// without an Origin header (non-browser clients) are allowed. This is not
	// CORS: cross-origin browser calls are not supported.
	AllowedOrigins []string
	// MaxBodyBytes limits request bodies; 0 means DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// Ready backs /readyz.
	Ready ReadyFunc
}

// NewMux routes health checks (no auth) and MCP traffic. Every MCP route runs
// through the same checks: body size, Origin, then bearer token.
func NewMux(o MuxOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := o.Ready(ctx); err != nil {
			// Details go to the log, not to unauthenticated callers.
			slog.Warn("readiness check failed", "error", err)
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	maxBody := o.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = DefaultMaxBodyBytes
	}
	protect := func(h http.Handler) http.Handler {
		if o.Token != "" {
			h = RequireBearer(o.Token, h)
		}
		return LimitBody(maxBody, CheckOrigin(o.AllowedOrigins, h))
	}

	mux.Handle("/mcp", protect(o.MCP))
	if o.LegacySSE != nil && o.LegacyMessage != nil {
		mux.Handle("/sse", protect(o.LegacySSE))
		mux.Handle("/message", protect(o.LegacyMessage))
	}
	return mux
}

// CheckOrigin rejects requests whose Origin header is present but not in
// allowed, as the MCP spec requires to prevent DNS rebinding attacks.
func CheckOrigin(allowed []string, next http.Handler) http.Handler {
	ok := map[string]bool{}
	for _, o := range allowed {
		if o = strings.TrimRight(strings.TrimSpace(o), "/"); o != "" {
			ok[strings.ToLower(o)] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && !ok[strings.ToLower(origin)] {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// LimitBody rejects request bodies larger than max with 413, whether or not
// the client sent Content-Length. The body is read here (at most max bytes)
// and handed on in memory.
func LimitBody(max int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > max {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		if r.Body != nil && r.Body != http.NoBody {
			b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, max))
			if err != nil {
				var tooBig *http.MaxBytesError
				if errors.As(err, &tooBig) {
					http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
					return
				}
				http.Error(w, "could not read request body", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(b))
		}
		next.ServeHTTP(w, r)
	})
}

// HTTPOptions configures remote (http) mode.
type HTTPOptions struct {
	Token          string
	AllowedOrigins []string
	// LegacySSE also serves the deprecated HTTP+SSE transport.
	LegacySSE bool
	// BaseURL is the public URL for legacy SSE message endpoints. Empty sends
	// a relative endpoint.
	BaseURL                    string
	DisableLocalhostProtection bool
	Ready                      ReadyFunc
	// HTTPServer is shut down by the legacy SSE server, which closes its
	// long-lived streams first.
	HTTPServer *http.Server
}

// NewHTTPHandler builds every route for http mode: stateless Streamable HTTP
// at /mcp (any replica can answer any request), optional legacy HTTP+SSE,
// and health checks. The returned SSE server is nil when legacy SSE is off.
func NewHTTPHandler(s *server.MCPServer, o HTTPOptions) (http.Handler, *server.SSEServer) {
	streamable := server.NewStreamableHTTPServer(s,
		server.WithStateLess(true),
		server.WithDisableLocalhostProtection(o.DisableLocalhostProtection),
		server.WithStreamableHTTPLogger(slog.Default()),
	)
	mux := MuxOptions{
		MCP:            streamable,
		Token:          o.Token,
		AllowedOrigins: o.AllowedOrigins,
		Ready:          o.Ready,
	}

	var sse *server.SSEServer
	if o.LegacySSE {
		sseOpts := []server.SSEOption{
			server.WithKeepAlive(true),
			server.WithSSEDisableLocalhostProtection(o.DisableLocalhostProtection),
		}
		if o.HTTPServer != nil {
			sseOpts = append(sseOpts, server.WithHTTPServer(o.HTTPServer))
		}
		if o.BaseURL != "" {
			sseOpts = append(sseOpts, server.WithBaseURL(o.BaseURL))
		} else {
			sseOpts = append(sseOpts, server.WithUseFullURLForMessageEndpoint(false))
		}
		sse = server.NewSSEServer(s, sseOpts...)
		mux.LegacySSE = sse.SSEHandler()
		mux.LegacyMessage = sse.MessageHandler()
	}
	return NewMux(mux), sse
}
