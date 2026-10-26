package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mark3labs/mcp-go/server"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
	mcpserver "github.com/meridian-io/meridian/mcp/internal/server"
	"github.com/meridian-io/meridian/mcp/internal/tools"
)

var version = "dev" // overridden by GoReleaser at build time

const shutdownTimeout = 30 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("meridian exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		transport       string
		addr            string
		baseURL         string
		namespace       string
		kubeconfig      string
		enableTools     string
		allowWrites     bool
		outputDir       string
		authTokenFile   string
		insecureNoAuth  bool
		cacheMaxEntries int
		logFormat       string
		showVer         bool
		legacySSE       bool
		allowedOrigins  string
		noLocalhostProt bool
		shutdownDelay   time.Duration
	)

	flag.StringVar(&transport, "transport", "stdio", "Transport: stdio, or http for remote clients (Streamable HTTP at /mcp)")
	flag.StringVar(&addr, "addr", ":8080", "Address to listen on when transport=http")
	// Removed in v1.5 with the HTTP+SSE transport. Still accepted so that
	// deployments which set --legacy-sse=false or --base-url keep starting.
	flag.BoolVar(&legacySSE, "legacy-sse", false, "Removed in v1.5 (the HTTP+SSE transport is gone). Only false is accepted")
	flag.StringVar(&allowedOrigins, "allowed-origins", "", "Comma-separated origins accepted when a request carries an Origin header, e.g. a web UI behind the same proxy (https://app.example.com). Any other Origin gets 403. Not CORS: cross-origin browser calls are not supported")
	flag.DurationVar(&shutdownDelay, "shutdown-delay", 5*time.Second, "In http mode, keep serving this long after SIGTERM (reporting not ready) so load balancers stop sending new requests before the server closes")
	flag.BoolVar(&noLocalhostProt, "disable-localhost-protection", false, "Accept loopback connections whose Host header isn't localhost (only for a reverse proxy on the same host)")
	flag.StringVar(&baseURL, "base-url", "", "Removed in v1.5 (only used by the HTTP+SSE transport). Ignored")
	flag.StringVar(&namespace, "namespace", "meridian", "Default Kubernetes namespace")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig (defaults to in-cluster, then ~/.kube/config)")
	flag.StringVar(&enableTools, "enable-tools", "", "Comma-separated held tools to enable, or \"all\". Held tools: create_cluster, delete_cluster, scale_pool, add_catalog, remove_catalog, rotate_credentials, promote_environment")
	flag.BoolVar(&allowWrites, "allow-writes", false, "Allow execute_query to run statements that change data (INSERT, DROP, ...). Off by default.")
	flag.StringVar(&outputDir, "output-dir", tools.DefaultOutputDir(), "Directory execute_query may write CSV files to (stdio mode only). Empty disables file output.")
	flag.StringVar(&authTokenFile, "auth-token-file", "", "File containing the bearer token required in http mode (or set MERIDIAN_MCP_TOKEN)")
	flag.BoolVar(&insecureNoAuth, "insecure-no-auth", false, "Allow http mode without a token. Only for local testing.")
	flag.IntVar(&cacheMaxEntries, "cache-max-entries", tools.DefaultCacheMaxEntries, "Maximum cached query results")
	flag.StringVar(&logFormat, "log-format", "", "Log format: json or text (default: json for http, text for stdio)")
	flag.BoolVar(&showVer, "version", false, "Print version and exit")
	flag.Parse()

	if showVer {
		fmt.Printf("meridian %s\n", version)
		return nil
	}

	if transport == "sse" {
		return errors.New("--transport sse was removed in v1.5: use --transport http (Streamable HTTP at /mcp). Clients connect to http(s)://<host>/mcp")
	}
	if legacySSE {
		return errors.New("--legacy-sse was removed in v1.5: the HTTP+SSE transport (/sse) is gone. Remove the flag and point clients at /mcp")
	}
	setupLogging(logFormat, transport)
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "legacy-sse" || f.Name == "base-url" {
			slog.Warn("flag has no effect since v1.5 and will be rejected in a future release; remove it", "flag", "--"+f.Name)
		}
	})

	extra, err := mcpserver.ParseEnableTools(enableTools)
	if err != nil {
		return err
	}

	queryOpts := tools.QueryOptions{AllowWrites: allowWrites, OutputDir: outputDir}
	if transport == "http" {
		// Remote callers must never write files onto this server's disk.
		queryOpts.OutputDir = ""
	}
	tools.SetCacheMaxEntries(cacheMaxEntries)

	var token string
	if transport == "http" {
		token, err = mcpserver.LoadToken(authTokenFile, os.Getenv("MERIDIAN_MCP_TOKEN"))
		if err != nil {
			return err
		}
		if token == "" && !insecureNoAuth {
			return errors.New("http mode requires a bearer token: set MERIDIAN_MCP_TOKEN or --auth-token-file (use --insecure-no-auth only for local testing)")
		}
	}

	k8sClient, err := k8s.NewClient(kubeconfig, namespace)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}

	s, registered := mcpserver.New(k8sClient, version, mcpserver.Options{ExtraTools: extra, Query: queryOpts})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal, restore default handling so a second Ctrl-C
	// quits immediately instead of waiting for the drain and shutdown.
	go func() {
		<-ctx.Done()
		stop()
	}()

	slog.Info("meridian starting",
		"version", version,
		"transport", transport,
		"namespace", namespace,
		"operator", k8sClient.HasMeridianOperator(),
		"tools", len(registered),
		"allow_writes", allowWrites,
	)

	switch transport {
	case "stdio":
		stdio := server.NewStdioServer(s)
		stdio.SetErrorLogger(log.New(os.Stderr, "", log.LstdFlags))
		err := stdio.Listen(ctx, os.Stdin, os.Stdout)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil

	case "http":
		if token == "" {
			slog.Warn("authentication is DISABLED (--insecure-no-auth) — anyone who can reach this port can use every enabled tool")
		}
		return serveHTTP(ctx, s, httpOptions{
			addr:            addr,
			token:           token,
			allowedOrigins:  splitList(allowedOrigins),
			noLocalhostProt: noLocalhostProt,
			ready:           k8sClient.Ready,
			shutdownDelay:   shutdownDelay,
		})

	default:
		return fmt.Errorf("unknown transport %q — use stdio or http", transport)
	}
}

type httpOptions struct {
	addr            string
	token           string
	allowedOrigins  []string
	noLocalhostProt bool
	ready           mcpserver.ReadyFunc
	shutdownDelay   time.Duration
}

// serveHTTP serves Streamable HTTP at /mcp (stateless, so any replica can
// answer any request).
func serveHTTP(ctx context.Context, s *server.MCPServer, o httpOptions) error {
	httpSrv := &http.Server{
		Addr:              o.addr,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: responses may stream while a long query runs.
	}

	// While draining, report not ready so no new traffic is routed here.
	var draining atomic.Bool
	ready := func(ctx context.Context) error {
		if draining.Load() {
			return errors.New("shutting down")
		}
		return o.ready(ctx)
	}
	httpSrv.Handler = mcpserver.NewHTTPHandler(s, mcpserver.HTTPOptions{
		Token:                      o.token,
		AllowedOrigins:             o.allowedOrigins,
		DisableLocalhostProtection: o.noLocalhostProt,
		Ready:                      ready,
	})

	errCh := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", o.addr, "endpoint", "/mcp")
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Kubernetes keeps routing to a terminating pod for a few seconds after
	// SIGTERM. Keep serving during that window instead of refusing connections.
	if o.shutdownDelay > 0 {
		draining.Store(true)
		slog.Info("draining before shutdown", "delay", o.shutdownDelay)
		select {
		case err := <-errCh:
			return err
		case <-time.After(o.shutdownDelay):
		}
	}
	slog.Info("shutting down", "timeout", shutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// setupLogging sends structured logs to stderr. stdout is reserved for the
// MCP protocol in stdio mode.
func setupLogging(format, transport string) {
	if format == "" {
		format = "text"
		if transport == "http" {
			format = "json"
		}
	}
	var h slog.Handler
	if format == "json" {
		h = slog.NewJSONHandler(os.Stderr, nil)
	} else {
		h = slog.NewTextHandler(os.Stderr, nil)
	}
	slog.SetDefault(slog.New(h))
}
