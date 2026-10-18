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
	)

	flag.StringVar(&transport, "transport", "stdio", "Transport: stdio or sse")
	flag.StringVar(&addr, "addr", ":8080", "Address to listen on when transport=sse")
	flag.StringVar(&baseURL, "base-url", "", "Public URL clients use to reach this server (e.g. https://meridian.example.com). Empty sends a relative message endpoint, which works behind most proxies.")
	flag.StringVar(&namespace, "namespace", "meridian", "Default Kubernetes namespace")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig (defaults to in-cluster, then ~/.kube/config)")
	flag.StringVar(&enableTools, "enable-tools", "", "Comma-separated held tools to enable, or \"all\". Held tools: create_cluster, delete_cluster, scale_pool, add_catalog, remove_catalog, rotate_credentials, promote_environment")
	flag.BoolVar(&allowWrites, "allow-writes", false, "Allow execute_query to run statements that change data (INSERT, DROP, ...). Off by default.")
	flag.StringVar(&outputDir, "output-dir", tools.DefaultOutputDir(), "Directory execute_query may write CSV files to (stdio mode only). Empty disables file output.")
	flag.StringVar(&authTokenFile, "auth-token-file", "", "File containing the bearer token required in SSE mode (or set MERIDIAN_MCP_TOKEN)")
	flag.BoolVar(&insecureNoAuth, "insecure-no-auth", false, "Allow SSE mode without a token. Only for local testing.")
	flag.IntVar(&cacheMaxEntries, "cache-max-entries", tools.DefaultCacheMaxEntries, "Maximum cached query results")
	flag.StringVar(&logFormat, "log-format", "", "Log format: json or text (default: json for sse, text for stdio)")
	flag.BoolVar(&showVer, "version", false, "Print version and exit")
	flag.Parse()

	if showVer {
		fmt.Printf("meridian %s\n", version)
		return nil
	}

	setupLogging(logFormat, transport)

	extra, err := mcpserver.ParseEnableTools(enableTools)
	if err != nil {
		return err
	}

	queryOpts := tools.QueryOptions{AllowWrites: allowWrites, OutputDir: outputDir}
	if transport == "sse" {
		// Remote callers must never write files onto this server's disk.
		queryOpts.OutputDir = ""
	}
	tools.SetCacheMaxEntries(cacheMaxEntries)

	var token string
	if transport == "sse" {
		token, err = mcpserver.LoadToken(authTokenFile, os.Getenv("MERIDIAN_MCP_TOKEN"))
		if err != nil {
			return err
		}
		if token == "" && !insecureNoAuth {
			return errors.New("SSE mode requires a bearer token: set MERIDIAN_MCP_TOKEN or --auth-token-file (use --insecure-no-auth only for local testing)")
		}
	}

	k8sClient, err := k8s.NewClient(kubeconfig, namespace)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}

	s, registered := mcpserver.New(k8sClient, version, mcpserver.Options{ExtraTools: extra, Query: queryOpts})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	case "sse":
		if token == "" {
			slog.Warn("SSE authentication is DISABLED (--insecure-no-auth) — anyone who can reach this port can use every enabled tool")
		}
		return serveSSE(ctx, s, addr, baseURL, token, k8sClient)

	default:
		return fmt.Errorf("unknown transport %q — use stdio or sse", transport)
	}
}

func serveSSE(ctx context.Context, s *server.MCPServer, addr, baseURL, token string, k8sClient *k8s.Client) error {
	httpSrv := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: SSE responses are long-lived streams.
	}

	sseOpts := []server.SSEOption{
		server.WithHTTPServer(httpSrv),
		server.WithKeepAlive(true),
	}
	if baseURL != "" {
		sseOpts = append(sseOpts, server.WithBaseURL(baseURL))
	} else {
		sseOpts = append(sseOpts, server.WithUseFullURLForMessageEndpoint(false))
	}
	sse := server.NewSSEServer(s, sseOpts...)
	httpSrv.Handler = mcpserver.NewMux(sse, token, k8sClient.Ready)

	errCh := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", addr)
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

	slog.Info("shutting down", "timeout", shutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := sse.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// setupLogging sends structured logs to stderr. stdout is reserved for the
// MCP protocol in stdio mode.
func setupLogging(format, transport string) {
	if format == "" {
		format = "text"
		if transport == "sse" {
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
