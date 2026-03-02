package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/mark3labs/mcp-go/server"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
	mcpserver "github.com/meridian-io/meridian/mcp/internal/server"
)

var version = "dev" // overridden by GoReleaser at build time

func main() {
	var (
		transport  string
		addr       string
		namespace  string
		kubeconfig string
		showVer    bool
	)

	flag.StringVar(&transport, "transport", "stdio", "Transport: stdio or sse")
	flag.StringVar(&addr, "addr", ":8080", "Address to listen on when transport=sse")
	flag.StringVar(&namespace, "namespace", "meridian", "Default Kubernetes namespace")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig (defaults to in-cluster, then ~/.kube/config)")
	flag.BoolVar(&showVer, "version", false, "Print version and exit")
	flag.Parse()

	if showVer {
		fmt.Printf("meridian-mcp %s\n", version)
		os.Exit(0)
	}

	k8sClient, err := k8s.NewClient(kubeconfig, namespace)
	if err != nil {
		log.Fatalf("failed to create Kubernetes client: %v", err)
	}

	s := mcpserver.New(k8sClient, version)

	switch transport {
	case "stdio":
		log.Printf("meridian-mcp %s starting (stdio, namespace=%s, meridian-operator=%v)",
			version, namespace, k8sClient.HasMeridianOperator())
		if err := server.ServeStdio(s); err != nil {
			log.Fatalf("server error: %v", err)
		}
	case "sse":
		log.Printf("meridian-mcp %s starting (sse %s, namespace=%s, meridian-operator=%v)",
			version, addr, namespace, k8sClient.HasMeridianOperator())
		sseServer := server.NewSSEServer(s, server.WithBaseURL("http://localhost"+addr))
		if err := sseServer.Start(addr); err != nil {
			log.Fatalf("server error: %v", err)
		}
	default:
		log.Fatalf("unknown transport %q — use stdio or sse", transport)
	}
}
