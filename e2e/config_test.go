//go:build e2e

// Package e2e runs Meridian on a real Kubernetes cluster (kind) with real
// Trino and checks it the way a user would: Helm install and upgrade, pools,
// the MCP server, the REST API, recovery and credential rotation.
//
// Run it with `make e2e`, which creates the kind cluster, builds and loads the
// images, and then runs `go test -tags e2e`. See hack/e2e/run.sh.
package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// config comes from E2E_* environment variables, set by hack/e2e/run.sh.
type config struct {
	kubeContext     string
	namespace       string
	operatorImage   string // image built from this checkout
	mcpImage        string
	previousVersion string // chart version to install first and upgrade from; empty = fresh install
	trinoImage      string
	mcpURL          string
	restURL         string
	repoRoot        string
}

var cfg config

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() config {
	_, file, _, _ := runtime.Caller(0)
	return config{
		kubeContext:     env("E2E_KUBE_CONTEXT", "kind-meridian-e2e"),
		namespace:       env("E2E_NAMESPACE", "meridian"),
		operatorImage:   env("E2E_OPERATOR_IMAGE", "ghcr.io/meridian-io/trino-operator:e2e"),
		mcpImage:        env("E2E_MCP_IMAGE", "ghcr.io/meridian-io/trino-mcp-server:e2e"),
		previousVersion: os.Getenv("E2E_PREVIOUS_VERSION"),
		trinoImage:      env("E2E_TRINO_IMAGE", "trinodb/trino:435"),
		mcpURL:          env("E2E_MCP_URL", "http://127.0.0.1:30080"),
		restURL:         env("E2E_REST_URL", "https://127.0.0.1:30443"),
		repoRoot:        filepath.Dir(filepath.Dir(file)),
	}
}

// splitImage splits "repo:tag" into its parts.
func splitImage(img string) (string, string) {
	i := strings.LastIndex(img, ":")
	if i < 0 || strings.Contains(img[i:], "/") {
		return img, "latest"
	}
	return img[:i], img[i+1:]
}

// Test sizing. A pool cluster is one coordinator and one worker running the
// e2e profile (1G heap, 1536Mi pods), so the suite fits a 16 GB CI runner.
const (
	poolName    = "e2e"
	profileName = "e2e"
	podMemory   = "1536Mi"
	podCPU      = "250m"

	// Pulling and starting Trino is the slow part of every scenario.
	clusterReadyTimeout = 8 * time.Minute
)

// poolSize is the pool's normal size: 2 in CI. E2E_POOL_SIZE=1 fits a laptop
// with less memory; the scale scenario then goes 1 → 2 → 1.
var poolSize = 2

func TestMain(m *testing.M) {
	cfg = loadConfig()
	if v := os.Getenv("E2E_POOL_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "E2E_POOL_SIZE must be a positive integer, got %q\n", v)
			os.Exit(2)
		}
		poolSize = n
	}
	os.Exit(m.Run())
}
