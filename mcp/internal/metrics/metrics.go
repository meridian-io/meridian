// Package metrics defines the MCP server's Prometheus metrics, served on
// --metrics-addr (default :9090 in http mode). Labels carry tool names only,
// never arguments, SQL or client identities.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Tool call results.
const (
	ResultOK    = "ok"
	ResultError = "error" // a tool error (isError result) or a panic
)

// Query cache results.
const (
	CacheHit    = "hit"    // served from the cache
	CacheMiss   = "miss"   // ran against Trino and was cached
	CacheShared = "shared" // waited for an identical query already running
)

var (
	ToolCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_mcp_tool_calls_total",
		Help: "MCP tool calls, by tool and result (ok or error).",
	}, []string{"tool", "result"})

	ToolDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "meridian_mcp_tool_duration_seconds",
		Help: "MCP tool call latency, by tool.",
		// Kubernetes reads take milliseconds; queries can take minutes.
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	}, []string{"tool"})

	QueryCache = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_mcp_query_cache_total",
		Help: "Cacheable query lookups (metadata tools, execute_query with ttl), by result: hit, miss or shared.",
	}, []string{"result"})
)

// Registry holds the MCP metrics plus Go runtime and process metrics.
var Registry = prometheus.NewRegistry()

func init() {
	Registry.MustRegister(
		ToolCalls, ToolDuration, QueryCache,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	for _, r := range []string{CacheHit, CacheMiss, CacheShared} {
		QueryCache.WithLabelValues(r)
	}
}

// Handler serves the metrics in the Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}
