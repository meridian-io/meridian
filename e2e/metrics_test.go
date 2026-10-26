//go:build e2e

package e2e

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// scrape fetches /metrics from every pod of a chart component through the
// API server's pod proxy, so no port-forward is needed.
func (k *kube) scrape(t *testing.T, component, port string) []string {
	t.Helper()
	pods, err := k.core.CoreV1().Pods(k.ns).List(ctx(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=" + component,
	})
	if err != nil || len(pods.Items) == 0 {
		t.Fatalf("list %s pods: %v (%d found)", component, err, len(pods.Items))
	}
	var out []string
	for _, p := range pods.Items {
		if p.DeletionTimestamp != nil || p.Status.Phase != "Running" {
			continue
		}
		body, err := k.core.CoreV1().Pods(k.ns).ProxyGet("http", p.Name, port, "/metrics", nil).DoRaw(ctx())
		if err != nil {
			t.Fatalf("scrape %s: %v", p.Name, err)
		}
		out = append(out, string(body))
	}
	return out
}

var labelRE = regexp.MustCompile(`(\w+)="([^"]*)"`)

// metricSum adds up every sample of name whose labels include want, across
// all scraped pods. found is false when no sample matched.
func metricSum(scrapes []string, name string, want map[string]string) (sum float64, found bool) {
	for _, body := range scrapes {
		for _, line := range strings.Split(body, "\n") {
			if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
				continue
			}
			series, value, ok := strings.Cut(line, " ")
			if !ok {
				continue
			}
			labels := map[string]string{}
			for _, m := range labelRE.FindAllStringSubmatch(series, -1) {
				labels[m[1]] = m[2]
			}
			match := true
			for k, v := range want {
				if labels[k] != v {
					match = false
					break
				}
			}
			if f, err := strconv.ParseFloat(strings.TrimSpace(value), 64); match && err == nil {
				sum += f
				found = true
			}
		}
	}
	return sum, found
}

// Scenario 10: the operator and MCP server export the metrics the dashboard
// uses, with values that reflect what the earlier scenarios did.
func (s *suite) metricsExported(t *testing.T) {
	k := s.k
	s.ready(t)
	pool := map[string]string{"namespace": k.ns, "pool": poolName}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range pool {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	op := k.scrape(t, "operator", "8081")
	checks := []struct {
		name   string
		labels map[string]string
		min    float64
		exact  bool
	}{
		// State: read at scrape time.
		{"meridian_clusters", with(map[string]string{"phase": "Idle"}), float64(poolSize), true},
		{"meridian_clusters", with(map[string]string{"phase": "Failed"}), 0, true},
		{"meridian_pool_desired_replicas", pool, float64(poolSize), true},
		// Events since the upgraded operator started: the scale scenario
		// provisioned and removed clusters, then REST reservations and a rotation.
		{"meridian_cluster_provision_seconds_count", with(nil), 1, false},
		{"meridian_cluster_phase_transitions_total", map[string]string{"from": "Pending", "to": "Idle"}, 1, false},
		{"meridian_pool_clusters_replaced_total", with(map[string]string{"reason": "scale_down"}), 1, false},
		{"meridian_reservation_duration_seconds_count", map[string]string{"source": "rest", "result": "ok"}, 1, false},
		{"meridian_credential_rotations_total", map[string]string{"backend": "kubernetes", "result": "success"}, 1, false},
	}
	for _, c := range checks {
		got, found := metricSum(op, c.name, c.labels)
		switch {
		case !found:
			t.Errorf("operator: %s%v not exported", c.name, c.labels)
		case c.exact && got != c.min:
			t.Errorf("operator: %s%v = %v, want %v", c.name, c.labels, got, c.min)
		case !c.exact && got < c.min:
			t.Errorf("operator: %s%v = %v, want at least %v", c.name, c.labels, got, c.min)
		}
	}

	// The MCP pods restarted in the previous scenario, which resets their
	// counters, so make fresh calls. Calls spread across both replicas, so
	// check totals over all pods.
	cluster := s.ready(t)[0].name
	for range 4 {
		s.mcp.call(t, "list_catalogs", map[string]any{"cluster_name": cluster})
	}
	if _, err := s.mcp.tryCall("get_cluster", map[string]any{"name": "e2e-no-such-cluster"}); err == nil {
		t.Error("get_cluster on a missing cluster succeeded")
	}
	mcp := k.scrape(t, "mcp", "9090")
	for _, c := range []struct {
		name   string
		labels map[string]string
		min    float64
	}{
		{"meridian_mcp_tool_calls_total", map[string]string{"tool": "list_catalogs", "result": "ok"}, 4},
		{"meridian_mcp_tool_calls_total", map[string]string{"tool": "get_cluster", "result": "error"}, 1},
		{"meridian_mcp_tool_duration_seconds_count", map[string]string{"tool": "list_catalogs"}, 4},
		{"meridian_mcp_query_cache_total", nil, 4},
	} {
		got, found := metricSum(mcp, c.name, c.labels)
		if !found || got < c.min {
			t.Errorf("mcp: %s%v = %v (found=%v), want at least %v", c.name, c.labels, got, found, c.min)
		}
	}
	t.Logf("scraped %d operator and %d MCP pods", len(op), len(mcp))
}
