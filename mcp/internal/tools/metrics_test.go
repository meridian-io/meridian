package tools

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/meridian-io/meridian/mcp/internal/metrics"
)

// TestQueryCacheMetrics: the first lookup is a miss, a repeat is a hit, and
// concurrent duplicates of an in-flight query are counted as shared (or as
// hits if they arrive after it finished). Every lookup is counted once.
func TestQueryCacheMetrics(t *testing.T) {
	f := newFakeTrino(t)
	count := func() (hit, miss, shared float64) {
		return testutil.ToFloat64(metrics.QueryCache.WithLabelValues(metrics.CacheHit)),
			testutil.ToFloat64(metrics.QueryCache.WithLabelValues(metrics.CacheMiss)),
			testutil.ToFloat64(metrics.QueryCache.WithLabelValues(metrics.CacheShared))
	}
	run := func(sql string) {
		if _, err := executeTrinoQueryCached(context.Background(), f.URL(), sql, 0, time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	h0, m0, s0 := count()
	run("SELECT 'metrics-1'")
	run("SELECT 'metrics-1'")
	h1, m1, s1 := count()
	if m1-m0 != 1 || h1-h0 != 1 || s1-s0 != 0 {
		t.Errorf("miss then hit: hit+%v miss+%v shared+%v, want 1/1/0", h1-h0, m1-m0, s1-s0)
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); run("SELECT 'metrics-2'") }()
	}
	wg.Wait()
	h2, m2, s2 := count()
	if m2-m1 != 1 || (h2-h1)+(s2-s1) != 9 {
		t.Errorf("10 concurrent: hit+%v miss+%v shared+%v, want 1 miss and 9 hit or shared", h2-h1, m2-m1, s2-s1)
	}

	// ttl=0 bypasses the cache and is not counted.
	if _, err := executeTrinoQueryCached(context.Background(), f.URL(), "SELECT 'nocache'", 0, 0); err != nil {
		t.Fatal(err)
	}
	h3, m3, s3 := count()
	if h3 != h2 || m3 != m2 || s3 != s2 {
		t.Error("an uncached query (ttl=0) changed the cache counters")
	}
}
