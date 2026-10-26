package tools

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/meridian-io/meridian/mcp/internal/metrics"
)

// DefaultCacheMaxEntries caps the query cache unless SetCacheMaxEntries is called.
const DefaultCacheMaxEntries = 1000

// queryCache is a thread-safe in-memory TTL cache for Trino query results.
// It includes a singleflight layer: if two identical queries arrive simultaneously,
// only one hits Trino — the second waits and is served from cache.
type queryCache struct {
	mu         sync.RWMutex
	entries    map[string]*cacheEntry
	inflight   map[string]*inflightCall
	maxEntries int
	// generations counts invalidations per coordinator prefix. A query that
	// started before an invalidation must not store its (stale) result.
	generations map[string]uint64
}

type cacheEntry struct {
	result    *trinoQueryResult
	expiresAt time.Time
}

// inflightCall represents a query that is currently executing.
// Concurrent callers for the same key wait on wg and then read from cache.
type inflightCall struct {
	done chan struct{}
	err  error
}

var globalCache = newQueryCache(DefaultCacheMaxEntries)

// SetCacheMaxEntries changes the cache size limit. Call once at startup.
func SetCacheMaxEntries(n int) {
	if n <= 0 {
		n = DefaultCacheMaxEntries
	}
	globalCache.mu.Lock()
	globalCache.maxEntries = n
	globalCache.mu.Unlock()
}

func newQueryCache(maxEntries int) *queryCache {
	c := &queryCache{
		entries:     make(map[string]*cacheEntry),
		inflight:    make(map[string]*inflightCall),
		maxEntries:  maxEntries,
		generations: make(map[string]uint64),
	}
	go c.cleanup()
	return c
}

func (c *queryCache) get(key string) (*trinoQueryResult, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.result, true
}

func (c *queryCache) set(key string, result *trinoQueryResult, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLocked(key, result, ttl)
}

// setIfGeneration stores the result only if no invalidation happened since gen was read.
func (c *queryCache) setIfGeneration(key, prefix string, gen uint64, result *trinoQueryResult, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generations[prefix] != gen {
		return
	}
	c.setLocked(key, result, ttl)
}

func (c *queryCache) setLocked(key string, result *trinoQueryResult, ttl time.Duration) {
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.maxEntries {
		c.evictLocked()
	}
	c.entries[key] = &cacheEntry{result: result, expiresAt: time.Now().Add(ttl)}
}

// evictLocked frees space: first drop expired entries, then the entry that
// expires soonest. Caller holds c.mu.
func (c *queryCache) evictLocked() {
	now := time.Now()
	for k, e := range c.entries {
		if now.After(e.expiresAt) {
			delete(c.entries, k)
		}
	}
	for len(c.entries) >= c.maxEntries {
		var oldestKey string
		var oldest time.Time
		for k, e := range c.entries {
			if oldestKey == "" || e.expiresAt.Before(oldest) {
				oldestKey, oldest = k, e.expiresAt
			}
		}
		delete(c.entries, oldestKey)
	}
}

func (c *queryCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// invalidatePrefix removes all entries whose key starts with prefix.
// Called when add_catalog / remove_catalog succeeds to bust SHOW CATALOGS cache.
func (c *queryCache) invalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generations[prefix]++
	for key := range c.entries {
		if strings.HasPrefix(key, prefix) {
			delete(c.entries, key)
		}
	}
}

// cleanup sweeps expired entries every minute.
func (c *queryCache) cleanup() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		c.mu.Lock()
		for k, e := range c.entries {
			if now.After(e.expiresAt) {
				delete(c.entries, k)
			}
		}
		c.mu.Unlock()
	}
}

// cacheKey identifies a cached result. It starts with the coordinator prefix so
// all of a coordinator's entries can be invalidated together, and includes
// maxRows so a truncated result is never served for a larger request.
func cacheKey(coordinatorURL, sql string, maxRows int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", coordinatorURL, maxRows, sql)))
	return coordinatorPrefix(coordinatorURL) + ":" + fmt.Sprintf("%x", h)
}

// coordinatorPrefix returns a 16-char prefix that identifies all cache entries
// belonging to a coordinator — used to invalidate on catalog changes.
func coordinatorPrefix(coordinatorURL string) string {
	h := sha256.Sum256([]byte(coordinatorURL + "\x00"))
	return fmt.Sprintf("%x", h)[:16]
}

// executeTrinoQueryCached wraps executeTrinoQuery with TTL caching and singleflight.
//
// If two identical queries arrive at the same time:
//   - The first caller executes the query and stores the result.
//   - The second caller waits for the first to finish, then reads from cache.
//
// A waiter stops waiting when its own context ends, and runs the query itself
// if the first caller failed only because its context was cancelled.
// ttl=0 bypasses the cache entirely (always hits Trino directly).
func executeTrinoQueryCached(ctx context.Context, coordinatorURL, sql string, maxRows int, ttl time.Duration) (*trinoQueryResult, error) {
	if ttl <= 0 {
		return executeTrinoQuery(ctx, coordinatorURL, sql, maxRows)
	}

	key := cacheKey(coordinatorURL, sql, maxRows)
	prefix := coordinatorPrefix(coordinatorURL)

	// Fast path: already cached.
	if cached, ok := globalCache.get(key); ok {
		metrics.QueryCache.WithLabelValues(metrics.CacheHit).Inc()
		return markCached(cached), nil
	}

	// Singleflight: check if an identical query is already in-flight.
	globalCache.mu.Lock()
	if call, ok := globalCache.inflight[key]; ok {
		globalCache.mu.Unlock()
		select {
		case <-call.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if call.err != nil {
			if errors.Is(call.err, context.Canceled) || errors.Is(call.err, context.DeadlineExceeded) {
				return executeTrinoQuery(ctx, coordinatorURL, sql, maxRows)
			}
			return nil, call.err
		}
		if cached, ok := globalCache.get(key); ok {
			metrics.QueryCache.WithLabelValues(metrics.CacheShared).Inc()
			return markCached(cached), nil
		}
		// Invalidated between the wait and the read.
		return executeTrinoQuery(ctx, coordinatorURL, sql, maxRows)
	}

	// This goroutine is first — register the in-flight call.
	call := &inflightCall{done: make(chan struct{})}
	globalCache.inflight[key] = call
	gen := globalCache.generations[prefix]
	globalCache.mu.Unlock()
	metrics.QueryCache.WithLabelValues(metrics.CacheMiss).Inc()

	result, err := executeTrinoQuery(ctx, coordinatorURL, sql, maxRows)
	if err == nil {
		globalCache.setIfGeneration(key, prefix, gen, result, ttl)
	}

	globalCache.mu.Lock()
	delete(globalCache.inflight, key)
	globalCache.mu.Unlock()

	call.err = err
	close(call.done)

	if err != nil {
		return nil, err
	}
	return result, nil
}

func markCached(r *trinoQueryResult) *trinoQueryResult {
	clone := *r
	clone.FromCache = true
	return &clone
}
