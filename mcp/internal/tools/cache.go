package tools

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"
)

// queryCache is a thread-safe in-memory TTL cache for Trino query results.
// It includes a singleflight layer: if two identical queries arrive simultaneously,
// only one hits Trino — the second waits and is served from cache.
type queryCache struct {
	mu       sync.RWMutex
	entries  map[string]*cacheEntry
	inflight map[string]*inflightCall
}

type cacheEntry struct {
	result    *trinoQueryResult
	expiresAt time.Time
}

// inflightCall represents a query that is currently executing.
// Concurrent callers for the same key wait on wg and then read from cache.
type inflightCall struct {
	wg  sync.WaitGroup
	err error
}

var globalCache = newQueryCache()

func newQueryCache() *queryCache {
	c := &queryCache{
		entries:  make(map[string]*cacheEntry),
		inflight: make(map[string]*inflightCall),
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
	c.entries[key] = &cacheEntry{result: result, expiresAt: time.Now().Add(ttl)}
}

// invalidatePrefix removes all entries whose key starts with prefix.
// Called when add_catalog / remove_catalog succeeds to bust SHOW CATALOGS cache.
func (c *queryCache) invalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
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

// cacheKey returns a stable SHA-256 hash for a (coordinatorURL, sql) pair.
func cacheKey(coordinatorURL, sql string) string {
	h := sha256.Sum256([]byte(coordinatorURL + "\x00" + sql))
	return fmt.Sprintf("%x", h)
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
// ttl=0 bypasses the cache entirely (always hits Trino directly).
func executeTrinoQueryCached(ctx context.Context, coordinatorURL, sql string, maxRows int, ttl time.Duration) (*trinoQueryResult, error) {
	if ttl <= 0 {
		return executeTrinoQuery(ctx, coordinatorURL, sql, maxRows)
	}

	key := cacheKey(coordinatorURL, sql)

	// Fast path: already cached.
	if cached, ok := globalCache.get(key); ok {
		clone := *cached
		clone.FromCache = true
		return &clone, nil
	}

	// Singleflight: check if an identical query is already in-flight.
	globalCache.mu.Lock()
	if call, ok := globalCache.inflight[key]; ok {
		// Another goroutine is already executing this query — wait for it.
		globalCache.mu.Unlock()
		call.wg.Wait()
		if call.err != nil {
			return nil, call.err
		}
		// Read the result that the first caller stored in cache.
		if cached, ok := globalCache.get(key); ok {
			clone := *cached
			clone.FromCache = true
			return &clone, nil
		}
		// Cache was invalidated between the wait and the read (very rare).
		return executeTrinoQuery(ctx, coordinatorURL, sql, maxRows)
	}

	// This goroutine is first — register the in-flight call.
	call := &inflightCall{}
	call.wg.Add(1)
	globalCache.inflight[key] = call
	globalCache.mu.Unlock()

	// Execute the query and clean up regardless of outcome.
	result, err := executeTrinoQuery(ctx, coordinatorURL, sql, maxRows)

	globalCache.mu.Lock()
	delete(globalCache.inflight, key)
	globalCache.mu.Unlock()

	call.err = err
	call.wg.Done()

	if err != nil {
		return nil, err
	}

	globalCache.set(key, result, ttl)
	return result, nil
}
