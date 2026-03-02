package credentials

import (
	"context"
	"sync"
	"time"
)

const (
	// DefaultTTL is how long a fetched secret is considered fresh.
	DefaultTTL = 5 * time.Minute

	// refreshThreshold is the age at which a cached entry is proactively
	// refreshed before it expires. Prevents cache misses on reconcile cycles
	// that happen to fall right at the TTL boundary.
	refreshThreshold = 4 * time.Minute
)

// cacheEntry holds a fetched secret and its expiry timestamp.
type cacheEntry struct {
	secret    *Secret
	expiresAt time.Time
}

// isStale returns true if the entry should be refreshed.
// An entry is stale when its age exceeds refreshThreshold (not the full TTL),
// enabling proactive refresh before the credential actually expires.
func (e *cacheEntry) isStale() bool {
	return time.Now().After(e.expiresAt.Add(-(DefaultTTL - refreshThreshold)))
}

// SecretCache is a thread-safe TTL cache that wraps a SecretProvider.
// It transparently refreshes entries whose age exceeds refreshThreshold,
// before they actually expire — the same lazy-refresh pattern used for
// OAuth tokens in production credential systems.
//
// One SecretCache instance should be created per provider.
// Cache keys are the secret path strings passed to Get.
type SecretCache struct {
	mu       sync.RWMutex
	entries  map[string]*cacheEntry
	provider SecretProvider
	ttl      time.Duration
}

// NewSecretCache creates a cache wrapping provider with the given TTL.
// Use DefaultTTL for standard operation.
func NewSecretCache(provider SecretProvider, ttl time.Duration) *SecretCache {
	return &SecretCache{
		entries:  make(map[string]*cacheEntry),
		provider: provider,
		ttl:      ttl,
	}
}

// Get returns a cached secret if fresh, or fetches a new one from the provider.
// If the cached entry is older than refreshThreshold, it fetches synchronously
// and updates the cache before returning — proactive refresh before expiry.
func (c *SecretCache) Get(ctx context.Context, path string) (*Secret, error) {
	// Fast path: check cache under read lock.
	c.mu.RLock()
	entry, ok := c.entries[path]
	c.mu.RUnlock()

	if ok && !entry.isStale() {
		return entry.secret, nil
	}

	// Slow path: fetch from provider and update cache under write lock.
	secret, err := c.provider.Fetch(ctx, path)
	if err != nil {
		// On fetch failure, return the stale cached value if we have one.
		// This prevents a transient Vault/ASM outage from breaking rotation
		// on clusters that are currently serving traffic.
		if ok {
			return entry.secret, nil
		}
		return nil, err
	}

	c.mu.Lock()
	c.entries[path] = &cacheEntry{
		secret:    secret,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()

	return secret, nil
}

// Invalidate removes a specific entry from the cache, forcing the next Get
// to fetch fresh from the provider. Called immediately after a successful
// rotation to prevent the old (now-rotated) credentials from being reused.
func (c *SecretCache) Invalidate(path string) {
	c.mu.Lock()
	delete(c.entries, path)
	c.mu.Unlock()
}
