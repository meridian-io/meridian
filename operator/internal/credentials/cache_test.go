package credentials

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockProvider is a test double that counts Fetch calls and returns a
// configurable secret or error.
type mockProvider struct {
	calls  atomic.Int32
	secret *Secret
	err    error
}

func (m *mockProvider) Fetch(_ context.Context, _ string) (*Secret, error) {
	m.calls.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return m.secret, nil
}

func (m *mockProvider) Name() string { return "mock" }

func newMockSecret(password string) *Secret {
	return &Secret{
		Properties: map[string]string{
			"connector.name":      "mysql",
			"connection-password": password,
		},
		FetchedAt: time.Now(),
	}
}

// TestSecretCache_CacheHit verifies that a second Get within TTL does not call the provider.
func TestSecretCache_CacheHit(t *testing.T) {
	mock := &mockProvider{secret: newMockSecret("pass1")}
	cache := NewSecretCache(mock, DefaultTTL)

	// First call — cache miss, provider called.
	s1, err := cache.Get(context.Background(), "my-secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.calls.Load() != 1 {
		t.Errorf("expected 1 provider call, got %d", mock.calls.Load())
	}

	// Second call within TTL — cache hit, provider not called again.
	s2, err := cache.Get(context.Background(), "my-secret")
	if err != nil {
		t.Fatalf("unexpected error on second get: %v", err)
	}
	if mock.calls.Load() != 1 {
		t.Errorf("expected provider still called once, got %d", mock.calls.Load())
	}

	// Both calls return the same secret content.
	if s1.Properties["connection-password"] != s2.Properties["connection-password"] {
		t.Error("expected same secret on cache hit")
	}
}

// TestSecretCache_Invalidate verifies that the next Get after Invalidate fetches fresh.
func TestSecretCache_Invalidate(t *testing.T) {
	mock := &mockProvider{secret: newMockSecret("pass1")}
	cache := NewSecretCache(mock, DefaultTTL)

	_, _ = cache.Get(context.Background(), "my-secret")
	if mock.calls.Load() != 1 {
		t.Fatalf("expected 1 call after first Get")
	}

	cache.Invalidate("my-secret")

	// Update the mock to return a new password.
	mock.secret = newMockSecret("pass2")

	s, err := cache.Get(context.Background(), "my-secret")
	if err != nil {
		t.Fatalf("unexpected error after invalidate: %v", err)
	}
	if mock.calls.Load() != 2 {
		t.Errorf("expected 2 provider calls after invalidate, got %d", mock.calls.Load())
	}
	if s.Properties["connection-password"] != "pass2" {
		t.Errorf("expected fresh secret pass2, got %q", s.Properties["connection-password"])
	}
}

// TestSecretCache_InvalidateUnknownKey verifies that invalidating a non-existent key is a no-op.
func TestSecretCache_InvalidateUnknownKey(t *testing.T) {
	mock := &mockProvider{secret: newMockSecret("pass1")}
	cache := NewSecretCache(mock, DefaultTTL)

	// Should not panic.
	cache.Invalidate("does-not-exist")
}

// TestSecretCache_ProactiveRefresh verifies that a stale entry triggers a fresh fetch.
func TestSecretCache_ProactiveRefresh(t *testing.T) {
	mock := &mockProvider{secret: newMockSecret("pass1")}
	// Use a very short TTL so we can simulate staleness without actually waiting.
	shortTTL := 100 * time.Millisecond
	cache := NewSecretCache(mock, shortTTL)

	_, _ = cache.Get(context.Background(), "my-secret")

	// Wait for the entry to become stale (TTL=100ms, threshold=80ms).
	time.Sleep(90 * time.Millisecond)

	mock.secret = newMockSecret("pass2")
	s, err := cache.Get(context.Background(), "my-secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.calls.Load() < 2 {
		t.Errorf("expected at least 2 provider calls after TTL, got %d", mock.calls.Load())
	}
	if s.Properties["connection-password"] != "pass2" {
		t.Errorf("expected refreshed secret pass2, got %q", s.Properties["connection-password"])
	}
}

// TestSecretCache_StaleOnFetchError verifies that a stale cached value is returned
// when the provider fails — prevents transient outages from breaking rotation.
func TestSecretCache_StaleOnFetchError(t *testing.T) {
	mock := &mockProvider{secret: newMockSecret("pass1")}
	shortTTL := 100 * time.Millisecond
	cache := NewSecretCache(mock, shortTTL)

	// Prime the cache.
	_, _ = cache.Get(context.Background(), "my-secret")

	// Wait for staleness then make the provider fail.
	time.Sleep(90 * time.Millisecond)
	mock.err = errors.New("vault unreachable")

	s, err := cache.Get(context.Background(), "my-secret")
	if err != nil {
		t.Fatalf("expected stale value returned on error, got error: %v", err)
	}
	if s.Properties["connection-password"] != "pass1" {
		t.Errorf("expected stale pass1, got %q", s.Properties["connection-password"])
	}
}

// TestSecretCache_ErrorWithNoCache verifies that a provider error is returned
// when there is no cached value to fall back to.
func TestSecretCache_ErrorWithNoCache(t *testing.T) {
	mock := &mockProvider{err: errors.New("vault unreachable")}
	cache := NewSecretCache(mock, DefaultTTL)

	_, err := cache.Get(context.Background(), "my-secret")
	if err == nil {
		t.Fatal("expected error when provider fails and cache is empty")
	}
}

// TestSecretCache_DifferentPaths verifies that different paths are cached independently.
func TestSecretCache_DifferentPaths(t *testing.T) {
	mock := &mockProvider{secret: newMockSecret("pass")}
	cache := NewSecretCache(mock, DefaultTTL)

	_, _ = cache.Get(context.Background(), "secret-a")
	_, _ = cache.Get(context.Background(), "secret-b")

	if mock.calls.Load() != 2 {
		t.Errorf("expected 2 provider calls for 2 different paths, got %d", mock.calls.Load())
	}

	// Second round — both should hit cache.
	_, _ = cache.Get(context.Background(), "secret-a")
	_, _ = cache.Get(context.Background(), "secret-b")

	if mock.calls.Load() != 2 {
		t.Errorf("expected no additional calls on cache hit, got %d", mock.calls.Load())
	}
}

// TestSecretCache_ConcurrentAccess verifies no data races under concurrent Get calls.
// Run with: go test -race ./internal/credentials/...
func TestSecretCache_ConcurrentAccess(t *testing.T) {
	mock := &mockProvider{secret: newMockSecret("pass")}
	cache := NewSecretCache(mock, DefaultTTL)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cache.Get(context.Background(), "my-secret")
		}()
	}
	wg.Wait()
}
