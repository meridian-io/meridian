package tools

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunTrinoFollowsPages(t *testing.T) {
	f := newFakeTrino(t)
	f.pages, f.rowsPerPage = 3, 2

	res, err := executeTrinoQuery(context.Background(), f.URL(), "SELECT 1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 6 {
		t.Fatalf("got %d rows, want 6", len(res.Rows))
	}
	if res.Rows[0][1] != "NULL" {
		t.Errorf("nil cell rendered as %q, want NULL", res.Rows[0][1])
	}
	if res.QueryID == "" || !strings.Contains(f.Statements()[0], "mcp_query_id="+res.QueryID) {
		t.Error("query must be tagged with its mcp_query_id")
	}
	if f.Deletes() != 0 {
		t.Error("finished query must not be cancelled")
	}
}

func TestRunTrinoTruncatesAndCancels(t *testing.T) {
	f := newFakeTrino(t)
	f.pages, f.rowsPerPage = 5, 10

	res, err := executeTrinoQuery(context.Background(), f.URL(), "SELECT 1", 15)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 15 || !res.Truncated {
		t.Fatalf("got %d rows truncated=%v, want 15 truncated", len(res.Rows), res.Truncated)
	}
	if f.Deletes() != 1 {
		t.Errorf("truncated query should be cancelled in Trino once, got %d DELETEs", f.Deletes())
	}
}

func TestRunTrinoRetriesWhenBusy(t *testing.T) {
	f := newFakeTrino(t)
	f.busyReplies = 2

	if _, err := executeTrinoQuery(context.Background(), f.URL(), "SELECT 1", 0); err != nil {
		t.Fatalf("should retry through 503s: %v", err)
	}
}

func TestRunTrinoGivesUpWhenAlwaysBusy(t *testing.T) {
	f := newFakeTrino(t)
	f.busyReplies = 100

	_, err := executeTrinoQuery(context.Background(), f.URL(), "SELECT 1", 0)
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("want unavailable error, got %v", err)
	}
}

func TestRunTrinoReportsQueryError(t *testing.T) {
	f := newFakeTrino(t)
	f.failWith = "line 1:1: mismatched input"

	_, err := executeTrinoQuery(context.Background(), f.URL(), "SELEC 1", 0)
	if err == nil || !strings.Contains(err.Error(), "mismatched input") {
		t.Fatalf("want Trino error, got %v", err)
	}
}

func TestRunTrinoHonorsContext(t *testing.T) {
	f := newFakeTrino(t)
	f.pages = 1000
	f.rowsPerPage = 0 // empty pages force polling waits

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := executeTrinoQuery(ctx, f.URL(), "SELECT 1", 0); err == nil {
		t.Fatal("expected context error")
	}
}

func TestCachedQuerySingleflight(t *testing.T) {
	f := newFakeTrino(t)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := executeTrinoQueryCached(context.Background(), f.URL(), "SELECT 'sf'", 0, time.Minute); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if n := len(f.Statements()); n != 1 {
		t.Errorf("10 identical concurrent queries hit Trino %d times, want 1", n)
	}
}

func TestFormatTrinoResult(t *testing.T) {
	out := formatTrinoResult(&trinoQueryResult{
		Columns: []string{"a", "bb"}, Rows: [][]string{{"1", "2"}}, Truncated: true, FromCache: true, QueryID: "q1",
	})
	for _, want := range []string{"[cache hit]", "query_id: q1", "| a | bb |", "(1 rows)", "truncated"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if formatTrinoResult(&trinoQueryResult{}) != "(no results)" {
		t.Error("empty result should render (no results)")
	}
}

func TestPostNotRetriedOnGatewayError(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	if _, err := executeTrinoQuery(context.Background(), srv.URL, "SELECT 1", 0); err == nil {
		t.Fatal("expected error")
	}
	if n := posts.Load(); n != 1 {
		t.Errorf("POST sent %d times on 502, want 1 (resubmitting could run the statement twice)", n)
	}
}

func TestSingleflightWaiterHonorsOwnContext(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		writeJSON(w, map[string]any{"id": "q", "stats": map[string]any{"state": "FINISHED"}})
	}))
	defer srv.Close()
	defer close(release)

	go func() { _, _ = executeTrinoQueryCached(context.Background(), srv.URL, "SELECT 'slow'", 0, time.Minute) }()
	time.Sleep(50 * time.Millisecond) // let the leader register

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := executeTrinoQueryCached(ctx, srv.URL, "SELECT 'slow'", 0, time.Minute)
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("waiter should give up with its own context: err=%v after %v", err, time.Since(start))
	}
}

func TestSingleflightWaiterRunsQueryWhenLeaderCancelled(t *testing.T) {
	f := newFakeTrino(t)
	f.pages, f.rowsPerPage = 1000, 0 // slow: many empty pages

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		_, _ = executeTrinoQueryCached(leaderCtx, f.URL(), "SELECT 'x'", 0, time.Minute)
		close(leaderDone)
	}()
	time.Sleep(50 * time.Millisecond)

	waiterErr := make(chan error, 1)
	go func() {
		f.mu.Lock()
		f.pages = 1
		f.rowsPerPage = 1
		f.mu.Unlock()
		_, err := executeTrinoQueryCached(context.Background(), f.URL(), "SELECT 'x'", 0, time.Minute)
		waiterErr <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	<-leaderDone

	if err := <-waiterErr; err != nil {
		t.Fatalf("waiter should rerun the query after the leader was cancelled, got %v", err)
	}
}

func TestStaleResultNotCachedAfterInvalidation(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		writeJSON(w, map[string]any{"id": "q", "columns": []map[string]any{{"name": "c"}}, "data": [][]any{{"old"}}, "stats": map[string]any{"state": "FINISHED"}})
	}))
	defer srv.Close()

	done := make(chan struct{})
	go func() {
		_, _ = executeTrinoQueryCached(context.Background(), srv.URL, "SHOW CATALOGS", 0, time.Minute)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	globalCache.invalidatePrefix(coordinatorPrefix(srv.URL)) // add_catalog finished mid-query
	close(release)
	<-done

	if _, ok := globalCache.get(cacheKey(srv.URL, "SHOW CATALOGS", 0)); ok {
		t.Fatal("a result started before invalidation must not be cached")
	}
}
