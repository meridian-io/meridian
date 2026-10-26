package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/meridian-io/meridian/mcp/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const (
	testToken     = "s3cret-token-0123456789"
	modernVersion = "2026-07-28"
	legacyVersion = "2025-06-18"
)

func ready(context.Context) error { return nil }

// newTestHTTP starts the real http-mode handler (as main.go builds it) with
// the core tools and a fake Kubernetes client.
func newTestHTTP(t *testing.T, o HTTPOptions) *httptest.Server {
	t.Helper()
	s, _ := New(testClient(), "test", Options{})
	if o.Ready == nil {
		o.Ready = ready
	}
	h := NewHTTPHandler(s, o)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

type rpcResponse struct {
	status int
	header http.Header
	Result map[string]any `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// post sends one JSON-RPC message to /mcp. Headers are passed explicitly so
// tests can omit or corrupt them.
func post(t *testing.T, url string, headers map[string]string, body any) rpcResponse {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url+"/mcp", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := rpcResponse{status: resp.StatusCode, header: resp.Header}
	// The reply is a JSON object or a request-scoped SSE stream.
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "data:") {
				raw = []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
	}
	if len(bytes.TrimSpace(raw)) > 0 && raw[0] == '{' {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %q: %v", raw, err)
		}
	}
	return out
}

// modern builds a 2026-07-28 request: per-request _meta plus the required
// MCP-Protocol-Version / Mcp-Method / Mcp-Name headers.
func modern(method, name string, params map[string]any) (map[string]string, map[string]any) {
	if params == nil {
		params = map[string]any{}
	}
	params["_meta"] = map[string]any{
		"io.modelcontextprotocol/protocolVersion":    modernVersion,
		"io.modelcontextprotocol/clientCapabilities": map[string]any{},
		"io.modelcontextprotocol/clientInfo":         map[string]any{"name": "test", "version": "1"},
	}
	h := map[string]string{
		"Authorization":        "Bearer " + testToken,
		"MCP-Protocol-Version": modernVersion,
		"Mcp-Method":           method,
	}
	if name != "" {
		h["Mcp-Name"] = name
	}
	return h, map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
}

func callTool(name string, args map[string]any) (map[string]string, map[string]any) {
	return modern("tools/call", name, map[string]any{"name": name, "arguments": args})
}

func TestModernDiscover(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h, body := modern("server/discover", "", nil)
	r := post(t, srv.URL, h, body)
	if r.status != http.StatusOK || r.Error != nil {
		t.Fatalf("server/discover = %d %+v", r.status, r.Error)
	}
	versions, _ := r.Result["supportedVersions"].([]any)
	have := map[string]bool{}
	for _, v := range versions {
		have[v.(string)] = true
	}
	if !have[modernVersion] || !have[legacyVersion] {
		t.Errorf("supportedVersions = %v, want %s and %s", versions, modernVersion, legacyVersion)
	}
}

func TestModernToolsList(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h, body := modern("tools/list", "", nil)
	r := post(t, srv.URL, h, body)
	if r.status != http.StatusOK || r.Error != nil {
		t.Fatalf("tools/list = %d %+v", r.status, r.Error)
	}
	if r.Result["resultType"] != "complete" {
		t.Errorf("resultType = %v, want complete", r.Result["resultType"])
	}
	list, _ := r.Result["tools"].([]any)
	var names []string
	for _, x := range list {
		tool := x.(map[string]any)
		names = append(names, tool["name"].(string))
		if tool["title"] == nil || tool["title"] == "" {
			t.Errorf("tool %v has no title", tool["name"])
		}
	}
	if len(names) != len(CoreTools) {
		t.Errorf("got %d tools, want %d core tools", len(names), len(CoreTools))
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("tools/list order must be deterministic (sorted), got %v", names)
	}
}

func TestModernToolsCall(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h, body := callTool("list_clusters", map[string]any{})
	r := post(t, srv.URL, h, body)
	if r.status != http.StatusOK || r.Error != nil {
		t.Fatalf("tools/call = %d %+v", r.status, r.Error)
	}
	if r.Result["isError"] == true {
		t.Errorf("list_clusters failed: %v", r.Result["content"])
	}
	if r.header.Get("Mcp-Session-Id") != "" {
		t.Error("stateless server must not mint a session id")
	}
}

func TestModernHeaderMismatch(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	for name, mutate := range map[string]func(map[string]string){
		"missing Mcp-Name":   func(h map[string]string) { delete(h, "Mcp-Name") },
		"wrong Mcp-Name":     func(h map[string]string) { h["Mcp-Name"] = "delete_cluster" },
		"wrong Mcp-Method":   func(h map[string]string) { h["Mcp-Method"] = "tools/list" },
		"missing Mcp-Method": func(h map[string]string) { delete(h, "Mcp-Method") },
	} {
		h, body := callTool("list_clusters", map[string]any{})
		mutate(h)
		r := post(t, srv.URL, h, body)
		if r.status != http.StatusBadRequest || r.Error == nil || r.Error.Code != -32020 {
			t.Errorf("%s: got %d %+v, want 400 with -32020", name, r.status, r.Error)
		}
	}
}

func TestModernUnsupportedVersion(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h, body := modern("tools/list", "", nil)
	h["MCP-Protocol-Version"] = "2099-01-01"
	body["params"].(map[string]any)["_meta"].(map[string]any)["io.modelcontextprotocol/protocolVersion"] = "2099-01-01"
	r := post(t, srv.URL, h, body)
	if r.status != http.StatusBadRequest || r.Error == nil || r.Error.Code != -32022 {
		t.Errorf("got %d %+v, want 400 with -32022", r.status, r.Error)
	}
}

func TestModernMethodErrors(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req, _ := http.NewRequest(method, srv.URL+"/mcp", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("MCP-Protocol-Version", modernVersion)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s /mcp = %d, want 405", method, resp.StatusCode)
		}
	}
	h, body := modern("no/such_method", "", nil)
	r := post(t, srv.URL, h, body)
	if r.status != http.StatusNotFound || r.Error == nil || r.Error.Code != -32601 {
		t.Errorf("unknown method: got %d %+v, want 404 with -32601", r.status, r.Error)
	}
}

func TestToolFailureIsResultNotProtocolError(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h, body := callTool("get_cluster", map[string]any{"name": "does-not-exist"})
	r := post(t, srv.URL, h, body)
	if r.Error != nil {
		t.Fatalf("tool failure must not be a JSON-RPC error, got %+v", r.Error)
	}
	if r.Result["isError"] != true {
		t.Errorf("isError = %v, want true", r.Result["isError"])
	}
}

// legacyInit sends a 2025-era initialize and returns the headers a legacy
// client would use afterwards.
func legacyInit(t *testing.T, url string) map[string]string {
	t.Helper()
	h := map[string]string{"Authorization": "Bearer " + testToken}
	r := post(t, url, h, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "initialize",
		"params": map[string]any{
			"protocolVersion": legacyVersion,
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "legacy", "version": "1"},
		},
	})
	if r.status != http.StatusOK || r.Error != nil {
		t.Fatalf("initialize = %d %+v", r.status, r.Error)
	}
	if r.Result["protocolVersion"] != legacyVersion {
		t.Errorf("negotiated %v, want %s", r.Result["protocolVersion"], legacyVersion)
	}
	if id := r.header.Get("Mcp-Session-Id"); id != "" {
		t.Errorf("stateless server returned session id %q", id)
	}
	h["MCP-Protocol-Version"] = legacyVersion
	return h
}

func legacyCall(name string) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": map[string]any{}}}
}

func TestLegacyStreamableHTTPClient(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h := legacyInit(t, srv.URL)
	r := post(t, srv.URL, h, legacyCall("list_clusters"))
	if r.status != http.StatusOK || r.Error != nil || r.Result["isError"] == true {
		t.Fatalf("legacy tools/call = %d %+v %v", r.status, r.Error, r.Result)
	}
}

// TestAnyReplicaServesAnyRequest simulates two MCP pods behind one Service:
// a legacy client initializes on one and calls a tool on the other.
func TestAnyReplicaServesAnyRequest(t *testing.T) {
	a := newTestHTTP(t, HTTPOptions{Token: testToken})
	b := newTestHTTP(t, HTTPOptions{Token: testToken})
	h := legacyInit(t, a.URL)
	if r := post(t, b.URL, h, legacyCall("list_pools")); r.status != http.StatusOK || r.Error != nil {
		t.Fatalf("call on second replica = %d %+v", r.status, r.Error)
	}
	mh, body := callTool("list_pools", map[string]any{})
	for _, srv := range []*httptest.Server{a, b} {
		if r := post(t, srv.URL, mh, body); r.status != http.StatusOK || r.Error != nil {
			t.Fatalf("modern call = %d %+v", r.status, r.Error)
		}
	}
}

// TestLegacySSERemoved: the deprecated HTTP+SSE transport was removed in
// v1.5. /sse and /message no longer exist, even with a valid token.
func TestLegacySSERemoved(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	for _, route := range []struct{ method, path string }{{http.MethodGet, "/sse"}, {http.MethodPost, "/message"}} {
		req, _ := http.NewRequest(route.method, srv.URL+route.path, strings.NewReader("{}"))
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", route.method, route.path, resp.StatusCode)
		}
	}
}

func TestAuthOnEveryMCPRoute(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	do := func(method, path, auth string) int {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader("{}"))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for _, path := range []string{"/healthz", "/readyz"} {
		if code := do(http.MethodGet, path, ""); code != http.StatusOK {
			t.Errorf("%s without auth = %d, want 200", path, code)
		}
	}
	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/mcp"},
	} {
		for _, auth := range []string{"", "Bearer wrong", "Basic " + testToken, "Bearer " + testToken + "x"} {
			if code := do(route.method, route.path, auth); code != http.StatusUnauthorized {
				t.Errorf("%s %s with auth %q = %d, want 401", route.method, route.path, auth, code)
			}
		}
	}
	// The scheme is case-insensitive.
	h, body := modern("tools/list", "", nil)
	h["Authorization"] = "bearer " + testToken
	if r := post(t, srv.URL, h, body); r.status != http.StatusOK {
		t.Errorf("lowercase bearer scheme = %d, want 200", r.status)
	}
}

func TestOriginValidation(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken, AllowedOrigins: []string{"https://app.example.com/"}})
	for origin, want := range map[string]int{
		"":                        http.StatusOK, // non-browser client
		"https://app.example.com": http.StatusOK,
		"HTTPS://APP.EXAMPLE.COM": http.StatusOK,
		"https://evil.example":    http.StatusForbidden,
		"null":                    http.StatusForbidden,
	} {
		h, body := modern("tools/list", "", nil)
		if origin != "" {
			h["Origin"] = origin
		}
		if r := post(t, srv.URL, h, body); r.status != want {
			t.Errorf("Origin %q = %d, want %d", origin, r.status, want)
		}
	}
}

func TestBodyLimit(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h, body := callTool("list_clusters", map[string]any{"pad": strings.Repeat("x", DefaultMaxBodyBytes+1)})
	if r := post(t, srv.URL, h, body); r.status != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body = %d, want 413", r.status)
	}
}

func TestReadyzReportsFailure(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken, Ready: func(context.Context) error {
		return errors.New("api down")
	}})
	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d, want 503", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "api down") {
		t.Error("/readyz must not leak error details to unauthenticated callers")
	}
}

// TestStdioBothEras drives the stdio transport with a modern client and a
// legacy one.
func TestStdioBothEras(t *testing.T) {
	run := func(t *testing.T, lines ...map[string]any) []map[string]any {
		t.Helper()
		s, _ := New(testClient(), "test", Options{})
		var in bytes.Buffer
		for _, l := range lines {
			b, _ := json.Marshal(l)
			in.Write(append(b, '\n'))
		}
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(in.Bytes())
		}()
		var out safeBuffer
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = server.NewStdioServer(s).Listen(ctx, pr, &out)
			close(done)
		}()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) && strings.Count(out.String(), "\n") < countRequests(lines) {
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		_ = pw.Close()
		<-done
		var res []map[string]any
		for _, l := range strings.Split(strings.TrimSpace(out.String()), "\n") {
			var m map[string]any
			if err := json.Unmarshal([]byte(l), &m); err == nil {
				res = append(res, m)
			}
		}
		return res
	}

	t.Run("modern", func(t *testing.T) {
		_, discover := modern("server/discover", "", nil)
		_, call := callTool("list_clusters", map[string]any{})
		call["id"] = 2
		res := run(t, discover, call)
		if len(res) != 2 || res[0]["error"] != nil || res[1]["error"] != nil {
			t.Fatalf("modern stdio replies: %v", res)
		}
	})
	t.Run("legacy", func(t *testing.T) {
		res := run(t,
			map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{
				"protocolVersion": legacyVersion, "capabilities": map[string]any{},
				"clientInfo": map[string]any{"name": "legacy", "version": "1"}}},
			map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"},
			legacyCall("list_clusters"),
		)
		if len(res) != 2 || res[0]["error"] != nil || res[1]["error"] != nil {
			t.Fatalf("legacy stdio replies: %v", res)
		}
	})
}

func countRequests(lines []map[string]any) int {
	n := 0
	for _, l := range lines {
		if _, ok := l["id"]; ok {
			n++
		}
	}
	return n
}

// safeBuffer is a bytes.Buffer safe for one writer and concurrent readers.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestLocalhostProtection: a loopback connection with a non-localhost Host
// header (DNS rebinding) is refused on every MCP route unless the operator
// explicitly opts out, e.g. for a same-host reverse proxy.
func TestLocalhostProtection(t *testing.T) {
	status := func(srv *httptest.Server, method, path string) int {
		h, body := modern("tools/list", "", nil)
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(method, srv.URL+path, bytes.NewReader(b))
		if method == http.MethodGet {
			req, _ = http.NewRequest(method, srv.URL+path, nil)
		}
		for k, v := range h {
			req.Header.Set(k, v)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Host = "rebind.example.com"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := http.DefaultClient.Do(req.WithContext(ctx))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	on := newTestHTTP(t, HTTPOptions{Token: testToken})
	off := newTestHTTP(t, HTTPOptions{Token: testToken, DisableLocalhostProtection: true})
	for _, route := range []struct{ method, path string }{{http.MethodPost, "/mcp"}} {
		if code := status(on, route.method, route.path); code != http.StatusForbidden {
			t.Errorf("protected %s %s with foreign Host = %d, want 403", route.method, route.path, code)
		}
		if code := status(off, route.method, route.path); code != http.StatusOK {
			t.Errorf("--disable-localhost-protection %s %s = %d, want 200", route.method, route.path, code)
		}
	}
}

func TestBodyLimitChunked(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	h, body := callTool("list_clusters", map[string]any{"pad": strings.Repeat("x", DefaultMaxBodyBytes+1)})
	b, _ := json.Marshal(body)
	// io.MultiReader hides the length, so the request is sent chunked.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", io.MultiReader(bytes.NewReader(b)))
	for k, v := range h {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if req.ContentLength != -1 && req.ContentLength != 0 {
		t.Fatalf("test request was not chunked (ContentLength=%d)", req.ContentLength)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized chunked body = %d, want 413", resp.StatusCode)
	}
}

// TestToolCallMetrics: every registered tool has ok and error series from
// startup, and calls are counted by result.
func TestToolCallMetrics(t *testing.T) {
	srv := newTestHTTP(t, HTTPOptions{Token: testToken})
	ok := metrics.ToolCalls.WithLabelValues("list_clusters", metrics.ResultOK)
	fail := metrics.ToolCalls.WithLabelValues("get_cluster", metrics.ResultError)
	okBefore, failBefore := testutil.ToFloat64(ok), testutil.ToFloat64(fail)

	h, body := callTool("list_clusters", map[string]any{})
	if r := post(t, srv.URL, h, body); r.status != http.StatusOK {
		t.Fatalf("list_clusters: %d %+v", r.status, r.Error)
	}
	h, body = callTool("get_cluster", map[string]any{"name": "does-not-exist"})
	if r := post(t, srv.URL, h, body); r.status != http.StatusOK {
		t.Fatalf("get_cluster: %d %+v", r.status, r.Error)
	}
	if got := testutil.ToFloat64(ok) - okBefore; got != 1 {
		t.Errorf("list_clusters ok grew by %v, want 1", got)
	}
	if got := testutil.ToFloat64(fail) - failBefore; got != 1 {
		t.Errorf("get_cluster error grew by %v, want 1", got)
	}

	// Every core tool has an ok and an error series before any call.
	if n := testutil.CollectAndCount(metrics.ToolCalls); n < 2*len(CoreTools) {
		t.Errorf("%d tool call series, want at least %d", n, 2*len(CoreTools))
	}
}
