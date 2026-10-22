package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const testNS = "meridian"

// fakeTrino is a minimal Trino statement API. Each submitted statement gets
// `pages` pages of `rowsPerPage` rows unless a custom responder is set.
type fakeTrino struct {
	srv *httptest.Server

	mu          sync.Mutex
	statements  []string
	deletes     int
	busyReplies int // reply 503 this many times before succeeding
	pages       int
	rowsPerPage int
	failWith    string // when set, the query fails with this Trino error
}

func newFakeTrino(t *testing.T) *fakeTrino {
	t.Helper()
	f := &fakeTrino{pages: 1, rowsPerPage: 1}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeTrino) URL() string { return f.srv.URL }

func (f *fakeTrino) Statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.statements...)
}

// LastSQL returns the most recent statement without the mcp_query_id tag.
func (f *fakeTrino) LastSQL() string {
	st := f.Statements()
	if len(st) == 0 {
		return ""
	}
	s := st[len(st)-1]
	if i := strings.Index(s, "\n-- mcp_query_id="); i >= 0 {
		s = s[:i]
	}
	return s
}

func (f *fakeTrino) Deletes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes
}

func (f *fakeTrino) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if r.Header.Get("X-Trino-User") == "" {
		http.Error(w, "missing user", http.StatusBadRequest)
		return
	}
	if f.busyReplies > 0 {
		f.busyReplies--
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	switch {
	case r.Method == http.MethodDelete:
		f.deletes++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
		body, _ := io.ReadAll(r.Body)
		f.statements = append(f.statements, string(body))
		if f.failWith != "" {
			writeJSON(w, map[string]any{"id": "q", "stats": map[string]any{"state": "FAILED"}, "error": map[string]any{"message": f.failWith}})
			return
		}
		// First response has no data yet, like real Trino.
		writeJSON(w, map[string]any{
			"id":      "q",
			"nextUri": fmt.Sprintf("%s/v1/statement/q/%d", f.srv.URL, 1),
			"stats":   map[string]any{"state": "QUEUED"},
		})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/statement/q/"):
		var page int
		_, _ = fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/v1/statement/q/"), "%d", &page)
		resp := map[string]any{
			"id":      "q",
			"columns": []map[string]any{{"name": "c1"}, {"name": "c2"}},
			"stats":   map[string]any{"state": "RUNNING"},
		}
		rows := [][]any{}
		for i := 0; i < f.rowsPerPage; i++ {
			rows = append(rows, []any{fmt.Sprintf("p%dr%d", page, i), nil})
		}
		resp["data"] = rows
		if page < f.pages {
			resp["nextUri"] = fmt.Sprintf("%s/v1/statement/q/%d", f.srv.URL, page+1)
		} else {
			resp["stats"] = map[string]any{"state": "FINISHED"}
		}
		writeJSON(w, resp)
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// ── Kubernetes fixtures ──────────────────────────────────────────────────────

type clusterFixture struct {
	name, phase, coordinatorUrl, idleAt, workload, clientID, reservationID string
	ready                                                                  bool
}

func clusterObj(f clusterFixture) *unstructured.Unstructured {
	labels := map[string]any{"meridian.io/profile": "default"}
	if f.workload != "" {
		labels["meridian.io/workload"] = f.workload
	}
	spec := map[string]any{"image": "trinodb/trino:latest", "workers": int64(2)}
	if f.clientID != "" {
		spec["clientId"] = f.clientID
		spec["reservationId"] = f.reservationID
	}
	status := map[string]any{"phase": f.phase, "ready": f.ready}
	if f.coordinatorUrl != "" {
		status["coordinatorUrl"] = f.coordinatorUrl
	}
	if f.idleAt != "" {
		status["idleAt"] = f.idleAt
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "meridian.io/v1alpha1",
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name":            f.name,
			"namespace":       testNS,
			"labels":          labels,
			"resourceVersion": "1",
		},
		"spec":   spec,
		"status": status,
	}}
}

func poolObj(name string, replicas int64, workload string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "meridian.io/v1alpha1",
		"kind":       "ClusterPool",
		"metadata":   map[string]any{"name": name, "namespace": testNS},
		"spec": map[string]any{
			"replicas": replicas,
			"workload": workload,
			"template": map[string]any{"profile": "default"},
		},
	}}
}

// newTestClient returns a k8s.Client backed by fakes with the operator's CRDs
// installed. With no objects at all, it behaves like a cluster without the
// operator (listing ClusterPools fails, as it does when the CRD is missing).
func newTestClient(t *testing.T, objs ...runtime.Object) (*k8s.Client, *dynamicfake.FakeDynamicClient, *kubefake.Clientset) {
	t.Helper()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			k8s.ClusterGVR:               "ClusterList",
			k8s.ClusterPoolGVR:           "ClusterPoolList",
			k8s.ClusterPoolAutoscalerGVR: "ClusterPoolAutoscalerList",
		}, objs...)
	if len(objs) == 0 {
		dyn.PrependReactor("list", "clusterpools", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "meridian.io", Resource: "clusterpools"}, "")
		})
	}
	typed := kubefake.NewSimpleClientset()
	return k8s.NewClientFromInterfaces(dyn, typed, testNS), dyn, typed
}

// ── MCP call helpers ─────────────────────────────────────────────────────────

type handler = func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)

func call(t *testing.T, h handler, args map[string]any) (string, error) {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	res, err := h(context.Background(), req)
	if err != nil {
		return "", err
	}
	if res == nil || len(res.Content) == 0 {
		t.Fatalf("empty tool result")
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("result is %T, want TextContent", res.Content[0])
	}
	return tc.Text, nil
}

func mustCall(t *testing.T, h handler, args map[string]any) string {
	t.Helper()
	out, err := call(t, h, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return out
}

func wantErr(t *testing.T, h handler, args map[string]any, contains string) {
	t.Helper()
	_, err := call(t, h, args)
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", contains)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Fatalf("error %q does not contain %q", err.Error(), contains)
	}
}
