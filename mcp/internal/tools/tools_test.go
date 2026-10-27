package tools

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

var readOnly = QueryOptions{}

// ── cluster + pool read tools ────────────────────────────────────────────────

func TestListClusters(t *testing.T) {
	c, _, _ := newTestClient(t,
		poolObj("p", 2, ""),
		clusterObj(clusterFixture{name: "a", phase: "Idle", ready: true, coordinatorUrl: "http://a:8080"}),
		clusterObj(clusterFixture{name: "b", phase: "Reserved", ready: true, clientID: "ci", reservationID: "r1"}),
	)
	out := mustCall(t, ListClustersHandler(c), nil)

	var got []ClusterSummary
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out)
	}
	if len(got) != 2 {
		t.Fatalf("got %d clusters, want 2", len(got))
	}
	byName := map[string]ClusterSummary{}
	for _, s := range got {
		byName[s.Name] = s
	}
	if byName["a"].Phase != "Idle" || byName["a"].CoordinatorURL != "http://a:8080" || byName["a"].Profile != "default" {
		t.Errorf("cluster a summary wrong: %+v", byName["a"])
	}
	if byName["b"].ClientID != "ci" || byName["b"].ReservationID != "r1" {
		t.Errorf("cluster b reservation missing: %+v", byName["b"])
	}
}

func TestListClustersWithoutOperator(t *testing.T) {
	c, _, _ := newTestClient(t) // no ClusterPool → operator not detected
	out := mustCall(t, ListClustersHandler(c), nil)
	if !strings.Contains(out, "operator not detected") {
		t.Errorf("want operator hint, got %q", out)
	}
}

func TestGetCluster(t *testing.T) {
	c, _, _ := newTestClient(t, clusterObj(clusterFixture{name: "a", phase: "Idle", ready: true}))
	out := mustCall(t, GetClusterHandler(c), map[string]any{"name": "a"})
	if !strings.Contains(out, `"phase": "Idle"`) {
		t.Errorf("unexpected output: %s", out)
	}
	wantErr(t, GetClusterHandler(c), map[string]any{"name": "missing"}, "not found")
}

func TestListPools(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("analytics", 3, "analytics"))
	out := mustCall(t, ListPoolsHandler(c), nil)
	for _, want := range []string{`"name": "analytics"`, `"desiredReplicas": 3`, `"workload": "analytics"`, `"profile": "default"`} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %s:\n%s", want, out)
		}
	}
}

func TestGetAuditLog(t *testing.T) {
	c, _, typed := newTestClient(t)
	now := time.Now()
	for i, name := range []string{"a", "b"} {
		_, err := typed.CoreV1().Events(testNS).Create(context.Background(), &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: name + "-ev", Namespace: testNS},
			InvolvedObject: corev1.ObjectReference{Kind: "Cluster", Name: name},
			Reason:         "PhaseChange",
			Message:        "Idle",
			Type:           "Normal",
			LastTimestamp:  metav1.NewTime(now.Add(time.Duration(i) * time.Minute)),
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	out := mustCall(t, GetAuditLogHandler(c), nil)
	var events []AuditEvent
	if err := json.Unmarshal([]byte(out), &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Resource != "Cluster/b" {
		t.Errorf("want newest first, got %+v", events)
	}
}

// ── reservation ──────────────────────────────────────────────────────────────

func TestReserveByName(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "a", phase: "Idle", ready: true, coordinatorUrl: "http://a"}))

	out := mustCall(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "ci", "reservation_id": "r1"})
	if !strings.Contains(out, `"clusterName": "a"`) {
		t.Fatalf("unexpected output: %s", out)
	}
	got, _ := c.GetCluster(context.Background(), "a", "")
	if strFromObj(got.Object, "spec", "clientId") != "ci" || strFromObj(got.Object, "spec", "reservationId") != "r1" {
		t.Fatalf("reservation not written: %v", got.Object["spec"])
	}

	// Same request again is idempotent even though the cluster is no longer free.
	mustCall(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "ci", "reservation_id": "r1"})

	// A different client is refused.
	wantErr(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "other", "reservation_id": "r2"}, "not available")
}

func TestReserveByNameRejectsUnhealthy(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "pending", phase: "Pending"}),
		clusterObj(clusterFixture{name: "notready", phase: "Idle", ready: false}))
	wantErr(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "pending", "client_id": "c", "reservation_id": "r"}, "not available")
	wantErr(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "notready", "client_id": "c", "reservation_id": "r"}, "not available")
}

func TestReserveSendsResourceVersion(t *testing.T) {
	c, dyn, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "a", phase: "Idle", ready: true}))

	var patch string
	dyn.PrependReactor("patch", "clusters", func(a k8stesting.Action) (bool, runtime.Object, error) {
		patch = string(a.(k8stesting.PatchAction).GetPatch())
		return false, nil, nil
	})
	mustCall(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "c", "reservation_id": "r"})
	if !strings.Contains(patch, `"resourceVersion":"1"`) {
		t.Fatalf("reservation patch must carry resourceVersion for optimistic locking, got %s", patch)
	}
}

func TestReserveByNameConflict(t *testing.T) {
	c, dyn, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "a", phase: "Idle", ready: true}))
	dyn.PrependReactor("patch", "clusters", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "meridian.io", Resource: "clusters"}, "a", nil)
	})
	wantErr(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "c", "reservation_id": "r"}, "kept changing")
}

func TestReserveByNameRetriesUnrelatedConflict(t *testing.T) {
	c, dyn, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "a", phase: "Idle", ready: true}))
	var n atomic.Int32
	dyn.PrependReactor("patch", "clusters", func(k8stesting.Action) (bool, runtime.Object, error) {
		if n.Add(1) == 1 { // e.g. the operator updated status between our read and write
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, "a", nil)
		}
		return false, nil, nil
	})
	mustCall(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "c", "reservation_id": "r"})
}

func TestReserveRequiresIDs(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, "etl"),
		clusterObj(clusterFixture{name: "a", phase: "Idle", ready: true, workload: "etl"}),
		clusterObj(clusterFixture{name: "f", phase: "Failed", workload: "etl"}))
	for _, args := range []map[string]any{
		{"cluster_name": "a"},
		{"cluster_name": "a", "client_id": "c"},
		{"workload": "etl", "reservation_id": "r"},
		{"workload": "etl", "client_id": "", "reservation_id": ""},
	} {
		wantErr(t, ReserveClusterHandler(c), args, "client_id and reservation_id are required")
	}
	got, _ := c.GetCluster(context.Background(), "a", "")
	if strFromObj(got.Object, "spec", "clientId") != "" {
		t.Fatal("cluster must not be claimed")
	}
}

func TestReserveByWorkloadPicksOldestAndSkipsConflicts(t *testing.T) {
	old := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	newer := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	c, dyn, _ := newTestClient(t, poolObj("p", 3, "etl"),
		clusterObj(clusterFixture{name: "oldest", phase: "Idle", ready: true, workload: "etl", idleAt: old}),
		clusterObj(clusterFixture{name: "newer", phase: "Idle", ready: true, workload: "etl", idleAt: newer}),
		clusterObj(clusterFixture{name: "other", phase: "Idle", ready: true, workload: "analytics", idleAt: old}),
		clusterObj(clusterFixture{name: "taken", phase: "Reserved", ready: true, workload: "etl", idleAt: old, clientID: "x", reservationID: "y"}),
	)

	// The first patch (on "oldest") loses a race to a rival client, which really
	// claims it; the tool must re-list and take "newer".
	var patches atomic.Int32
	dyn.PrependReactor("patch", "clusters", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if patches.Add(1) == 1 {
			name := a.(k8stesting.PatchAction).GetName()
			if name != "oldest" {
				t.Errorf("first candidate = %s, want oldest", name)
			}
			rival := clusterObj(clusterFixture{name: "oldest", phase: "Idle", ready: true, workload: "etl", idleAt: old, clientID: "rival", reservationID: "x"})
			if err := dyn.Tracker().Update(k8s.ClusterGVR, rival, testNS); err != nil {
				t.Errorf("simulate rival: %v", err)
			}
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, name, nil)
		}
		return false, nil, nil
	})

	out := mustCall(t, ReserveClusterHandler(c), map[string]any{"workload": "etl", "client_id": "c", "reservation_id": "r"})
	if !strings.Contains(out, `"clusterName": "newer"`) {
		t.Fatalf("want newer after conflict, got %s", out)
	}
}

func TestReserveByWorkloadNoneIdle(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, "etl"),
		clusterObj(clusterFixture{name: "a", phase: "Pending", workload: "etl"}))
	wantErr(t, ReserveClusterHandler(c), map[string]any{"workload": "etl", "client_id": "c", "reservation_id": "r"}, "no idle clusters")
}

func TestReserveValidation(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""))
	wantErr(t, ReserveClusterHandler(c), map[string]any{"client_id": "c", "reservation_id": "r"}, "either cluster_name or workload")
	wantErr(t, ReserveClusterHandler(c), map[string]any{"workload": "etl,foo!=bar", "client_id": "c", "reservation_id": "r"}, "invalid workload")
}

func TestReserveWithoutOperator(t *testing.T) {
	c, _, _ := newTestClient(t)
	wantErr(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "c", "reservation_id": "r"}, "operator required")
}

func TestRelease(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "a", phase: "Reserved", ready: true, clientID: "ci", reservationID: "r1"}),
		clusterObj(clusterFixture{name: "free", phase: "Idle", ready: true}))

	wantErr(t, ReleaseClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "someone-else"}, "different client")

	out := mustCall(t, ReleaseClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "ci"})
	if !strings.Contains(out, "released") {
		t.Fatalf("unexpected output: %s", out)
	}
	got, _ := c.GetCluster(context.Background(), "a", "")
	if strFromObj(got.Object, "spec", "clientId") != "" {
		t.Fatal("clientId should be cleared")
	}

	out = mustCall(t, ReleaseClusterHandler(c), map[string]any{"cluster_name": "free"})
	if !strings.Contains(out, "not reserved") {
		t.Errorf("releasing a free cluster should say so, got %s", out)
	}
}

// ── metadata + query tools ───────────────────────────────────────────────────

func trinoCluster(t *testing.T) (*k8s.Client, *fakeTrino) {
	t.Helper()
	f := newFakeTrino(t)
	c, _, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "t", phase: "Reserved", ready: true, coordinatorUrl: f.URL()}))
	return c, f
}

func TestMetadataToolsQuoteIdentifiers(t *testing.T) {
	c, f := trinoCluster(t)

	mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	if f.LastSQL() != "SHOW CATALOGS" {
		t.Errorf("list_catalogs sent %q", f.LastSQL())
	}

	mustCall(t, ListSchemasHandler(c), map[string]any{"cluster_name": "t", "catalog": `hive; DROP TABLE x --`})
	if f.LastSQL() != `SHOW SCHEMAS FROM "hive; DROP TABLE x --"` {
		t.Errorf("list_schemas sent %q", f.LastSQL())
	}

	mustCall(t, ListTablesHandler(c), map[string]any{"cluster_name": "t", "catalog": "hive", "schema": `we"ird`})
	if f.LastSQL() != `SHOW TABLES FROM "hive"."we""ird"` {
		t.Errorf("list_tables sent %q", f.LastSQL())
	}

	mustCall(t, GetTableSchemaHandler(c), map[string]any{"cluster_name": "t", "table": "orders", "catalog": "hive", "schema": "sales"})
	if f.LastSQL() != `DESCRIBE "hive"."sales"."orders"` {
		t.Errorf("get_table_schema sent %q", f.LastSQL())
	}

	mustCall(t, GetTableSchemaHandler(c), map[string]any{"cluster_name": "t", "table": "hive.sales.items"})
	if f.LastSQL() != `DESCRIBE "hive"."sales"."items"` {
		t.Errorf("get_table_schema (qualified) sent %q", f.LastSQL())
	}

	wantErr(t, ListSchemasHandler(c), map[string]any{"cluster_name": "t", "catalog": ""}, "catalog")
	wantErr(t, GetTableSchemaHandler(c), map[string]any{"cluster_name": "t", "table": "a.b.c.d"}, "too many parts")
}

func TestMetadataToolsCache(t *testing.T) {
	c, f := trinoCluster(t)
	mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	out := mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	if !strings.Contains(out, "[cache hit]") || len(f.Statements()) != 1 {
		t.Errorf("second list_catalogs should be cached (statements=%d)", len(f.Statements()))
	}
}

func TestExecuteQueryReadOnly(t *testing.T) {
	c, f := trinoCluster(t)
	h := ExecuteQueryHandler(c, readOnly)

	out := mustCall(t, h, map[string]any{"cluster_name": "t", "sql": "SELECT 1"})
	if !strings.Contains(out, "(1 rows)") {
		t.Errorf("unexpected output: %s", out)
	}

	wantErr(t, h, map[string]any{"cluster_name": "t", "sql": "DROP TABLE hive.s.t"}, "read-only")
	wantErr(t, h, map[string]any{"cluster_name": "t", "sql": "EXPLAIN ANALYZE DELETE FROM t"}, "EXPLAIN ANALYZE")
	if n := len(f.Statements()); n != 1 {
		t.Errorf("blocked statements must not reach Trino; Trino saw %d statements", n)
	}
}

func TestExecuteQueryAllowWrites(t *testing.T) {
	c, f := trinoCluster(t)
	mustCall(t, ExecuteQueryHandler(c, QueryOptions{AllowWrites: true}), map[string]any{"cluster_name": "t", "sql": "DROP TABLE x"})
	if f.LastSQL() != "DROP TABLE x" {
		t.Errorf("Trino got %q", f.LastSQL())
	}
}

func TestExecuteQueryMaxRows(t *testing.T) {
	c, f := trinoCluster(t)
	f.pages, f.rowsPerPage = 3, 5
	out := mustCall(t, ExecuteQueryHandler(c, readOnly), map[string]any{"cluster_name": "t", "sql": "SELECT 1", "max_rows": 7})
	if !strings.Contains(out, "(7 rows)") || !strings.Contains(out, "truncated") {
		t.Errorf("want 7 truncated rows, got:\n%s", out)
	}
}

func TestExecuteQueryOutputFile(t *testing.T) {
	c, _ := trinoCluster(t)
	dir := t.TempDir()
	h := ExecuteQueryHandler(c, QueryOptions{OutputDir: dir})

	out := mustCall(t, h, map[string]any{"cluster_name": "t", "sql": "SELECT 1", "output_file": "r.csv"})
	if !strings.Contains(out, "Results written to") {
		t.Fatalf("unexpected output: %s", out)
	}
	data, err := os.ReadFile(filepath.Join(dir, "r.csv"))
	if err != nil || !strings.HasPrefix(string(data), "c1,c2\n") {
		t.Fatalf("csv not written correctly: %q, %v", data, err)
	}

	wantErr(t, h, map[string]any{"cluster_name": "t", "sql": "SELECT 1", "output_file": "/etc/cron.d/x"}, "inside")
	wantErr(t, h, map[string]any{"cluster_name": "t", "sql": "SELECT 1", "output_file": "../x.csv"}, "plain file name")

	// Disabled entirely when no output dir is configured (SSE mode).
	wantErr(t, ExecuteQueryHandler(c, readOnly), map[string]any{"cluster_name": "t", "sql": "SELECT 1", "output_file": "r.csv"}, "disabled")
}

func TestExecuteQueryToolAnnotations(t *testing.T) {
	ro := ExecuteQueryTool(readOnly)
	if ro.Annotations.ReadOnlyHint == nil || !*ro.Annotations.ReadOnlyHint {
		t.Error("read-only execute_query should carry readOnlyHint=true")
	}
	if _, ok := ro.InputSchema.Properties["output_file"]; ok {
		t.Error("output_file must not be advertised when file output is disabled")
	}
	rw := ExecuteQueryTool(QueryOptions{AllowWrites: true, OutputDir: "/tmp/x"})
	if rw.Annotations.DestructiveHint == nil || !*rw.Annotations.DestructiveHint {
		t.Error("execute_query with writes must carry destructiveHint=true")
	}
	if _, ok := rw.InputSchema.Properties["output_file"]; !ok {
		t.Error("output_file should be advertised when an output dir is set")
	}
}

func TestExplainQuery(t *testing.T) {
	c, f := trinoCluster(t)
	h := ExplainQueryHandler(c, readOnly)

	mustCall(t, h, map[string]any{"cluster_name": "t", "sql": "SELECT 1"})
	if f.LastSQL() != "EXPLAIN SELECT 1" {
		t.Errorf("Trino got %q", f.LastSQL())
	}
	wantErr(t, h, map[string]any{"cluster_name": "t", "sql": "ANALYZE DELETE FROM t"}, "EXPLAIN ANALYZE")
}

func TestQueryToolsNeedCoordinator(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "pending", phase: "Pending"}))
	wantErr(t, ExecuteQueryHandler(c, readOnly), map[string]any{"cluster_name": "pending", "sql": "SELECT 1"}, "no coordinator URL")
	wantErr(t, ExecuteQueryHandler(c, readOnly), map[string]any{"cluster_name": "pending", "sql": ""}, "sql is required")
}

// ── held catalog tools (input validation) ────────────────────────────────────

func TestBuildCreateCatalogSQL(t *testing.T) {
	sql, err := buildCreateCatalogSQL("pg", "postgresql", map[string]string{"connection-url": "jdbc:postgresql://h/db", "connection-password": "it's"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sql, "CREATE CATALOG pg USING postgresql WITH (") || !strings.Contains(sql, `"connection-password"='it''s'`) {
		t.Errorf("unexpected SQL: %s", sql)
	}
	for _, tc := range []struct{ cat, conn, key string }{
		{"pg; DROP CATALOG x", "postgresql", "k"},
		{"pg", "postgresql USING x", "k"},
		{"pg", "postgresql", `k"=1) --`},
	} {
		if _, err := buildCreateCatalogSQL(tc.cat, tc.conn, map[string]string{tc.key: "v"}); err == nil {
			t.Errorf("buildCreateCatalogSQL(%q, %q, %q) should fail", tc.cat, tc.conn, tc.key)
		}
	}
}

func TestRemoveCatalogValidatesName(t *testing.T) {
	c, f := trinoCluster(t)
	wantErr(t, RemoveCatalogHandler(c), map[string]any{"cluster_name": "t", "catalog_name": "x; DROP CATALOG y"}, "invalid catalog name")
	if len(f.Statements()) != 0 {
		t.Error("invalid name must not reach Trino")
	}
}

func TestCatalogChangeBustsCache(t *testing.T) {
	c, f := trinoCluster(t)
	mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	mustCall(t, AddCatalogHandler(c), map[string]any{"cluster_name": "t", "catalog_name": "pg", "properties": "connector.name=postgresql"})
	out := mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	if strings.Contains(out, "[cache hit]") {
		t.Error("list_catalogs after add_catalog must not be served from cache")
	}
	if n := len(f.Statements()); n != 3 {
		t.Errorf("want 3 Trino statements, got %d", n)
	}
}

func TestListPoolsReportsStatusCounts(t *testing.T) {
	p := poolObj("analytics", 3, "analytics")
	p.Object["status"] = map[string]any{"readyReplicas": int64(2), "reservedReplicas": int64(1), "failedReplicas": int64(1)}
	c, _, _ := newTestClient(t, p)
	out := mustCall(t, ListPoolsHandler(c), nil)
	for _, want := range []string{`"readyReplicas": 2`, `"reservedReplicas": 1`, `"failedReplicas": 1`, `"pendingReplicas": 0`} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %s:\n%s", want, out)
		}
	}
}

// The Cluster CRD requires spec.profile; the API server rejects objects
// without it, which the fake client would not catch.
func TestCreateClusterSetsRequiredSpecFields(t *testing.T) {
	c, dyn, _ := newTestClient(t, poolObj("p", 1, ""))
	mustCall(t, CreateClusterHandler(c), map[string]any{"name": "new", "profile": "large"})
	obj, err := dyn.Resource(k8s.ClusterGVR).Namespace(testNS).Get(context.Background(), "new", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := obj.Object["spec"].(map[string]any)
	if spec["profile"] != "large" || spec["image"] == "" || spec["image"] == nil {
		t.Errorf("spec = %v, want profile=large and an image", spec)
	}
}

func TestScalePoolReturnsCounts(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""))
	out := mustCall(t, ScalePoolHandler(c), map[string]any{"pool_name": "p", "replicas": float64(4)})
	if !strings.Contains(out, `"desiredReplicas": 4`) || !strings.Contains(out, `"previousReplicas": 1`) || strings.Contains(out, `"status"`) {
		t.Errorf("unexpected scale_pool output: %s", out)
	}
}

// An agent holding an old reservation_id must not release a newer
// reservation of the same cluster.
func TestReleaseChecksReservationID(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "a", phase: "Reserved", ready: true, clientID: "agent", reservationID: "r2"}))
	wantErr(t, ReleaseClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "agent", "reservation_id": "r1"}, "no longer holds")
	got, _ := c.GetCluster(context.Background(), "a", "")
	if strFromObj(got.Object, "spec", "reservationId") != "r2" {
		t.Fatal("the newer reservation was released")
	}
	mustCall(t, ReleaseClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "agent", "reservation_id": "r2"})
}

// A repeated reserve call is not answered with a failed cluster.
func TestReserveIgnoresFailedClusterWithSameReservation(t *testing.T) {
	c, _, _ := newTestClient(t, poolObj("p", 1, ""),
		clusterObj(clusterFixture{name: "a", phase: "Failed", clientID: "ci", reservationID: "r1"}))
	wantErr(t, ReserveClusterHandler(c), map[string]any{"cluster_name": "a", "client_id": "ci", "reservation_id": "r1"}, "not available")
}

// A coordinator that doesn't answer connections (a pod that was just killed)
// fails within the dial timeout instead of hanging for the 30s default.
func TestTrinoUnreachableFailsFast(t *testing.T) {
	oldDial, oldTimeout := trinoDial, trinoDialTimeout
	t.Cleanup(func() { trinoDial, trinoDialTimeout = oldDial, oldTimeout })
	trinoDialTimeout = 200 * time.Millisecond
	trinoDial = func(ctx context.Context, network, addr string) (net.Conn, error) {
		<-ctx.Done() // never answers, like a dead pod's address
		return nil, ctx.Err()
	}
	start := time.Now()
	_, err := trinoCall(context.Background(), http.MethodPost, "http://dead-coordinator.invalid:8080/v1/statement", "SELECT 1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %v, want about the 200ms dial timeout", d)
	}
}

// Queries on a cluster that is being deleted fail at once with a clear error.
func TestQueryOnDeletingClusterFailsFast(t *testing.T) {
	cl := clusterObj(clusterFixture{name: "going", phase: "Idle", ready: true, coordinatorUrl: "http://going:8080"})
	now := metav1.Now()
	cl.SetDeletionTimestamp(&now)
	cl.SetFinalizers([]string{"meridian.io/cluster-protection"})
	c, _, _ := newTestClient(t, poolObj("p", 1, ""), cl)
	if _, err := getCoordinatorURL(context.Background(), c, "going", ""); err == nil || !strings.Contains(err.Error(), "being deleted") {
		t.Errorf("got %v, want a 'being deleted' error", err)
	}
}

// setClusterField updates a Cluster in the fake API server, as the operator
// or another MCP replica would.
func setClusterField(t *testing.T, c *k8s.Client, mutate func(*unstructured.Unstructured)) {
	t.Helper()
	cl, err := c.GetCluster(context.Background(), "t", "")
	if err != nil {
		t.Fatal(err)
	}
	mutate(cl)
	if _, err := c.Dynamic.Resource(k8s.ClusterGVR).Namespace(testNS).Update(context.Background(), cl, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// A credential rotation finished by the operator changes the cluster's
// catalogs; list_catalogs must not keep serving the old list from cache.
func TestCatalogCacheDroppedAfterRotation(t *testing.T) {
	c, f := trinoCluster(t)
	mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	if out := mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"}); !strings.Contains(out, "[cache hit]") {
		t.Fatal("second call should be a cache hit")
	}
	setClusterField(t, c, func(cl *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(cl.Object, "2026-10-27T10:00:00Z", "status", "lastRotatedAt")
	})
	if out := mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"}); strings.Contains(out, "[cache hit]") {
		t.Error("list_catalogs after a rotation was served from cache")
	}
	if n := len(f.Statements()); n != 2 {
		t.Errorf("want 2 Trino statements, got %d", n)
	}
}

// add_catalog through one MCP replica must reach the others: it stamps the
// Cluster, and a replica that sees the new stamp drops its cache.
func TestAddCatalogMarksClusterForOtherReplicas(t *testing.T) {
	c, _ := trinoCluster(t)
	mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	mustCall(t, AddCatalogHandler(c), map[string]any{"cluster_name": "t", "catalog_name": "pg", "properties": "connector.name=postgresql"})
	cl, _ := c.GetCluster(context.Background(), "t", "")
	if cl.GetAnnotations()[catalogsChangedAnnotation] == "" {
		t.Fatalf("add_catalog did not stamp %s", catalogsChangedAnnotation)
	}

	// Another replica's cache: fill it, then let it see a newer stamp.
	mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	setClusterField(t, c, func(cl *unstructured.Unstructured) {
		a := cl.GetAnnotations()
		a[catalogsChangedAnnotation] = "2099-01-01T00:00:00Z"
		cl.SetAnnotations(a)
	})
	if out := mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"}); strings.Contains(out, "[cache hit]") {
		t.Error("list_catalogs after another replica changed catalogs was served from cache")
	}
}

// A new cluster that reuses a deleted cluster's name (and so its coordinator
// URL) must not see the old cluster's cached results.
func TestCatalogCacheDroppedForRecreatedCluster(t *testing.T) {
	c, _ := trinoCluster(t)
	mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"})
	setClusterField(t, c, func(cl *unstructured.Unstructured) { cl.SetUID("new-uid") })
	if out := mustCall(t, ListCatalogsHandler(c), map[string]any{"cluster_name": "t"}); strings.Contains(out, "[cache hit]") {
		t.Error("recreated cluster was served the old cluster's cached catalogs")
	}
}
