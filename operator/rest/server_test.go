package rest

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

const testNamespace = "meridian"

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = meridianv1alpha1.AddToScheme(s)
	return s
}

func newTestClient(s *runtime.Scheme, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithIndex(&meridianv1alpha1.Cluster{}, "spec.reservationId", func(obj client.Object) []string {
			return []string{obj.(*meridianv1alpha1.Cluster).Spec.ReservationID}
		}).
		WithStatusSubresource(&meridianv1alpha1.Cluster{}, &meridianv1alpha1.ClusterPool{}).
		Build()
}

// fakeTLS returns a *tls.ConnectionState with the given CN in the peer cert.
// x509.Certificate is a plain struct — no crypto operations needed.
func fakeTLS(cn string) *tls.ConnectionState {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
	return &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
}

func req(method, path, body string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func reqWithCert(method, path, body, cn string) *http.Request {
	r := req(method, path, body)
	r.TLS = fakeTLS(cn)
	return r
}

// ── Fixtures ──────────────────────────────────────────────────────────────────

func idleClusterObj(name, profile string) *meridianv1alpha1.Cluster {
	now := metav1.Now()
	c := &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{"meridian.io/profile": profile},
		},
		Spec: meridianv1alpha1.ClusterSpec{Profile: profile, Image: "trinodb/trino:435"},
	}
	c.Status.Phase = meridianv1alpha1.ClusterPhaseIdle
	c.Status.Ready = true
	c.Status.IdleAt = &now
	return c
}

func reservedClusterObj(name, clientID, reservationID string) *meridianv1alpha1.Cluster {
	now := metav1.Now()
	c := &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{"meridian.io/profile": "default"},
		},
		Spec: meridianv1alpha1.ClusterSpec{
			Profile:       "default",
			Image:         "trinodb/trino:435",
			ClientID:      clientID,
			ReservationID: reservationID,
		},
	}
	c.Status.Phase = meridianv1alpha1.ClusterPhaseReserved
	c.Status.Ready = true
	c.Status.ReservedAt = &now
	return c
}

func poolObj(name string, replicas int32) *meridianv1alpha1.ClusterPool {
	return &meridianv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: meridianv1alpha1.ClusterPoolSpec{
			Replicas: replicas,
			Template: meridianv1alpha1.ClusterTemplate{
				Profile: "default",
				Image:   "trinodb/trino:435",
				Workers: 2,
			},
		},
	}
}

// ── Release tests ─────────────────────────────────────────────────────────────

func TestRelease_OK(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, reservedClusterObj("c1", "client-a", "res-123"))

	h := &ReleaseHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := reqWithCert("DELETE", "/api/v1/clusters/reservations/res-123", "", "client-a")
	r.SetPathValue("reservationId", "res-123")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRelease_NotFound(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s)

	h := &ReleaseHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := reqWithCert("DELETE", "/api/v1/clusters/reservations/missing", "", "client-a")
	r.SetPathValue("reservationId", "missing")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestRelease_Forbidden(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, reservedClusterObj("c1", "client-a", "res-123"))

	h := &ReleaseHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := reqWithCert("DELETE", "/api/v1/clusters/reservations/res-123", "", "client-b")
	r.SetPathValue("reservationId", "res-123")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

func TestRelease_MissingCert(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, reservedClusterObj("c1", "client-a", "res-123"))

	h := &ReleaseHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := req("DELETE", "/api/v1/clusters/reservations/res-123", "")
	r.SetPathValue("reservationId", "res-123")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

// ── List clusters tests ───────────────────────────────────────────────────────

func TestListClusters_NoFilter(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, idleClusterObj("c1", "default"), idleClusterObj("c2", "large"))

	h := &ListClustersHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req("GET", "/api/v1/clusters", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []clusterSummary
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(result))
	}
}

func TestListClusters_PhaseFilter(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s,
		idleClusterObj("c1", "default"),
		reservedClusterObj("c2", "client-a", "res-1"),
	)

	h := &ListClustersHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req("GET", "/api/v1/clusters?phase=Idle", ""))

	var result []clusterSummary
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 1 || result[0].Name != "c1" {
		t.Fatalf("expected only c1 (Idle), got %+v", result)
	}
}

func TestListClusters_ProfileFilter(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, idleClusterObj("c1", "default"), idleClusterObj("c2", "large"))

	h := &ListClustersHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req("GET", "/api/v1/clusters?profile=large", ""))

	var result []clusterSummary
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 1 || result[0].Profile != "large" {
		t.Fatalf("expected only large-profile cluster, got %+v", result)
	}
}

// ── Get cluster tests ─────────────────────────────────────────────────────────

func TestGetCluster_OK(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, idleClusterObj("c1", "default"))

	h := &GetClusterHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := req("GET", "/api/v1/clusters/c1", "")
	r.SetPathValue("name", "c1")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result clusterSummary
	json.NewDecoder(w.Body).Decode(&result)
	if result.Name != "c1" {
		t.Fatalf("expected cluster c1, got %s", result.Name)
	}
}

func TestGetCluster_NotFound(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s)

	h := &GetClusterHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := req("GET", "/api/v1/clusters/missing", "")
	r.SetPathValue("name", "missing")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// ── List pools tests ──────────────────────────────────────────────────────────

func TestListPools_OK(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, poolObj("pool-a", 3), poolObj("pool-b", 2))

	h := &ListPoolsHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req("GET", "/api/v1/pools", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result []poolSummary
	json.NewDecoder(w.Body).Decode(&result)
	if len(result) != 2 {
		t.Fatalf("expected 2 pools, got %d", len(result))
	}
}

// ── Scale pool tests ──────────────────────────────────────────────────────────

func TestScalePool_OK(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, poolObj("pool-a", 3))

	h := &ScalePoolHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := req("PATCH", "/api/v1/pools/pool-a/replicas", `{"replicas":5}`)
	r.SetPathValue("name", "pool-a")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var result poolSummary
	json.NewDecoder(w.Body).Decode(&result)
	if result.Replicas != 5 {
		t.Fatalf("expected replicas=5, got %d", result.Replicas)
	}
}

func TestScalePool_InvalidReplicas(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s, poolObj("pool-a", 3))

	h := &ScalePoolHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := req("PATCH", "/api/v1/pools/pool-a/replicas", `{"replicas":0}`)
	r.SetPathValue("name", "pool-a")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestScalePool_NotFound(t *testing.T) {
	s := newTestScheme()
	k8s := newTestClient(s)

	h := &ScalePoolHandler{Client: k8s, Namespace: testNamespace}
	w := httptest.NewRecorder()
	r := req("PATCH", "/api/v1/pools/missing/replicas", `{"replicas":3}`)
	r.SetPathValue("name", "missing")
	h.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}
