package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/credentials"
)

func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = meridianv1alpha1.AddToScheme(s)
	return s
}

func newCluster(name, namespace string, phase meridianv1alpha1.ClusterPhase) *meridianv1alpha1.Cluster {
	return &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			ResourceVersion:   "1",
			CreationTimestamp: metav1.Now(), // prevents pending-timeout from firing in tests
		},
		Spec: meridianv1alpha1.ClusterSpec{
			Profile: "default",
			Image:   "trinodb/trino:435",
			Workers: 2,
		},
		Status: meridianv1alpha1.ClusterStatus{
			Phase: phase,
		},
	}
}

// TestClusterController_EmptyTosPending verifies that a cluster in Empty phase
// gets coordinator deployment, worker deployment, and service created, then moves to Pending.
func TestClusterController_EmptyToPending(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseEmpty)
	s := newTestScheme()

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &ClusterController{Client: c, Scheme: s}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue after health check delay")
	}

	// Coordinator deployment should exist.
	dep := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster-coordinator", Namespace: "meridian"}, dep); err != nil {
		t.Errorf("coordinator deployment not created: %v", err)
	}

	// Worker deployment should exist.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster-worker", Namespace: "meridian"}, dep); err != nil {
		t.Errorf("worker deployment not created: %v", err)
	}

	// Service should exist.
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster-coordinator", Namespace: "meridian"}, svc); err != nil {
		t.Errorf("coordinator service not created: %v", err)
	}

	// Phase should be Pending.
	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)
	if updated.Status.Phase != meridianv1alpha1.ClusterPhasePending {
		t.Errorf("expected phase Pending, got %q", updated.Status.Phase)
	}
}

// TestClusterController_PendingToIdle verifies that a healthy coordinator transitions
// the cluster from Pending to Idle.
func TestClusterController_PendingToIdle(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhasePending)
	ready := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-coordinator",
			Namespace: "meridian",
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
	s := newTestScheme()

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster, dep).
		WithStatusSubresource(cluster).
		Build()

	r := &ClusterController{Client: c, Scheme: s}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)
	if updated.Status.Phase != meridianv1alpha1.ClusterPhaseIdle {
		t.Errorf("expected phase Idle, got %q", updated.Status.Phase)
	}
	if !updated.Status.Ready {
		t.Error("expected Ready=true")
	}
	if updated.Status.IdleAt == nil {
		t.Error("expected IdleAt to be set")
	}
}

// TestClusterController_PendingUnhealthy verifies that an unhealthy coordinator
// causes a requeue without phase change.
func TestClusterController_PendingUnhealthy(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhasePending)
	s := newTestScheme()

	// No coordinator deployment — health check returns false.
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &ClusterController{Client: c, Scheme: s}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue while waiting for health")
	}

	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)
	if updated.Status.Phase != meridianv1alpha1.ClusterPhasePending {
		t.Errorf("expected phase to remain Pending, got %q", updated.Status.Phase)
	}
}

// TestClusterController_IdleToReserved verifies that setting clientId + reservationId
// on an Idle cluster transitions it to Reserved.
func TestClusterController_IdleToReserved(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseIdle)
	cluster.Spec.ClientID = "client-abc"
	cluster.Spec.ReservationID = "res-123"
	s := newTestScheme()

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &ClusterController{Client: c, Scheme: s}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)
	if updated.Status.Phase != meridianv1alpha1.ClusterPhaseReserved {
		t.Errorf("expected phase Reserved, got %q", updated.Status.Phase)
	}
	if updated.Status.ReservedAt == nil {
		t.Error("expected ReservedAt to be set")
	}
}

// TestClusterController_ReservedToIdle verifies that clearing clientId + reservationId
// on a Reserved cluster returns it to Idle.
func TestClusterController_ReservedToIdle(t *testing.T) {
	now := metav1.Now()
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseReserved)
	cluster.Status.ReservedAt = &now
	// clientId and reservationId are empty — cluster has been released.
	cluster.Spec.ClientID = ""
	cluster.Spec.ReservationID = ""
	s := newTestScheme()

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &ClusterController{Client: c, Scheme: s}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)
	if updated.Status.Phase != meridianv1alpha1.ClusterPhaseIdle {
		t.Errorf("expected phase Idle after release, got %q", updated.Status.Phase)
	}
	if updated.Status.IdleAt == nil {
		t.Error("expected IdleAt to be set after release")
	}
}

// TestClusterController_FailedSkipped verifies that Failed clusters are not reconciled.
func TestClusterController_FailedSkipped(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseFailed)
	s := newTestScheme()

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := &ClusterController{Client: c, Scheme: s}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Error("expected no requeue for Failed cluster")
	}
}

// ── Credential rotation tests ─────────────────────────────────────────────────

// newTrinoServer returns an httptest.Server that responds FINISHED to every
// POST /v1/statement — simulates a Trino cluster accepting DDL statements.
func newTrinoServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		type resp struct {
			Stats struct {
				State string `json:"state"`
			} `json:"stats"`
		}
		var v resp
		v.Stats.State = "FINISHED"
		json.NewEncoder(w).Encode(v)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newRotationController builds a ClusterController wired with the kubernetes
// credential provider pointing at the fake client.
func newRotationController(c interface{ Get(context.Context, types.NamespacedName, interface{ DeepCopyObject() runtime.Object }, ...interface{}) error }, s *runtime.Scheme, fakeClient interface{}) *ClusterController {
	// We accept the fake client directly; callers pass a sigs client.Client.
	return nil // replaced below
}

// TestClusterController_RotationSuccess verifies that a rotation annotation
// causes the catalog to be rotated and the annotation to be cleared.
func TestClusterController_RotationSuccess(t *testing.T) {
	trino := newTrinoServer(t)

	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseIdle)
	cluster.Annotations = map[string]string{
		credentials.AnnotationRotateCredentials: "kubernetes/mysql_prod/mysql-secret",
	}
	cluster.Status.CoordinatorURL = trino.URL

	k8sSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-secret", Namespace: "meridian"},
		Data: map[string][]byte{
			"connector.name":      []byte("mysql"),
			"connection-url":      []byte("jdbc:mysql://mysql:3306"),
			"connection-password": []byte("newpass"),
		},
	}

	s := newTestScheme()
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster, k8sSecret).
		WithStatusSubresource(cluster).
		Build()

	provider := credentials.NewKubernetesProvider(c, "meridian")
	cache := map[string]*credentials.SecretCache{
		"kubernetes": credentials.NewSecretCache(provider, credentials.DefaultTTL),
	}

	r := &ClusterController{
		Client:    c,
		Scheme:    s,
		Providers: map[string]credentials.SecretProvider{"kubernetes": provider},
		Rotator:   credentials.NewRotator(),
		Cache:     cache,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Annotation must be cleared.
	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)
	if ann := updated.Annotations[credentials.AnnotationRotateCredentials]; ann != "" {
		t.Errorf("expected annotation to be cleared, got %q", ann)
	}

	// LastRotatedAt must be set.
	if updated.Status.LastRotatedAt == nil {
		t.Error("expected LastRotatedAt to be set after successful rotation")
	}

	// RotationFailures must be reset.
	if updated.Status.RotationFailures != 0 {
		t.Errorf("expected RotationFailures=0 after success, got %d", updated.Status.RotationFailures)
	}

	// RotatingCatalog must be cleared.
	if updated.Status.RotatingCatalog != "" {
		t.Errorf("expected RotatingCatalog to be cleared, got %q", updated.Status.RotatingCatalog)
	}
}

// TestClusterController_RotationBackoff verifies that a rotation failure
// increments RotationFailures and requeues with exponential backoff.
func TestClusterController_RotationBackoff(t *testing.T) {
	// No Trino server — rotation will fail with a connection error.
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseIdle)
	cluster.Annotations = map[string]string{
		credentials.AnnotationRotateCredentials: "kubernetes/mysql_prod/mysql-secret",
	}
	cluster.Status.CoordinatorURL = "http://localhost:19999" // nothing listening
	cluster.Status.RotationFailures = 2                      // already failed twice

	k8sSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-secret", Namespace: "meridian"},
		Data: map[string][]byte{
			"connector.name":      []byte("mysql"),
			"connection-password": []byte("pass"),
		},
	}

	s := newTestScheme()
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster, k8sSecret).
		WithStatusSubresource(cluster).
		Build()

	provider := credentials.NewKubernetesProvider(c, "meridian")
	cache := map[string]*credentials.SecretCache{
		"kubernetes": credentials.NewSecretCache(provider, credentials.DefaultTTL),
	}

	r := &ClusterController{
		Client:    c,
		Scheme:    s,
		Providers: map[string]credentials.SecretProvider{"kubernetes": provider},
		Rotator:   credentials.NewRotator(),
		Cache:     cache,
	}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// RequeueAfter must match BackoffDuration(3) = 20s.
	expected := credentials.BackoffDuration(3)
	if result.RequeueAfter != expected {
		t.Errorf("expected RequeueAfter=%v (failures=3), got %v", expected, result.RequeueAfter)
	}

	// RotationFailures must be incremented to 3.
	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)
	if updated.Status.RotationFailures != 3 {
		t.Errorf("expected RotationFailures=3, got %d", updated.Status.RotationFailures)
	}

	// Annotation must still be present — rotation will retry.
	if ann := updated.Annotations[credentials.AnnotationRotateCredentials]; ann == "" {
		t.Error("expected annotation to remain present after failed rotation")
	}
}

// TestClusterController_RotationUnknownProvider verifies that an annotation
// referencing an unconfigured provider clears the annotation without retrying
// and does not change the cluster Phase.
func TestClusterController_RotationUnknownProvider(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseIdle)
	cluster.Annotations = map[string]string{
		// Operator started with kubernetes-only; aws-secrets-manager is unknown.
		credentials.AnnotationRotateCredentials: "aws-secrets-manager/mysql_prod/arn:aws:secret:us-east-1:123:secret:trino",
	}
	cluster.Status.CoordinatorURL = "http://trino:8080"

	s := newTestScheme()
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	provider := credentials.NewKubernetesProvider(c, "meridian")

	r := &ClusterController{
		Client:    c,
		Scheme:    s,
		Providers: map[string]credentials.SecretProvider{"kubernetes": provider},
		Rotator:   credentials.NewRotator(),
		Cache: map[string]*credentials.SecretCache{
			"kubernetes": credentials.NewSecretCache(provider, credentials.DefaultTTL),
		},
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}, updated)

	// Annotation must be cleared — non-retriable error.
	if ann := updated.Annotations[credentials.AnnotationRotateCredentials]; ann != "" {
		t.Errorf("expected annotation cleared for unknown provider, got %q", ann)
	}

	// Cluster Phase must be unchanged — rotation failure ≠ cluster failure.
	if updated.Status.Phase != meridianv1alpha1.ClusterPhaseIdle {
		t.Errorf("expected phase Idle (unchanged), got %q", updated.Status.Phase)
	}
}
