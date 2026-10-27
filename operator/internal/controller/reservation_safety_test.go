package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/credentials"
)

var clusterKey = types.NamespacedName{Name: "test-cluster", Namespace: "meridian"}

func getCluster(t *testing.T, c client.Client, key types.NamespacedName) *meridianv1alpha1.Cluster {
	t.Helper()
	got := &meridianv1alpha1.Cluster{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("get %s: %v", key.Name, err)
	}
	return got
}

// A reserved cluster whose coordinator goes down stays Reserved with its
// owner. It used to go back to Pending, and a cluster older than the pending
// timeout was then marked Failed and deleted by the pool.
func TestClusterController_ReservedCoordinatorDownStaysReserved(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseReserved)
	cluster.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	reservedAt := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	cluster.Status.ReservedAt = &reservedAt
	cluster.Spec.ClientID = "app"
	cluster.Spec.ReservationID = "job-1"
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(cluster, readyDeployment("test-cluster", "coordinator", 0), readyDeployment("test-cluster", "worker", 1)).
		WithStatusSubresource(cluster).Build()
	r := &ClusterController{Client: c, Scheme: s}

	for i := 0; i < 3; i++ {
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterKey})
		if err != nil {
			t.Fatalf("reconcile %d: %v", i+1, err)
		}
		if res.RequeueAfter == 0 {
			t.Errorf("reconcile %d: no requeue while the coordinator is down", i+1)
		}
	}
	got := getCluster(t, c, clusterKey)
	if got.Status.Phase != meridianv1alpha1.ClusterPhaseReserved || got.Spec.ClientID != "app" {
		t.Fatalf("phase=%s clientId=%q, want Reserved owned by app", got.Status.Phase, got.Spec.ClientID)
	}
	if got.Status.ReservedAt == nil || got.Status.ReservedAt.Unix() != reservedAt.Unix() {
		t.Errorf("reservedAt changed from %v to %v: the reservation TTL was reset", reservedAt, got.Status.ReservedAt)
	}
}

// A reserved cluster whose coordinator Deployment was deleted gets it back.
func TestClusterController_ReservedRecreatesMissingCoordinator(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseReserved)
	cluster.Spec.ClientID = "app"
	cluster.Spec.ReservationID = "job-1"
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &ClusterController{Client: c, Scheme: s}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterKey}); err != nil {
		t.Fatal(err)
	}
	dep := &appsv1.Deployment{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster-coordinator", Namespace: "meridian"}, dep); err != nil {
		t.Fatalf("coordinator deployment not recreated: %v", err)
	}
	if got := getCluster(t, c, clusterKey); got.Status.Phase != meridianv1alpha1.ClusterPhaseReserved {
		t.Errorf("phase = %s, want Reserved", got.Status.Phase)
	}
}

// rotationCluster is a cluster with a pending rotation that is backing off.
func rotationCluster(phase meridianv1alpha1.ClusterPhase) *meridianv1alpha1.Cluster {
	cluster := newCluster("test-cluster", "meridian", phase)
	cluster.Annotations = map[string]string{credentials.AnnotationRotateCredentials: "kubernetes/sales/missing-secret"}
	cluster.Status.RotationFailures = 3
	cluster.Status.Conditions = []metav1.Condition{{
		Type: "CredentialRotation", Status: metav1.ConditionFalse, Reason: "RotationFailed",
		LastTransitionTime: metav1.Now(),
	}}
	return cluster
}

// A reservation is recorded even while a credential rotation is backing off.
func TestClusterController_ReservationNotBlockedByRotation(t *testing.T) {
	cluster := rotationCluster(meridianv1alpha1.ClusterPhaseIdle)
	cluster.Spec.ClientID = "app"
	cluster.Spec.ReservationID = "job-1"
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &ClusterController{Client: c, Scheme: s}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterKey}); err != nil {
		t.Fatal(err)
	}
	got := getCluster(t, c, clusterKey)
	if got.Status.Phase != meridianv1alpha1.ClusterPhaseReserved || got.Status.ReservedAt == nil {
		t.Errorf("phase=%s reservedAt=%v, want Reserved with reservedAt", got.Status.Phase, got.Status.ReservedAt)
	}
}

// A release is recorded even while a credential rotation is backing off.
func TestClusterController_ReleaseNotBlockedByRotation(t *testing.T) {
	cluster := rotationCluster(meridianv1alpha1.ClusterPhaseReserved)
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &ClusterController{Client: c, Scheme: s}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterKey}); err != nil {
		t.Fatal(err)
	}
	if got := getCluster(t, c, clusterKey); got.Status.Phase != meridianv1alpha1.ClusterPhaseIdle {
		t.Errorf("phase = %s, want Idle", got.Status.Phase)
	}
}

// An API timeout while creating a cluster's resources is retried; it no
// longer marks the cluster Failed (which made the pool delete it).
func TestClusterController_TransientErrorDoesNotFailCluster(t *testing.T) {
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseEmpty)
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*appsv1.Deployment); ok {
					return apierrors.NewServerTimeout(schema.GroupResource{Group: "apps", Resource: "deployments"}, "create", 1)
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).Build()
	r := &ClusterController{Client: c, Scheme: s}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterKey}); err == nil {
		t.Error("expected the error to be returned for a retry")
	}
	if got := getCluster(t, c, clusterKey); got.Status.Phase == meridianv1alpha1.ClusterPhaseFailed {
		t.Error("cluster marked Failed after a transient API error")
	}
}

func TestIsTransient(t *testing.T) {
	gr := schema.GroupResource{Resource: "deployments"}
	for _, err := range []error{
		apierrors.NewServerTimeout(gr, "create", 1), apierrors.NewTooManyRequests("slow down", 1),
		apierrors.NewInternalError(errors.New("etcd")), context.DeadlineExceeded,
	} {
		if !isTransient(err) {
			t.Errorf("isTransient(%v) = false", err)
		}
	}
	if isTransient(apierrors.NewForbidden(gr, "x", errors.New("no"))) {
		t.Error("Forbidden treated as transient")
	}
}

// ── pool ─────────────────────────────────────────────────────────────────────

func reconcilePool(t *testing.T, c client.Client, pool *meridianv1alpha1.ClusterPool) {
	t.Helper()
	r := &ClusterPoolController{Client: c, Scheme: c.Scheme()}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace}}); err != nil {
		t.Fatalf("pool reconcile: %v", err)
	}
}

func liveNames(t *testing.T, c client.Client) map[string]bool {
	t.Helper()
	list := &meridianv1alpha1.ClusterList{}
	if err := c.List(context.Background(), list); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, cl := range list.Items {
		if cl.DeletionTimestamp == nil {
			out[cl.Name] = true
		}
	}
	return out
}

func idlePoolCluster(pool *meridianv1alpha1.ClusterPool, suffix string, idleFor time.Duration) *meridianv1alpha1.Cluster {
	c := poolCluster(pool, suffix, meridianv1alpha1.ClusterPhaseIdle)
	at := metav1.NewTime(time.Now().Add(-idleFor))
	c.Status.IdleAt = &at
	c.Status.Ready = true
	c.CreationTimestamp = at
	return c
}

// Scale-down picks the longest-idle cluster, which is also the one the
// reserver hands out first. A cluster claimed a moment ago (clientId set,
// still Idle) must not be the one deleted.
func TestClusterPool_ScaleDownSkipsClaimedCluster(t *testing.T) {
	pool := newPool("p", "meridian", 1)
	claimed := idlePoolCluster(pool, "claimed", time.Hour)
	claimed.Spec.ClientID, claimed.Spec.ReservationID = "app", "job-1"
	free := idlePoolCluster(pool, "free", time.Minute)
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(pool, claimed, free).
		WithStatusSubresource(pool, claimed, free).Build()

	reconcilePool(t, c, pool)
	names := liveNames(t, c)
	if !names["p-claimed"] {
		t.Fatal("the claimed cluster was deleted")
	}
	if names["p-free"] {
		t.Error("the free idle cluster survived; the pool is over its size")
	}
}

// Rolling upgrades and age recycling must not delete a claimed cluster either.
func TestClusterPool_RecyclingSkipsClaimedCluster(t *testing.T) {
	pool := newPool("p", "meridian", 1)
	pool.Spec.MaxClusterAge = "30m"
	claimed := idlePoolCluster(pool, "claimed", time.Hour) // old enough to recycle
	claimed.Spec.Image = "trinodb/trino:434"               // and on an old image
	claimed.Spec.ClientID, claimed.Spec.ReservationID = "app", "job-1"
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(pool, claimed).
		WithStatusSubresource(pool, claimed).Build()

	reconcilePool(t, c, pool)
	if !liveNames(t, c)["p-claimed"] {
		t.Fatal("the claimed cluster was deleted by a rolling upgrade or age recycling")
	}
}

// A cluster that is already being deleted doesn't count toward the pool, so
// the pool doesn't delete a healthy cluster to make up for it.
func TestClusterPool_TerminatingClusterNotCounted(t *testing.T) {
	pool := newPool("p", "meridian", 2)
	healthy := idlePoolCluster(pool, "aaa", time.Hour) // oldest: first in line for scale-down
	leaving := idlePoolCluster(pool, "zzz", time.Minute)
	now := metav1.Now()
	leaving.DeletionTimestamp = &now
	leaving.Finalizers = []string{clusterFinalizer}
	starting := poolCluster(pool, "ppp", meridianv1alpha1.ClusterPhasePending)
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(pool, healthy, leaving, starting).
		WithStatusSubresource(pool, healthy, leaving, starting).Build()

	reconcilePool(t, c, pool)
	if !liveNames(t, c)["p-aaa"] {
		t.Fatal("a healthy idle cluster was deleted while another was still terminating")
	}
}

// The pool deletes a cluster only if it hasn't changed since the pool read
// it: a reservation that lands in between wins.
func TestClusterPool_DeleteIsConditional(t *testing.T) {
	pool := newPool("p", "meridian", 1)
	cl := idlePoolCluster(pool, "a", time.Hour)
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(pool, cl).Build()
	stale := cl.DeepCopy()
	// Someone reserves the cluster after the pool read it.
	fresh := getCluster(t, c, types.NamespacedName{Name: "p-a", Namespace: "meridian"})
	fresh.Spec.ClientID, fresh.Spec.ReservationID = "app", "job-1"
	if err := c.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}

	r := &ClusterPoolController{Client: c, Scheme: c.Scheme()}
	if err := r.deleteCluster(context.Background(), pool, stale, "scale_down"); err != nil {
		t.Fatalf("deleteCluster: %v", err)
	}
	if !liveNames(t, c)["p-a"] {
		t.Fatal("deleted a cluster that was reserved after the pool read it")
	}
}
