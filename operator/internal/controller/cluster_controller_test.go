package controller

import (
	"context"
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
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: "1",
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
