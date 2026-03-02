package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

func newPool(name, namespace string, replicas int32) *meridianv1alpha1.ClusterPool {
	return &meridianv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: "1",
		},
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

func poolCluster(pool *meridianv1alpha1.ClusterPool, suffix string, phase meridianv1alpha1.ClusterPhase) *meridianv1alpha1.Cluster {
	return &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("%s-%s", pool.Name, suffix),
			Namespace:       pool.Namespace,
			ResourceVersion: "1",
			Labels:          map[string]string{poolLabel: pool.Name},
		},
		Status: meridianv1alpha1.ClusterStatus{Phase: phase},
	}
}

// TestClusterPoolController_ScaleUp verifies that missing clusters are created.
func TestClusterPoolController_ScaleUp(t *testing.T) {
	pool := newPool("test-pool", "meridian", 3)
	s := newTestScheme()

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pool).
		WithStatusSubresource(pool).
		Build()

	r := &ClusterPoolController{Client: c, Scheme: s}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-pool", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list := &meridianv1alpha1.ClusterList{}
	_ = c.List(context.Background(), list)
	if len(list.Items) != 3 {
		t.Errorf("expected 3 clusters created, got %d", len(list.Items))
	}
}

// TestClusterPoolController_ScaleDown verifies that the oldest idle cluster is deleted
// when the pool is over-provisioned.
func TestClusterPoolController_ScaleDown(t *testing.T) {
	pool := newPool("test-pool", "meridian", 1)
	s := newTestScheme()

	older := poolCluster(pool, "older", meridianv1alpha1.ClusterPhaseIdle)
	oldTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	older.Status.IdleAt = &oldTime

	newer := poolCluster(pool, "newer", meridianv1alpha1.ClusterPhaseIdle)
	newTime := metav1.Now()
	newer.Status.IdleAt = &newTime

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pool, older, newer).
		WithStatusSubresource(pool).
		Build()

	r := &ClusterPoolController{Client: c, Scheme: s}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-pool", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list := &meridianv1alpha1.ClusterList{}
	_ = c.List(context.Background(), list)
	if len(list.Items) != 1 {
		t.Errorf("expected 1 cluster after scale down, got %d", len(list.Items))
	}
	// The older cluster should have been deleted — only the newer one remains.
	if list.Items[0].Name != newer.Name {
		t.Errorf("expected newer cluster %q to survive, got %q", newer.Name, list.Items[0].Name)
	}
}

// TestClusterPoolController_DeleteFailed verifies that failed clusters are removed immediately.
func TestClusterPoolController_DeleteFailed(t *testing.T) {
	pool := newPool("test-pool", "meridian", 2)
	s := newTestScheme()

	idle := poolCluster(pool, "idle", meridianv1alpha1.ClusterPhaseIdle)
	failed := poolCluster(pool, "failed", meridianv1alpha1.ClusterPhaseFailed)

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pool, idle, failed).
		WithStatusSubresource(pool).
		Build()

	r := &ClusterPoolController{Client: c, Scheme: s}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-pool", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Failed cluster should be gone.
	list := &meridianv1alpha1.ClusterList{}
	_ = c.List(context.Background(), list)
	for _, cl := range list.Items {
		if cl.Status.Phase == meridianv1alpha1.ClusterPhaseFailed {
			t.Errorf("failed cluster %q should have been deleted", cl.Name)
		}
	}
}

// TestClusterPoolController_NoChangeAtDesired verifies no creates/deletes when at desired count.
func TestClusterPoolController_NoChangeAtDesired(t *testing.T) {
	pool := newPool("test-pool", "meridian", 2)
	s := newTestScheme()

	c1 := poolCluster(pool, "c1", meridianv1alpha1.ClusterPhaseIdle)
	c2 := poolCluster(pool, "c2", meridianv1alpha1.ClusterPhaseIdle)

	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pool, c1, c2).
		WithStatusSubresource(pool).
		Build()

	r := &ClusterPoolController{Client: c, Scheme: s}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-pool", Namespace: "meridian"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	list := &meridianv1alpha1.ClusterList{}
	_ = c.List(context.Background(), list)
	if len(list.Items) != 2 {
		t.Errorf("expected 2 clusters unchanged, got %d", len(list.Items))
	}
}
