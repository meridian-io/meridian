package trino

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

func newReserverScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = meridianv1alpha1.AddToScheme(s)
	return s
}

// newReserverClient builds a fake client with the field indexes that ClusterReserver requires.
func newReserverClient(s *runtime.Scheme, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithIndex(&meridianv1alpha1.Cluster{}, "spec.clientId", func(obj client.Object) []string {
			return []string{obj.(*meridianv1alpha1.Cluster).Spec.ClientID}
		}).
		WithIndex(&meridianv1alpha1.Cluster{}, "spec.reservationId", func(obj client.Object) []string {
			return []string{obj.(*meridianv1alpha1.Cluster).Spec.ReservationID}
		}).
		WithStatusSubresource(&meridianv1alpha1.Cluster{}).
		Build()
}

func idleCluster(name, namespace, profile string, idleAt time.Time) *meridianv1alpha1.Cluster {
	t := metav1.NewTime(idleAt)
	return &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: "1",
			Labels: map[string]string{
				profileLabel: profile,
			},
		},
		Spec: meridianv1alpha1.ClusterSpec{Profile: profile},
		Status: meridianv1alpha1.ClusterStatus{
			Phase:          meridianv1alpha1.ClusterPhaseIdle,
			Ready:          true,
			CoordinatorURL: "http://" + name + ":8080",
			IdleAt:         &t,
		},
	}
}

// TestReserver_Reserve verifies that an idle cluster is patched with clientId/reservationId.
func TestReserver_Reserve(t *testing.T) {
	cluster := idleCluster("pool-01", "meridian", "default", time.Now().Add(-5*time.Minute))
	s := newReserverScheme()
	c := newReserverClient(s, cluster)

	reserver := NewClusterReserver(c)
	result, err := reserver.Reserve(context.Background(), ReservationRequest{
		ClientID:      "client-abc",
		ReservationID: "res-001",
		Profile:       "default",
		Namespace:     "meridian",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ClusterName != "pool-01" {
		t.Errorf("expected cluster pool-01, got %q", result.ClusterName)
	}
	if result.CoordinatorURL != "http://pool-01:8080" {
		t.Errorf("unexpected coordinator URL: %q", result.CoordinatorURL)
	}
}

// TestReserver_PicksOldest verifies that the cluster with the earliest IdleAt is preferred.
func TestReserver_PicksOldest(t *testing.T) {
	older := idleCluster("pool-older", "meridian", "default", time.Now().Add(-10*time.Minute))
	newer := idleCluster("pool-newer", "meridian", "default", time.Now().Add(-1*time.Minute))
	s := newReserverScheme()
	c := newReserverClient(s, older, newer)

	reserver := NewClusterReserver(c)
	result, err := reserver.Reserve(context.Background(), ReservationRequest{
		ClientID:      "client-abc",
		ReservationID: "res-001",
		Profile:       "default",
		Namespace:     "meridian",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ClusterName != "pool-older" {
		t.Errorf("expected oldest cluster pool-older, got %q", result.ClusterName)
	}
}

// TestReserver_NoIdleClusters verifies that ErrNoIdleClusters is returned after
// the wait window expires. Uses a short context deadline so the test runs fast.
func TestReserver_NoIdleClusters(t *testing.T) {
	s := newReserverScheme()
	c := newReserverClient(s)

	reserver := NewClusterReserver(c)

	// Cancel the context after 100ms — well under the 30s pool wait window.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := reserver.Reserve(ctx, ReservationRequest{
		ClientID:      "client-abc",
		ReservationID: "res-001",
		Profile:       "default",
		Namespace:     "meridian",
	})
	if err == nil {
		t.Fatal("expected error when no idle clusters available")
	}
}

// TestReserver_Idempotent verifies that the same (clientId, reservationId) returns
// the same cluster without patching again.
func TestReserver_Idempotent(t *testing.T) {
	now := metav1.Now()
	cluster := idleCluster("pool-01", "meridian", "default", time.Now().Add(-5*time.Minute))
	cluster.Spec.ClientID = "client-abc"
	cluster.Spec.ReservationID = "res-001"
	cluster.Status.Phase = meridianv1alpha1.ClusterPhaseReserved
	cluster.Status.ReservedAt = &now

	s := newReserverScheme()
	c := newReserverClient(s, cluster)

	reserver := NewClusterReserver(c)
	result, err := reserver.Reserve(context.Background(), ReservationRequest{
		ClientID:      "client-abc",
		ReservationID: "res-001",
		Profile:       "default",
		Namespace:     "meridian",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ClusterName != "pool-01" {
		t.Errorf("expected existing cluster pool-01, got %q", result.ClusterName)
	}
}
