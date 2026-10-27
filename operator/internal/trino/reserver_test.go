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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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
	return idleClusterWithWorkload(name, namespace, profile, "", idleAt)
}

func idleClusterWithWorkload(name, namespace, profile, workload string, idleAt time.Time) *meridianv1alpha1.Cluster {
	t := metav1.NewTime(idleAt)
	labels := map[string]string{profileLabel: profile}
	if workload != "" {
		labels[workloadLabel] = workload
	}
	return &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: "1",
			Labels:          labels,
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

	reserver := NewClusterReserver(c, nil)
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

	reserver := NewClusterReserver(c, nil)
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

	reserver := NewClusterReserver(c, nil)

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

// TestReserver_WorkloadMatch verifies that a reservation with a workload filter only
// selects clusters labeled with that workload.
func TestReserver_WorkloadMatch(t *testing.T) {
	analytics := idleClusterWithWorkload("analytics-01", "meridian", "default", "analytics", time.Now().Add(-5*time.Minute))
	etl := idleClusterWithWorkload("etl-01", "meridian", "default", "etl", time.Now().Add(-10*time.Minute))
	s := newReserverScheme()
	c := newReserverClient(s, analytics, etl)

	reserver := NewClusterReserver(c, nil)
	result, err := reserver.Reserve(context.Background(), ReservationRequest{
		ClientID:      "client-abc",
		ReservationID: "res-001",
		Profile:       "default",
		Workload:      "analytics",
		Namespace:     "meridian",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ClusterName != "analytics-01" {
		t.Errorf("expected analytics-01, got %q", result.ClusterName)
	}
}

// TestReserver_WorkloadNoMatch verifies that no cluster is returned when no cluster
// has the requested workload label.
func TestReserver_WorkloadNoMatch(t *testing.T) {
	etl := idleClusterWithWorkload("etl-01", "meridian", "default", "etl", time.Now().Add(-5*time.Minute))
	s := newReserverScheme()
	c := newReserverClient(s, etl)

	reserver := NewClusterReserver(c, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := reserver.Reserve(ctx, ReservationRequest{
		ClientID:      "client-abc",
		ReservationID: "res-001",
		Profile:       "default",
		Workload:      "analytics",
		Namespace:     "meridian",
	})
	if err == nil {
		t.Fatal("expected error when no cluster matches the workload")
	}
}

// TestReserver_WorkloadEmpty verifies backward compatibility: an empty workload
// falls back to profile-only selection and considers all matching clusters.
func TestReserver_WorkloadEmpty(t *testing.T) {
	c1 := idleClusterWithWorkload("pool-01", "meridian", "default", "analytics", time.Now().Add(-5*time.Minute))
	c2 := idleClusterWithWorkload("pool-02", "meridian", "default", "etl", time.Now().Add(-3*time.Minute))
	s := newReserverScheme()
	fc := newReserverClient(s, c1, c2)

	reserver := NewClusterReserver(fc, nil)
	result, err := reserver.Reserve(context.Background(), ReservationRequest{
		ClientID:      "client-abc",
		ReservationID: "res-001",
		Profile:       "default",
		Workload:      "",
		Namespace:     "meridian",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Oldest by idleAt should win regardless of workload label.
	if result.ClusterName != "pool-01" {
		t.Errorf("expected oldest cluster pool-01, got %q", result.ClusterName)
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

	reserver := NewClusterReserver(c, nil)
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

// TestReserver_RetryBeforeStatusUpdate verifies that repeating a reservation
// before the cluster controller has set status.reservedAt returns the same
// cluster. A client retrying after a timeout hits this window; it used to
// panic and drop the connection.
func TestReserver_RetryBeforeStatusUpdate(t *testing.T) {
	cluster := idleCluster("pool-01", "meridian", "default", time.Now().Add(-5*time.Minute))
	c := newReserverClient(newReserverScheme(), cluster)
	reserver := NewClusterReserver(c, nil)
	req := ReservationRequest{ClientID: "client-abc", ReservationID: "res-001", Profile: "default", Namespace: "meridian"}

	first, err := reserver.Reserve(context.Background(), req)
	if err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	// No controller runs here, so status.reservedAt is still unset.
	again, err := reserver.Reserve(context.Background(), req)
	if err != nil {
		t.Fatalf("repeated reserve: %v", err)
	}
	if again.ClusterName != first.ClusterName || again.CoordinatorURL != first.CoordinatorURL {
		t.Errorf("repeated reserve returned %+v, want %+v", again, first)
	}
	if again.ReservedAt.IsZero() {
		t.Error("repeated reserve returned a zero ReservedAt")
	}
}

// Two reservations arrive before the cluster controller marks the first
// cluster Reserved. The second must get a different cluster, not overwrite
// the first reservation.
func TestReserver_BackToBackReservationsGetDifferentClusters(t *testing.T) {
	a := idleCluster("pool-a", "meridian", "default", time.Now().Add(-5*time.Minute))
	b := idleCluster("pool-b", "meridian", "default", time.Now().Add(-1*time.Minute))
	c := newReserverClient(newReserverScheme(), a, b)
	r := NewClusterReserver(c, nil)
	ctx := context.Background()

	r1, err := r.Reserve(ctx, ReservationRequest{ClientID: "app", ReservationID: "job-1", Profile: "default", Namespace: "meridian"})
	if err != nil {
		t.Fatal(err)
	}
	r2, err := r.Reserve(ctx, ReservationRequest{ClientID: "app", ReservationID: "job-2", Profile: "default", Namespace: "meridian"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.ClusterName == r2.ClusterName {
		t.Fatalf("both reservations got %s", r1.ClusterName)
	}
	got := &meridianv1alpha1.Cluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "meridian", Name: r1.ClusterName}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.ReservationID != "job-1" {
		t.Errorf("%s has reservationId %q, want job-1", r1.ClusterName, got.Spec.ReservationID)
	}
}

var job1 = ReservationRequest{ClientID: "app", ReservationID: "job-1", Profile: "default", Namespace: "meridian"}

// staleLister reads lists from an out-of-date copy (like the informer cache
// right after a write) and sends everything else to the API server.
type staleLister struct {
	client.Client
	stale client.Client
}

func (s staleLister) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return s.stale.List(ctx, list, opts...)
}

// A retry sent right after a reservation must get the same cluster even when
// the cache hasn't seen the reservation yet: the check reads the API server.
func TestReserver_RetryWithStaleCacheGetsSameCluster(t *testing.T) {
	s := newReserverScheme()
	at := time.Now().Add(-5 * time.Minute)
	stale := newReserverClient(s, idleCluster("pool-a", "meridian", "default", at), idleCluster("pool-b", "meridian", "default", at.Add(time.Minute)))
	api := newReserverClient(s, idleCluster("pool-a", "meridian", "default", at), idleCluster("pool-b", "meridian", "default", at.Add(time.Minute)))
	// The first request reserved pool-a; the cache hasn't caught up.
	a := &meridianv1alpha1.Cluster{}
	_ = api.Get(context.Background(), client.ObjectKey{Namespace: "meridian", Name: "pool-a"}, a)
	a.Spec.ClientID, a.Spec.ReservationID = "app", "job-1"
	if err := api.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}

	got, err := NewClusterReserver(staleLister{Client: api, stale: stale}, api).Reserve(context.Background(), job1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClusterName != "pool-a" {
		t.Fatalf("retry got %s, want pool-a (the cluster it already holds)", got.ClusterName)
	}
	b := &meridianv1alpha1.Cluster{}
	_ = api.Get(context.Background(), client.ObjectKey{Namespace: "meridian", Name: "pool-b"}, b)
	if b.Spec.ClientID != "" {
		t.Error("the retry also claimed pool-b")
	}
}

// Two identical requests waiting for capacity: when one gets a cluster, the
// other returns that cluster instead of claiming a second one.
func TestReserver_WaitingDuplicateReturnsSameCluster(t *testing.T) {
	c := newReserverClient(newReserverScheme())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cl := idleCluster("pool-a", "meridian", "default", time.Now())
		cl.ResourceVersion = ""
		cl.Spec.ClientID, cl.Spec.ReservationID = "app", "job-1"
		_ = c.Create(context.Background(), cl)
		other := idleCluster("pool-b", "meridian", "default", time.Now())
		other.ResourceVersion = ""
		_ = c.Create(context.Background(), other)
	}()
	got, err := NewClusterReserver(c, nil).Reserve(context.Background(), job1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClusterName != "pool-a" {
		t.Fatalf("got %s, want pool-a (reserved by the duplicate)", got.ClusterName)
	}
}

// Two identical requests race for the same cluster: the loser's patch
// conflicts, and it returns the winner's cluster.
func TestReserver_ConflictWithDuplicateReturnsItsCluster(t *testing.T) {
	s := newReserverScheme()
	first := true
	var c client.Client
	c = fake.NewClientBuilder().WithScheme(s).
		WithObjects(idleCluster("pool-a", "meridian", "default", time.Now().Add(-time.Hour)),
			idleCluster("pool-b", "meridian", "default", time.Now())).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if first {
					first = false
					// The duplicate request claims pool-a just before us.
					won := &meridianv1alpha1.Cluster{}
					_ = cl.Get(ctx, client.ObjectKeyFromObject(obj), won)
					won.Spec.ClientID, won.Spec.ReservationID = "app", "job-1"
					_ = cl.Update(ctx, won)
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).Build()

	got, err := NewClusterReserver(c, nil).Reserve(context.Background(), job1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClusterName != "pool-a" {
		t.Fatalf("got %s, want pool-a", got.ClusterName)
	}
	b := &meridianv1alpha1.Cluster{}
	_ = c.Get(context.Background(), client.ObjectKey{Namespace: "meridian", Name: "pool-b"}, b)
	if b.Spec.ClientID != "" {
		t.Error("a second cluster was claimed for the same reservation")
	}
}

// A failed cluster still carrying the reservation is not handed back.
func TestReserver_FailedReservationGetsNewCluster(t *testing.T) {
	failed := idleCluster("pool-a", "meridian", "default", time.Now().Add(-time.Hour))
	failed.Status.Phase = meridianv1alpha1.ClusterPhaseFailed
	failed.Spec.ClientID, failed.Spec.ReservationID = "app", "job-1"
	c := newReserverClient(newReserverScheme(), failed, idleCluster("pool-b", "meridian", "default", time.Now()))
	got, err := NewClusterReserver(c, nil).Reserve(context.Background(), job1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClusterName != "pool-b" {
		t.Errorf("got %s, want pool-b", got.ClusterName)
	}
}

// With equal idle times, the order is by name, the same for every request.
func TestReserver_PickOrderIsDeterministic(t *testing.T) {
	at := time.Now().Add(-time.Minute)
	c := newReserverClient(newReserverScheme(),
		idleCluster("pool-c", "meridian", "default", at),
		idleCluster("pool-a", "meridian", "default", at),
		idleCluster("pool-b", "meridian", "default", at))
	for i := 0; i < 5; i++ {
		best, err := NewClusterReserver(c, nil).pickIdleCluster(context.Background(), job1)
		if err != nil || best.Name != "pool-a" {
			t.Fatalf("pick %d: %v %v, want pool-a", i, best, err)
		}
	}
}
