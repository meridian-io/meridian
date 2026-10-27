package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

// autoscalerFixture is a pool with an autoscaler (min 2, max 4, threshold
// 0.70) and a clock the test controls.
type autoscalerFixture struct {
	t    *testing.T
	c    client.Client
	r    *ClusterPoolAutoscalerController
	now  time.Time
	pool types.NamespacedName
}

func newAutoscalerFixture(t *testing.T, replicas, ready, reserved int32, lastScale *time.Time) *autoscalerFixture {
	t.Helper()
	pool := newPool("busy-pool", "meridian", replicas)
	pool.Status = meridianv1alpha1.ClusterPoolStatus{ReadyReplicas: ready, ReservedReplicas: reserved}
	as := &meridianv1alpha1.ClusterPoolAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "busy-pool", Namespace: "meridian", ResourceVersion: "1"},
		Spec: meridianv1alpha1.ClusterPoolAutoscalerSpec{
			TargetRef:            meridianv1alpha1.TargetRef{Name: "busy-pool"},
			MinReplicas:          2,
			MaxReplicas:          4,
			UtilizationThreshold: "0.70",
		},
	}
	if lastScale != nil {
		ts := metav1.NewTime(*lastScale)
		as.Status.LastScaleTime = &ts
	}
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(pool, as).WithStatusSubresource(pool, as).Build()
	f := &autoscalerFixture{t: t, c: c, now: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC),
		pool: types.NamespacedName{Name: "busy-pool", Namespace: "meridian"}}
	f.r = &ClusterPoolAutoscalerController{Client: c, Scheme: s, Now: func() time.Time { return f.now }}
	return f
}

// reconcile runs the autoscaler once and returns the pool's spec.replicas.
func (f *autoscalerFixture) reconcile() int32 {
	f.t.Helper()
	if _, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: f.pool}); err != nil {
		f.t.Fatalf("reconcile: %v", err)
	}
	pool := &meridianv1alpha1.ClusterPool{}
	if err := f.c.Get(context.Background(), f.pool, pool); err != nil {
		f.t.Fatal(err)
	}
	return pool.Spec.Replicas
}

// A fully reserved pool of 2 needs ceil(2 / 0.70) = 3 clusters. Running again
// before the third cluster exists must not scale again: the autoscaler used to
// divide by the clusters that existed, saw 100% and went straight to max.
func TestAutoscaler_ScalesUpOnceForBusyPool(t *testing.T) {
	f := newAutoscalerFixture(t, 2, 0, 2, nil)
	if got := f.reconcile(); got != 3 {
		t.Fatalf("after first run: replicas = %d, want 3", got)
	}
	for i := 0; i < 3; i++ { // pool status still shows the old 2 clusters
		f.now = f.now.Add(30 * time.Second)
		if got := f.reconcile(); got != 3 {
			t.Fatalf("run %d before the new cluster exists: replicas = %d, want 3", i+2, got)
		}
	}
}

// Right after a scale-up, the new clusters make utilization drop. The
// autoscaler must not delete them at once; it waits scaleDownDelay.
func TestAutoscaler_WaitsBeforeScalingDown(t *testing.T) {
	f := newAutoscalerFixture(t, 4, 2, 2, nil) // 2/4 = 0.50, below 0.70 * 0.75
	scaledUp := f.now.Add(-30 * time.Second)
	as := &meridianv1alpha1.ClusterPoolAutoscaler{}
	_ = f.c.Get(context.Background(), f.pool, as)
	ts := metav1.NewTime(scaledUp)
	as.Status.LastScaleTime = &ts
	if err := f.c.Status().Update(context.Background(), as); err != nil {
		t.Fatal(err)
	}

	if got := f.reconcile(); got != 4 {
		t.Fatalf("30s after scaling: replicas = %d, want 4 (no scale-down yet)", got)
	}
	f.now = scaledUp.Add(scaleDownDelay - time.Second)
	if got := f.reconcile(); got != 4 {
		t.Fatalf("just before the delay ends: replicas = %d, want 4", got)
	}
	f.now = scaledUp.Add(scaleDownDelay)
	if got := f.reconcile(); got != 3 {
		t.Fatalf("after the delay: replicas = %d, want 3", got)
	}
	// The scale-down itself starts a new delay.
	f.now = f.now.Add(time.Minute)
	if got := f.reconcile(); got != 3 {
		t.Fatalf("1m after scaling down: replicas = %d, want 3", got)
	}
}

func TestAutoscaler_ScalesDownIdlePool(t *testing.T) {
	f := newAutoscalerFixture(t, 4, 4, 0, nil)
	if got := f.reconcile(); got != 3 {
		t.Fatalf("replicas = %d, want 3", got)
	}
}

func TestAutoscaler_RespectsLimits(t *testing.T) {
	if got := newAutoscalerFixture(t, 4, 0, 4, nil).reconcile(); got != 4 {
		t.Errorf("fully reserved at max: replicas = %d, want 4", got)
	}
	if got := newAutoscalerFixture(t, 2, 2, 0, nil).reconcile(); got != 2 {
		t.Errorf("idle at min: replicas = %d, want 2", got)
	}
}

// The status update at the end of every run must not trigger another run.
func TestAutoscaler_IgnoresItsOwnStatusUpdates(t *testing.T) {
	old := &meridianv1alpha1.ClusterPoolAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "a", Generation: 1}}
	statusOnly := old.DeepCopy()
	statusOnly.Status.CurrentUtilization = "1.00"
	specChange := old.DeepCopy()
	specChange.Generation = 2
	specChange.Spec.MaxReplicas = 8

	p := autoscalerEvents()
	if p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: statusOnly}) {
		t.Error("a status-only update triggers a reconcile")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: specChange}) {
		t.Error("a spec change does not trigger a reconcile")
	}
}

func setLimits(t *testing.T, f *autoscalerFixture, min, max int32, threshold string) {
	t.Helper()
	as := &meridianv1alpha1.ClusterPoolAutoscaler{}
	if err := f.c.Get(context.Background(), f.pool, as); err != nil {
		t.Fatal(err)
	}
	as.Spec.MinReplicas, as.Spec.MaxReplicas, as.Spec.UtilizationThreshold = min, max, threshold
	if err := f.c.Update(context.Background(), as); err != nil {
		t.Fatal(err)
	}
}

// A pool already above maxReplicas (scaled by hand or through scale_pool)
// is never shrunk by the scale-up branch, which used to cut it to max at once.
func TestAutoscaler_BusyPoolAboveMaxIsNotShrunk(t *testing.T) {
	f := newAutoscalerFixture(t, 8, 1, 7, nil) // 7/8 busy, max is 4
	if got := f.reconcile(); got != 8 {
		t.Errorf("replicas = %d, want 8", got)
	}
}

// minReplicas > maxReplicas is treated as max = min instead of flipping.
func TestAutoscaler_MinAboveMax(t *testing.T) {
	f := newAutoscalerFixture(t, 5, 1, 4, nil)
	setLimits(t, f, 6, 4, "0.70")
	if got := f.reconcile(); got != 6 {
		t.Fatalf("first run: replicas = %d, want 6", got)
	}
	if got := f.reconcile(); got != 6 {
		t.Fatalf("second run: replicas = %d, want 6", got)
	}
}

// One scale-up is enough: reserved/desired lands strictly below the
// threshold, so the next check doesn't add another cluster.
func TestAutoscaler_NoExtraClusterAtExactThreshold(t *testing.T) {
	f := newAutoscalerFixture(t, 9, 2, 7, nil)
	setLimits(t, f, 2, 20, "0.70")
	first := f.reconcile()
	if first != 11 {
		t.Fatalf("first run: replicas = %d, want 11 (7/11 < 0.70)", first)
	}
	if got := f.reconcile(); got != first {
		t.Errorf("second run: replicas = %d, want it to stay %d", got, first)
	}
}

func TestParseThreshold(t *testing.T) {
	for in, want := range map[string]float64{
		"0.70": 0.70, "0.5": 0.5, "1": 1, "70%": 0.70, " 0.8 ": 0.8,
		"70": 0.70, "1.5": 0.70, "0": 0.70, "-1": 0.70, "abc": 0.70, "": 0.70, "NaN": 0.70,
	} {
		if got := parseThreshold(in); got != want {
			t.Errorf("parseThreshold(%q) = %v, want %v", in, got, want)
		}
	}
}

// An autoscaler whose pool doesn't exist waits quietly instead of erroring.
func TestAutoscaler_MissingPool(t *testing.T) {
	f := newAutoscalerFixture(t, 2, 2, 0, nil)
	pool := &meridianv1alpha1.ClusterPool{}
	_ = f.c.Get(context.Background(), f.pool, pool)
	if err := f.c.Delete(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	res, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: f.pool})
	if err != nil || res.RequeueAfter == 0 {
		t.Errorf("got (%v, %v), want a quiet requeue", res, err)
	}
}
