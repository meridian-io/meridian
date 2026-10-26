package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"

	"github.com/meridian-io/meridian/operator/internal/metrics"
)

// sampleCount returns how many observations a histogram series has.
func sampleCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	m := &dto.Metric{}
	if err := o.(prometheus.Metric).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

// delta runs fn and returns how much the counter grew.
func delta(c prometheus.Counter, fn func()) float64 {
	before := testutil.ToFloat64(c)
	fn()
	return testutil.ToFloat64(c) - before
}

// The metric tests re-run existing scenarios and check what they recorded.
// Counters are process-global, so they compare before and after.

func TestMetrics_PendingToIdleRecordsTransitionAndProvisionTime(t *testing.T) {
	hist := metrics.ClusterProvisionSeconds.WithLabelValues("meridian", "", "default")
	samples := sampleCount(t, hist)
	got := delta(metrics.ClusterPhaseTransitions.WithLabelValues("Pending", "Idle"), func() {
		TestClusterController_PendingToIdle(t)
	})
	if got != 1 {
		t.Errorf("Pending→Idle transitions grew by %v, want 1", got)
	}
	if n := sampleCount(t, hist) - samples; n != 1 {
		t.Errorf("provision time samples grew by %d, want 1", n)
	}
}

func TestMetrics_NoTransitionWhenPhaseUnchanged(t *testing.T) {
	got := delta(metrics.ClusterPhaseTransitions.WithLabelValues("Pending", "Pending"), func() {
		TestClusterController_PendingWaitsForWorker(t)
	})
	if got != 0 {
		t.Errorf("an unchanged phase was counted as a transition (%v)", got)
	}
}

func TestMetrics_PoolCountsFailedClusterReplacement(t *testing.T) {
	got := delta(metrics.PoolClustersReplaced.WithLabelValues("meridian", "test-pool", metrics.ReasonFailed), func() {
		TestClusterPoolController_DeleteFailed(t)
	})
	if got != 1 {
		t.Errorf("failed replacements grew by %v, want 1", got)
	}
}

func TestMetrics_PoolCountsScaleDown(t *testing.T) {
	got := delta(metrics.PoolClustersReplaced.WithLabelValues("meridian", "test-pool", metrics.ReasonScaleDown), func() {
		TestClusterPoolController_ScaleDown(t)
	})
	if got != 1 {
		t.Errorf("scale-down deletions grew by %v, want 1", got)
	}
}

func TestMetrics_RotationResults(t *testing.T) {
	if got := delta(metrics.CredentialRotations.WithLabelValues("kubernetes", metrics.ResultSuccess), func() {
		TestClusterController_RotationSuccess(t)
	}); got != 1 {
		t.Errorf("successful rotations grew by %v, want 1", got)
	}
	if got := delta(metrics.CredentialRotations.WithLabelValues("aws-secrets-manager", metrics.ResultInvalid), func() {
		TestClusterController_RotationUnknownProvider(t)
	}); got != 1 {
		t.Errorf("invalid rotations grew by %v, want 1", got)
	}
}

// TestMetrics_TerminatingClusterNotCountedTwice: a cluster already being
// deleted (finalizer pending) still shows in the cache on the next
// reconcile. The pool must not delete it again or count it twice.
func TestMetrics_TerminatingClusterNotCountedTwice(t *testing.T) {
	pool := newPool("term-pool", "meridian", 1)
	keep := poolCluster(pool, "keep", meridianv1alpha1.ClusterPhaseIdle)
	keep.CreationTimestamp = metav1.NewTime(time.Now())
	going := poolCluster(pool, "going", meridianv1alpha1.ClusterPhaseIdle)
	going.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour)) // oldest, so scale-down picks it
	going.Finalizers = []string{clusterFinalizer}
	now := metav1.Now()
	going.DeletionTimestamp = &now

	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(pool, keep, going).WithStatusSubresource(pool).Build()
	r := &ClusterPoolController{Client: c, Scheme: s}
	got := delta(metrics.PoolClustersReplaced.WithLabelValues("meridian", "term-pool", metrics.ReasonScaleDown), func() {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "term-pool", Namespace: "meridian"},
		}); err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Errorf("a cluster already being deleted was counted again (%v)", got)
	}
}

// TestMetrics_PoolSeriesStartAtZero: a pool's event series exist at zero as
// soon as the pool is reconciled, so the first event shows up in rate(), and
// they are removed when the pool is deleted.
func TestMetrics_PoolSeriesStartAtZero(t *testing.T) {
	metrics.InitPool("meridian", "fresh-pool", "default")
	if v := testutil.ToFloat64(metrics.ReservationsExpired.WithLabelValues("meridian", "fresh-pool")); v != 0 {
		t.Errorf("expired reservations = %v, want 0", v)
	}
	count := func() int {
		n := 0
		for _, r := range []string{metrics.ReasonFailed, metrics.ReasonDegraded, metrics.ReasonImage, metrics.ReasonAge, metrics.ReasonScaleDown} {
			if metrics.PoolClustersReplaced.DeleteLabelValues("meridian", "fresh-pool-probe", r) {
				n++
			}
		}
		return n
	}
	metrics.InitPool("meridian", "fresh-pool-probe", "default")
	if n := count(); n != 5 {
		t.Errorf("InitPool created %d replacement series, want 5", n)
	}

	metrics.InitPool("meridian", "gone-pool", "default")
	metrics.ForgetPool("meridian", "gone-pool")
	if metrics.ReservationsExpired.DeleteLabelValues("meridian", "gone-pool") {
		t.Error("ForgetPool left the expired-reservations series behind")
	}
}
