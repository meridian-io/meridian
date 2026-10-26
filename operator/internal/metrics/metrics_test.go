package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

func cluster(name, pool string, phase meridianv1alpha1.ClusterPhase) *meridianv1alpha1.Cluster {
	c := &meridianv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "meridian"}}
	if pool != "" {
		c.Labels = map[string]string{poolLabel: pool}
	}
	c.Status.Phase = phase
	return c
}

// TestStateCollector checks the scrape-time gauges: every pool reports every
// phase (zeros included), standalone clusters have an empty pool label, and
// a brand-new cluster counts as Empty.
func TestStateCollector(t *testing.T) {
	s := runtime.NewScheme()
	_ = meridianv1alpha1.AddToScheme(s)
	pool := &meridianv1alpha1.ClusterPool{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics", Namespace: "meridian"},
		Spec:       meridianv1alpha1.ClusterPoolSpec{Replicas: 3},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(
		pool,
		cluster("analytics-a", "analytics", meridianv1alpha1.ClusterPhaseIdle),
		cluster("analytics-b", "analytics", meridianv1alpha1.ClusterPhaseIdle),
		cluster("analytics-c", "analytics", meridianv1alpha1.ClusterPhaseReserved),
		cluster("solo", "", meridianv1alpha1.ClusterPhaseEmpty),
	).Build()

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(&stateCollector{reader: c})

	want := `
# HELP meridian_clusters Clusters by namespace, pool and phase. pool is empty for clusters outside a pool.
# TYPE meridian_clusters gauge
meridian_clusters{namespace="meridian",phase="Degraded",pool="analytics"} 0
meridian_clusters{namespace="meridian",phase="Empty",pool=""} 1
meridian_clusters{namespace="meridian",phase="Failed",pool="analytics"} 0
meridian_clusters{namespace="meridian",phase="Idle",pool="analytics"} 2
meridian_clusters{namespace="meridian",phase="Pending",pool="analytics"} 0
meridian_clusters{namespace="meridian",phase="Reserved",pool="analytics"} 1
# HELP meridian_pool_desired_replicas spec.replicas of each ClusterPool.
# TYPE meridian_pool_desired_replicas gauge
meridian_pool_desired_replicas{namespace="meridian",pool="analytics"} 3
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want)); err != nil {
		t.Error(err)
	}
}
