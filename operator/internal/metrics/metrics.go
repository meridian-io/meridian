// Package metrics defines the operator's Prometheus metrics. They are served
// on the controller-runtime metrics endpoint (--metrics-bind-address, :8081).
//
// Labels never carry cluster names, client IDs or SQL, so the number of series
// stays bounded by the number of pools.
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

// Label values.
const (
	ReasonFailed    = "failed"
	ReasonDegraded  = "degraded"
	ReasonImage     = "image"
	ReasonAge       = "age"
	ReasonScaleDown = "scale_down"

	ResultSuccess = "success"
	ResultFailure = "failure"
	ResultInvalid = "invalid"

	ReserveOK          = "ok"
	ReserveNoCapacity  = "no_capacity"
	ReserveError       = "error"
	ReserveBadRequest  = "bad_request"
	ReserveUnauthorize = "unauthorized"
)

// poolLabel matches the label the pool controller puts on its clusters.
const poolLabel = "meridian.io/cluster-pool"

var (
	ClusterProvisionSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "meridian_cluster_provision_seconds",
		Help: "Time from cluster creation until it first became Idle (ready for queries).",
		// Trino start-up is tens of seconds to several minutes.
		Buckets: []float64{15, 30, 45, 60, 90, 120, 180, 240, 300, 450, 600, 900},
	}, []string{"namespace", "pool", "profile"})

	ClusterPhaseTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_cluster_phase_transitions_total",
		Help: "Cluster phase changes, by previous and new phase.",
	}, []string{"from", "to"})

	PoolClustersReplaced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_pool_clusters_replaced_total",
		Help: "Clusters a pool deleted, by reason: failed, degraded, image (rolling upgrade), age (maxClusterAge) or scale_down.",
	}, []string{"namespace", "pool", "reason"})

	ReservationsExpired = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_reservations_expired_total",
		Help: "Reservations released by the pool because they outlived reservationTtl (usually a client that crashed).",
	}, []string{"namespace", "pool"})

	ReservationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "meridian_reservation_duration_seconds",
		Help: "Latency of REST reservation requests, including time spent waiting for an idle cluster.",
		// Instant when a cluster is idle; up to the 30s wait window otherwise.
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 45},
	}, []string{"source", "result"})

	CredentialRotations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "meridian_credential_rotations_total",
		Help: "Credential rotation attempts, by backend and result (success, failure, invalid).",
	}, []string{"backend", "result"})
)

// Register adds the metrics to the controller-runtime registry. reader lists
// clusters and pools at scrape time for the meridian_clusters and
// meridian_pool_desired_replicas gauges. backends are the credential
// providers this operator was started with.
func Register(reader client.Reader, backends ...string) {
	// Start event series at zero so the first event shows up in rate().
	for _, r := range []string{ReserveOK, ReserveNoCapacity, ReserveError, ReserveBadRequest, ReserveUnauthorize} {
		ReservationDuration.WithLabelValues("rest", r)
	}
	for _, b := range backends {
		for _, r := range []string{ResultSuccess, ResultFailure, ResultInvalid} {
			CredentialRotations.WithLabelValues(b, r)
		}
	}

	ctrlmetrics.Registry.MustRegister(
		ClusterProvisionSeconds,
		ClusterPhaseTransitions,
		PoolClustersReplaced,
		ReservationsExpired,
		ReservationDuration,
		CredentialRotations,
		&stateCollector{reader: reader},
	)
}

// InitPool creates a pool's event series at zero. A series that first appears
// with value 1 is invisible to rate() and increase(), so without this the
// first provisioning, replacement or expiry in a pool would not show up.
func InitPool(namespace, pool, profile string) {
	ClusterProvisionSeconds.WithLabelValues(namespace, pool, profile)
	ReservationsExpired.WithLabelValues(namespace, pool)
	for _, r := range []string{ReasonFailed, ReasonDegraded, ReasonImage, ReasonAge, ReasonScaleDown} {
		PoolClustersReplaced.WithLabelValues(namespace, pool, r)
	}
}

// ForgetPool removes a deleted pool's series.
func ForgetPool(namespace, pool string) {
	l := prometheus.Labels{"namespace": namespace, "pool": pool}
	ClusterProvisionSeconds.DeletePartialMatch(l)
	ReservationsExpired.DeletePartialMatch(l)
	PoolClustersReplaced.DeletePartialMatch(l)
}

// PoolOf returns the pool a cluster belongs to, or "" for a standalone cluster.
func PoolOf(c *meridianv1alpha1.Cluster) string {
	return c.Labels[poolLabel]
}

// PhaseName returns the phase, with "Empty" for a cluster not yet reconciled.
func PhaseName(p meridianv1alpha1.ClusterPhase) string {
	if p == "" {
		return "Empty"
	}
	return string(p)
}

// stateCollector reports current cluster and pool state, read from the
// manager's cache when Prometheus scrapes. Reading at scrape time means a
// deleted pool's series disappear instead of going stale.
type stateCollector struct {
	reader client.Reader
}

var (
	clustersDesc = prometheus.NewDesc("meridian_clusters",
		"Clusters by namespace, pool and phase. pool is empty for clusters outside a pool.",
		[]string{"namespace", "pool", "phase"}, nil)
	desiredDesc = prometheus.NewDesc("meridian_pool_desired_replicas",
		"spec.replicas of each ClusterPool.",
		[]string{"namespace", "pool"}, nil)
)

func (s *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- clustersDesc
	ch <- desiredDesc
}

func (s *stateCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type key struct{ ns, pool, phase string }
	counts := map[key]int{}

	// Every pool reports every phase, so "0 Idle clusters" is a value an
	// alert can fire on rather than a missing series.
	var pools meridianv1alpha1.ClusterPoolList
	if err := s.reader.List(ctx, &pools); err == nil {
		for _, p := range pools.Items {
			ch <- prometheus.MustNewConstMetric(desiredDesc, prometheus.GaugeValue, float64(p.Spec.Replicas), p.Namespace, p.Name)
			for _, ph := range poolPhases {
				counts[key{p.Namespace, p.Name, ph}] = 0
			}
		}
	}

	var clusters meridianv1alpha1.ClusterList
	if err := s.reader.List(ctx, &clusters); err == nil {
		for i := range clusters.Items {
			c := &clusters.Items[i]
			if c.DeletionTimestamp != nil {
				continue
			}
			counts[key{c.Namespace, PoolOf(c), PhaseName(c.Status.Phase)}]++
		}
	}
	for k, n := range counts {
		ch <- prometheus.MustNewConstMetric(clustersDesc, prometheus.GaugeValue, float64(n), k.ns, k.pool, k.phase)
	}
}

var poolPhases = []string{
	PhaseName(meridianv1alpha1.ClusterPhasePending),
	PhaseName(meridianv1alpha1.ClusterPhaseIdle),
	PhaseName(meridianv1alpha1.ClusterPhaseReserved),
	PhaseName(meridianv1alpha1.ClusterPhaseFailed),
	PhaseName(meridianv1alpha1.ClusterPhaseDegraded),
}
