package controller

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/metrics"
)

const (
	reconcileInterval      = 30 * time.Second
	poolLabel              = "meridian.io/cluster-pool"
	workloadLabel          = "meridian.io/workload"
	maxDeletesPerReconcile = 1 // gradual operations — one delete per cycle
	defaultReservationTTL  = 4 * time.Hour

	// DefaultMaxCreatesPerCycle bounds how many clusters one reconcile creates.
	// A small batch fills a pool ~3x faster than one-at-a-time while still
	// avoiding a burst of scheduling on a cold node pool.
	DefaultMaxCreatesPerCycle = 3

	suffixAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffixLength   = 8
)

// ClusterPoolController maintains the hot standby pool of Trino clusters.
// Each reconcile cycle performs at most MaxCreatesPerCycle creates and one
// delete to avoid overwhelming the Kubernetes scheduler (gradual operations).
type ClusterPoolController struct {
	client.Client
	Scheme *runtime.Scheme

	// MaxCreatesPerCycle caps creates per reconcile. 0 means DefaultMaxCreatesPerCycle.
	MaxCreatesPerCycle int

	// APIReader reads straight from the API server (mgr.GetAPIReader()). It is
	// used to recount clusters right before creating more, because the
	// informer cache can lag behind clusters created in the previous cycle and
	// would otherwise make the pool over-create. If nil, the cached client is used.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=meridian.io,resources=clusterpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=meridian.io,resources=clusterpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=meridian.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=meridian.io,resources=clusterpools/finalizers;clusters/finalizers,verbs=update

func (r *ClusterPoolController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	pool := &meridianv1alpha1.ClusterPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if apierrors.IsNotFound(err) {
			metrics.ForgetPool(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	metrics.InitPool(pool.Namespace, pool.Name, pool.Spec.Template.Profile)

	clusters, err := r.listPoolClusters(ctx, pool)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Categorise clusters by phase.
	var idle, reserved, pending, failed, degraded []meridianv1alpha1.Cluster
	for _, c := range clusters.Items {
		switch c.Status.Phase {
		case meridianv1alpha1.ClusterPhaseIdle:
			idle = append(idle, c)
		case meridianv1alpha1.ClusterPhaseReserved:
			reserved = append(reserved, c)
		case meridianv1alpha1.ClusterPhasePending, meridianv1alpha1.ClusterPhaseEmpty:
			pending = append(pending, c)
		case meridianv1alpha1.ClusterPhaseFailed:
			failed = append(failed, c)
		case meridianv1alpha1.ClusterPhaseDegraded:
			degraded = append(degraded, c)
		}
	}

	// 1. Delete ALL failed clusters immediately (no per-cycle limit — they are
	//    already broken and must be replaced by the pool).
	for _, c := range failed {
		log.Info("deleting failed cluster", "cluster", c.Name)
		if err := r.deleteCluster(ctx, pool, &c, metrics.ReasonFailed); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 1b. Delete ALL degraded clusters immediately — pool creates replacements.
	for _, c := range degraded {
		log.Info("deleting degraded cluster", "cluster", c.Name)
		if err := r.deleteCluster(ctx, pool, &c, metrics.ReasonDegraded); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 2. Release expired reservations so crashed clients don't leak clusters.
	ttl := r.reservationTTL(pool)
	for i := range reserved {
		c := &reserved[i]
		if c.Status.ReservedAt != nil && time.Since(c.Status.ReservedAt.Time) > ttl {
			log.Info("releasing expired reservation",
				"cluster", c.Name,
				"age", time.Since(c.Status.ReservedAt.Time).Round(time.Second))
			c.Spec.ClientID = ""
			c.Spec.ReservationID = ""
			if err := r.Update(ctx, c); err != nil && !apierrors.IsNotFound(err) {
				log.Error(err, "failed to release expired reservation", "cluster", c.Name)
			} else if err == nil {
				metrics.ReservationsExpired.WithLabelValues(pool.Namespace, pool.Name).Inc()
			}
		}
	}

	total := int32(len(idle) + len(reserved) + len(pending))
	desired := pool.Spec.Replicas

	// 3. Rolling upgrade: delete ONE idle cluster with an outdated image per cycle.
	//    The pool will create a replacement with the current image on the next step.
	rollingUpgrade := false
	for _, c := range idle {
		if c.Spec.Image != pool.Spec.Template.Image {
			log.Info("rolling upgrade: replacing outdated cluster",
				"cluster", c.Name,
				"current", c.Spec.Image,
				"target", pool.Spec.Template.Image)
			if err := r.deleteCluster(ctx, pool, &c, metrics.ReasonImage); err != nil {
				return ctrl.Result{}, err
			}
			total--
			rollingUpgrade = true
			break
		}
	}

	// 3.5. Age recycling — delete ONE idle cluster older than maxClusterAge per cycle.
	//      Pool creates a replacement in step 4. Skipped if a rolling upgrade already
	//      performed a deletion this cycle (gradual operations — one delete per cycle).
	if !rollingUpgrade && pool.Spec.MaxClusterAge != "" {
		if maxAge, err := time.ParseDuration(pool.Spec.MaxClusterAge); err == nil {
			for _, c := range idle {
				if time.Since(c.CreationTimestamp.Time) > maxAge {
					log.Info("age recycling: replacing aged-out cluster",
						"cluster", c.Name,
						"age", time.Since(c.CreationTimestamp.Time).Round(time.Second))
					if err := r.deleteCluster(ctx, pool, &c, metrics.ReasonAge); err != nil {
						return ctrl.Result{}, err
					}
					total--
					rollingUpgrade = true // prevent double-delete in scale-down step
					break
				}
			}
		}
	}

	// 4. Scale up — create up to MaxCreatesPerCycle clusters per reconcile
	//    (gradual operations: fast enough to fill a pool, bounded so a large
	//    pool doesn't flood the scheduler at once).
	if missing := int(desired - total); missing > 0 {
		// Recount against the API server: the cache may not have seen
		// clusters created moments ago.
		if live, err := r.countLiveClusters(ctx, pool); err != nil {
			log.Error(err, "failed to recount pool clusters; skipping scale-up this cycle")
			missing = 0
		} else {
			missing = int(desired) - live
		}
		toCreate := min(max(missing, 0), r.maxCreates())
		for i := 0; i < toCreate; i++ {
			if err := r.createCluster(ctx, pool); err != nil {
				log.Error(err, "failed to create cluster")
				break
			}
		}
	}

	// 5. Scale down — delete at most one idle cluster per cycle.
	//    Skip if we already performed a rolling-upgrade deletion this cycle.
	if !rollingUpgrade && total > desired && len(idle) > 0 {
		oldest := oldestCluster(idle)
		log.Info("scaling down, deleting idle cluster", "cluster", oldest.Name)
		if err := r.deleteCluster(ctx, pool, oldest, metrics.ReasonScaleDown); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 6. Update pool status.
	pool.Status.ReadyReplicas = int32(len(idle))
	pool.Status.ReservedReplicas = int32(len(reserved))
	pool.Status.PendingReplicas = int32(len(pending))
	pool.Status.FailedReplicas = int32(len(failed))
	pool.Status.DegradedReplicas = int32(len(degraded))
	if err := r.Status().Update(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: reconcileInterval}, nil
}

// deleteCluster deletes one of the pool's clusters and counts it by reason.
// A cluster that is already gone, or already being deleted (the cache can
// still show it on the next reconcile), is not deleted or counted again.
func (r *ClusterPoolController) deleteCluster(ctx context.Context, pool *meridianv1alpha1.ClusterPool, c *meridianv1alpha1.Cluster, reason string) error {
	if c.DeletionTimestamp != nil {
		return nil
	}
	if err := r.Delete(ctx, c); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	metrics.PoolClustersReplaced.WithLabelValues(pool.Namespace, pool.Name, reason).Inc()
	return nil
}

// reservationTTL parses pool.Spec.ReservationTTL or falls back to the default.
func (r *ClusterPoolController) reservationTTL(pool *meridianv1alpha1.ClusterPool) time.Duration {
	if pool.Spec.ReservationTTL != "" {
		if d, err := time.ParseDuration(pool.Spec.ReservationTTL); err == nil {
			return d
		}
	}
	return defaultReservationTTL
}

// oldestCluster returns a pointer to the cluster with the earliest IdleAt timestamp.
func oldestCluster(clusters []meridianv1alpha1.Cluster) *meridianv1alpha1.Cluster {
	oldest := &clusters[0]
	for i := range clusters[1:] {
		c := &clusters[i+1]
		if c.Status.IdleAt != nil && oldest.Status.IdleAt != nil &&
			c.Status.IdleAt.Before(oldest.Status.IdleAt) {
			oldest = c
		}
	}
	return oldest
}

func (r *ClusterPoolController) listPoolClusters(ctx context.Context, pool *meridianv1alpha1.ClusterPool) (*meridianv1alpha1.ClusterList, error) {
	list := &meridianv1alpha1.ClusterList{}
	if err := r.List(ctx, list,
		client.InNamespace(pool.Namespace),
		client.MatchingLabels{poolLabel: pool.Name},
	); err != nil {
		return nil, err
	}
	return list, nil
}

func (r *ClusterPoolController) createCluster(ctx context.Context, pool *meridianv1alpha1.ClusterPool) error {
	name := fmt.Sprintf("%s-%s", pool.Name, generateSuffix())
	labels := map[string]string{
		poolLabel:             pool.Name,
		"meridian.io/profile": pool.Spec.Template.Profile,
	}
	if pool.Spec.Workload != "" {
		labels[workloadLabel] = pool.Spec.Workload
	}

	annotations := map[string]string{}
	if pool.Spec.Gateway != nil && pool.Spec.Gateway.Endpoint != "" {
		annotations[gatewayEndpointAnnotation] = pool.Spec.Gateway.Endpoint
		rg := pool.Spec.Gateway.RoutingGroup
		if rg == "" {
			rg = pool.Spec.Workload
		}
		if rg == "" {
			rg = "adhoc"
		}
		annotations[gatewayRoutingGroupAnnotation] = rg
	}

	cluster := &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   pool.Namespace,
			Labels:      labels,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(pool, meridianv1alpha1.GroupVersion.WithKind("ClusterPool")),
			},
		},
		Spec: meridianv1alpha1.ClusterSpec{
			Profile: pool.Spec.Template.Profile,
			Image:   pool.Spec.Template.Image,
			Workers: pool.Spec.Template.Workers,
		},
	}
	err := r.Create(ctx, cluster)
	if apierrors.IsAlreadyExists(err) {
		// Astronomically unlikely with 36^8 suffixes, but cheap to handle.
		cluster.Name = fmt.Sprintf("%s-%s", pool.Name, generateSuffix())
		err = r.Create(ctx, cluster)
	}
	return err
}

// countLiveClusters counts pool clusters that occupy a slot: not being
// deleted and not Failed/Degraded (those are replaced, not counted).
func (r *ClusterPoolController) countLiveClusters(ctx context.Context, pool *meridianv1alpha1.ClusterPool) (int, error) {
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	list := &meridianv1alpha1.ClusterList{}
	if err := reader.List(ctx, list,
		client.InNamespace(pool.Namespace),
		client.MatchingLabels{poolLabel: pool.Name},
	); err != nil {
		return 0, err
	}
	n := 0
	for _, c := range list.Items {
		if !c.DeletionTimestamp.IsZero() {
			continue
		}
		switch c.Status.Phase {
		case meridianv1alpha1.ClusterPhaseFailed, meridianv1alpha1.ClusterPhaseDegraded:
			continue
		}
		n++
	}
	return n, nil
}

func (r *ClusterPoolController) maxCreates() int {
	if r.MaxCreatesPerCycle > 0 {
		return r.MaxCreatesPerCycle
	}
	return DefaultMaxCreatesPerCycle
}

// generateSuffix returns a random DNS-safe suffix (8 chars, ~41 bits).
// The old time-based suffix (UnixNano % 100000) collided when several
// clusters were created in the same cycle.
func generateSuffix() string {
	b := make([]byte, suffixLength)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	for i := range b {
		b[i] = suffixAlphabet[int(b[i])%len(suffixAlphabet)]
	}
	return string(b)
}

// SetupWithManager registers the pool controller. Pools reconcile one at a
// time (MaxConcurrentReconciles=1): a pool reconcile is cheap, and serial
// processing keeps scale decisions simple to reason about.
func (r *ClusterPoolController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&meridianv1alpha1.ClusterPool{}).
		Owns(&meridianv1alpha1.Cluster{}).
		Complete(r)
}
