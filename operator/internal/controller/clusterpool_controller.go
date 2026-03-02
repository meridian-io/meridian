package controller

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

const (
	reconcileInterval      = 30 * time.Second
	poolLabel              = "meridian.io/cluster-pool"
	maxDeletesPerReconcile = 1 // gradual operations — one delete per cycle
	defaultReservationTTL  = 4 * time.Hour
)

// ClusterPoolController maintains the hot standby pool of Trino clusters.
// Each reconcile cycle performs at most one create and one delete to avoid
// overwhelming the Kubernetes scheduler (gradual operations).
type ClusterPoolController struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=meridian.io,resources=clusterpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=meridian.io,resources=clusterpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=meridian.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete

func (r *ClusterPoolController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	pool := &meridianv1alpha1.ClusterPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	clusters, err := r.listPoolClusters(ctx, pool)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Categorise clusters by phase.
	var idle, reserved, pending, failed []meridianv1alpha1.Cluster
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
		}
	}

	// 1. Delete ALL failed clusters immediately (no per-cycle limit — they are
	//    already broken and must be replaced by the pool).
	for _, c := range failed {
		log.Info("deleting failed cluster", "cluster", c.Name)
		if err := r.Delete(ctx, &c); err != nil && !apierrors.IsNotFound(err) {
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
			if err := r.Delete(ctx, &c); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			total--
			rollingUpgrade = true
			break
		}
	}

	// 4. Scale up — create at most ONE cluster per reconcile (gradual operations).
	//    This prevents thundering-herd on the Kubernetes scheduler and keeps
	//    logs readable (one operation = one clear failure point).
	if total < desired {
		if err := r.createCluster(ctx, pool); err != nil {
			log.Error(err, "failed to create cluster")
		}
	}

	// 5. Scale down — delete at most one idle cluster per cycle.
	//    Skip if we already performed a rolling-upgrade deletion this cycle.
	if !rollingUpgrade && total > desired && len(idle) > 0 {
		oldest := oldestCluster(idle)
		log.Info("scaling down, deleting idle cluster", "cluster", oldest.Name)
		if err := r.Delete(ctx, oldest); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}

	// 6. Update pool status.
	pool.Status.ReadyReplicas = int32(len(idle))
	pool.Status.ReservedReplicas = int32(len(reserved))
	pool.Status.PendingReplicas = int32(len(pending))
	pool.Status.FailedReplicas = int32(len(failed))
	if err := r.Status().Update(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: reconcileInterval}, nil
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
	cluster := &meridianv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: pool.Namespace,
			Labels: map[string]string{
				poolLabel:            pool.Name,
				"meridian.io/profile": pool.Spec.Template.Profile,
			},
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
	return r.Create(ctx, cluster)
}

func generateSuffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano()%100000)
}

func (r *ClusterPoolController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&meridianv1alpha1.ClusterPool{}).
		Owns(&meridianv1alpha1.Cluster{}).
		Complete(r)
}
