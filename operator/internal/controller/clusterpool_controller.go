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
)

// ClusterPoolController maintains the warm pool of Trino clusters.
// It reconciles every 30 seconds to scale up/down and perform rolling upgrades.
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

	// 1. Delete failed clusters immediately.
	for _, c := range failed {
		log.Info("deleting failed cluster", "cluster", c.Name)
		if err := r.Delete(ctx, &c); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}

	total := int32(len(idle) + len(reserved) + len(pending))
	desired := pool.Spec.Replicas

	// 2. Scale up — create clusters until we reach desired count.
	if total < desired {
		toCreate := desired - total
		for i := int32(0); i < toCreate; i++ {
			if err := r.createCluster(ctx, pool); err != nil {
				log.Error(err, "failed to create cluster")
				break
			}
		}
	}

	// 3. Scale down — delete at most one idle cluster per cycle (gradual).
	if total > desired && len(idle) > 0 {
		oldest := idle[0]
		for _, c := range idle[1:] {
			if c.Status.IdleAt != nil && oldest.Status.IdleAt != nil &&
				c.Status.IdleAt.Before(oldest.Status.IdleAt) {
				oldest = c
			}
		}
		log.Info("scaling down, deleting idle cluster", "cluster", oldest.Name)
		if err := r.Delete(ctx, &oldest); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}

	// 4. Update pool status.
	pool.Status.ReadyReplicas = int32(len(idle))
	pool.Status.ReservedReplicas = int32(len(reserved))
	pool.Status.PendingReplicas = int32(len(pending))
	pool.Status.FailedReplicas = int32(len(failed))
	if err := r.Status().Update(ctx, pool); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: reconcileInterval}, nil
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
