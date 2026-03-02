package controller

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

const (
	autoscalerInterval = 30 * time.Second
	// hysteresisFactor prevents flapping: scale down only when well below threshold.
	hysteresisFactor = 0.75
)

// ClusterPoolAutoscalerController adjusts ClusterPool replica count
// based on utilization = reserved_clusters / total_clusters.
type ClusterPoolAutoscalerController struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=meridian.io,resources=clusterpoolautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=meridian.io,resources=clusterpoolautoscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=meridian.io,resources=clusterpools,verbs=get;list;watch;update;patch

func (r *ClusterPoolAutoscalerController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	autoscaler := &meridianv1alpha1.ClusterPoolAutoscaler{}
	if err := r.Get(ctx, req.NamespacedName, autoscaler); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Fetch target ClusterPool.
	pool := &meridianv1alpha1.ClusterPool{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      autoscaler.Spec.TargetRef.Name,
		Namespace: autoscaler.Namespace,
	}, pool); err != nil {
		return ctrl.Result{}, err
	}

	total := pool.Status.ReadyReplicas + pool.Status.ReservedReplicas + pool.Status.PendingReplicas
	if total == 0 {
		return ctrl.Result{RequeueAfter: autoscalerInterval}, nil
	}

	utilization := float64(pool.Status.ReservedReplicas) / float64(total)

	threshold, err := strconv.ParseFloat(autoscaler.Spec.UtilizationThreshold, 64)
	if err != nil {
		threshold = 0.70
	}

	current := pool.Spec.Replicas
	desired := current

	if utilization >= threshold {
		// Scale up to the minimum pool size that keeps utilization below threshold.
		// desired = ceil(reserved / threshold) ensures reserved/desired < threshold.
		needed := int32(math.Ceil(float64(pool.Status.ReservedReplicas) / threshold))
		// Always increase by at least 1 so we make progress even when reserved=0.
		desired = min32(max32(needed, current+1), autoscaler.Spec.MaxReplicas)
		log.Info("scaling up pool",
			"pool", pool.Name,
			"utilization", fmt.Sprintf("%.2f", utilization),
			"current", current,
			"desired", desired,
		)
	} else if utilization < threshold*hysteresisFactor {
		// Scale down — remove 10%, floored at MinReplicas.
		desired = max32(int32(float64(current)*0.9), autoscaler.Spec.MinReplicas)
		log.Info("scaling down pool",
			"pool", pool.Name,
			"utilization", fmt.Sprintf("%.2f", utilization),
			"current", current,
			"desired", desired,
		)
	}

	if desired != current {
		now := metav1.Now()
		pool.Spec.Replicas = desired
		if err := r.Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}
		autoscaler.Status.LastScaleTime = &now
	}

	autoscaler.Status.CurrentReplicas = current
	autoscaler.Status.DesiredReplicas = desired
	autoscaler.Status.CurrentUtilization = fmt.Sprintf("%.2f", utilization)
	if err := r.Status().Update(ctx, autoscaler); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: autoscalerInterval}, nil
}

func min32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func max32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func (r *ClusterPoolAutoscalerController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&meridianv1alpha1.ClusterPoolAutoscaler{}).
		Complete(r)
}
