package controller

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

const (
	autoscalerInterval = 30 * time.Second
	// hysteresisFactor prevents flapping: scale down only when well below threshold.
	hysteresisFactor = 0.75
	// scaleDownDelay is how long after any scaling the autoscaler waits before
	// scaling down, so new clusters are not deleted as soon as they are ready.
	scaleDownDelay = 5 * time.Minute
)

// ClusterPoolAutoscalerController adjusts ClusterPool replica count
// based on utilization = reserved clusters / the pool's spec.replicas.
type ClusterPoolAutoscalerController struct {
	client.Client
	Scheme *runtime.Scheme
	// Now returns the current time; tests override it. Defaults to time.Now.
	Now func() time.Time
}

func (r *ClusterPoolAutoscalerController) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
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
		if apierrors.IsNotFound(err) {
			// The target pool doesn't exist (yet): check again later.
			log.Info("target pool not found", "pool", autoscaler.Spec.TargetRef.Name)
			return ctrl.Result{RequeueAfter: autoscalerInterval}, nil
		}
		return ctrl.Result{}, err
	}

	// Measure against the size the pool is heading to, not the clusters that
	// exist right now: right after a scale-up the new clusters don't exist yet,
	// and counting only the old ones would scale up again.
	size := pool.Spec.Replicas
	if size <= 0 {
		size = pool.Status.ReadyReplicas + pool.Status.ReservedReplicas + pool.Status.PendingReplicas
	}
	if size == 0 {
		return ctrl.Result{RequeueAfter: autoscalerInterval}, nil
	}

	utilization := float64(pool.Status.ReservedReplicas) / float64(size)

	threshold := parseThreshold(autoscaler.Spec.UtilizationThreshold)
	minReplicas := autoscaler.Spec.MinReplicas
	maxReplicas := max32(autoscaler.Spec.MaxReplicas, minReplicas) // min > max: treat max as min

	current := pool.Spec.Replicas
	desired := current

	if utilization >= threshold {
		// Scale up to the smallest size that keeps utilization below the
		// threshold: floor(reserved / threshold) + 1, so reserved/desired is
		// strictly below it and the next check doesn't scale again.
		needed := int32(math.Floor(float64(pool.Status.ReservedReplicas)/threshold)) + 1
		// Always increase by at least 1 so we make progress even when reserved=0,
		// and never shrink here: a pool already above max is left alone.
		desired = max32(min32(max32(needed, current+1), maxReplicas), current)
	} else if utilization < threshold*hysteresisFactor {
		// Scale down — remove 10%, floored at MinReplicas — but not within
		// scaleDownDelay of the last change, and never grow here.
		last := autoscaler.Status.LastScaleTime
		if last == nil || r.now().Sub(last.Time) >= scaleDownDelay {
			desired = min32(max32(int32(float64(current)*0.9), minReplicas), current)
		}
	}

	if desired != current {
		direction := "scaling up pool"
		if desired < current {
			direction = "scaling down pool"
		}
		log.Info(direction,
			"pool", pool.Name,
			"utilization", fmt.Sprintf("%.2f", utilization),
			"current", current,
			"desired", desired,
		)
		now := metav1.NewTime(r.now())
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

// parseThreshold reads utilizationThreshold as a fraction in (0, 1]. Anything
// else ("70", "0", "-1", "abc") falls back to the default 0.70; "70%" is read
// as 0.70.
func parseThreshold(v string) float64 {
	v = strings.TrimSpace(v)
	pct := strings.HasSuffix(v, "%")
	t, err := strconv.ParseFloat(strings.TrimSuffix(v, "%"), 64)
	if err != nil {
		return 0.70
	}
	if pct {
		t /= 100
	}
	if math.IsNaN(t) || t <= 0 || t > 1 {
		return 0.70
	}
	return t
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
		For(&meridianv1alpha1.ClusterPoolAutoscaler{}, builder.WithPredicates(autoscalerEvents())).
		Complete(r)
}

// autoscalerEvents lets only spec changes trigger a reconcile. The controller
// updates its own status every run; reacting to that would re-run it at once
// and apply several scaling steps before the pool catches up.
func autoscalerEvents() predicate.Predicate {
	return predicate.GenerationChangedPredicate{}
}
