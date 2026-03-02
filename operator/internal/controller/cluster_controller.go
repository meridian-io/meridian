package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meridianv1alpha1 "github.com/project-meridian/meridian/api/v1alpha1"
)

// ClusterController manages the lifecycle of individual Trino clusters.
// Phase transitions: Empty → Pending → Idle → Reserved (or Failed at any point).
type ClusterController struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=meridian.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=meridian.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *ClusterController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	cluster := &meridianv1alpha1.Cluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	switch cluster.Status.Phase {
	case meridianv1alpha1.ClusterPhaseEmpty, "":
		return r.reconcilePending(ctx, cluster)
	case meridianv1alpha1.ClusterPhasePending:
		return r.reconcileHealthCheck(ctx, cluster)
	case meridianv1alpha1.ClusterPhaseIdle:
		return r.reconcileIdle(ctx, cluster)
	case meridianv1alpha1.ClusterPhaseReserved:
		return r.reconcileReserved(ctx, cluster)
	case meridianv1alpha1.ClusterPhaseFailed:
		log.Info("cluster in failed state, skipping", "cluster", cluster.Name)
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// reconcilePending creates the Kubernetes resources for a new Trino cluster.
func (r *ClusterController) reconcilePending(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("creating resources for cluster", "cluster", cluster.Name)

	if err := r.ensureCoordinatorDeployment(ctx, cluster); err != nil {
		return r.setFailed(ctx, cluster, fmt.Sprintf("failed to create coordinator: %v", err))
	}

	if err := r.ensureWorkerDeployment(ctx, cluster); err != nil {
		return r.setFailed(ctx, cluster, fmt.Sprintf("failed to create workers: %v", err))
	}

	if err := r.ensureService(ctx, cluster); err != nil {
		return r.setFailed(ctx, cluster, fmt.Sprintf("failed to create service: %v", err))
	}

	cluster.Status.Phase = meridianv1alpha1.ClusterPhasePending
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue to check health
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// reconcileHealthCheck verifies the cluster is healthy before marking it Idle.
func (r *ClusterController) reconcileHealthCheck(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	healthy, err := r.isClusterHealthy(ctx, cluster)
	if err != nil {
		log.Error(err, "health check error", "cluster", cluster.Name)
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if !healthy {
		log.Info("cluster not yet healthy, requeueing", "cluster", cluster.Name)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	now := metav1.Now()
	cluster.Status.Phase = meridianv1alpha1.ClusterPhaseIdle
	cluster.Status.Ready = true
	cluster.Status.IdleAt = &now
	cluster.Status.CoordinatorURL = fmt.Sprintf("http://%s-coordinator.%s.svc.cluster.local:8080", cluster.Name, cluster.Namespace)

	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("cluster is now idle", "cluster", cluster.Name)
	return ctrl.Result{}, nil
}

// reconcileIdle watches for a reservation patch (clientId + reservationId set).
func (r *ClusterController) reconcileIdle(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	if cluster.Spec.ClientID != "" && cluster.Spec.ReservationID != "" {
		now := metav1.Now()
		cluster.Status.Phase = meridianv1alpha1.ClusterPhaseReserved
		cluster.Status.ReservedAt = &now
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		log.FromContext(ctx).Info("cluster reserved",
			"cluster", cluster.Name,
			"clientId", cluster.Spec.ClientID,
			"reservationId", cluster.Spec.ReservationID,
		)
	}
	return ctrl.Result{}, nil
}

// reconcileReserved handles eviction recovery and long-idle garbage collection.
func (r *ClusterController) reconcileReserved(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	return r.ensureCoordinatorRunning(ctx, cluster)
}

// isClusterHealthy checks that the coordinator deployment has at least one ready replica.
func (r *ClusterController) isClusterHealthy(ctx context.Context, cluster *meridianv1alpha1.Cluster) (bool, error) {
	dep := &appsv1.Deployment{}
	key := types.NamespacedName{
		Name:      fmt.Sprintf("%s-coordinator", cluster.Name),
		Namespace: cluster.Namespace,
	}
	if err := r.Get(ctx, key, dep); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return dep.Status.ReadyReplicas >= 1, nil
}

// ensureCoordinatorRunning recovers from coordinator pod eviction.
func (r *ClusterController) ensureCoordinatorRunning(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	healthy, err := r.isClusterHealthy(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !healthy {
		log.FromContext(ctx).Info("coordinator evicted, recovering", "cluster", cluster.Name)
		return r.reconcilePending(ctx, cluster)
	}
	return ctrl.Result{}, nil
}

func (r *ClusterController) setFailed(ctx context.Context, cluster *meridianv1alpha1.Cluster, reason string) (ctrl.Result, error) {
	log.FromContext(ctx).Error(fmt.Errorf(reason), "cluster failed", "cluster", cluster.Name)
	cluster.Status.Phase = meridianv1alpha1.ClusterPhaseFailed
	_ = r.Status().Update(ctx, cluster)
	return ctrl.Result{}, nil
}

// ensureCoordinatorDeployment creates the Trino coordinator Deployment if it doesn't exist.
func (r *ClusterController) ensureCoordinatorDeployment(ctx context.Context, cluster *meridianv1alpha1.Cluster) error {
	name := fmt.Sprintf("%s-coordinator", cluster.Name)
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, dep)
	if err == nil {
		return nil // already exists
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	replicas := int32(1)
	dep = &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    clusterLabels(cluster),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, meridianv1alpha1.GroupVersion.WithKind("Cluster")),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: clusterLabels(cluster)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: clusterLabels(cluster)},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "trino-coordinator",
						Image: cluster.Spec.Image,
						Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
						Env: []corev1.EnvVar{
							{Name: "TRINO_NODE_TYPE", Value: "coordinator"},
						},
					}},
				},
			},
		},
	}
	return r.Create(ctx, dep)
}

// ensureWorkerDeployment creates the Trino worker Deployment if it doesn't exist.
func (r *ClusterController) ensureWorkerDeployment(ctx context.Context, cluster *meridianv1alpha1.Cluster) error {
	name := fmt.Sprintf("%s-worker", cluster.Name)
	dep := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, dep)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	workers := cluster.Spec.Workers
	if workers == 0 {
		workers = 2
	}

	dep = &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    clusterLabels(cluster),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, meridianv1alpha1.GroupVersion.WithKind("Cluster")),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &workers,
			Selector: &metav1.LabelSelector{MatchLabels: clusterLabels(cluster)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: clusterLabels(cluster)},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "trino-worker",
						Image: cluster.Spec.Image,
						Env: []corev1.EnvVar{
							{Name: "TRINO_NODE_TYPE", Value: "worker"},
						},
					}},
				},
			},
		},
	}
	return r.Create(ctx, dep)
}

// ensureService creates the coordinator Service if it doesn't exist.
func (r *ClusterController) ensureService(ctx context.Context, cluster *meridianv1alpha1.Cluster) error {
	name := fmt.Sprintf("%s-coordinator", cluster.Name)
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, svc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}

	svc = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    clusterLabels(cluster),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, meridianv1alpha1.GroupVersion.WithKind("Cluster")),
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: clusterLabels(cluster),
			Ports: []corev1.ServicePort{{
				Name: "http",
				Port: 8080,
			}},
		},
	}
	return r.Create(ctx, svc)
}

func clusterLabels(cluster *meridianv1alpha1.Cluster) map[string]string {
	return map[string]string{
		"meridian.io/cluster": cluster.Name,
		"meridian.io/profile": cluster.Spec.Profile,
	}
}

func (r *ClusterController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&meridianv1alpha1.Cluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
