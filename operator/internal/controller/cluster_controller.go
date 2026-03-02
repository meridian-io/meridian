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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/credentials"
)

const (
	// clusterFinalizer guards against abrupt deletion of Reserved clusters
	// and gives the controller a window for any pre-delete cleanup.
	clusterFinalizer = "meridian.io/cluster-protection"

	// pendingTimeout is the maximum time a cluster may remain Pending before
	// being marked Failed (covers CrashLoopBackOff, OOMKilled, node full, etc.).
	pendingTimeout = 10 * time.Minute
)

// ClusterController manages the lifecycle of individual Trino clusters.
// Phase transitions: Empty → Pending → Idle → Reserved (or Failed at any point).
type ClusterController struct {
	client.Client
	Scheme    *runtime.Scheme
	Providers map[string]credentials.SecretProvider
	Rotator   *credentials.Rotator
	Cache     map[string]*credentials.SecretCache
}

// +kubebuilder:rbac:groups=meridian.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=meridian.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list

func (r *ClusterController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	cluster := &meridianv1alpha1.Cluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion: run pre-delete cleanup then release the finalizer.
	if !cluster.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, cluster)
	}

	// Ensure the protection finalizer is present on every live cluster.
	// We do not return after adding it — the update triggers a re-queue,
	// but we continue in this cycle to avoid an extra round-trip.
	if !controllerutil.ContainsFinalizer(cluster, clusterFinalizer) {
		controllerutil.AddFinalizer(cluster, clusterFinalizer)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Credential rotation takes priority over lifecycle reconciliation.
	// The annotation is set by the MCP rotate_credentials tool and cleared
	// only after a successful rotation (or a non-retriable error).
	if ann, ok := cluster.Annotations[credentials.AnnotationRotateCredentials]; ok && ann != "" {
		return r.reconcileRotation(ctx, cluster, ann)
	}

	switch cluster.Status.Phase {
	case meridianv1alpha1.ClusterPhaseEmpty:
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
// If the coordinator has not become ready within pendingTimeout the cluster is
// marked Failed so the pool controller can delete and replace it.
func (r *ClusterController) reconcileHealthCheck(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Guard: stuck-pending detection. creationTimestamp approximates when the
	// cluster entered Pending (Empty → Pending transition is near-instant).
	if time.Since(cluster.CreationTimestamp.Time) > pendingTimeout {
		return r.setFailed(ctx, cluster,
			fmt.Sprintf("coordinator not ready after %v — marking Failed for replacement", pendingTimeout))
	}

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

// reconcileReserved handles release (clientId cleared) and eviction recovery.
func (r *ClusterController) reconcileReserved(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	// Reservation cleared — transition back to Idle.
	if cluster.Spec.ClientID == "" && cluster.Spec.ReservationID == "" {
		now := metav1.Now()
		cluster.Status.Phase = meridianv1alpha1.ClusterPhaseIdle
		cluster.Status.ReservedAt = nil
		cluster.Status.IdleAt = &now
		log.FromContext(ctx).Info("cluster released, returning to idle", "cluster", cluster.Name)
		return ctrl.Result{}, r.Status().Update(ctx, cluster)
	}
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

// reconcileDelete runs pre-delete cleanup and removes the protection finalizer
// so Kubernetes can proceed with garbage-collecting the Cluster object and its
// owned Deployments/Services (via owner references).
func (r *ClusterController) reconcileDelete(ctx context.Context, cluster *meridianv1alpha1.Cluster) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	if cluster.Status.Phase == meridianv1alpha1.ClusterPhaseReserved {
		log.Info("deleting Reserved cluster — in-flight Trino queries may fail",
			"cluster", cluster.Name,
			"clientId", cluster.Spec.ClientID)
	}
	controllerutil.RemoveFinalizer(cluster, clusterFinalizer)
	return ctrl.Result{}, r.Update(ctx, cluster)
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

// ── Credential rotation ───────────────────────────────────────────────────────

// reconcileRotation handles the meridian.io/rotate-credentials annotation.
// It parses the annotation, fetches the secret, and executes DROP + CREATE
// on the live Trino cluster. Backoff is applied on failure; the annotation
// is only cleared on success or a non-retriable error.
func (r *ClusterController) reconcileRotation(ctx context.Context, cluster *meridianv1alpha1.Cluster, annotationValue string) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	rotReq, err := credentials.ParseRotationAnnotation(annotationValue)
	if err != nil {
		return r.clearAnnotationAndFail(ctx, cluster, fmt.Sprintf("malformed rotation annotation: %v", err))
	}

	cache, ok := r.Cache[rotReq.Provider]
	if !ok {
		return r.clearAnnotationAndFail(ctx, cluster,
			fmt.Sprintf("unknown credential provider %q — operator was not started with this provider", rotReq.Provider))
	}

	// Mark rotation in progress for visibility in kubectl / Web UI.
	if cluster.Status.RotatingCatalog != rotReq.CatalogName {
		cluster.Status.RotatingCatalog = rotReq.CatalogName
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	secret, err := cache.Get(ctx, rotReq.SecretPath)
	if err != nil {
		return r.handleRotationFailure(ctx, cluster, fmt.Sprintf("fetch secret %q: %v", rotReq.SecretPath, err))
	}

	if err := r.Rotator.Rotate(ctx, cluster.Status.CoordinatorURL, rotReq.CatalogName, secret); err != nil {
		// Reactive path: catalog was missing — creating it fresh counts as success.
		if credentials.IsCatalogNotFound(err) {
			log.Info("catalog not found during rotation — recreated from secret",
				"cluster", cluster.Name, "catalog", rotReq.CatalogName)
			return r.completeRotation(ctx, cluster, rotReq.CatalogName, cache, rotReq.SecretPath)
		}
		return r.handleRotationFailure(ctx, cluster, fmt.Sprintf("rotate catalog %q: %v", rotReq.CatalogName, err))
	}

	cache.Invalidate(rotReq.SecretPath)
	log.Info("credential rotation complete", "cluster", cluster.Name, "catalog", rotReq.CatalogName)
	return r.completeRotation(ctx, cluster, rotReq.CatalogName, cache, rotReq.SecretPath)
}

// completeRotation clears the annotation, resets failure counters, and records LastRotatedAt.
func (r *ClusterController) completeRotation(ctx context.Context, cluster *meridianv1alpha1.Cluster, catalogName string, cache *credentials.SecretCache, secretPath string) (ctrl.Result, error) {
	// Clear the annotation via a metadata-only patch to avoid conflicting with
	// concurrent spec changes from the REST API.
	patch := []byte(`{"metadata":{"annotations":{"` + credentials.AnnotationRotateCredentials + `":null}}}`)
	if err := r.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	cluster.Status.LastRotatedAt = &now
	cluster.Status.RotationFailures = 0
	cluster.Status.RotatingCatalog = ""
	cluster.Status.Conditions = setCondition(cluster.Status.Conditions, metav1.Condition{
		Type:               "CredentialRotation",
		Status:             metav1.ConditionTrue,
		Reason:             "RotationSucceeded",
		Message:            fmt.Sprintf("Catalog %q rotated successfully", catalogName),
		LastTransitionTime: now,
	})
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

// handleRotationFailure increments the failure counter and requeues with exponential backoff.
func (r *ClusterController) handleRotationFailure(ctx context.Context, cluster *meridianv1alpha1.Cluster, reason string) (ctrl.Result, error) {
	log.FromContext(ctx).Error(fmt.Errorf(reason), "rotation failed",
		"cluster", cluster.Name,
		"failures", cluster.Status.RotationFailures+1)

	cluster.Status.RotationFailures++
	cluster.Status.Conditions = setCondition(cluster.Status.Conditions, metav1.Condition{
		Type:               "CredentialRotation",
		Status:             metav1.ConditionFalse,
		Reason:             "RotationFailed",
		Message:            reason,
		LastTransitionTime: metav1.Now(),
	})
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	backoff := credentials.BackoffDuration(cluster.Status.RotationFailures)
	return ctrl.Result{RequeueAfter: backoff}, nil
}

// clearAnnotationAndFail handles non-retriable errors (bad annotation, unknown provider).
// Clears the annotation so the operator does not loop, sets a Failed condition,
// but does NOT change the cluster Phase — rotation failure ≠ cluster failure.
func (r *ClusterController) clearAnnotationAndFail(ctx context.Context, cluster *meridianv1alpha1.Cluster, reason string) (ctrl.Result, error) {
	log.FromContext(ctx).Error(fmt.Errorf(reason), "non-retriable rotation error", "cluster", cluster.Name)

	patch := []byte(`{"metadata":{"annotations":{"` + credentials.AnnotationRotateCredentials + `":null}}}`)
	if err := r.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return ctrl.Result{}, err
	}

	cluster.Status.RotatingCatalog = ""
	cluster.Status.Conditions = setCondition(cluster.Status.Conditions, metav1.Condition{
		Type:               "CredentialRotation",
		Status:             metav1.ConditionFalse,
		Reason:             "RotationFailed",
		Message:            reason,
		LastTransitionTime: metav1.Now(),
	})
	return ctrl.Result{}, r.Status().Update(ctx, cluster)
}

// setCondition upserts a condition into the conditions slice.
func setCondition(conditions []metav1.Condition, newCond metav1.Condition) []metav1.Condition {
	for i, c := range conditions {
		if c.Type == newCond.Type {
			conditions[i] = newCond
			return conditions
		}
	}
	return append(conditions, newCond)
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
