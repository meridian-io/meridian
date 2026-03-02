package trino

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

const (
	maxRetries    = 5
	retryBackoff  = 200 * time.Millisecond
	poolLabel     = "meridian.io/cluster-pool"
	profileLabel  = "meridian.io/profile"
)

// ReservationRequest holds the parameters for reserving a cluster.
type ReservationRequest struct {
	ClientID      string
	ReservationID string
	Profile       string
	Namespace     string
}

// ReservationResult is returned on a successful reservation.
type ReservationResult struct {
	ClusterName    string
	CoordinatorURL string
	ReservedAt     time.Time
}

// ClusterReserver handles idempotent cluster reservation from the warm pool.
type ClusterReserver struct {
	client client.Client
}

func NewClusterReserver(c client.Client) *ClusterReserver {
	return &ClusterReserver{client: c}
}

// Reserve finds a healthy idle cluster and reserves it for the given client.
// It is idempotent: the same (clientId, reservationId) always returns the same cluster.
// Uses optimistic concurrency with up to maxRetries attempts.
func (r *ClusterReserver) Reserve(ctx context.Context, req ReservationRequest) (*ReservationResult, error) {
	// Check for existing reservation (idempotency).
	existing, err := r.findExistingReservation(ctx, req)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return &ReservationResult{
			ClusterName:    existing.Name,
			CoordinatorURL: existing.Status.CoordinatorURL,
			ReservedAt:     existing.Status.ReservedAt.Time,
		}, nil
	}

	// Retry loop with optimistic concurrency.
	for attempt := 0; attempt < maxRetries; attempt++ {
		cluster, err := r.pickIdleCluster(ctx, req)
		if err != nil {
			return nil, err
		}
		if cluster == nil {
			return nil, errors.New("no idle clusters available in pool")
		}

		result, err := r.patchIfCurrent(ctx, cluster, req)
		if err != nil {
			if apierrors.IsConflict(err) {
				// Another request grabbed this cluster — retry.
				time.Sleep(retryBackoff)
				continue
			}
			return nil, err
		}
		return result, nil
	}

	return nil, fmt.Errorf("reservation failed after %d attempts: all idle clusters were claimed concurrently", maxRetries)
}

// findExistingReservation returns a cluster already reserved for this (clientId, reservationId).
func (r *ClusterReserver) findExistingReservation(ctx context.Context, req ReservationRequest) (*meridianv1alpha1.Cluster, error) {
	list := &meridianv1alpha1.ClusterList{}
	if err := r.client.List(ctx, list,
		client.InNamespace(req.Namespace),
		client.MatchingFields{
			"spec.clientId":      req.ClientID,
			"spec.reservationId": req.ReservationID,
		},
	); err != nil {
		return nil, err
	}
	if len(list.Items) > 0 {
		return &list.Items[0], nil
	}
	return nil, nil
}

// pickIdleCluster returns the oldest healthy idle cluster matching the profile.
func (r *ClusterReserver) pickIdleCluster(ctx context.Context, req ReservationRequest) (*meridianv1alpha1.Cluster, error) {
	list := &meridianv1alpha1.ClusterList{}
	if err := r.client.List(ctx, list,
		client.InNamespace(req.Namespace),
		client.MatchingLabels{profileLabel: req.Profile},
	); err != nil {
		return nil, err
	}

	var best *meridianv1alpha1.Cluster
	for i := range list.Items {
		c := &list.Items[i]
		if c.Status.Phase == meridianv1alpha1.ClusterPhaseIdle && c.Status.Ready {
			if best == nil {
				best = c
				continue
			}
			// Prefer the cluster that became idle earliest (longest-waiting warm cluster).
			if c.Status.IdleAt != nil && best.Status.IdleAt != nil &&
				c.Status.IdleAt.Before(best.Status.IdleAt) {
				best = c
			}
		}
	}
	return best, nil
}

// patchIfCurrent atomically patches the cluster with clientId + reservationId.
// Returns a conflict error if the resource version changed since we read it.
func (r *ClusterReserver) patchIfCurrent(ctx context.Context, cluster *meridianv1alpha1.Cluster, req ReservationRequest) (*ReservationResult, error) {
	patch := client.MergeFromWithOptions(cluster.DeepCopy(), client.MergeFromWithOptimisticLock{})
	cluster.Spec.ClientID = req.ClientID
	cluster.Spec.ReservationID = req.ReservationID

	if err := r.client.Patch(ctx, cluster, patch); err != nil {
		return nil, err
	}

	return &ReservationResult{
		ClusterName:    cluster.Name,
		CoordinatorURL: cluster.Status.CoordinatorURL,
		ReservedAt:     time.Now(),
	}, nil
}
