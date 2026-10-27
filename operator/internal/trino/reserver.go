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
	maxRetries      = 5
	retryBackoff    = 200 * time.Millisecond
	poolLabel       = "meridian.io/cluster-pool"
	profileLabel    = "meridian.io/profile"
	workloadLabel   = "meridian.io/workload"
	pollInterval    = 2 * time.Second
	poolWaitTimeout = 30 * time.Second
)

// ErrNoIdleClusters is returned when no idle cluster is available in the pool.
// The REST handler may surface this to the client as a 503 after the wait window.
var ErrNoIdleClusters = errors.New("no idle clusters available in pool")

// ReservationRequest holds the parameters for reserving a cluster.
type ReservationRequest struct {
	ClientID      string
	ReservationID string
	Profile       string
	Workload      string
	Namespace     string
}

// ReservationResult is returned on a successful reservation.
type ReservationResult struct {
	ClusterName    string
	CoordinatorURL string
	ReservedAt     time.Time
}

// ClusterReserver handles idempotent cluster reservation from the hot standby pool.
type ClusterReserver struct {
	client client.Client
	// reader reads straight from the API server. Idempotency checks use it so
	// a retry sent right after a reservation sees it even if the cache hasn't.
	reader client.Reader
}

// NewClusterReserver returns a reserver. reader should be an uncached reader
// (mgr.GetAPIReader()); nil falls back to c.
func NewClusterReserver(c client.Client, reader client.Reader) *ClusterReserver {
	if reader == nil {
		reader = c
	}
	return &ClusterReserver{client: c, reader: reader}
}

// Reserve finds a healthy idle cluster and reserves it for the given client.
// It is idempotent: the same (clientId, reservationId) always returns the same cluster.
//
// If no idle cluster is immediately available, Reserve polls every 2 seconds
// for up to poolWaitTimeout (30s) so the caller does not have to implement its
// own retry loop — the autoscaler will spin up new capacity in parallel.
func (r *ClusterReserver) Reserve(ctx context.Context, req ReservationRequest) (*ReservationResult, error) {
	// Poll until an idle cluster is available or the wait window expires.
	// Every round first checks for an existing reservation (idempotency): a
	// duplicate request that was waiting alongside this one may have got a
	// cluster in the meantime.
	deadline := time.Now().Add(poolWaitTimeout)
	for {
		if existing, err := r.existingResult(ctx, req); err != nil || existing != nil {
			return existing, err
		}
		result, err := r.tryReserve(ctx, req)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, ErrNoIdleClusters) {
			return nil, err
		}

		if time.Now().After(deadline) {
			return nil, ErrNoIdleClusters
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// tryReserve attempts one round of optimistic-concurrency reservation.
// Returns ErrNoIdleClusters if the pool is empty right now.
func (r *ClusterReserver) tryReserve(ctx context.Context, req ReservationRequest) (*ReservationResult, error) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		cluster, err := r.pickIdleCluster(ctx, req)
		if err != nil {
			return nil, err
		}
		if cluster == nil {
			return nil, ErrNoIdleClusters
		}

		result, err := r.patchIfCurrent(ctx, cluster, req)
		if err != nil {
			if apierrors.IsConflict(err) {
				// Someone else changed this cluster first. If it was a
				// duplicate of this request, return its cluster; otherwise
				// retry with the next one.
				if existing, err := r.existingResult(ctx, req); err != nil || existing != nil {
					return existing, err
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(retryBackoff):
				}
				continue
			}
			return nil, err
		}
		return result, nil
	}
	return nil, fmt.Errorf("reservation failed after %d attempts: all idle clusters were claimed concurrently", maxRetries)
}

// existingResult returns the result for a live cluster already reserved for
// this (clientId, reservationId), or nil if there is none.
func (r *ClusterReserver) existingResult(ctx context.Context, req ReservationRequest) (*ReservationResult, error) {
	existing, err := r.findExistingReservation(ctx, req)
	if err != nil || existing == nil {
		return nil, err
	}
	// status.reservedAt is set by the cluster controller shortly after
	// the reservation; a retry can arrive before that.
	reservedAt := time.Now()
	if existing.Status.ReservedAt != nil {
		reservedAt = existing.Status.ReservedAt.Time
	}
	return &ReservationResult{
		ClusterName:    existing.Name,
		CoordinatorURL: existing.Status.CoordinatorURL,
		ReservedAt:     reservedAt,
	}, nil
}

// findExistingReservation returns a live cluster already reserved for this
// (clientId, reservationId). It reads from the API server, not the cache, so
// it sees a reservation made a moment ago. Clusters that are failed or being
// deleted don't count: the client gets a working cluster instead.
func (r *ClusterReserver) findExistingReservation(ctx context.Context, req ReservationRequest) (*meridianv1alpha1.Cluster, error) {
	list := &meridianv1alpha1.ClusterList{}
	if err := r.reader.List(ctx, list, client.InNamespace(req.Namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		c := &list.Items[i]
		if c.Spec.ClientID == req.ClientID && c.Spec.ReservationID == req.ReservationID &&
			c.DeletionTimestamp == nil && c.Status.Phase != meridianv1alpha1.ClusterPhaseFailed {
			return c, nil
		}
	}
	return nil, nil
}

// pickIdleCluster returns the oldest healthy idle cluster matching the profile and optional workload.
func (r *ClusterReserver) pickIdleCluster(ctx context.Context, req ReservationRequest) (*meridianv1alpha1.Cluster, error) {
	labels := client.MatchingLabels{profileLabel: req.Profile}
	if req.Workload != "" {
		labels[workloadLabel] = req.Workload
	}

	list := &meridianv1alpha1.ClusterList{}
	if err := r.client.List(ctx, list,
		client.InNamespace(req.Namespace),
		labels,
	); err != nil {
		return nil, err
	}

	var best *meridianv1alpha1.Cluster
	for i := range list.Items {
		c := &list.Items[i]
		// A cluster with a clientId is already claimed: the cluster controller
		// just hasn't moved it to Reserved yet.
		if c.Status.Phase != meridianv1alpha1.ClusterPhaseIdle || !c.Status.Ready ||
			c.Spec.ClientID != "" || c.DeletionTimestamp != nil {
			continue
		}
		if best == nil || idleBefore(c, best) {
			best = c
		}
	}
	return best, nil
}

// idleBefore orders candidates: the cluster that became idle earliest first
// (longest-waiting standby), then by name. The order is the same for every
// request, so duplicate requests racing each other go for the same cluster
// and the loser sees the winner's reservation.
func idleBefore(a, b *meridianv1alpha1.Cluster) bool {
	switch {
	case a.Status.IdleAt != nil && b.Status.IdleAt != nil && !a.Status.IdleAt.Equal(b.Status.IdleAt):
		return a.Status.IdleAt.Before(b.Status.IdleAt)
	case a.Status.IdleAt != nil && b.Status.IdleAt == nil:
		return true
	case a.Status.IdleAt == nil && b.Status.IdleAt != nil:
		return false
	}
	return a.Name < b.Name
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
