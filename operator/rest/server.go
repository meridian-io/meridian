package rest

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/trino"
)

// ── Shared types ──────────────────────────────────────────────────────────────

type clusterSummary struct {
	Name           string `json:"name"`
	Phase          string `json:"phase"`
	Profile        string `json:"profile"`
	Workload       string `json:"workload,omitempty"`
	CoordinatorURL string `json:"coordinatorUrl,omitempty"`
	ReservedAt     string `json:"reservedAt,omitempty"`
	IdleAt         string `json:"idleAt,omitempty"`
	CreatedAt      string `json:"createdAt"`
}

type poolSummary struct {
	Name             string `json:"name"`
	Replicas         int32  `json:"replicas"`
	ReadyReplicas    int32  `json:"readyReplicas"`
	ReservedReplicas int32  `json:"reservedReplicas"`
	PendingReplicas  int32  `json:"pendingReplicas"`
	FailedReplicas   int32  `json:"failedReplicas"`
	DegradedReplicas int32  `json:"degradedReplicas,omitempty"`
	Workload         string `json:"workload,omitempty"`
	Profile          string `json:"profile"`
	Image            string `json:"image"`
}

func toClusterSummary(c meridianv1alpha1.Cluster) clusterSummary {
	s := clusterSummary{
		Name:           c.Name,
		Phase:          string(c.Status.Phase),
		Profile:        c.Spec.Profile,
		Workload:       c.Labels["meridian.io/workload"],
		CoordinatorURL: c.Status.CoordinatorURL,
		CreatedAt:      c.CreationTimestamp.UTC().Format(time.RFC3339),
	}
	if c.Status.ReservedAt != nil {
		s.ReservedAt = c.Status.ReservedAt.UTC().Format(time.RFC3339)
	}
	if c.Status.IdleAt != nil {
		s.IdleAt = c.Status.IdleAt.UTC().Format(time.RFC3339)
	}
	return s
}

func toPoolSummary(p meridianv1alpha1.ClusterPool) poolSummary {
	return poolSummary{
		Name:             p.Name,
		Replicas:         p.Spec.Replicas,
		ReadyReplicas:    p.Status.ReadyReplicas,
		ReservedReplicas: p.Status.ReservedReplicas,
		PendingReplicas:  p.Status.PendingReplicas,
		FailedReplicas:   p.Status.FailedReplicas,
		DegradedReplicas: p.Status.DegradedReplicas,
		Workload:         p.Spec.Workload,
		Profile:          p.Spec.Template.Profile,
		Image:            p.Spec.Template.Image,
	}
}

// ── POST /api/v1/clusters/reservations ───────────────────────────────────────

// ReservationHandler handles POST /api/v1/clusters/reservations.
// ClientID is extracted from the mTLS client certificate CN.
type ReservationHandler struct {
	Reserver  *trino.ClusterReserver
	Namespace string
}

type reservationRequest struct {
	ReservationID string `json:"reservationId"`
	Profile       string `json:"profile"`
	Workload      string `json:"workload,omitempty"`
}

type reservationResponse struct {
	ClusterName    string `json:"clusterName"`
	CoordinatorURL string `json:"coordinatorUrl"`
	ReservedAt     string `json:"reservedAt"`
}

func (h *ReservationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	clientID := extractClientID(r.TLS)
	if clientID == "" {
		http.Error(w, "missing client certificate", http.StatusUnauthorized)
		return
	}

	var req reservationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ReservationID == "" || req.Profile == "" {
		http.Error(w, "reservationId and profile are required", http.StatusBadRequest)
		return
	}

	result, err := h.Reserver.Reserve(r.Context(), trino.ReservationRequest{
		ClientID:      clientID,
		ReservationID: req.ReservationID,
		Profile:       req.Profile,
		Workload:      req.Workload,
		Namespace:     h.Namespace,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(reservationResponse{
		ClusterName:    result.ClusterName,
		CoordinatorURL: result.CoordinatorURL,
		ReservedAt:     result.ReservedAt.Format(time.RFC3339),
	})
}

// ── DELETE /api/v1/clusters/reservations/{reservationId} ─────────────────────

// ReleaseHandler releases a cluster reservation.
// The requesting client must own the reservation (clientID from cert must match).
type ReleaseHandler struct {
	Client    client.Client
	Namespace string
}

func (h *ReleaseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	clientID := extractClientID(r.TLS)
	if clientID == "" {
		http.Error(w, "missing client certificate", http.StatusUnauthorized)
		return
	}

	reservationID := r.PathValue("reservationId")
	if reservationID == "" {
		http.Error(w, "reservationId is required", http.StatusBadRequest)
		return
	}

	list := &meridianv1alpha1.ClusterList{}
	if err := h.Client.List(r.Context(), list,
		client.InNamespace(h.Namespace),
		client.MatchingFields{"spec.reservationId": reservationID},
	); err != nil {
		http.Error(w, "failed to list clusters", http.StatusInternalServerError)
		return
	}
	if len(list.Items) == 0 {
		http.Error(w, "reservation not found", http.StatusNotFound)
		return
	}

	cluster := &list.Items[0]
	if cluster.Spec.ClientID != clientID {
		http.Error(w, "reservation owned by a different client", http.StatusForbidden)
		return
	}

	patch := client.MergeFrom(cluster.DeepCopy())
	cluster.Spec.ClientID = ""
	cluster.Spec.ReservationID = ""
	if err := h.Client.Patch(r.Context(), cluster, patch); err != nil {
		http.Error(w, "failed to release reservation", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ── GET /api/v1/clusters ─────────────────────────────────────────────────────

// ListClustersHandler lists clusters with optional filters.
// Query params: phase, profile, workload.
type ListClustersHandler struct {
	Client    client.Client
	Namespace string
}

func (h *ListClustersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	opts := []client.ListOption{client.InNamespace(h.Namespace)}

	labels := client.MatchingLabels{}
	if profile := r.URL.Query().Get("profile"); profile != "" {
		labels["meridian.io/profile"] = profile
	}
	if workload := r.URL.Query().Get("workload"); workload != "" {
		labels["meridian.io/workload"] = workload
	}
	if len(labels) > 0 {
		opts = append(opts, labels)
	}

	list := &meridianv1alpha1.ClusterList{}
	if err := h.Client.List(r.Context(), list, opts...); err != nil {
		http.Error(w, "failed to list clusters", http.StatusInternalServerError)
		return
	}

	phase := r.URL.Query().Get("phase")
	summaries := make([]clusterSummary, 0, len(list.Items))
	for _, c := range list.Items {
		if phase != "" && string(c.Status.Phase) != phase {
			continue
		}
		summaries = append(summaries, toClusterSummary(c))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summaries)
}

// ── GET /api/v1/clusters/{name} ──────────────────────────────────────────────

// GetClusterHandler returns a single cluster by name.
type GetClusterHandler struct {
	Client    client.Client
	Namespace string
}

func (h *GetClusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cluster := &meridianv1alpha1.Cluster{}
	if err := h.Client.Get(r.Context(),
		types.NamespacedName{Name: name, Namespace: h.Namespace}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "cluster not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get cluster", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(toClusterSummary(*cluster))
}

// ── GET /api/v1/pools ────────────────────────────────────────────────────────

// ListPoolsHandler lists all ClusterPools.
type ListPoolsHandler struct {
	Client    client.Client
	Namespace string
}

func (h *ListPoolsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	list := &meridianv1alpha1.ClusterPoolList{}
	if err := h.Client.List(r.Context(), list, client.InNamespace(h.Namespace)); err != nil {
		http.Error(w, "failed to list pools", http.StatusInternalServerError)
		return
	}

	summaries := make([]poolSummary, 0, len(list.Items))
	for _, p := range list.Items {
		summaries = append(summaries, toPoolSummary(p))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summaries)
}

// ── PATCH /api/v1/pools/{name}/replicas ──────────────────────────────────────

// ScalePoolHandler updates the desired replica count of a ClusterPool.
type ScalePoolHandler struct {
	Client    client.Client
	Namespace string
}

type scaleRequest struct {
	Replicas int32 `json:"replicas"`
}

func (h *ScalePoolHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var req scaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Replicas < 1 {
		http.Error(w, "replicas must be >= 1", http.StatusBadRequest)
		return
	}

	pool := &meridianv1alpha1.ClusterPool{}
	if err := h.Client.Get(r.Context(),
		types.NamespacedName{Name: name, Namespace: h.Namespace}, pool); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "pool not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get pool", http.StatusInternalServerError)
		return
	}

	patch := client.MergeFrom(pool.DeepCopy())
	pool.Spec.Replicas = req.Replicas
	if err := h.Client.Patch(r.Context(), pool, patch); err != nil {
		http.Error(w, "failed to scale pool", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(toPoolSummary(*pool))
}

// ── Server wiring ─────────────────────────────────────────────────────────────

func extractClientID(tlsState *tls.ConnectionState) string {
	if tlsState == nil || len(tlsState.PeerCertificates) == 0 {
		return ""
	}
	return tlsState.PeerCertificates[0].Subject.CommonName
}

// NewServer builds the mTLS HTTP server for the Meridian REST API.
func NewServer(addr string, k8sClient client.Client, namespace string) *http.Server {
	reserver := trino.NewClusterReserver(k8sClient)

	mux := http.NewServeMux()

	// Reservation lifecycle
	mux.Handle("POST /api/v1/clusters/reservations", &ReservationHandler{
		Reserver: reserver, Namespace: namespace,
	})
	mux.Handle("DELETE /api/v1/clusters/reservations/{reservationId}", &ReleaseHandler{
		Client: k8sClient, Namespace: namespace,
	})

	// Cluster reads
	mux.Handle("GET /api/v1/clusters", &ListClustersHandler{
		Client: k8sClient, Namespace: namespace,
	})
	mux.Handle("GET /api/v1/clusters/{name}", &GetClusterHandler{
		Client: k8sClient, Namespace: namespace,
	})

	// Pool operations
	mux.Handle("GET /api/v1/pools", &ListPoolsHandler{
		Client: k8sClient, Namespace: namespace,
	})
	mux.Handle("PATCH /api/v1/pools/{name}/replicas", &ScalePoolHandler{
		Client: k8sClient, Namespace: namespace,
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return &http.Server{
		Addr:    addr,
		Handler: mux,
		TLSConfig: &tls.Config{
			ClientAuth: tls.RequireAndVerifyClientCert,
			MinVersion: tls.VersionTLS13,
		},
	}
}
