package http

import (
	"crypto/tls"
	"encoding/json"
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/project-meridian/meridian/internal/trino"
)

// ReservationHandler handles POST /api/v1/clusters/reservations.
// ClientID is extracted from the mTLS client certificate CN.
type ReservationHandler struct {
	Reserver  *trino.ClusterReserver
	Namespace string
}

type reservationRequest struct {
	ReservationID string `json:"reservationId"`
	Profile       string `json:"profile"`
}

type reservationResponse struct {
	ClusterName    string `json:"clusterName"`
	CoordinatorURL string `json:"coordinatorUrl"`
	ReservedAt     string `json:"reservedAt"`
}

func (h *ReservationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract clientId from mTLS certificate CN.
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
		ReservedAt:     result.ReservedAt.Format("2006-01-02T15:04:05Z"),
	})
}

// extractClientID pulls the CommonName from the mTLS client certificate.
func extractClientID(tlsState *tls.ConnectionState) string {
	if tlsState == nil || len(tlsState.PeerCertificates) == 0 {
		return ""
	}
	return tlsState.PeerCertificates[0].Subject.CommonName
}

// NewServer builds the mTLS HTTP server for the reservation API.
func NewServer(addr string, k8sClient client.Client, namespace string) *http.Server {
	reserver := trino.NewClusterReserver(k8sClient)

	mux := http.NewServeMux()
	mux.Handle("/api/v1/clusters/reservations", &ReservationHandler{
		Reserver:  reserver,
		Namespace: namespace,
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
