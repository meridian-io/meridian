package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/metrics"
	"github.com/meridian-io/meridian/operator/internal/trino"
)

func reservationSamples(t *testing.T, result string) uint64 {
	t.Helper()
	m := &dto.Metric{}
	if err := metrics.ReservationDuration.WithLabelValues("rest", result).(prometheus.Metric).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestReservationMetrics: each REST reservation is timed and labelled with
// its outcome.
func TestReservationMetrics(t *testing.T) {
	s := newTestScheme()
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(idleClusterObj("pool-a", "default")).
		WithIndex(&meridianv1alpha1.Cluster{}, "spec.clientId", func(o client.Object) []string {
			return []string{o.(*meridianv1alpha1.Cluster).Spec.ClientID}
		}).
		WithIndex(&meridianv1alpha1.Cluster{}, "spec.reservationId", func(o client.Object) []string {
			return []string{o.(*meridianv1alpha1.Cluster).Spec.ReservationID}
		}).
		WithStatusSubresource(&meridianv1alpha1.Cluster{}).
		Build()
	h := &ReservationHandler{Reserver: trino.NewClusterReserver(c, nil), Namespace: testNamespace}

	for _, tc := range []struct {
		name   string
		req    *http.Request
		code   int
		result string
	}{
		{"ok", reqWithCert(http.MethodPost, "/api/v1/clusters/reservations", `{"reservationId":"r1","profile":"default"}`, "client-a"), http.StatusCreated, metrics.ReserveOK},
		{"no certificate", req(http.MethodPost, "/api/v1/clusters/reservations", `{"reservationId":"r1","profile":"default"}`), http.StatusUnauthorized, metrics.ReserveUnauthorize},
		{"bad body", reqWithCert(http.MethodPost, "/api/v1/clusters/reservations", `{"reservationId":""}`, "client-a"), http.StatusBadRequest, metrics.ReserveBadRequest},
	} {
		before := reservationSamples(t, tc.result)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tc.req)
		if w.Code != tc.code {
			t.Errorf("%s: HTTP %d, want %d (%s)", tc.name, w.Code, tc.code, w.Body)
		}
		if n := reservationSamples(t, tc.result) - before; n != 1 {
			t.Errorf("%s: %q samples grew by %d, want 1", tc.name, tc.result, n)
		}
	}
}
