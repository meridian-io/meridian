package rest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/trino"
)

func release(h http.Handler, rid, cn string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r := reqWithCert("DELETE", "/api/v1/clusters/reservations/"+rid, "", cn)
	r.SetPathValue("reservationId", rid)
	h.ServeHTTP(w, r)
	return w
}

func owner(t *testing.T, c client.Client, name string) string {
	t.Helper()
	cl := &meridianv1alpha1.Cluster{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: name}, cl); err != nil {
		t.Fatal(err)
	}
	return cl.Spec.ClientID
}

// Two clients may use the same reservationId. Each releases its own cluster.
func TestRelease_SharedReservationID(t *testing.T) {
	k8s := newTestClient(newTestScheme(),
		reservedClusterObj("ca", "team-a", "nightly"),
		reservedClusterObj("cb", "team-b", "nightly"))
	h := &ReleaseHandler{Client: k8s, Namespace: testNamespace}

	if w := release(h, "nightly", "team-b"); w.Code != http.StatusNoContent {
		t.Fatalf("team-b release: %d %s", w.Code, w.Body.String())
	}
	if got := owner(t, k8s, "cb"); got != "" {
		t.Errorf("cb still owned by %q", got)
	}
	if got := owner(t, k8s, "ca"); got != "team-a" {
		t.Errorf("team-a's cluster changed owner to %q", got)
	}
	if w := release(h, "nightly", "team-c"); w.Code != http.StatusForbidden {
		t.Errorf("team-c release: %d, want 403", w.Code)
	}
}

// A release decided on a stale copy must not wipe a newer reservation: the
// patch carries the version it read and fails with 409.
func TestRelease_StaleReadDoesNotWipeNewReservation(t *testing.T) {
	s := newTestScheme()
	stale := newTestClient(s, reservedClusterObj("c1", "team-a", "r1"))
	live := newTestClient(s, reservedClusterObj("c1", "team-a", "r1"))
	// After team-a's reservation expired, team-c reserved the cluster.
	cur := &meridianv1alpha1.Cluster{}
	_ = live.Get(context.Background(), client.ObjectKey{Namespace: testNamespace, Name: "c1"}, cur)
	cur.Spec.ClientID, cur.Spec.ReservationID = "team-c", "r9"
	if err := live.Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}

	h := &ReleaseHandler{Client: live, Reader: stale, Namespace: testNamespace}
	if w := release(h, "r1", "team-a"); w.Code != http.StatusConflict {
		t.Errorf("release on a stale read: %d, want 409", w.Code)
	}
	if got := owner(t, live, "c1"); got != "team-c" {
		t.Errorf("team-c's reservation was wiped: owner %q", got)
	}
}

// Only "no idle cluster" is a 503; other failures are 500.
func TestReserve_ErrorCodes(t *testing.T) {
	broken := fake.NewClientBuilder().WithScheme(newTestScheme()).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return errors.New("api server down")
			},
		}).Build()
	h := &ReservationHandler{Reserver: trino.NewClusterReserver(broken, nil), Namespace: testNamespace}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, reqWithCert("POST", "/api/v1/clusters/reservations", `{"reservationId":"r1","profile":"default"}`, "app"))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("API error: %d, want 500", w.Code)
	}
}
