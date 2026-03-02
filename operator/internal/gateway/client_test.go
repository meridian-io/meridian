package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegister_OK(t *testing.T) {
	var received backendRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/gateway/backend/modify/add" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := Register(context.Background(), srv.URL, "pool-abc", "http://pool-abc:8080", "analytics")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.Name != "pool-abc" {
		t.Errorf("expected name pool-abc, got %q", received.Name)
	}
	if received.ProxyTo != "http://pool-abc:8080" {
		t.Errorf("unexpected proxyTo: %q", received.ProxyTo)
	}
	if !received.Active {
		t.Error("expected active=true")
	}
	if received.RoutingGroup != "analytics" {
		t.Errorf("expected routingGroup analytics, got %q", received.RoutingGroup)
	}
}

func TestDeregister_OK(t *testing.T) {
	var received backendRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/gateway/backend/modify/delete" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := Deregister(context.Background(), srv.URL, "pool-abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if received.Name != "pool-abc" {
		t.Errorf("expected name pool-abc, got %q", received.Name)
	}
}

func TestRegister_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := Register(context.Background(), srv.URL, "pool-abc", "http://pool-abc:8080", "adhoc")
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}
