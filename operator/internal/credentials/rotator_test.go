package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// trinoHandler builds an http.Handler that returns a fixed Trino response for
// every POST to /v1/statement. DDL statements in real Trino often complete
// inline (no nextUri), so we simulate that here for simplicity.
func trinoHandler(responses []trinoResponse) http.Handler {
	idx := 0
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if idx >= len(responses) {
			http.Error(w, "no more responses", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(responses[idx])
		idx++
	})
}

func finishedResp() trinoResponse {
	return trinoResponse{Stats: struct{ State string `json:"state"` }{State: "FINISHED"}}
}

func failedResp(msg string) trinoResponse {
	return trinoResponse{
		Stats: struct{ State string `json:"state"` }{State: "FAILED"},
		Error: &struct {
			Message string `json:"message"`
		}{Message: msg},
	}
}

func mysqlSecret() *Secret {
	return &Secret{
		Properties: map[string]string{
			"connector.name":      "mysql",
			"connection-url":      "jdbc:mysql://mysql:3306",
			"connection-user":     "root",
			"connection-password": "newsecret",
		},
	}
}

// TestRotator_Success verifies a full DROP + CREATE rotation completes without error.
func TestRotator_Success(t *testing.T) {
	srv := httptest.NewServer(trinoHandler([]trinoResponse{
		finishedResp(), // DROP CATALOG IF EXISTS
		finishedResp(), // CREATE CATALOG
	}))
	defer srv.Close()

	r := NewRotator()
	err := r.Rotate(context.Background(), srv.URL, "mysql_prod", mysqlSecret())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestRotator_DropFails verifies that a Trino error on DROP is returned.
func TestRotator_DropFails(t *testing.T) {
	srv := httptest.NewServer(trinoHandler([]trinoResponse{
		failedResp("internal error during drop"),
	}))
	defer srv.Close()

	r := NewRotator()
	err := r.Rotate(context.Background(), srv.URL, "mysql_prod", mysqlSecret())
	if err == nil {
		t.Fatal("expected error when DROP fails")
	}
	if !strings.Contains(err.Error(), "internal error during drop") {
		t.Errorf("error message %q does not contain expected text", err.Error())
	}
}

// TestRotator_CreateFails verifies that a Trino error on CREATE is returned.
func TestRotator_CreateFails(t *testing.T) {
	srv := httptest.NewServer(trinoHandler([]trinoResponse{
		finishedResp(),
		failedResp("invalid connector properties"),
	}))
	defer srv.Close()

	r := NewRotator()
	err := r.Rotate(context.Background(), srv.URL, "mysql_prod", mysqlSecret())
	if err == nil {
		t.Fatal("expected error when CREATE fails")
	}
	if !strings.Contains(err.Error(), "invalid connector properties") {
		t.Errorf("error message %q does not contain expected text", err.Error())
	}
}

// TestRotator_MissingConnectorName verifies an error when connector.name is absent.
func TestRotator_MissingConnectorName(t *testing.T) {
	secret := &Secret{
		Properties: map[string]string{
			"connection-url":      "jdbc:mysql://mysql:3306",
			"connection-password": "pass",
		},
	}
	r := NewRotator()
	err := r.Rotate(context.Background(), "http://localhost:8080", "mysql_prod", secret)
	if err == nil {
		t.Fatal("expected error for missing connector.name")
	}
}

// TestRotator_EmptyCoordinatorURL verifies an error when the cluster has no URL.
func TestRotator_EmptyCoordinatorURL(t *testing.T) {
	r := NewRotator()
	err := r.Rotate(context.Background(), "", "mysql_prod", mysqlSecret())
	if err == nil {
		t.Fatal("expected error for empty coordinatorURL")
	}
}

// TestRotator_ContextCancelled verifies that a cancelled context stops the rotation.
func TestRotator_ContextCancelled(t *testing.T) {
	// Server that blocks — never responds.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	r := NewRotator()
	err := r.Rotate(ctx, srv.URL, "mysql_prod", mysqlSecret())
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// ── IsCatalogNotFound ─────────────────────────────────────────────────────────

func TestIsCatalogNotFound_True(t *testing.T) {
	cases := []error{
		errors.New("trino error: catalog not found"),
		errors.New("trino error: no such catalog 'mysql_prod'"),
		errors.New("trino error: catalog does not exist"),
		errors.New("CATALOG NOT FOUND"), // case insensitive
	}
	for _, err := range cases {
		if !IsCatalogNotFound(err) {
			t.Errorf("expected IsCatalogNotFound=true for %q", err)
		}
	}
}

func TestIsCatalogNotFound_False(t *testing.T) {
	cases := []error{
		errors.New("connection refused"),
		errors.New("invalid connector properties"),
		errors.New("timeout"),
		nil,
	}
	for _, err := range cases {
		if IsCatalogNotFound(err) {
			t.Errorf("expected IsCatalogNotFound=false for %v", err)
		}
	}
}

// ── buildCatalogSQL ───────────────────────────────────────────────────────────

func TestBuildCatalogSQL_WithProperties(t *testing.T) {
	sql := buildCatalogSQL("mysql_prod", "mysql", map[string]string{
		"connection-url":      "jdbc:mysql://host:3306",
		"connection-user":     "root",
		"connection-password": "secret",
	})

	if !strings.HasPrefix(sql, "CREATE CATALOG mysql_prod USING mysql WITH (") {
		t.Errorf("unexpected SQL prefix: %q", sql)
	}
	if !strings.Contains(sql, `"connection-url"='jdbc:mysql://host:3306'`) {
		t.Errorf("connection-url not found in SQL: %q", sql)
	}
}

func TestBuildCatalogSQL_NoProperties(t *testing.T) {
	sql := buildCatalogSQL("tpch", "tpch", map[string]string{})
	expected := "CREATE CATALOG tpch USING tpch"
	if sql != expected {
		t.Errorf("expected %q, got %q", expected, sql)
	}
}

func TestBuildCatalogSQL_EscapesSingleQuotes(t *testing.T) {
	sql := buildCatalogSQL("test", "mysql", map[string]string{
		"connection-password": "it's'complex",
	})
	if !strings.Contains(sql, `'it''s''complex'`) {
		t.Errorf("single quotes not escaped in SQL: %q", sql)
	}
}
