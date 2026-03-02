package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const requestTimeout = 10 * time.Second

type backendRequest struct {
	Name         string `json:"name"`
	ProxyTo      string `json:"proxyTo,omitempty"`
	Active       bool   `json:"active,omitempty"`
	RoutingGroup string `json:"routingGroup,omitempty"`
}

// Register adds a Trino cluster to the Trino Gateway as an active backend.
// Best-effort — callers should log errors but not fail the lifecycle transition.
func Register(ctx context.Context, endpoint, clusterName, coordinatorURL, routingGroup string) error {
	body := backendRequest{
		Name:         clusterName,
		ProxyTo:      coordinatorURL,
		Active:       true,
		RoutingGroup: routingGroup,
	}
	return post(ctx, endpoint+"/gateway/backend/modify/add", body)
}

// Deregister removes a Trino cluster from the Trino Gateway.
// Best-effort — callers should log errors but not block on failure.
func Deregister(ctx context.Context, endpoint, clusterName string) error {
	body := backendRequest{Name: clusterName}
	return post(ctx, endpoint+"/gateway/backend/modify/delete", body)
}

func post(ctx context.Context, url string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("gateway returned %d", resp.StatusCode)
	}
	return nil
}
