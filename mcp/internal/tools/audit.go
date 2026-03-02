package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/mark3labs/mcp-go/mcp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/meridian-io/meridian/mcp/internal/k8s"
)

// AuditEvent represents a single management or query event.
type AuditEvent struct {
	Time      string `json:"time"`
	Type      string `json:"type"`
	Resource  string `json:"resource"`
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Namespace string `json:"namespace"`
}

// ── get_audit_log ─────────────────────────────────────────────────────────────

func GetAuditLogTool() mcp.Tool {
	return mcp.NewTool("get_audit_log",
		mcp.WithDescription("Fetch recent management and cluster lifecycle events from the Kubernetes event log. Shows cluster phase transitions, catalog changes, scaling events, and errors."),
		mcp.WithString("namespace", mcp.Description("Kubernetes namespace to query. Defaults to the configured namespace.")),
		mcp.WithNumber("limit", mcp.Description("Maximum number of events to return. Defaults to 50.")),
		mcp.WithString("cluster_name", mcp.Description("Filter events to a specific cluster by name.")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithIdempotentHintAnnotation(true),
	)
}

func GetAuditLogHandler(c *k8s.Client) func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ns := mcp.ParseString(req, "namespace", "")
		limitF := mcp.ParseFloat64(req, "limit", 0)
		clusterFilter := mcp.ParseString(req, "cluster_name", "")

		if ns == "" {
			ns = c.Namespace
		}
		limit := int64(50)
		if limitF > 0 {
			limit = int64(limitF)
		}

		listOpts := metav1.ListOptions{Limit: limit}
		if clusterFilter != "" {
			listOpts.FieldSelector = fmt.Sprintf("involvedObject.name=%s", clusterFilter)
		}

		events, err := c.Typed.CoreV1().Events(ns).List(ctx, listOpts)
		if err != nil {
			return nil, fmt.Errorf("list events: %w", err)
		}

		// Sort by newest first.
		sort.Slice(events.Items, func(i, j int) bool {
			return events.Items[i].LastTimestamp.After(events.Items[j].LastTimestamp.Time)
		})

		log := make([]AuditEvent, 0, len(events.Items))
		for _, e := range events.Items {
			log = append(log, AuditEvent{
				Time:      e.LastTimestamp.UTC().Format("2006-01-02T15:04:05Z"),
				Type:      e.Type,
				Resource:  fmt.Sprintf("%s/%s", e.InvolvedObject.Kind, e.InvolvedObject.Name),
				Reason:    e.Reason,
				Message:   e.Message,
				Namespace: e.Namespace,
			})
		}

		out, _ := json.MarshalIndent(log, "", "  ")
		return mcp.NewToolResultText(string(out)), nil
	}
}
