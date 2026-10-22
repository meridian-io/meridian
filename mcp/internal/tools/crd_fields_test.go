package tools

import (
	"os"
	"strings"
	"testing"
)

// The MCP server reads Cluster and ClusterPool objects as untyped maps, so a
// field-name mismatch with the operator's CRD fails silently: the API server
// prunes unknown fields and the tools see empty values. These tests pin the
// keys the tools read to the generated CRDs.
func TestStatusKeysMatchCRDs(t *testing.T) {
	for file, keys := range map[string][]string{
		"meridian.io_clusters.yaml": {"coordinatorUrl:", "phase:", "ready:", "idleAt:", "reservedAt:", "clientId:", "reservationId:", "workers:"},
		"meridian.io_clusterpools.yaml": {
			"readyReplicas:", "reservedReplicas:", "pendingReplicas:", "failedReplicas:", "degradedReplicas:", "replicas:", "workload:", "profile:",
		},
	} {
		b, err := os.ReadFile("../../../operator/config/crd/bases/" + file)
		if err != nil {
			t.Fatalf("read CRD: %v", err)
		}
		crd := string(b)
		for _, k := range keys {
			if !strings.Contains(crd, " "+k) {
				t.Errorf("%s has no field %q that the MCP tools read", file, strings.TrimSuffix(k, ":"))
			}
		}
	}
}

func TestClusterSummaryReadsCoordinatorURL(t *testing.T) {
	obj := map[string]interface{}{
		"metadata": map[string]interface{}{"name": "c1", "namespace": "meridian"},
		"status":   map[string]interface{}{"phase": "Idle", "coordinatorUrl": "http://c1-coordinator:8080"},
	}
	if got := clusterSummary(obj).CoordinatorURL; got != "http://c1-coordinator:8080" {
		t.Errorf("CoordinatorURL = %q, want the status.coordinatorUrl value", got)
	}
}
