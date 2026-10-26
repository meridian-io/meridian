package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
)

func profileCM(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "meridian-profile-" + name, Namespace: "meridian"},
		Data:       data,
	}
}

func qty(s string) resource.Quantity { return resource.MustParse(s) }

func assertRes(t *testing.T, what string, got podResources, mem, cpu string) {
	t.Helper()
	if got.Memory.Cmp(qty(mem)) != 0 || got.CPU.Cmp(qty(cpu)) != 0 {
		t.Errorf("%s: got memory=%s cpu=%s, want memory=%s cpu=%s", what, got.Memory.String(), got.CPU.String(), mem, cpu)
	}
}

func TestBuiltinProfileResources(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).Build()
	for name, want := range map[string][2]string{
		"default": {"5Gi", "1"},
		"medium":  {"10Gi", "2"},
		"large":   {"20Gi", "4"},
		"unknown": {"5Gi", "1"}, // falls back to default
	} {
		p := lookupProfile(context.Background(), c, "meridian", name)
		assertRes(t, name+" coordinator", p.Coordinator, want[0], want[1])
		assertRes(t, name+" worker", p.Worker, want[0], want[1])
		heap, _ := parseJavaSize(p.JVMHeap)
		if p.Worker.Memory.Value() <= heap {
			t.Errorf("%s: pod memory %s must exceed JVM heap %s", name, p.Worker.Memory.String(), p.JVMHeap)
		}
	}
}

func TestCustomProfileResources(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(
		profileCM("both", map[string]string{"memory": "12Gi", "cpu": "3"}),
		profileCM("split", map[string]string{"memory": "12Gi", "cpu": "3", "coordinatorMemory": "6Gi", "workerCPU": "8"}),
		profileCM("heap", map[string]string{"jvmHeap": "6G"}),
		profileCM("heapm", map[string]string{"jvmHeap": "3072m"}),
		profileCM("bad", map[string]string{"memory": "lots", "cpu": "-1", "workerMemory": "7Gi"}),
		profileCM("large", map[string]string{"cpu": "6"}),
	).Build()
	ctx := context.Background()

	p := lookupProfile(ctx, c, "meridian", "both")
	assertRes(t, "both coordinator", p.Coordinator, "12Gi", "3")
	assertRes(t, "both worker", p.Worker, "12Gi", "3")

	p = lookupProfile(ctx, c, "meridian", "split")
	assertRes(t, "split coordinator", p.Coordinator, "6Gi", "3")
	assertRes(t, "split worker", p.Worker, "12Gi", "8")

	// Memory derived from heap: 6G * 1.25 = 7.5Gi.
	p = lookupProfile(ctx, c, "meridian", "heap")
	assertRes(t, "heap worker", p.Worker, "7680Mi", "1")
	p = lookupProfile(ctx, c, "meridian", "heapm")
	assertRes(t, "heapm worker", p.Worker, "3840Mi", "1")

	// A ConfigMap named after a built-in tier starts from that tier.
	p = lookupProfile(ctx, c, "meridian", "large")
	assertRes(t, "large override worker", p.Worker, "20Gi", "6")
	if p.JVMHeap != "16G" {
		t.Errorf("large override heap = %s, want 16G", p.JVMHeap)
	}

	// Invalid values are ignored; valid ones still apply.
	p = lookupProfile(ctx, c, "meridian", "bad")
	assertRes(t, "bad coordinator", p.Coordinator, "5Gi", "1")
	assertRes(t, "bad worker", p.Worker, "7Gi", "1")
}

func TestParseJavaSize(t *testing.T) {
	cases := map[string]int64{"4G": 4 << 30, "4g": 4 << 30, "512m": 512 << 20, "1024K": 1 << 20, "100": 100, "1T": 1 << 40}
	for in, want := range cases {
		if got, ok := parseJavaSize(in); !ok || got != want {
			t.Errorf("parseJavaSize(%q) = %d, %v; want %d", in, got, ok, want)
		}
	}
	for _, bad := range []string{"", "G", "4GB", "-4G", "4.5G"} {
		if _, ok := parseJavaSize(bad); ok {
			t.Errorf("parseJavaSize(%q) should fail", bad)
		}
	}
}

// TestDeploymentsCarryResources verifies the Trino containers get the profile's
// resources, and the init container gets small explicit ones.
func TestDeploymentsCarryResources(t *testing.T) {
	s := newTestScheme()
	cluster := newCluster("res", "meridian", meridianv1alpha1.ClusterPhaseEmpty)
	cluster.Spec.Profile = "medium"
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &ClusterController{Client: c, Scheme: s}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "res", Namespace: "meridian"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"res-coordinator", "res-worker"} {
		dep := &appsv1.Deployment{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "meridian"}, dep); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		spec := dep.Spec.Template.Spec
		res := spec.Containers[0].Resources
		if res.Requests.Memory().Cmp(qty("10Gi")) != 0 || res.Limits.Memory().Cmp(qty("10Gi")) != 0 {
			t.Errorf("%s memory request/limit = %s/%s, want 10Gi/10Gi", name, res.Requests.Memory(), res.Limits.Memory())
		}
		if res.Requests.Cpu().Cmp(qty("2")) != 0 {
			t.Errorf("%s cpu request = %s, want 2", name, res.Requests.Cpu())
		}
		if _, hasCPULimit := res.Limits[corev1.ResourceCPU]; hasCPULimit {
			t.Errorf("%s must not set a CPU limit (throttles the JVM)", name)
		}
		initRes := spec.InitContainers[0].Resources
		if initRes.Limits.Memory().IsZero() || initRes.Requests.Cpu().IsZero() {
			t.Errorf("%s init container needs explicit resources for ResourceQuota namespaces", name)
		}
	}
}

// TestSampleCatalogs: the built-in default profile (and custom profiles built
// on it) include tpch; medium and large do not; a profile can opt out with
// sampleCatalogs: "" or choose tpch/tpcds. Unknown names are ignored.
func TestSampleCatalogs(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newTestScheme()).WithObjects(
		profileCM("small", map[string]string{"memory": "2Gi"}),
		profileCM("none", map[string]string{"sampleCatalogs": ""}),
		profileCM("both", map[string]string{"sampleCatalogs": " tpch, tpcds ,mysql"}),
		profileCM("large", map[string]string{"sampleCatalogs": "tpcds"}),
	).Build()
	for name, want := range map[string]string{
		"default": "tpch",
		"unknown": "tpch", // falls back to default
		"small":   "tpch", // custom profile starts from default
		"medium":  "",
		"none":    "",
		"both":    "tpch,tpcds",
		"large":   "tpcds", // a ConfigMap can turn them on for a built-in tier
	} {
		p := lookupProfile(context.Background(), c, "meridian", name)
		if got := strings.Join(p.SampleCatalogs, ","); got != want {
			t.Errorf("%s: sample catalogs %q, want %q", name, got, want)
		}
	}
}

// TestSampleCatalogInClusterConfig: the tpch catalog is in the cluster
// ConfigMap and both init containers copy catalogs into Trino's catalog
// directory, so it survives pod restarts.
func TestSampleCatalogInClusterConfig(t *testing.T) {
	s := newTestScheme()
	cluster := newCluster("test-cluster", "meridian", meridianv1alpha1.ClusterPhaseEmpty)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &ClusterController{Client: c, Scheme: s}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "meridian"},
	}); err != nil {
		t.Fatal(err)
	}
	cm := &corev1.ConfigMap{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster-config", Namespace: "meridian"}, cm); err != nil {
		t.Fatal(err)
	}
	if got := cm.Data["catalog-tpch.properties"]; got != "connector.name=tpch\n" {
		t.Errorf("catalog-tpch.properties = %q", got)
	}
	for _, role := range []string{"coordinator", "worker"} {
		dep := &appsv1.Deployment{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster-" + role, Namespace: "meridian"}, dep); err != nil {
			t.Fatal(err)
		}
		cmd := strings.Join(dep.Spec.Template.Spec.InitContainers[0].Command, " ")
		if !strings.Contains(cmd, "/etc/trino/catalog/") {
			t.Errorf("%s init container does not copy catalogs: %s", role, cmd)
		}
	}
}
