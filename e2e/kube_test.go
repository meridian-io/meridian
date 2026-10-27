//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	clusterGVR = schema.GroupVersionResource{Group: "meridian.io", Version: "v1alpha1", Resource: "clusters"}
	poolGVR    = schema.GroupVersionResource{Group: "meridian.io", Version: "v1alpha1", Resource: "clusterpools"}
	scalerGVR  = schema.GroupVersionResource{Group: "meridian.io", Version: "v1alpha1", Resource: "clusterpoolautoscalers"}
)

type kube struct {
	core kubernetes.Interface
	dyn  dynamic.Interface
	ns   string
}

func newKube(t *testing.T) *kube {
	t.Helper()
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules,
		&clientcmd.ConfigOverrides{CurrentContext: cfg.kubeContext}).ClientConfig()
	if err != nil {
		t.Fatalf("kubeconfig context %q: %v", cfg.kubeContext, err)
	}
	rc.Timeout = 30 * time.Second
	return &kube{
		core: kubernetes.NewForConfigOrDie(rc),
		dyn:  dynamic.NewForConfigOrDie(rc),
		ns:   cfg.namespace,
	}
}

// run executes a CLI (kubectl, helm) and fails the test on error.
func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := runErr(name, args...)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return out
}

func runErr(name string, args ...string) (string, error) {
	switch name {
	case "kubectl":
		args = append([]string{"--context", cfg.kubeContext}, args...)
	case "helm":
		args = append([]string{"--kube-context", cfg.kubeContext}, args...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	return string(out), err
}

// eventually polls cond until it reports done, or fails the test after
// timeout with the last status cond returned.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	start := time.Now()
	last := ""
	for {
		done, status := cond()
		if done {
			t.Logf("%s: done after %s", what, time.Since(start).Round(time.Second))
			return
		}
		if status != last {
			t.Logf("%s: %s", what, status)
			last = status
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: timed out after %s (last: %s)", what, timeout, last)
		}
		time.Sleep(3 * time.Second)
	}
}

// ctx is the context for Kubernetes API calls; the client itself times out
// each request (see newKube).
func ctx() context.Context { return context.Background() }

// ── Meridian resources ───────────────────────────────────────────────────────

type clusterInfo struct {
	name, phase, coordinatorURL, clientID, profile string
	lastRotatedAt                                  string
	annotations                                    map[string]string
}

func toClusterInfo(u unstructured.Unstructured) clusterInfo {
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	url, _, _ := unstructured.NestedString(u.Object, "status", "coordinatorUrl")
	client, _, _ := unstructured.NestedString(u.Object, "spec", "clientId")
	profile, _, _ := unstructured.NestedString(u.Object, "spec", "profile")
	rotated, _, _ := unstructured.NestedString(u.Object, "status", "lastRotatedAt")
	return clusterInfo{
		name: u.GetName(), phase: phase, coordinatorURL: url, clientID: client,
		profile: profile, lastRotatedAt: rotated, annotations: u.GetAnnotations(),
	}
}

func (k *kube) getCluster(name string) (clusterInfo, bool, error) {
	u, err := k.dyn.Resource(clusterGVR).Namespace(k.ns).Get(ctx(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return clusterInfo{}, false, nil
	}
	if err != nil {
		return clusterInfo{}, false, err
	}
	return toClusterInfo(*u), true, nil
}

// poolClusters returns the pool's clusters that are not being deleted, sorted by name.
func (k *kube) poolClusters(pool string) ([]clusterInfo, error) {
	l, err := k.dyn.Resource(clusterGVR).Namespace(k.ns).List(ctx(), metav1.ListOptions{
		LabelSelector: "meridian.io/cluster-pool=" + pool,
	})
	if err != nil {
		return nil, err
	}
	var out []clusterInfo
	for _, u := range l.Items {
		if u.GetDeletionTimestamp() != nil {
			continue
		}
		out = append(out, toClusterInfo(u))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, nil
}

func phases(cs []clusterInfo) string {
	var s []string
	for _, c := range cs {
		s = append(s, c.name+"="+c.phase)
	}
	if len(s) == 0 {
		return "no clusters"
	}
	return strings.Join(s, " ")
}

func byPhase(cs []clusterInfo, phase string) []clusterInfo {
	var out []clusterInfo
	for _, c := range cs {
		if c.phase == phase {
			out = append(out, c)
		}
	}
	return out
}

// waitPool waits until the pool has exactly total live clusters, idle of them Idle.
func (k *kube) waitPool(t *testing.T, total, idle int, timeout time.Duration) []clusterInfo {
	t.Helper()
	var cs []clusterInfo
	eventually(t, timeout, fmt.Sprintf("pool %s: %d clusters, %d Idle", poolName, total, idle), func() (bool, string) {
		var err error
		cs, err = k.poolClusters(poolName)
		if err != nil {
			return false, err.Error()
		}
		return len(cs) == total && len(byPhase(cs, "Idle")) == idle, phases(cs)
	})
	return cs
}

func (k *kube) waitPhase(t *testing.T, name, phase string, timeout time.Duration) clusterInfo {
	t.Helper()
	var c clusterInfo
	eventually(t, timeout, fmt.Sprintf("cluster %s → %s", name, phase), func() (bool, string) {
		var ok bool
		var err error
		c, ok, err = k.getCluster(name)
		switch {
		case err != nil:
			return false, err.Error()
		case !ok:
			return false, "not found"
		}
		return c.phase == phase, "phase=" + c.phase
	})
	return c
}

func (k *kube) createPool(t *testing.T, replicas int) {
	t.Helper()
	pool := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "meridian.io/v1alpha1",
		"kind":       "ClusterPool",
		"metadata":   map[string]any{"name": poolName, "namespace": k.ns},
		"spec": map[string]any{
			"replicas": int64(replicas),
			"template": map[string]any{
				"profile": profileName,
				"image":   cfg.trinoImage,
				"workers": int64(1),
			},
		},
	}}
	if _, err := k.dyn.Resource(poolGVR).Namespace(k.ns).Create(ctx(), pool, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pool: %v", err)
	}
}

func (k *kube) poolReplicas(t *testing.T) int64 {
	t.Helper()
	u, err := k.dyn.Resource(poolGVR).Namespace(k.ns).Get(ctx(), poolName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pool: %v", err)
	}
	n, _, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")
	return n
}

// deploymentByComponent returns the name of the chart Deployment with the
// given app.kubernetes.io/component label.
func (k *kube) deploymentByComponent(t *testing.T, component string) string {
	t.Helper()
	l, err := k.core.AppsV1().Deployments(k.ns).List(ctx(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=" + component,
	})
	if err != nil || len(l.Items) != 1 {
		t.Fatalf("find %s deployment: %v (found %d)", component, err, len(l.Items))
	}
	return l.Items[0].Name
}
