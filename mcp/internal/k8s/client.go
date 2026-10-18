package k8s

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// GVRs for Meridian CRDs.
var (
	ClusterGVR = schema.GroupVersionResource{
		Group: "meridian.io", Version: "v1alpha1", Resource: "clusters",
	}
	ClusterPoolGVR = schema.GroupVersionResource{
		Group: "meridian.io", Version: "v1alpha1", Resource: "clusterpools",
	}
	ClusterPoolAutoscalerGVR = schema.GroupVersionResource{
		Group: "meridian.io", Version: "v1alpha1", Resource: "clusterpoolautoscalers",
	}
)

// operatorReprobeInterval is how often a missing operator is checked for again,
// so the MCP server notices an operator installed after it started.
const operatorReprobeInterval = 30 * time.Second

// Client wraps the Kubernetes dynamic + typed clients.
type Client struct {
	Dynamic   dynamic.Interface
	Typed     kubernetes.Interface
	Namespace string

	mu          sync.Mutex
	hasMeridian bool
	lastProbe   time.Time
}

// NewClientFromInterfaces builds a Client from existing clients (used in tests
// and by callers that already hold clients).
func NewClientFromInterfaces(dyn dynamic.Interface, typed kubernetes.Interface, namespace string) *Client {
	c := &Client{Dynamic: dyn, Typed: typed, Namespace: namespace}
	c.HasMeridianOperator()
	return c
}

// NewClient builds a Kubernetes client from kubeconfig or in-cluster config.
func NewClient(kubeconfig, namespace string) (*Client, error) {
	cfg, err := buildConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build kubeconfig: %w", err)
	}

	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}

	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("typed client: %w", err)
	}

	return NewClientFromInterfaces(dyn, typed, namespace), nil
}

// HasMeridianOperator reports whether the Meridian operator CRDs are installed.
// A positive result is remembered; a negative one is re-checked periodically.
func (c *Client) HasMeridianOperator() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hasMeridian || (!c.lastProbe.IsZero() && time.Since(c.lastProbe) < operatorReprobeInterval) {
		return c.hasMeridian
	}
	c.lastProbe = time.Now()
	c.hasMeridian = c.detectMeridianOperator()
	return c.hasMeridian
}

// detectMeridianOperator probes for the ClusterPool CRD.
func (c *Client) detectMeridianOperator() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Dynamic.Resource(ClusterPoolGVR).Namespace(c.Namespace).
		List(ctx, metav1.ListOptions{Limit: 1})
	return err == nil
}

// Ready checks that the Kubernetes API server is reachable.
func (c *Client) Ready(ctx context.Context) error {
	rc := c.Typed.Discovery().RESTClient()
	if rc == nil || reflect.ValueOf(rc).IsNil() { // fake clients have no REST client
		_, err := c.Typed.Discovery().ServerVersion()
		return err
	}
	return rc.Get().AbsPath("/version").Do(ctx).Error()
}

// ListClusters returns all Cluster CRs in the configured namespace.
func (c *Client) ListClusters(ctx context.Context, namespace string) ([]unstructured.Unstructured, error) {
	ns := c.resolveNamespace(namespace)
	list, err := c.Dynamic.Resource(ClusterGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// ListClustersWithSelector returns Cluster CRs matching a label selector string.
func (c *Client) ListClustersWithSelector(ctx context.Context, namespace, labelSelector string) ([]unstructured.Unstructured, error) {
	ns := c.resolveNamespace(namespace)
	list, err := c.Dynamic.Resource(ClusterGVR).Namespace(ns).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// GetCluster returns a single Cluster CR by name.
func (c *Client) GetCluster(ctx context.Context, name, namespace string) (*unstructured.Unstructured, error) {
	ns := c.resolveNamespace(namespace)
	return c.Dynamic.Resource(ClusterGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
}

// CreateCluster creates a Cluster CR.
func (c *Client) CreateCluster(ctx context.Context, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	ns := obj.GetNamespace()
	if ns == "" {
		ns = c.Namespace
		obj.SetNamespace(ns)
	}
	return c.Dynamic.Resource(ClusterGVR).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
}

// DeleteCluster deletes a Cluster CR by name.
func (c *Client) DeleteCluster(ctx context.Context, name, namespace string) error {
	ns := c.resolveNamespace(namespace)
	return c.Dynamic.Resource(ClusterGVR).Namespace(ns).Delete(ctx, name, metav1.DeleteOptions{})
}

// PatchCluster applies a merge patch to a Cluster CR.
func (c *Client) PatchCluster(ctx context.Context, name, namespace string, patch []byte) (*unstructured.Unstructured, error) {
	ns := c.resolveNamespace(namespace)
	return c.Dynamic.Resource(ClusterGVR).Namespace(ns).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
}

// GetClusterPool returns a ClusterPool CR by name.
func (c *Client) GetClusterPool(ctx context.Context, name, namespace string) (*unstructured.Unstructured, error) {
	ns := c.resolveNamespace(namespace)
	return c.Dynamic.Resource(ClusterPoolGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
}

// PatchClusterPool applies a merge patch to a ClusterPool CR.
func (c *Client) PatchClusterPool(ctx context.Context, name, namespace string, patch []byte) (*unstructured.Unstructured, error) {
	ns := c.resolveNamespace(namespace)
	return c.Dynamic.Resource(ClusterPoolGVR).Namespace(ns).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{},
	)
}

// ListClusterPools returns all ClusterPool CRs.
func (c *Client) ListClusterPools(ctx context.Context, namespace string) ([]unstructured.Unstructured, error) {
	ns := c.resolveNamespace(namespace)
	list, err := c.Dynamic.Resource(ClusterPoolGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (c *Client) resolveNamespace(ns string) string {
	if ns != "" {
		return ns
	}
	return c.Namespace
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	cfg, err := loadConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	// Never let a hung API call block a tool forever.
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	return cfg, nil
}

func loadConfig(kubeconfig string) (*rest.Config, error) {
	// 1. Explicit path via flag.
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	// 2. In-cluster (running inside a pod).
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	// 3. Default ~/.kube/config.
	home, _ := os.UserHomeDir()
	defaultKubeconfig := filepath.Join(home, ".kube", "config")
	return clientcmd.BuildConfigFromFlags("", defaultKubeconfig)
}
