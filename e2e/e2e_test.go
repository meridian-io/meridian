//go:build e2e

package e2e

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestE2E runs the scenarios in order on one install. Each scenario starts by
// waiting for the pool to be back at full strength, so one failure does not
// cascade into the rest.
func TestE2E(t *testing.T) {
	k := newKube(t)
	s := &suite{k: k, token: randHex(16)}

	if !t.Run("install", s.install) {
		t.FailNow()
	}
	if cfg.previousVersion != "" {
		if !t.Run("upgrade from "+cfg.previousVersion, s.upgrade) {
			t.FailNow()
		}
	}
	s.mcp = newMCPClient(s.token)

	t.Run("pool fills with profile resources", s.poolFills)
	t.Run("mcp queries an operator cluster", s.mcpQueries)
	t.Run("mcp creates and deletes a cluster", s.mcpCreateDelete)
	t.Run("reserve and release over rest", s.restReserveRelease)
	t.Run("reserve and release over mcp", s.mcpReserveRelease)
	t.Run("scale pool", s.scalePool)
	t.Run("sample catalog on every cluster", s.sampleCatalog)
	t.Run("coordinator crash recovery", s.coordinatorCrash)
	t.Run("credential rotation", s.credentialRotation)
	t.Run("mcp rolling restart drops no calls", s.mcpRollingRestart)
	t.Run("metrics are exported", s.metricsExported)
}

type suite struct {
	k     *kube
	token string
	pki   *pki
	mcp   *mcpClient
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ready waits for the pool to have poolSize Idle clusters and returns them.
func (s *suite) ready(t *testing.T) []clusterInfo {
	t.Helper()
	return s.k.waitPool(t, poolSize, poolSize, clusterReadyTimeout)
}

// ── install / upgrade ────────────────────────────────────────────────────────

func (s *suite) install(t *testing.T) {
	k := s.k
	_, err := k.core.CoreV1().Namespaces().Create(ctx(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: k.ns},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}

	s.pki = newPKI(t)
	secrets := []*corev1.Secret{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "meridian-mcp-token"},
			StringData: map[string]string{"token": s.token},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "meridian-rest-tls"},
			Data: map[string][]byte{
				"tls.crt": s.pki.serverCertPEM,
				"tls.key": s.pki.serverKeyPEM,
				"ca.crt":  s.pki.caPEM,
			},
		},
	}
	for _, sec := range secrets {
		if _, err := k.core.CoreV1().Secrets(k.ns).Create(ctx(), sec, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create secret %s: %v", sec.Name, err)
		}
	}

	// A small custom profile: exercises the profile ConfigMap feature and fits CI.
	profile := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "meridian-profile-" + profileName},
		Data: map[string]string{
			"jvmHeap":               "1G",
			"memory":                podMemory,
			"cpu":                   podCPU,
			"queryMaxMemory":        "512MB",
			"queryMaxMemoryPerNode": "256MB",
		},
	}
	if _, err := k.core.CoreV1().ConfigMaps(k.ns).Create(ctx(), profile, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create profile: %v", err)
	}

	values := filepath.Join(cfg.repoRoot, "hack/e2e/values.yaml")
	if cfg.previousVersion != "" {
		t.Logf("installing the published chart %s", cfg.previousVersion)
		run(t, "helm", "install", "meridian", "oci://ghcr.io/meridian-io/charts/meridian",
			"--version", cfg.previousVersion, "-n", k.ns, "-f", values, "--wait", "--timeout", "5m")
	} else {
		t.Log("installing the chart from this checkout")
		args := append([]string{"install", "meridian", filepath.Join(cfg.repoRoot, "charts/meridian"),
			"-n", k.ns, "-f", values, "--wait", "--timeout", "5m"}, imageArgs()...)
		run(t, "helm", args...)
	}

	k.createPool(t, poolSize)
	s.ready(t)
}

func imageArgs() []string {
	or, ot := splitImage(cfg.operatorImage)
	mr, mt := splitImage(cfg.mcpImage)
	return []string{
		"--set", "operator.image.repository=" + or, "--set", "operator.image.tag=" + ot,
		"--set", "mcp.image.repository=" + mr, "--set", "mcp.image.tag=" + mt,
	}
}

// upgrade follows the README: apply the new CRDs, then helm upgrade. Running
// clusters must survive the operator upgrade untouched.
func (s *suite) upgrade(t *testing.T) {
	k := s.k
	before := s.ready(t)

	run(t, "kubectl", "apply", "--server-side", "-f", filepath.Join(cfg.repoRoot, "charts/meridian/crds/"))
	args := append([]string{"upgrade", "meridian", filepath.Join(cfg.repoRoot, "charts/meridian"),
		"-n", k.ns, "-f", filepath.Join(cfg.repoRoot, "hack/e2e/values.yaml"), "--wait", "--timeout", "5m"}, imageArgs()...)
	run(t, "helm", args...)

	for component, img := range map[string]string{"operator": cfg.operatorImage, "mcp": cfg.mcpImage} {
		name := k.deploymentByComponent(t, component)
		run(t, "kubectl", "-n", k.ns, "rollout", "status", "deployment/"+name, "--timeout", "3m")
		dep, err := k.core.AppsV1().Deployments(k.ns).Get(ctx(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := dep.Spec.Template.Spec.Containers[0].Image; got != img {
			t.Errorf("%s image after upgrade = %s, want %s", component, got, img)
		}
	}

	// Give the new operator a few reconcile cycles, then compare.
	time.Sleep(45 * time.Second)
	after, err := k.poolClusters(poolName)
	if err != nil {
		t.Fatal(err)
	}
	if phases(after) != phases(before) {
		t.Errorf("clusters changed across the upgrade:\nbefore: %s\nafter:  %s", phases(before), phases(after))
	}
}

// ── scenarios ────────────────────────────────────────────────────────────────

// Scenario 1: the pool fills and every Trino pod is sized by the profile.
func (s *suite) poolFills(t *testing.T) {
	k := s.k
	for _, c := range s.ready(t) {
		if c.profile != profileName {
			t.Errorf("%s: spec.profile = %q, want %q", c.name, c.profile, profileName)
		}
		pods, err := k.core.CoreV1().Pods(k.ns).List(ctx(), metav1.ListOptions{LabelSelector: "meridian.io/cluster=" + c.name})
		if err != nil {
			t.Fatal(err)
		}
		if len(pods.Items) != 2 {
			t.Errorf("%s: %d pods, want 2 (coordinator + 1 worker)", c.name, len(pods.Items))
		}
		for _, p := range pods.Items {
			r := p.Spec.Containers[0].Resources
			if got := r.Requests.Memory().String(); got != podMemory {
				t.Errorf("%s: memory request %s, want %s", p.Name, got, podMemory)
			}
			if got := r.Limits.Memory().String(); got != podMemory {
				t.Errorf("%s: memory limit %s, want %s", p.Name, got, podMemory)
			}
			if got := r.Requests.Cpu().String(); got != podCPU {
				t.Errorf("%s: cpu request %s, want %s", p.Name, got, podCPU)
			}
		}
	}
	eventually(t, time.Minute, "pool status counts", func() (bool, string) {
		u, err := k.dyn.Resource(poolGVR).Namespace(k.ns).Get(ctx(), poolName, metav1.GetOptions{})
		if err != nil {
			return false, err.Error()
		}
		st, _ := u.Object["status"].(map[string]any)
		return fmt.Sprint(st["readyReplicas"]) == fmt.Sprint(poolSize), fmt.Sprintf("status=%v", st)
	})
}

// Scenario 2: a 2026-07-28 MCP client reads and queries an operator-created
// cluster. This is the path the v1.3.0 coordinatorUrl bug broke.
func (s *suite) mcpQueries(t *testing.T) {
	clusters := s.ready(t)

	if _, err := s.mcp.rpc("server/discover", "", nil); err != nil {
		t.Fatalf("server/discover: %v", err)
	}
	if _, err := newMCPClient("wrong-token").rpc("server/discover", "", nil); err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("wrong token: got %v, want HTTP 401", err)
	}

	pools := s.mcp.call(t, "list_pools", nil)
	if !strings.Contains(pools, `"`+poolName+`"`) {
		t.Errorf("list_pools does not list %s:\n%s", poolName, pools)
	}

	list := s.mcp.call(t, "list_clusters", nil)
	for _, c := range clusters {
		want := fmt.Sprintf(`"coordinatorUrl": "http://%s-coordinator.`, c.name)
		if !strings.Contains(list, want) {
			t.Errorf("list_clusters is missing %s for %s:\n%s", want, c.name, list)
		}
	}

	c := clusters[0].name
	if cats := s.mcp.call(t, "list_catalogs", map[string]any{"cluster_name": c}); !strings.Contains(cats, "system") {
		t.Errorf("list_catalogs on %s does not list system:\n%s", c, cats)
	}
	out, err := s.mcp.query(c, nodesQuery)
	if err != nil || !strings.Contains(out, "e2e-nodes-2") {
		t.Errorf("execute_query on %s: %v\n%s", c, err, out)
	}
	if _, err := s.mcp.query(c, "DROP TABLE system.runtime.nodes"); err == nil {
		t.Error("execute_query ran a write statement without --allow-writes")
	}
}

// Scenario 3: create_cluster makes a cluster the operator accepts and
// provisions; a query works as soon as it is Idle; delete_cluster removes it.
func (s *suite) mcpCreateDelete(t *testing.T) {
	k := s.k
	s.ready(t)
	name := "e2e-adhoc-" + randHex(3)

	s.mcp.call(t, "create_cluster", map[string]any{
		"name": name, "profile": profileName, "image": cfg.trinoImage, "workers": 1,
	})
	k.waitPhase(t, name, "Idle", clusterReadyTimeout)

	// Idle promises the cluster is ready for queries.
	if out, err := s.mcp.query(name, nodesQuery); err != nil || !strings.Contains(out, "e2e-nodes-2") {
		t.Errorf("query right after %s became Idle failed: %v\n%s", name, err, out)
		start := time.Now()
		eventually(t, 3*time.Minute, "query on "+name, func() (bool, string) {
			out, err := s.mcp.query(name, nodesQuery)
			if err != nil {
				return false, err.Error()
			}
			return strings.Contains(out, "e2e-nodes-2"), out
		})
		t.Logf("%s answered queries %s after it became Idle", name, time.Since(start).Round(time.Second))
	}

	s.mcp.call(t, "delete_cluster", map[string]any{"name": name})
	eventually(t, 2*time.Minute, "delete "+name, func() (bool, string) {
		_, ok, err := k.getCluster(name)
		if err != nil {
			return false, err.Error()
		}
		deps, _ := k.core.AppsV1().Deployments(k.ns).List(ctx(), metav1.ListOptions{LabelSelector: "meridian.io/cluster=" + name})
		return !ok && len(deps.Items) == 0, fmt.Sprintf("cluster exists=%v deployments=%d", ok, len(deps.Items))
	})
}

type reservation struct {
	ClusterName    string `json:"clusterName"`
	CoordinatorURL string `json:"coordinatorUrl"`
}

// Scenario 4a: reserve and release through the mTLS REST API.
func (s *suite) restReserveRelease(t *testing.T) {
	k := s.k
	s.ready(t)
	owner := newRESTClient(s.pki.pool, s.pki.clientCert(t, "e2e-client"))
	other := newRESTClient(s.pki.pool, s.pki.clientCert(t, "other-client"))

	if code, _, err := newRESTClient(s.pki.pool, nil).do(http.MethodGet, "/api/v1/pools", nil); err == nil && code != http.StatusUnauthorized {
		t.Errorf("request without a client certificate: HTTP %d, want a TLS error or 401", code)
	}
	if code, body, err := owner.do(http.MethodGet, "/api/v1/pools", nil); err != nil || code != http.StatusOK || !strings.Contains(string(body), poolName) {
		t.Fatalf("GET /api/v1/pools: %d %v %s", code, err, body)
	}

	rid := "r-rest-" + randHex(4)
	reserve := func() reservation {
		code, body, err := owner.do(http.MethodPost, "/api/v1/clusters/reservations",
			map[string]string{"reservationId": rid, "profile": profileName})
		if err != nil || code != http.StatusCreated {
			t.Fatalf("reserve: %d %v %s", code, err, body)
		}
		var r reservation
		if err := json.Unmarshal(body, &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := reserve()
	// Release even if a check below fails, so later scenarios get the cluster back.
	t.Cleanup(func() {
		_, _, _ = owner.do(http.MethodDelete, "/api/v1/clusters/reservations/"+rid, nil)
	})
	if again := reserve(); again.ClusterName != r.ClusterName {
		t.Errorf("same reservationId returned %s, then %s", r.ClusterName, again.ClusterName)
	}
	c := k.waitPhase(t, r.ClusterName, "Reserved", time.Minute)
	if c.clientID != "e2e-client" {
		t.Errorf("spec.clientId = %q, want e2e-client (from the certificate CN)", c.clientID)
	}
	if out, err := s.mcp.query(r.ClusterName, nodesQuery); err != nil || !strings.Contains(out, "e2e-nodes-2") {
		t.Errorf("query on the reserved cluster: %v\n%s", err, out)
	}

	if code, body, _ := other.do(http.MethodDelete, "/api/v1/clusters/reservations/"+rid, nil); code != http.StatusForbidden {
		t.Errorf("release by another client: HTTP %d %s, want 403", code, body)
	}
	if code, body, err := owner.do(http.MethodDelete, "/api/v1/clusters/reservations/"+rid, nil); err != nil || code != http.StatusNoContent {
		t.Fatalf("release: %d %v %s", code, err, body)
	}
	k.waitPhase(t, r.ClusterName, "Idle", time.Minute)

	// A reserved cluster counts toward the pool's replicas: no extra cluster.
	if cs, _ := k.poolClusters(poolName); len(cs) != poolSize {
		t.Errorf("pool has %d clusters after reserve and release, want %d: %s", len(cs), poolSize, phases(cs))
	}
}

// Scenario 4b: reserve and release through MCP, with the ownership check.
func (s *suite) mcpReserveRelease(t *testing.T) {
	k := s.k
	name := s.ready(t)[0].name

	s.mcp.call(t, "reserve_cluster", map[string]any{
		"cluster_name": name, "client_id": "e2e-mcp", "reservation_id": "r-mcp-" + randHex(4),
	})
	t.Cleanup(func() {
		_, _ = s.mcp.tryCall("release_cluster", map[string]any{"cluster_name": name})
	})
	c := k.waitPhase(t, name, "Reserved", time.Minute)
	if c.clientID != "e2e-mcp" {
		t.Errorf("spec.clientId = %q, want e2e-mcp", c.clientID)
	}
	if _, err := s.mcp.tryCall("release_cluster", map[string]any{"cluster_name": name, "client_id": "someone-else"}); err == nil {
		t.Error("release_cluster by another client succeeded")
	}
	s.mcp.call(t, "release_cluster", map[string]any{"cluster_name": name, "client_id": "e2e-mcp"})
	k.waitPhase(t, name, "Idle", time.Minute)
}

// Scenario 5: scale up by one through REST, down to one and back through MCP.
func (s *suite) scalePool(t *testing.T) {
	k := s.k
	s.ready(t)
	rest := newRESTClient(s.pki.pool, s.pki.clientCert(t, "e2e-client"))
	up := poolSize + 1

	if code, body, err := rest.do(http.MethodPatch, "/api/v1/pools/"+poolName+"/replicas", map[string]int{"replicas": up}); err != nil || code != http.StatusOK {
		t.Fatalf("scale to %d: %d %v %s", up, code, err, body)
	}
	k.waitPool(t, up, up, clusterReadyTimeout)

	s.mcp.call(t, "scale_pool", map[string]any{"pool_name": poolName, "replicas": 1})
	k.waitPool(t, 1, 1, 3*time.Minute)

	s.mcp.call(t, "scale_pool", map[string]any{"pool_name": poolName, "replicas": poolSize})
	if got := k.poolReplicas(t); got != int64(poolSize) {
		t.Errorf("spec.replicas = %d, want %d", got, poolSize)
	}
	s.ready(t)
}

// tpchQuery reads the sample catalog; tpch.tiny.nation has 25 rows.
const tpchQuery = "SELECT 'e2e-nation-' || CAST(count(*) AS varchar) FROM tpch.tiny.nation"

// Scenario 10: a new cluster can answer a demo query right away. The e2e
// profile has no sampleCatalogs key, so it inherits tpch from "default".
func (s *suite) sampleCatalog(t *testing.T) {
	for _, c := range s.ready(t) {
		if cats := s.mcp.call(t, "list_catalogs", map[string]any{"cluster_name": c.name}); !strings.Contains(cats, "tpch") {
			t.Errorf("%s: list_catalogs does not list tpch:\n%s", c.name, cats)
		}
		if out, err := s.mcp.query(c.name, tpchQuery); err != nil || !strings.Contains(out, "e2e-nation-25") {
			t.Errorf("%s: %v\n%s", c.name, err, out)
		}
	}
}

// Scenario 6: kill a coordinator. The pool must get back to full strength
// with every Idle cluster answering queries.
func (s *suite) coordinatorCrash(t *testing.T) {
	k := s.k
	victim := s.ready(t)[0].name

	err := k.core.CoreV1().Pods(k.ns).DeleteCollection(ctx(), metav1.DeleteOptions{
		GracePeriodSeconds: new(int64),
	}, metav1.ListOptions{LabelSelector: "meridian.io/role=coordinator,meridian.io/cluster=" + victim})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("killed the coordinator of %s", victim)

	eventually(t, clusterReadyTimeout, "every Idle cluster answers queries", func() (bool, string) {
		cs, err := k.poolClusters(poolName)
		if err != nil {
			return false, err.Error()
		}
		idle := byPhase(cs, "Idle")
		if len(cs) != poolSize || len(idle) != poolSize {
			return false, phases(cs)
		}
		for _, c := range idle {
			if out, err := s.mcp.query(c.name, nodesQuery); err != nil || !strings.Contains(out, "e2e-nodes-2") {
				return false, fmt.Sprintf("%s: query failed: %v", c.name, err)
			}
			// The sample catalog is a static file, so it survives the coordinator being replaced.
			if out, err := s.mcp.query(c.name, tpchQuery); err != nil || !strings.Contains(out, "e2e-nation-25") {
				return false, fmt.Sprintf("%s: tpch query failed: %v", c.name, err)
			}
		}
		return true, phases(cs)
	})
	if _, ok, _ := k.getCluster(victim); ok {
		t.Logf("%s recovered in place", victim)
	} else {
		t.Logf("%s was replaced", victim)
	}
}

// Scenario 8: rotate a catalog from a Kubernetes Secret through MCP. The tpch
// connector needs no external database.
func (s *suite) credentialRotation(t *testing.T) {
	k := s.k
	name := s.ready(t)[0].name
	const catalog, secret = "e2e_rotated", "e2e-rotation-secret"

	_, err := k.core.CoreV1().Secrets(k.ns).Create(ctx(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secret},
		StringData: map[string]string{"connector.name": "tpch"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	s.mcp.call(t, "rotate_credentials", map[string]any{
		"cluster_name": name, "catalog_name": catalog, "secret_path": secret, "provider": "kubernetes",
	})
	eventually(t, 2*time.Minute, "rotation on "+name, func() (bool, string) {
		c, _, err := k.getCluster(name)
		if err != nil {
			return false, err.Error()
		}
		_, pending := c.annotations["meridian.io/rotate-credentials"]
		return !pending && c.lastRotatedAt != "", fmt.Sprintf("annotation=%v lastRotatedAt=%q", pending, c.lastRotatedAt)
	})

	if cats := s.mcp.call(t, "list_catalogs", map[string]any{"cluster_name": name}); !strings.Contains(cats, catalog) {
		t.Errorf("list_catalogs does not list %s after rotation:\n%s", catalog, cats)
	}
	sql := "SELECT 'e2e-nation-' || CAST(count(*) AS varchar) FROM " + catalog + ".tiny.nation"
	if out, err := s.mcp.query(name, sql); err != nil || !strings.Contains(out, "e2e-nation-25") {
		t.Errorf("query on the rotated catalog: %v\n%s", err, out)
	}
}

// Scenario 7: restart the MCP Deployment under steady load through the
// Service. With the shutdown drain, no call may fail.
func (s *suite) mcpRollingRestart(t *testing.T) {
	k := s.k
	s.ready(t)
	name := k.deploymentByComponent(t, "mcp")

	var ok, failed atomic.Int64
	var mu sync.Mutex
	var errs []string
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := newMCPClient(s.token) // own connection pool, like separate clients
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := c.tryCall("list_pools", nil); err != nil {
					failed.Add(1)
					mu.Lock()
					if len(errs) < 5 {
						errs = append(errs, err.Error())
					}
					mu.Unlock()
				} else {
					ok.Add(1)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}

	time.Sleep(5 * time.Second)
	run(t, "kubectl", "-n", k.ns, "rollout", "restart", "deployment/"+name)
	run(t, "kubectl", "-n", k.ns, "rollout", "status", "deployment/"+name, "--timeout", "4m")
	// Keep the load on until the old pods are gone from the Service.
	time.Sleep(15 * time.Second)
	close(stop)
	wg.Wait()

	t.Logf("%d calls succeeded, %d failed during the rolling restart", ok.Load(), failed.Load())
	if failed.Load() > 0 {
		t.Errorf("%d calls failed during the rolling restart; first errors: %s",
			failed.Load(), strings.Join(slices.Compact(errs), " | "))
	}
	if ok.Load() < 100 {
		t.Errorf("only %d calls succeeded; the load did not run long enough", ok.Load())
	}
}
