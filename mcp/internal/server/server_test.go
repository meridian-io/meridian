package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/meridian-io/meridian/mcp/internal/k8s"
	"github.com/meridian-io/meridian/mcp/internal/tools"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func testClient() *k8s.Client {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		k8s.ClusterGVR:     "ClusterList",
		k8s.ClusterPoolGVR: "ClusterPoolList",
	})
	return k8s.NewClientFromInterfaces(dyn, kubefake.NewSimpleClientset(), "meridian")
}

func TestToolSplitIsComplete(t *testing.T) {
	if len(CoreTools) != 12 || len(HeldTools) != 7 {
		t.Fatalf("approved split is 12 core + 7 held, got %d + %d", len(CoreTools), len(HeldTools))
	}
	entries := allTools(testClient(), tools.QueryOptions{})
	seen := map[string]bool{}
	for _, n := range append(append([]string{}, CoreTools...), HeldTools...) {
		if seen[n] {
			t.Errorf("tool %q listed twice", n)
		}
		seen[n] = true
		e, ok := entries[n]
		if !ok {
			t.Errorf("tool %q has no implementation", n)
			continue
		}
		if e.tool.Name != n {
			t.Errorf("registry key %q maps to tool named %q", n, e.tool.Name)
		}
	}
	if len(entries) != len(seen) {
		t.Errorf("registry has %d tools but the split lists %d", len(entries), len(seen))
	}
}

func TestNewRegistersCoreToolsByDefault(t *testing.T) {
	s, names := New(testClient(), "test", Options{})
	got := s.ListTools()
	if len(got) != 12 || len(names) != 12 {
		t.Fatalf("default server registers %d tools, want 12", len(got))
	}
	for _, n := range HeldTools {
		if _, ok := got[n]; ok {
			t.Errorf("held tool %q must be off by default", n)
		}
	}
}

func TestNewWithEnabledTools(t *testing.T) {
	extra, err := ParseEnableTools("scale_pool, add_catalog")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := New(testClient(), "test", Options{ExtraTools: extra})
	got := s.ListTools()
	if len(got) != 14 {
		t.Fatalf("got %d tools, want 14", len(got))
	}
	for _, n := range []string{"scale_pool", "add_catalog"} {
		if _, ok := got[n]; !ok {
			t.Errorf("%q should be enabled", n)
		}
	}

	all, _ := ParseEnableTools("all")
	s, _ = New(testClient(), "test", Options{ExtraTools: all})
	if n := len(s.ListTools()); n != 19 {
		t.Fatalf("--enable-tools=all registers %d tools, want 19", n)
	}
}

func TestParseEnableTools(t *testing.T) {
	if got, err := ParseEnableTools(""); err != nil || len(got) != 0 {
		t.Errorf("empty spec: %v, %v", got, err)
	}
	if got, err := ParseEnableTools("execute_query,scale_pool"); err != nil || len(got) != 1 || !got["scale_pool"] {
		t.Errorf("core names should be accepted and ignored: %v, %v", got, err)
	}
	if _, err := ParseEnableTools("scale_pools"); err == nil || !strings.Contains(err.Error(), "unknown tool") {
		t.Errorf("typo should be rejected, got %v", err)
	}
}

func TestLoadToken(t *testing.T) {
	if tok, err := LoadToken("", ""); err != nil || tok != "" {
		t.Errorf("no token: %q, %v", tok, err)
	}
	if tok, err := LoadToken("", "  env-token-0123456789  "); err != nil || tok != "env-token-0123456789" {
		t.Errorf("env token: %q, %v", tok, err)
	}
	if _, err := LoadToken("", "short"); err == nil {
		t.Error("short token should be rejected")
	}

	f := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(f, []byte("file-token-0123456789\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if tok, err := LoadToken(f, "env-token-0123456789"); err != nil || tok != "file-token-0123456789" {
		t.Errorf("file should win over env: %q, %v", tok, err)
	}
	if _, err := LoadToken(filepath.Join(t.TempDir(), "missing"), ""); err == nil {
		t.Error("missing token file should be an error")
	}
}
