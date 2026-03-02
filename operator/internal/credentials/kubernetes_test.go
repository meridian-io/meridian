package credentials

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newK8sScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	return s
}

func newSecret(name, namespace string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
}

// TestKubernetesProvider_Fetch verifies that Secret data is returned as properties.
func TestKubernetesProvider_Fetch(t *testing.T) {
	secret := newSecret("mysql-credentials", "meridian", map[string][]byte{
		"connector.name":     []byte("mysql"),
		"connection-url":     []byte("jdbc:mysql://mysql:3306"),
		"connection-user":    []byte("root"),
		"connection-password": []byte("supersecret"),
	})

	c := fake.NewClientBuilder().
		WithScheme(newK8sScheme()).
		WithObjects(secret).
		Build()

	p := NewKubernetesProvider(c, "meridian")
	result, err := p.Fetch(context.Background(), "mysql-credentials")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cases := map[string]string{
		"connector.name":      "mysql",
		"connection-url":      "jdbc:mysql://mysql:3306",
		"connection-user":     "root",
		"connection-password": "supersecret",
	}
	for k, want := range cases {
		got, ok := result.Properties[k]
		if !ok {
			t.Errorf("property %q missing from result", k)
			continue
		}
		if got != want {
			t.Errorf("property %q = %q, want %q", k, got, want)
		}
	}

	if result.FetchedAt.IsZero() {
		t.Error("expected FetchedAt to be set")
	}
}

// TestKubernetesProvider_NotFound verifies a clear error when the Secret is missing.
func TestKubernetesProvider_NotFound(t *testing.T) {
	c := fake.NewClientBuilder().
		WithScheme(newK8sScheme()).
		Build()

	p := NewKubernetesProvider(c, "meridian")
	_, err := p.Fetch(context.Background(), "does-not-exist")
	if err == nil {
		t.Fatal("expected error for missing secret, got nil")
	}
}

// TestKubernetesProvider_EmptyData verifies an error when the Secret exists but has no data.
func TestKubernetesProvider_EmptyData(t *testing.T) {
	secret := newSecret("empty-secret", "meridian", nil)

	c := fake.NewClientBuilder().
		WithScheme(newK8sScheme()).
		WithObjects(secret).
		Build()

	p := NewKubernetesProvider(c, "meridian")
	_, err := p.Fetch(context.Background(), "empty-secret")
	if err == nil {
		t.Fatal("expected error for secret with no data, got nil")
	}
}

// TestKubernetesProvider_WrongNamespace verifies that cross-namespace reads fail.
func TestKubernetesProvider_WrongNamespace(t *testing.T) {
	// Secret exists in "other-ns", provider is scoped to "meridian".
	secret := newSecret("mysql-credentials", "other-ns", map[string][]byte{
		"connection-password": []byte("secret"),
	})

	c := fake.NewClientBuilder().
		WithScheme(newK8sScheme()).
		WithObjects(secret).
		Build()

	p := NewKubernetesProvider(c, "meridian")
	_, err := p.Fetch(context.Background(), "mysql-credentials")
	if err == nil {
		t.Fatal("expected error when secret is in a different namespace")
	}
}

// TestKubernetesProvider_Name verifies the provider identifier.
func TestKubernetesProvider_Name(t *testing.T) {
	p := NewKubernetesProvider(nil, "meridian")
	if p.Name() != "kubernetes" {
		t.Errorf("expected name kubernetes, got %q", p.Name())
	}
}

// TestKubernetesProvider_ImplementsInterface verifies the type satisfies SecretProvider.
func TestKubernetesProvider_ImplementsInterface(t *testing.T) {
	var _ SecretProvider = &KubernetesProvider{}
}
