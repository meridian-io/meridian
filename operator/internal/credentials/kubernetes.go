package credentials

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesProvider reads catalog credentials from a Kubernetes Secret.
//
// The Secret's data map is used directly as Trino catalog properties.
// Each key in Secret.Data becomes a property key; the value is the raw
// string (the Kubernetes API base64-decodes it before delivery).
//
// Example Secret:
//
//	apiVersion: v1
//	kind: Secret
//	metadata:
//	  name: mysql-credentials
//	  namespace: meridian
//	type: Opaque
//	stringData:
//	  connector.name: mysql
//	  connection-url: "jdbc:mysql://mysql:3306"
//	  connection-user: root
//	  connection-password: secret
//
// Annotation usage:
//
//	meridian.io/rotate-credentials: "kubernetes/mysql_catalog/mysql-credentials"
type KubernetesProvider struct {
	client    client.Client
	namespace string
}

// NewKubernetesProvider creates a provider that reads Secrets from the given namespace.
func NewKubernetesProvider(c client.Client, namespace string) *KubernetesProvider {
	return &KubernetesProvider{client: c, namespace: namespace}
}

// Fetch retrieves the Kubernetes Secret named path from the operator namespace
// and returns its data as catalog properties.
func (p *KubernetesProvider) Fetch(ctx context.Context, path string) (*Secret, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: path, Namespace: p.namespace}

	if err := p.client.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("kubernetes secret %q not found in namespace %q: %w", path, p.namespace, err)
	}

	if len(secret.Data) == 0 {
		return nil, fmt.Errorf("kubernetes secret %q in namespace %q has no data", path, p.namespace)
	}

	props := make(map[string]string, len(secret.Data))
	for k, v := range secret.Data {
		props[k] = string(v)
	}

	return &Secret{
		Properties: props,
		FetchedAt:  time.Now(),
	}, nil
}

func (p *KubernetesProvider) Name() string { return "kubernetes" }
