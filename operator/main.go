package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/controller"
	"github.com/meridian-io/meridian/operator/internal/credentials"
	"github.com/meridian-io/meridian/operator/rest"
)

var scheme = runtime.NewScheme()

// buildCredentialProvider initialises the configured secret backend and wraps it
// in a TTL cache. Returns maps keyed by provider name for injection into ClusterController.
func buildCredentialProvider(
	ctx context.Context,
	providerName string,
	c client.Reader,
	namespace string,
	vaultAddr, vaultRole, vaultMount string,
	awsRegion string,
) (map[string]credentials.SecretProvider, map[string]*credentials.SecretCache, error) {
	providers := make(map[string]credentials.SecretProvider)
	caches := make(map[string]*credentials.SecretCache)

	switch providerName {
	case "kubernetes":
		p := credentials.NewKubernetesProvider(c, namespace)
		providers[p.Name()] = p
		caches[p.Name()] = credentials.NewSecretCache(p, credentials.DefaultTTL)

	case "vault":
		if vaultAddr == "" {
			return nil, nil, fmt.Errorf("--vault-addr is required when --credential-provider=vault")
		}
		p := credentials.NewVaultProvider(vaultAddr, vaultRole, vaultMount)
		providers[p.Name()] = p
		caches[p.Name()] = credentials.NewSecretCache(p, credentials.DefaultTTL)

	case "aws-secrets-manager":
		if awsRegion == "" {
			return nil, nil, fmt.Errorf("--aws-region is required when --credential-provider=aws-secrets-manager")
		}
		p, err := credentials.NewAWSProvider(ctx, awsRegion)
		if err != nil {
			return nil, nil, fmt.Errorf("init AWS Secrets Manager provider: %w", err)
		}
		providers[p.Name()] = p
		caches[p.Name()] = credentials.NewSecretCache(p, credentials.DefaultTTL)

	default:
		return nil, nil, fmt.Errorf("unknown --credential-provider %q: must be kubernetes, vault, or aws-secrets-manager", providerName)
	}

	// Always include kubernetes as a fallback so clusters can use either provider.
	// The annotation determines which one is actually used per rotation request.
	if providerName != "kubernetes" {
		p := credentials.NewKubernetesProvider(c, namespace)
		providers[p.Name()] = p
		caches[p.Name()] = credentials.NewSecretCache(p, credentials.DefaultTTL)
	}

	return providers, caches, nil
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(meridianv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr string
		probeAddr   string
		namespace   string
		restAddr    string
		tlsCert     string
		tlsKey      string
		tlsCA       string

		// Credential provider flags.
		credentialProvider string
		vaultAddr          string
		vaultRole          string
		vaultMount         string
		awsRegion          string

		clusterConcurrency int
		poolMaxCreates     int
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8081", "Address for the metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8082", "Address for health probes.")
	flag.StringVar(&namespace, "namespace", "meridian", "Namespace to manage clusters in.")
	flag.StringVar(&restAddr, "rest-addr", ":8443", "Address for the reservation REST API (mTLS). Set to empty to disable.")
	flag.StringVar(&tlsCert, "tls-cert", "", "Path to TLS certificate file for the REST API.")
	flag.StringVar(&tlsKey, "tls-key", "", "Path to TLS key file for the REST API.")
	flag.StringVar(&tlsCA, "tls-ca", "", "Path to CA certificate for verifying client certs (mTLS).")
	flag.StringVar(&credentialProvider, "credential-provider", "kubernetes",
		"Secret backend for credential rotation: kubernetes, vault, or aws-secrets-manager.")
	flag.StringVar(&vaultAddr, "vault-addr", "",
		"Vault server address (required when --credential-provider=vault).")
	flag.StringVar(&vaultRole, "vault-role", "meridian-operator",
		"Vault Kubernetes auth role (used when --credential-provider=vault).")
	flag.StringVar(&vaultMount, "vault-mount", "secret",
		"Vault KV v2 mount path (used when --credential-provider=vault).")
	flag.StringVar(&awsRegion, "aws-region", "",
		"AWS region (required when --credential-provider=aws-secrets-manager).")
	flag.IntVar(&clusterConcurrency, "cluster-concurrency", controller.DefaultClusterConcurrency,
		"Number of Cluster resources reconciled in parallel.")
	flag.IntVar(&poolMaxCreates, "pool-max-creates-per-cycle", controller.DefaultMaxCreatesPerCycle,
		"Maximum clusters a ClusterPool creates per reconcile cycle.")
	flag.Parse()

	if clusterConcurrency < 1 || poolMaxCreates < 1 {
		fmt.Fprintln(os.Stderr, "--cluster-concurrency and --pool-max-creates-per-cycle must be at least 1")
		os.Exit(1)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{})))
	log := ctrl.Log.WithName("main")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          true,
		LeaderElectionID:        "meridian-operator-leader.meridian.io",
		LeaderElectionNamespace: namespace,
	})
	if err != nil {
		log.Error(err, "failed to create manager")
		os.Exit(1)
	}

	// Register field indexes required by ClusterReserver.
	ctx := context.Background()
	if err := mgr.GetFieldIndexer().IndexField(ctx, &meridianv1alpha1.Cluster{}, "spec.clientId",
		func(obj client.Object) []string {
			return []string{obj.(*meridianv1alpha1.Cluster).Spec.ClientID}
		}); err != nil {
		log.Error(err, "failed to index spec.clientId")
		os.Exit(1)
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &meridianv1alpha1.Cluster{}, "spec.reservationId",
		func(obj client.Object) []string {
			return []string{obj.(*meridianv1alpha1.Cluster).Spec.ReservationID}
		}); err != nil {
		log.Error(err, "failed to index spec.reservationId")
		os.Exit(1)
	}

	// Build credential provider and cache based on --credential-provider flag.
	providers, caches, err := buildCredentialProvider(ctx, credentialProvider, mgr.GetAPIReader(), namespace,
		vaultAddr, vaultRole, vaultMount, awsRegion)
	if err != nil {
		log.Error(err, "failed to initialise credential provider", "provider", credentialProvider)
		os.Exit(1)
	}
	log.Info("credential provider initialised", "provider", credentialProvider)

	if err = (&controller.ClusterController{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Providers:               providers,
		Rotator:                 credentials.NewRotator(),
		Cache:                   caches,
		MaxConcurrentReconciles: clusterConcurrency,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "failed to setup ClusterController")
		os.Exit(1)
	}

	if err = (&controller.ClusterPoolController{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		MaxCreatesPerCycle: poolMaxCreates,
		APIReader:          mgr.GetAPIReader(),
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "failed to setup ClusterPoolController")
		os.Exit(1)
	}

	if err = (&controller.ClusterPoolAutoscalerController{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "failed to setup ClusterPoolAutoscalerController")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "failed to add healthz check")
		os.Exit(1)
	}
	// Ready only once the informer caches have synced; before that the
	// operator would act on an empty view of the cluster.
	if err := mgr.AddReadyzCheck("readyz", cacheSyncedCheck(mgr.GetCache())); err != nil {
		log.Error(err, "failed to add readyz check")
		os.Exit(1)
	}

	// Run the mTLS reservation REST API under the manager so it shuts down
	// gracefully with the operator.
	if restAddr != "" && tlsCert != "" && tlsKey != "" {
		srv := rest.NewServer(restAddr, mgr.GetClient(), namespace, tlsCA)
		if err := mgr.Add(&rest.Runnable{Server: srv, CertFile: tlsCert, KeyFile: tlsKey}); err != nil {
			log.Error(err, "failed to register REST API")
			os.Exit(1)
		}
		log.Info("REST API enabled", "addr", restAddr)
	} else {
		log.Info("REST API disabled — set --rest-addr, --tls-cert, --tls-key to enable")
	}

	log.Info("starting Project Meridian operator", "namespace", namespace)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "operator exited with error")
		os.Exit(1)
	}
}

// cacheSyncedCheck reports ready once the manager's informer caches are synced.
func cacheSyncedCheck(c cache.Cache) healthz.Checker {
	return func(req *http.Request) error {
		ctx, cancel := context.WithTimeout(req.Context(), time.Second)
		defer cancel()
		if !c.WaitForCacheSync(ctx) {
			return fmt.Errorf("informer caches not synced")
		}
		return nil
	}
}
