package main

import (
	"flag"
	"net/http"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	meridianv1alpha1 "github.com/meridian-io/meridian/operator/api/v1alpha1"
	"github.com/meridian-io/meridian/operator/internal/controller"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(meridianv1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr string
		probeAddr   string
		namespace   string
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8081", "Address for the metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8082", "Address for health probes.")
	flag.StringVar(&namespace, "namespace", "meridian", "Namespace to manage clusters in.")
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{})))
	log := ctrl.Log.WithName("main")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		log.Error(err, "failed to create manager")
		os.Exit(1)
	}

	if err = (&controller.ClusterController{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "failed to setup ClusterController")
		os.Exit(1)
	}

	if err = (&controller.ClusterPoolController{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
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

	if err := mgr.AddHealthzCheck("healthz", func(_ *http.Request) error { return nil }); err != nil {
		log.Error(err, "failed to add healthz check")
		os.Exit(1)
	}

	log.Info("starting Project Meridian operator", "namespace", namespace)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "operator exited with error")
		os.Exit(1)
	}
}
