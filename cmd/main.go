package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/kube-mirror/kube-mirror/internal/controller"
)

func main() {
	var metricsAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		`Address the metrics endpoint binds to. Use "0" to disable.`)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log := ctrl.Log.WithName("setup")

	cfg, err := ctrl.GetConfig()
	if err != nil {
		log.Error(err, "unable to load kubeconfig")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Metrics: metricsserver.Options{BindAddress: metricsAddr},
		Cache:   mirroredKindsWithLabel(controller.LabelEnabled, "true"),
	})

	if err != nil {
		log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	mirrorCluster, err := newMirrorCluster(cfg, mgr)
	if err != nil {
		log.Error(err, "unable to create mirror cluster")
		os.Exit(1)
	}
	if err := mgr.Add(mirrorCluster); err != nil {
		log.Error(err, "unable to add mirror cluster to manager")
		os.Exit(1)
	}

	mirrors := mirrorCluster.GetCache()
	rec := mgr.GetEventRecorder("kube-mirror")
	if err := controller.NewSecretReconciler(mgr.GetClient(), mirrors, rec).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to create controller", "controller", "secret")
		os.Exit(1)
	}
	if err := controller.NewConfigMapReconciler(mgr.GetClient(), mirrors, rec).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to create controller", "controller", "configmap")
		os.Exit(1)
	}

	log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}

// newMirrorCluster returns a cluster whose cache holds kube-mirror's copies.
// A separate cache is needed because a cache allows one label selector per type.
func newMirrorCluster(cfg *rest.Config, mgr ctrl.Manager) (cluster.Cluster, error) {
	return cluster.New(cfg, func(o *cluster.Options) {
		o.Scheme = mgr.GetScheme()
		o.HTTPClient = mgr.GetHTTPClient()
		o.Cache = mirroredKindsWithLabel(controller.LabelManagedBy, controller.ManagedByValue)
	})
}

// mirroredKindsWithLabel caches only Secrets and ConfigMaps labeled key=value.
func mirroredKindsWithLabel(key, value string) cache.Options {
	selector := labels.SelectorFromSet(labels.Set{key: value})
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}:    {Label: selector},
			&corev1.ConfigMap{}: {Label: selector},
		},
	}
}
