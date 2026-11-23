package controller

import (
	"maps"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/recorder"
)

// NewConfigMapReconciler returns a reconciler that mirrors ConfigMaps.
func NewConfigMapReconciler(c client.Client, mirrors cache.Cache, rec recorder.EventRecorder) *MirrorReconciler[*corev1.ConfigMap] {
	return &MirrorReconciler[*corev1.ConfigMap]{
		Client:   c,
		Mirrors:  mirrors,
		Recorder: rec,
		Name:     "configmap",
		Adapter:  configMapAdapter{},
	}
}

type configMapAdapter struct{}

func (configMapAdapter) New() *corev1.ConfigMap { return &corev1.ConfigMap{} }

func (configMapAdapter) NewList() client.ObjectList { return &corev1.ConfigMapList{} }

func (configMapAdapter) CopyData(dst, src *corev1.ConfigMap) {
	dst.Data = maps.Clone(src.Data)
	dst.BinaryData = maps.Clone(src.BinaryData)
}

func (configMapAdapter) CanUpdate(_, _ *corev1.ConfigMap) (bool, string) { return true, "" }
