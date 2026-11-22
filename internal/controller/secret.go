package controller

import (
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewSecretReconciler returns a reconciler that mirrors Secrets (including TLS certificates).
func NewSecretReconciler(c client.Client, mirrors cache.Cache) *MirrorReconciler[*corev1.Secret] {
	return &MirrorReconciler[*corev1.Secret]{
		Client:  c,
		Mirrors: mirrors,
		Name:    "secret",
		Adapter: secretAdapter{},
	}
}

type secretAdapter struct{}

func (secretAdapter) New() *corev1.Secret { return &corev1.Secret{} }

func (secretAdapter) CopyData(dst, src *corev1.Secret) {
	dst.Type = src.Type
	dst.Data = maps.Clone(src.Data)
}

// A Secret's type is immutable; updating it would fail on every retry.
func (secretAdapter) CanUpdate(dst, src *corev1.Secret) (bool, string) {
	if dst.Type != src.Type {
		return false, fmt.Sprintf("secret type changed from %q to %q and is immutable", dst.Type, src.Type)
	}
	return true, ""
}
