package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// Adapter provides the kind-specific parts of mirroring.
type Adapter[T client.Object] interface {
	New() T
	CopyData(dst, src T)
	CanUpdate(dst, src T) (ok bool, reason string)
}

// MirrorReconciler mirrors labeled objects of kind T into their target namespaces.
type MirrorReconciler[T client.Object] struct {
	client.Client

	// Mirrors caches kube-mirror's copies; the embedded Client only sees sources.
	Mirrors cache.Cache
	// Name is the controller name and must be unique per manager.
	Name    string
	Adapter Adapter[T]
}

// Reconcile syncs a source to its targets. It also runs when one of its copies changes.
func (r *MirrorReconciler[T]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	src := r.Adapter.New()
	if err := r.Get(ctx, req.NamespacedName, src); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("source not found (deleted or unlabeled)")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	targets := targetNamespaces(src)
	logger.Info("reconciling", "targets", targets)

	var errs []error
	for _, ns := range targets {
		if err := r.syncTo(ctx, src, ns); err != nil {
			errs = append(errs, fmt.Errorf("sync to namespace %q: %w", ns, err))
		}
	}
	return ctrl.Result{}, errors.Join(errs...)
}

func (r *MirrorReconciler[T]) syncTo(ctx context.Context, src T, ns string) error {
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("target", ns))

	ready, err := r.namespaceReady(ctx, ns)
	if err != nil || !ready {
		return err
	}

	dst, found, err := r.getCopy(ctx, ns, src.GetName())
	if err != nil {
		return err
	}
	if !found {
		return r.createCopy(ctx, src, ns)
	}
	return r.updateCopy(ctx, dst, src)
}

func (r *MirrorReconciler[T]) namespaceReady(ctx context.Context, ns string) (bool, error) {
	logger := log.FromContext(ctx)

	var namespace corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: ns}, &namespace); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("target namespace not found, skipping")
			return false, nil
		}
		return false, err
	}
	if namespace.Status.Phase == corev1.NamespaceTerminating {
		logger.Info("target namespace is terminating, skipping")
		return false, nil
	}
	return true, nil
}

func (r *MirrorReconciler[T]) getCopy(ctx context.Context, ns, name string) (T, bool, error) {
	var zero T

	dst := r.Adapter.New()
	err := r.Mirrors.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, dst)
	if apierrors.IsNotFound(err) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	return dst, true, nil
}

func (r *MirrorReconciler[T]) createCopy(ctx context.Context, src T, ns string) error {
	logger := log.FromContext(ctx)

	dst := r.Adapter.New()
	dst.SetNamespace(ns)
	dst.SetName(src.GetName())
	r.applySource(dst, src)

	if err := r.Create(ctx, dst); err != nil {
		if apierrors.IsAlreadyExists(err) {
			logger.Info("object exists and is not managed by kube-mirror, skipping")
			return nil
		}
		return err
	}
	logger.Info("copy created")
	return nil
}

func (r *MirrorReconciler[T]) updateCopy(ctx context.Context, dst, src T) error {
	logger := log.FromContext(ctx)

	if owner := dst.GetAnnotations()[AnnotationSource]; owner != sourceKey(src) {
		logger.Info("copy belongs to another source, skipping", "owner", owner)
		return nil
	}
	if ok, reason := r.Adapter.CanUpdate(dst, src); !ok {
		logger.Info("copy cannot be updated in place, skipping", "reason", reason)
		return nil
	}

	before := dst.DeepCopyObject()
	r.applySource(dst, src)
	if equality.Semantic.DeepEqual(before, dst) {
		return nil
	}
	if err := r.Update(ctx, dst); err != nil {
		return err
	}
	logger.Info("copy updated")
	return nil
}

// applySource only touches the fields kube-mirror owns; other labels on the copy are kept.
func (r *MirrorReconciler[T]) applySource(dst, src T) {
	labels := dst.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[LabelManagedBy] = ManagedByValue
	dst.SetLabels(labels)

	annotations := dst.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[AnnotationSource] = sourceKey(src)
	// Only Retain is stamped; absence means the default (Delete). Removing it
	// also covers a source that switched from Retain back to the default.
	if src.GetAnnotations()[AnnotationDeletionPolicy] == DeletionPolicyRetain {
		annotations[AnnotationDeletionPolicy] = DeletionPolicyRetain
	} else {
		delete(annotations, AnnotationDeletionPolicy)
	}
	dst.SetAnnotations(annotations)

	r.Adapter.CopyData(dst, src)
}

// mapCopyToSource enqueues the source of a changed copy. On delete events dst is
// the copy's last known state, so the source annotation is still available.
func (r *MirrorReconciler[T]) mapCopyToSource(_ context.Context, dst T) []reconcile.Request {
	key, ok := parseSourceKey(dst.GetAnnotations()[AnnotationSource])
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: key}}
}

// SetupWithManager registers this reconciler with the manager.
func (r *MirrorReconciler[T]) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(r.Adapter.New()).
		WatchesRawSource(source.Kind(r.Mirrors, r.Adapter.New(),
			handler.TypedEnqueueRequestsFromMapFunc(r.mapCopyToSource))).
		Named(r.Name).
		Complete(r)
}

func sourceKey(src client.Object) string {
	return client.ObjectKeyFromObject(src).String()
}

// parseSourceKey splits "namespace/name"; Kubernetes names cannot contain "/".
func parseSourceKey(v string) (types.NamespacedName, bool) {
	ns, name, ok := strings.Cut(v, "/")
	if !ok || ns == "" || name == "" {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: ns, Name: name}, true
}
