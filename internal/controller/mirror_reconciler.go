package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/recorder"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	indexCopySource      = "copySource"
	indexTargetNamespace = "targetNamespace"
)

var errNotAccepted = errors.New("namespace does not accept mirrors")

// Adapter provides the kind-specific parts of mirroring.
type Adapter[T client.Object] interface {
	New() T
	NewList() client.ObjectList
	CopyData(dst, src T)
	CanUpdate(dst, src T) (ok bool, reason string)
}

// MirrorReconciler mirrors labeled objects of kind T into their target namespaces.
type MirrorReconciler[T client.Object] struct {
	client.Client

	// Mirrors caches kube-mirror's copies; the embedded Client only sees sources.
	Mirrors cache.Cache
	// Name is the controller name and must be unique per manager.
	Name     string
	Adapter  Adapter[T]
	Recorder recorder.EventRecorder
}

// Reconcile syncs a source to its targets. It also runs when one of its copies changes.
func (r *MirrorReconciler[T]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	src := r.Adapter.New()
	if err := r.Get(ctx, req.NamespacedName, src); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("source not found (deleted or unlabeled), cleaning up copies")
			return ctrl.Result{}, r.deleteCopies(ctx, nil, req.NamespacedName, nil, retainPolicy)
		}
		return ctrl.Result{}, err
	}

	targets := targetNamespaces(src)
	logger.Info("reconciling", "targets", targets)

	var errs []error
	keep := make(map[string]bool, len(targets))
	for _, ns := range targets {
		err := r.syncTo(ctx, src, ns)
		if errors.Is(err, errNotAccepted) {
			r.Recorder.Eventf(src, nil, corev1.EventTypeWarning, "TargetNotAllowed", "Sync",
				"namespace %q does not accept mirrors (missing label %s=true)", ns, LabelAccept)
			continue
		}
		keep[ns] = true
		if err != nil {
			r.Recorder.Eventf(src, nil, corev1.EventTypeWarning, "SyncFailed", "Sync",
				"sync to namespace %q failed: %v", ns, err)
			errs = append(errs, fmt.Errorf("sync to namespace %q: %w", ns, err))
		}
	}

	retain := retainPolicy(src)
	if err := r.deleteCopies(ctx, src, req.NamespacedName, keep,
		func(client.Object) bool { return retain }); err != nil {
		errs = append(errs, err)
	}
	return ctrl.Result{}, errors.Join(errs...)
}

// deleteCopies deletes the copies of srcKey outside the keep namespaces,
// except those for which retain returns true. A nil keep matches all copies.
// Events go to src; it is nil when the source is gone.
func (r *MirrorReconciler[T]) deleteCopies(ctx context.Context, src client.Object, srcKey types.NamespacedName,
	keep map[string]bool, retain func(client.Object) bool) error {
	logger := log.FromContext(ctx)

	copies := r.Adapter.NewList()
	if err := r.Mirrors.List(ctx, copies, client.MatchingFields{indexCopySource: srcKey.String()}); err != nil {
		return err
	}

	var errs []error
	err := meta.EachListItem(copies, func(o runtime.Object) error {
		dst := o.(client.Object)
		target := client.ObjectKeyFromObject(dst)

		if keep[dst.GetNamespace()] {
			return nil
		}
		if retain(dst) {
			logger.V(1).Info("retaining copy", "copy", target)
			return nil
		}
		if err := r.Delete(ctx, dst); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete copy %s: %w", target, err))
			return nil
		}
		logger.Info("copy deleted", "copy", target)
		if src != nil {
			r.Recorder.Eventf(src, nil, corev1.EventTypeNormal, "CopyDeleted", "Delete",
				"deleted copy in namespace %q", dst.GetNamespace())
		}
		return nil
	})
	if err != nil {
		return err
	}
	return errors.Join(errs...)
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
	if namespace.Labels[LabelAccept] != "true" {
		logger.Info("target namespace does not accept mirrors, skipping")
		return false, errNotAccepted
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
			r.Recorder.Eventf(src, nil, corev1.EventTypeWarning, "TargetExists", "Create",
				"namespace %q already has an object named %q not managed by kube-mirror", ns, src.GetName())
			return nil
		}
		return err
	}
	logger.Info("copy created")
	r.Recorder.Eventf(src, nil, corev1.EventTypeNormal, "CopyCreated", "Create",
		"copied to namespace %q", ns)
	return nil
}

func (r *MirrorReconciler[T]) updateCopy(ctx context.Context, dst, src T) error {
	logger := log.FromContext(ctx)

	if owner := dst.GetAnnotations()[AnnotationSource]; owner != sourceKey(src) {
		logger.Info("copy belongs to another source, skipping", "owner", owner)
		r.Recorder.Eventf(src, nil, corev1.EventTypeWarning, "TargetConflict", "Update",
			"copy in namespace %q belongs to source %q", dst.GetNamespace(), owner)
		return nil
	}
	if ok, reason := r.Adapter.CanUpdate(dst, src); !ok {
		logger.Info("copy cannot be updated in place, skipping", "reason", reason)
		r.Recorder.Eventf(src, nil, corev1.EventTypeWarning, "UpdateBlocked", "Update",
			"copy in namespace %q not updated: %s", dst.GetNamespace(), reason)
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
	r.Recorder.Eventf(src, nil, corev1.EventTypeNormal, "CopyUpdated", "Update",
		"updated copy in namespace %q", dst.GetNamespace())
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
	if retainPolicy(src) {
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

// mapNamespaceToSources enqueues the sources that target a namespace.
func (r *MirrorReconciler[T]) mapNamespaceToSources(ctx context.Context, ns client.Object) []reconcile.Request {
	logger := log.FromContext(ctx).WithValues("targetNamespace", ns.GetName())

	sources := r.Adapter.NewList()
	if err := r.List(ctx, sources, client.MatchingFields{indexTargetNamespace: ns.GetName()}); err != nil {
		logger.Error(err, "unable to list sources targeting namespace")
		return nil
	}

	var reqs []reconcile.Request
	err := meta.EachListItem(sources, func(o runtime.Object) error {
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(o.(client.Object))})
		return nil
	})
	if err != nil {
		logger.Error(err, "unable to read sources targeting namespace")
		return nil
	}
	return reqs
}

// acceptChanged passes new namespaces and changes to their accept label.
// Deleted namespaces take their copies with them, so they need no reconcile.
var acceptChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		return e.ObjectOld.GetLabels()[LabelAccept] != e.ObjectNew.GetLabels()[LabelAccept]
	},
	DeleteFunc:  func(event.DeleteEvent) bool { return false },
	GenericFunc: func(event.GenericEvent) bool { return false },
}

// SetupWithManager registers this reconciler with the manager.
func (r *MirrorReconciler[T]) SetupWithManager(mgr ctrl.Manager) error {
	err := r.Mirrors.IndexField(context.Background(), r.Adapter.New(), indexCopySource,
		func(o client.Object) []string {
			if v := o.GetAnnotations()[AnnotationSource]; v != "" {
				return []string{v}
			}
			return nil
		})
	if err != nil {
		return fmt.Errorf("index copies by source: %w", err)
	}

	err = mgr.GetFieldIndexer().IndexField(context.Background(), r.Adapter.New(), indexTargetNamespace,
		func(o client.Object) []string { return targetNamespaces(o) })
	if err != nil {
		return fmt.Errorf("index sources by target namespace: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(r.Adapter.New()).
		WatchesRawSource(source.Kind(r.Mirrors, r.Adapter.New(),
			handler.TypedEnqueueRequestsFromMapFunc(r.mapCopyToSource))).
		Watches(&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.mapNamespaceToSources),
			builder.WithPredicates(acceptChanged)).
		Named(r.Name).
		Complete(r)
}

func retainPolicy(obj client.Object) bool {
	return obj.GetAnnotations()[AnnotationDeletionPolicy] == DeletionPolicyRetain
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
