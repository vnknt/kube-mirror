package controller

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func targetNamespaces(obj metav1.Object) []string {
	raw := obj.GetAnnotations()[AnnotationTargetNamespaces]

	var targets []string
	for _, ns := range strings.Split(raw, ",") {
		ns = strings.TrimSpace(ns)
		if ns == "" || ns == obj.GetNamespace() {
			continue
		}
		targets = append(targets, ns)
	}
	return targets
}
