package controller

const (
	Prefix = "kube-mirror.io/"

	LabelEnabled = Prefix + "enabled"

	AnnotationTargetNamespaces = Prefix + "target-namespaces"

	AnnotationDeletionPolicy = Prefix + "deletion-policy"
	DeletionPolicyDelete     = "Delete" // default
	DeletionPolicyRetain     = "Retain"

	AnnotationSource = Prefix + "source"

	LabelManagedBy = "app.kubernetes.io/managed-by"
	ManagedByValue = "kube-mirror"
)
