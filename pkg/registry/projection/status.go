package projection

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
)

// StatusREST serves <resource>/status.
//
// Enabling the subresource splits the object in two, the same way it does for a
// custom resource: a write to the main resource cannot change status, and a
// write here cannot change anything else. That is what lets a controller own
// status while a user owns the spec.
type StatusREST struct {
	writable *WritableREST
}

var (
	_ rest.Storage                  = &StatusREST{}
	_ rest.Getter                   = &StatusREST{}
	_ rest.Updater                  = &StatusREST{}
	_ rest.Patcher                  = &StatusREST{}
	_ rest.Scoper                   = &StatusREST{}
	_ rest.GroupVersionKindProvider = &StatusREST{}
)

// New returns an empty object of the projected kind.
func (s *StatusREST) New() runtime.Object { return s.writable.New() }

// Destroy releases resources owned by this storage. The main storage owns them.
func (s *StatusREST) Destroy() {}

// NamespaceScoped reports whether the projected kind lives in namespaces.
func (s *StatusREST) NamespaceScoped() bool { return s.writable.NamespaceScoped() }

// GroupVersionKind reports the kind served.
func (s *StatusREST) GroupVersionKind(gv schema.GroupVersion) schema.GroupVersionKind {
	return s.writable.GroupVersionKind(gv)
}

// GetSingularName returns the singular resource name.
func (s *StatusREST) GetSingularName() string { return s.writable.GetSingularName() }

// Get returns the whole object; a status read is a read of the same row.
func (s *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return s.writable.Get(ctx, name, options)
}

// Update writes status and nothing else.
func (s *StatusREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	if s.writable.updateStatus == nil {
		return nil, false, errors.NewMethodNotSupported(s.writable.groupResource(), "update")
	}

	return s.writable.applyUpdate(ctx, name, objInfo, updateValidation, options, s.writable.updateStatus, statusOnly)
}

// statusOnly keeps everything except status as it is stored, so a status write
// cannot smuggle in a spec change.
func statusOnly(incoming, existing *unstructured.Unstructured) {
	status, found, err := unstructured.NestedFieldCopy(incoming.Object, "status")

	// Start from what is stored and graft the submitted status onto it.
	spec := existing.DeepCopy()
	if err == nil && found {
		_ = unstructured.SetNestedField(spec.Object, status, "status")
	} else {
		unstructured.RemoveNestedField(spec.Object, "status")
	}

	incoming.Object = spec.Object
}

// specOnly is the mirror image, applied to writes against the main resource
// when the status subresource is enabled.
func specOnly(incoming, existing *unstructured.Unstructured) {
	status, found, err := unstructured.NestedFieldCopy(existing.Object, "status")
	if err == nil && found {
		_ = unstructured.SetNestedField(incoming.Object, status, "status")
		return
	}
	unstructured.RemoveNestedField(incoming.Object, "status")
}
