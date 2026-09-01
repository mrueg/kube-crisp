package projection

import (
	"k8s.io/apiserver/pkg/registry/rest"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// Server-side apply tracks who owns which field, and the two halves of an
// object split by the status subresource have to be tracked apart.
//
// The field manager learns that from the storage: it asks each one which fields
// that storage refuses to write, and excludes them from what an apply through it
// can come to own. Without an answer it assumes a storage writes everything, so
// an apply to <resource>/status recorded the applier as the owner of spec as
// well -- and an apply to the main resource recorded ownership of status. Both
// are wrong in the direction that matters: the next apply from the other side
// finds a manager already holding the field and reports a conflict that is not
// one, or takes a field away from a manager that really did own it, depending
// on which way round the two applies arrive.
//
// It is the whole point of the split. A controller owning status while a user
// owns the spec is what the subresource is for, and it only holds if the field
// manager is told where the line is. Nothing here changes what a write may
// touch -- StatusREST already refuses to write anything but status, and the main
// storage already refuses to write status. This tells the field manager the same
// thing, so the ownership it records matches the writes it will allow.
//
// The sets are the ones apiextensions uses for a custom resource, which is the
// right reference: a projected kind is unstructured and split the same way. The
// status side resets metadata wholesale rather than the labels and annotations
// it actually leaves alone, because the mechanism excludes whole paths and
// apiextensions made the same trade -- its comment says an inverse of
// resetFields, naming what to keep, is what this would want.
var (
	_ rest.ResetFieldsStrategy = readable{}
	_ rest.ResetFieldsStrategy = &StatusREST{}
)

// GetResetFields reports the fields a write to the main resource cannot touch.
//
// Only status, and only when the subresource exists to hold it. A projection
// without one writes status like any other field, and claiming otherwise would
// hide a field its applier does own.
func (r *REST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	if !r.statusSubresource {
		return map[fieldpath.APIVersion]*fieldpath.Set{}
	}
	return map[fieldpath.APIVersion]*fieldpath.Set{
		fieldpath.APIVersion(r.gvk.GroupVersion().String()): fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
}

// GetResetFields forwards from the storage the installer actually sees.
//
// Spelled out rather than promoted by embedding, for the reason the rest of
// verbs.go is: readable carries *REST as a field so that nothing on it reaches
// a projection by accident.
func (s readable) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return s.r.GetResetFields()
}

// GetResetFields reports the fields a write to <resource>/status cannot touch,
// which is everything else.
func (s *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		fieldpath.APIVersion(s.writable.gvk.GroupVersion().String()): fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}
}
