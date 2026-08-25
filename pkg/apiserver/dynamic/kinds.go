package dynamic

import (
	"fmt"
	"strings"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// registerKinds teaches one scheme about every projected kind it will serve.
//
// Projected objects are always unstructured: kube-crisp never generates Go
// types for user schemas, which is what lets a projection be added without
// recompiling.
//
// The scheme passed in belongs to the rebuild that is assembling it and is not
// serving anything yet. runtime.Scheme has no locking of its own, so teaching
// one that is already answering requests would race every read the serializer
// makes; each generation of the API surface therefore gets its own.
//
// Kinds are accumulated before anything is registered, because the scheme keeps
// one field-label conversion per GroupVersionKind and a second registration
// replaces the first. Two projections may declare the same kind in one group
// version — only the plural has to be unique — and registering them one at a
// time left whichever came first with its declared field selectors rejected as
// unknown, silently.
func registerKinds(s *runtime.Scheme, resources []Resource) {
	type projected struct {
		listKind schema.GroupVersionKind

		// plurals are every resource served as this kind, named in the error a
		// rejected selector produces.
		plurals []string

		// selectable is the union of what those resources declare. A field one
		// of them offers is a field the kind accepts; narrowing it to the last
		// registration is what used to go wrong.
		selectable map[string]bool
	}

	kinds := make(map[schema.GroupVersionKind]*projected, len(resources))

	// Registration order follows the resources rather than map iteration, so a
	// rebuild of the same set produces the same scheme.
	order := make([]schema.GroupVersionKind, 0, len(resources))

	for _, res := range resources {
		gv := schema.GroupVersion{Group: res.Group, Version: res.Version}
		kind := gv.WithKind(res.Kind)

		entry, seen := kinds[kind]
		if !seen {
			listKind := gv.WithKind(res.Kind + "List")
			if res.ListKind != "" {
				listKind = gv.WithKind(res.ListKind)
			}
			entry = &projected{
				listKind: listKind,
				// Guaranteed for every Kubernetes resource.
				selectable: map[string]bool{"metadata.name": true, "metadata.namespace": true},
			}
			kinds[kind] = entry
			order = append(order, kind)
		}

		entry.plurals = append(entry.plurals, res.Plural)
		for _, field := range res.SelectableFields {
			entry.selectable[strings.TrimPrefix(field.JSONPath, ".")] = true
		}
	}

	for _, kind := range order {
		registerKind(s, kind, kinds[kind].listKind, kinds[kind].selectable, kinds[kind].plurals)
	}

	// The scale subresource answers with an autoscaling/v1 Scale, so the codecs
	// this scheme builds have to know that type. Registering it unconditionally
	// keeps the scheme the same whether or not a projection uses scale.
	if !s.Recognizes(autoscalingv1.SchemeGroupVersion.WithKind("Scale")) {
		utilruntime.Must(autoscalingv1.AddToScheme(s))
	}
}

// registerKind teaches the scheme about one kind and the field selectors it
// accepts.
func registerKind(
	s *runtime.Scheme,
	kind, listKind schema.GroupVersionKind,
	selectable map[string]bool,
	plurals []string,
) {
	if !s.Recognizes(kind) {
		s.AddKnownTypeWithName(kind, &unstructured.Unstructured{})
	}
	if !s.Recognizes(listKind) {
		s.AddKnownTypeWithName(listKind, &unstructured.UnstructuredList{})
	}

	// Field selectors are validated against the scheme before a request ever
	// reaches storage, so the fields a projection declares have to be
	// registered here or they are rejected as unknown.
	//
	// The error is discarded deliberately: this only registers a function
	// against the kind registered just above, and the scheme has no other
	// failure mode here.
	named := strings.Join(plurals, ", ")
	_ = s.AddFieldLabelConversionFunc(kind, func(label, value string) (string, string, error) {
		if selectable[label] {
			return label, value, nil
		}
		return "", "", fmt.Errorf("%q is not a known field selector for %s", label, named)
	})

	metav1.AddToGroupVersion(s, kind.GroupVersion())
}
