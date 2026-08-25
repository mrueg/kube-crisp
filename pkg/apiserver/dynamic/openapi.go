package dynamic

import (
	"fmt"
	"sort"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/controller/openapi/builder"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/spec3"
)

// publishOpenAPI renders the projected kinds as an OpenAPI v3 document per
// group version and hands it to the endpoint the apiserver already serves.
//
// This is what makes `kubectl explain orders.spec.customer` work: the schema a
// projection declares is otherwise invisible to clients.
func (r *Router) publishOpenAPI(documents map[schema.GroupVersion]*spec3.OpenAPI) {
	if r.opts.OpenAPIV3Service == nil {
		return
	}

	// Rebuild publishes from the controller's goroutine; PublishOpenAPI does it
	// again from the post-start hook, once the endpoint exists. Both write the
	// record of what is currently advertised.
	r.publishedMu.Lock()
	defer r.publishedMu.Unlock()

	service := r.opts.OpenAPIV3Service()
	if service == nil {
		// The endpoint is installed in PrepareRun; until then there is nowhere
		// to publish to, and PublishOpenAPI will catch up afterwards.
		return
	}

	served := make(map[string]struct{}, len(documents))
	for gv, document := range documents {
		path := openAPIV3Path(gv)
		served[path] = struct{}{}
		service.UpdateGroupVersion(path, document)
	}

	// Group versions that are no longer served must stop being advertised.
	for path := range r.publishedOpenAPI {
		if _, still := served[path]; !still {
			service.DeleteGroupVersion(path)
		}
	}
	r.publishedOpenAPI = served
}

// buildOpenAPIV3 renders one document covering every resource in a group
// version, by describing each projection as the CustomResourceDefinition it
// would have been and reusing the builder that serves real custom resources.
func buildOpenAPIV3(gv schema.GroupVersion, resources []Resource) (*spec3.OpenAPI, error) {
	documents := make([]*spec3.OpenAPI, 0, len(resources))

	// Sorted so that a rebuild with unchanged input produces an unchanged
	// document rather than one that differs by map iteration order.
	sorted := append([]Resource(nil), resources...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Plural < sorted[j].Plural })

	for _, res := range sorted {
		if res.Schema == nil {
			// No schema could be resolved for this version, so there is nothing
			// to describe. It still serves; it just cannot be explained.
			continue
		}

		document, err := builder.BuildOpenAPIV3(syntheticCRD(res), gv.Version, builder.Options{V2: false})
		if err != nil {
			return nil, fmt.Errorf("building the document for %s: %w", res.Plural, err)
		}
		documents = append(documents, document)
	}

	if len(documents) == 0 {
		return &spec3.OpenAPI{Version: "3.0.0"}, nil
	}
	return builder.MergeSpecsV3(documents...)
}

// syntheticCRD describes a projection in the shape the OpenAPI builder expects.
// No CustomResourceDefinition is ever created in the cluster: this object only
// travels as far as the builder.
func syntheticCRD(res Resource) *apiextensionsv1.CustomResourceDefinition {
	scope := apiextensionsv1.ClusterScoped
	if res.Namespaced {
		scope = apiextensionsv1.NamespaceScoped
	}

	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: res.Plural + "." + res.Group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: res.Group,
			Scope: scope,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:     res.Plural,
				Singular:   res.Singular,
				Kind:       res.Kind,
				ListKind:   res.ListKind,
				ShortNames: res.ShortNames,
				Categories: res.Categories,
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name:                     res.Version,
				Served:                   true,
				Storage:                  true,
				Schema:                   &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: res.Schema},
				AdditionalPrinterColumns: res.PrinterColumns,
				Subresources:             subresources(res),
			}},
		},
	}
}

// subresources mirrors the projection's subresources into the synthesized CRD,
// so the published document describes the same endpoints the router installs.
func subresources(res Resource) *apiextensionsv1.CustomResourceSubresources {
	if res.StatusStorage == nil && res.ScaleStorage == nil {
		return nil
	}

	// The scale paths matter beyond documentation: describing them is what puts
	// the autoscaling Scale schema into the document, and field management
	// needs that schema to track ownership of a scale write.
	out := &apiextensionsv1.CustomResourceSubresources{}
	if res.StatusStorage != nil {
		out.Status = &apiextensionsv1.CustomResourceSubresourceStatus{}
	}
	if res.ScaleStorage != nil {
		out.Scale = res.ScaleSubresource
	}
	return out
}

// typeConverterFor derives field management's view of a group version from the
// schemas the projections declare.
//
// With a real schema, server-side apply knows which lists merge by key and
// which maps are atomic. Without one it can only deduce structure from the
// object in hand, which merges lists by replacing them.
func typeConverterFor(document *spec3.OpenAPI) managedfields.TypeConverter {
	if document == nil || document.Components == nil || len(document.Components.Schemas) == 0 {
		return managedfields.NewDeducedTypeConverter()
	}

	converter, err := managedfields.NewTypeConverter(document.Components.Schemas, false)
	if err != nil {
		klog.V(2).InfoS("falling back to deduced field management", "err", err)
		return managedfields.NewDeducedTypeConverter()
	}
	return converter
}

// openAPIV3Path is the key the OpenAPI endpoint indexes a group version under.
func openAPIV3Path(gv schema.GroupVersion) string {
	return "apis/" + gv.Group + "/" + gv.Version
}
