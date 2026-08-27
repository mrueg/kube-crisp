package projection

import (
	"context"
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// CRDGVR is the resource a borrowed schema is read from. Exported because the
// informer that watches for changes to one has to name the same resource, and
// two spellings that agree by habit is one more than is needed.
var CRDGVR = schema.GroupVersionResource{
	Group:    "apiextensions.k8s.io",
	Version:  "v1",
	Resource: "customresourcedefinitions",
}

// SchemaResolver reads the schema a projection borrows from a CRD.
type SchemaResolver interface {
	Resolve(ctx context.Context, ref crispv1alpha1.CRDReference) (*apiextensionsv1.JSONSchemaProps, error)
}

// CRDSchemaResolver borrows a schema from a CustomResourceDefinition in the
// cluster, so a projection can reuse a shape that is already defined instead of
// restating it.
//
// Only the schema is read. The CRD is not served for the projected group, and
// nothing about it is modified.
type CRDSchemaResolver struct {
	Client dynamic.Interface
}

// Resolve returns the OpenAPI schema of the referenced CRD version.
func (r *CRDSchemaResolver) Resolve(ctx context.Context, ref crispv1alpha1.CRDReference) (*apiextensionsv1.JSONSchemaProps, error) {
	if ref.Name == "" {
		return nil, fmt.Errorf("schemaFrom.name is required")
	}

	crd, err := r.Client.Resource(CRDGVR).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("reading CustomResourceDefinition %s: %w", ref.Name, err)
	}

	versions, found, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if err != nil || !found {
		return nil, fmt.Errorf("CustomResourceDefinition %s declares no versions", ref.Name)
	}

	raw, err := selectCRDVersion(versions, ref)
	if err != nil {
		return nil, fmt.Errorf("CustomResourceDefinition %s: %w", ref.Name, err)
	}

	openAPISchema, found, err := unstructured.NestedMap(raw, "schema", "openAPIV3Schema")
	if err != nil || !found {
		return nil, fmt.Errorf("CustomResourceDefinition %s has no schema for version %q", ref.Name, raw["name"])
	}

	// The CRD is unstructured here, so the schema travels through JSON rather
	// than a conversion function.
	encoded, err := json.Marshal(openAPISchema)
	if err != nil {
		return nil, fmt.Errorf("encoding the borrowed schema: %w", err)
	}

	props := &apiextensionsv1.JSONSchemaProps{}
	if err := json.Unmarshal(encoded, props); err != nil {
		return nil, fmt.Errorf("decoding the borrowed schema: %w", err)
	}
	return props, nil
}

// selectCRDVersion picks the referenced version, or the storage version when
// none was named.
func selectCRDVersion(versions []any, ref crispv1alpha1.CRDReference) (map[string]any, error) {
	var storage map[string]any

	for _, entry := range versions {
		version, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if ref.Version != "" {
			if version["name"] == ref.Version {
				return version, nil
			}
			continue
		}
		if isStorage, _ := version["storage"].(bool); isStorage {
			storage = version
		}
	}

	if ref.Version != "" {
		return nil, fmt.Errorf("no version %q", ref.Version)
	}
	if storage == nil {
		return nil, fmt.Errorf("no storage version")
	}
	return storage, nil
}
