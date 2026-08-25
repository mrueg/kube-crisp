package projection

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// crdObject builds a CustomResourceDefinition with two versions, only one of
// which is the storage version.
func crdObject() *unstructured.Unstructured {
	version := func(name string, storage bool, field string) map[string]any {
		return map[string]any{
			"name":    name,
			"served":  true,
			"storage": storage,
			"schema": map[string]any{
				"openAPIV3Schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"spec": map[string]any{
							"type":       "object",
							"properties": map[string]any{field: map[string]any{"type": "string"}},
						},
					},
				},
			},
		}
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": "orders.acme.example.com"},
		"spec": map[string]any{
			"group": "acme.example.com",
			"versions": []any{
				version("v1alpha1", false, "oldField"),
				version("v1", true, "newField"),
			},
		},
	}}
}

func newResolver(objects ...runtime.Object) *CRDSchemaResolver {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{crdGVR: "CustomResourceDefinitionList"},
		objects...,
	)
	return &CRDSchemaResolver{Client: client}
}

// TestResolveDefaultsToTheStorageVersion is the behaviour a projection gets
// when it names a CRD but not a version.
func TestResolveDefaultsToTheStorageVersion(t *testing.T) {
	resolver := newResolver(crdObject())

	props, err := resolver.Resolve(context.Background(), crispv1alpha1.CRDReference{Name: "orders.acme.example.com"})
	if err != nil {
		t.Fatalf("Resolve() returned error: %v", err)
	}

	spec, found := props.Properties["spec"]
	if !found {
		t.Fatalf("the resolved schema has no spec: %+v", props)
	}
	if _, found := spec.Properties["newField"]; !found {
		t.Errorf("resolved the wrong version; spec has %v", spec.Properties)
	}
}

func TestResolveNamedVersion(t *testing.T) {
	resolver := newResolver(crdObject())

	props, err := resolver.Resolve(context.Background(), crispv1alpha1.CRDReference{
		Name:    "orders.acme.example.com",
		Version: "v1alpha1",
	})
	if err != nil {
		t.Fatalf("Resolve() returned error: %v", err)
	}
	if _, found := props.Properties["spec"].Properties["oldField"]; !found {
		t.Errorf("resolved the wrong version; spec has %v", props.Properties["spec"].Properties)
	}
}

func TestResolveReportsWhatIsMissing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		objects []runtime.Object
		ref     crispv1alpha1.CRDReference
		wants   string
	}{
		{
			name:  "no name",
			ref:   crispv1alpha1.CRDReference{},
			wants: "name is required",
		},
		{
			name:  "no such CRD",
			ref:   crispv1alpha1.CRDReference{Name: "absent.acme.example.com"},
			wants: "reading CustomResourceDefinition",
		},
		{
			name:    "no such version",
			objects: []runtime.Object{crdObject()},
			ref:     crispv1alpha1.CRDReference{Name: "orders.acme.example.com", Version: "v2"},
			wants:   `no version "v2"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newResolver(tc.objects...).Resolve(context.Background(), tc.ref)
			if err == nil {
				t.Fatal("Resolve() succeeded")
			}
			if !strings.Contains(err.Error(), tc.wants) {
				t.Errorf("error %q does not mention %q", err, tc.wants)
			}
		})
	}
}

// TestResolveWithoutAStorageVersion covers a CRD that names no storage
// version: there is nothing to default to, and guessing would be worse.
func TestResolveWithoutAStorageVersion(t *testing.T) {
	crd := crdObject()
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	for _, raw := range versions {
		raw.(map[string]any)["storage"] = false
	}
	if err := unstructured.SetNestedSlice(crd.Object, versions, "spec", "versions"); err != nil {
		t.Fatalf("rewriting the CRD: %v", err)
	}

	_, err := newResolver(crd).Resolve(context.Background(), crispv1alpha1.CRDReference{Name: "orders.acme.example.com"})
	if err == nil || !strings.Contains(err.Error(), "storage version") {
		t.Fatalf("Resolve() error = %v, want it to name the missing storage version", err)
	}
}
