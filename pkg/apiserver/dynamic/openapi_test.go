package dynamic

import (
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
)

func schemaFor(field string) *apiextensionsv1.JSONSchemaProps {
	return &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type:       "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{field: {Type: "string"}},
			},
		},
	}
}

func resourceFor(plural, kind string, schema *apiextensionsv1.JSONSchemaProps) Resource {
	return Resource{
		Group:      "store.example.com",
		Version:    "v1alpha1",
		Plural:     plural,
		Kind:       kind,
		Singular:   strings.ToLower(kind),
		ListKind:   kind + "List",
		Namespaced: true,
		Schema:     schema,
	}
}

func TestBuildOpenAPIV3MergesEveryResourceInAGroupVersion(t *testing.T) {
	gv := schema.GroupVersion{Group: "store.example.com", Version: "v1alpha1"}

	document, err := buildOpenAPIV3(gv, []Resource{
		resourceFor("orders", "Order", schemaFor("customer")),
		resourceFor("widgets", "Widget", schemaFor("colour")),
	})
	if err != nil {
		t.Fatalf("buildOpenAPIV3() returned error: %v", err)
	}
	if document.Components == nil {
		t.Fatal("the document carries no components")
	}

	var sawOrder, sawWidget bool
	for name := range document.Components.Schemas {
		switch {
		case strings.HasSuffix(name, "v1alpha1.Order"):
			sawOrder = true
		case strings.HasSuffix(name, "v1alpha1.Widget"):
			sawWidget = true
		}
	}
	if !sawOrder || !sawWidget {
		t.Errorf("published schemas %v, want both kinds", document.Components.Schemas)
	}

	// The paths a client would call have to be there too, or kubectl explain
	// finds the schema and nothing else.
	if document.Paths == nil || len(document.Paths.Paths) == 0 {
		t.Error("the document describes no paths")
	}
}

// TestBuildOpenAPIV3SkipsResourcesWithoutASchema covers the borrowed-schema
// case that has not been resolved: it still serves, it just cannot be explained.
func TestBuildOpenAPIV3SkipsResourcesWithoutASchema(t *testing.T) {
	gv := schema.GroupVersion{Group: "store.example.com", Version: "v1alpha1"}

	document, err := buildOpenAPIV3(gv, []Resource{resourceFor("orders", "Order", nil)})
	if err != nil {
		t.Fatalf("buildOpenAPIV3() returned error: %v", err)
	}
	if document.Components != nil && len(document.Components.Schemas) > 0 {
		t.Errorf("a resource with no schema published %v", document.Components.Schemas)
	}
}

// TestTypeConverterFollowsTheSchema is what server-side apply depends on: with
// a schema, merging is structural; without one it can only deduce.
func TestTypeConverterFollowsTheSchema(t *testing.T) {
	gv := schema.GroupVersion{Group: "store.example.com", Version: "v1alpha1"}

	document, err := buildOpenAPIV3(gv, []Resource{resourceFor("orders", "Order", schemaFor("customer"))})
	if err != nil {
		t.Fatalf("buildOpenAPIV3() returned error: %v", err)
	}

	converter := typeConverterFor(document)
	if converter == nil {
		t.Fatal("no type converter was built")
	}
	if converter == managedfields.NewDeducedTypeConverter() {
		t.Error("field management fell back to deduction despite a schema")
	}

	// Nothing to go on: the deduced converter is the honest answer.
	if typeConverterFor(nil) == nil {
		t.Error("typeConverterFor(nil) returned nothing")
	}
}

func TestSyntheticCRDDescribesTheProjection(t *testing.T) {
	res := resourceFor("orders", "Order", schemaFor("customer"))
	res.ShortNames = []string{"ord"}
	res.PrinterColumns = []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Customer", Type: "string", JSONPath: ".spec.customer"},
	}

	crd := syntheticCRD(res)
	if got, want := crd.Name, "orders.store.example.com"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	if got, want := crd.Spec.Scope, apiextensionsv1.NamespaceScoped; got != want {
		t.Errorf("scope = %q, want %q", got, want)
	}
	if got, want := crd.Spec.Names.ListKind, "OrderList"; got != want {
		t.Errorf("list kind = %q, want %q", got, want)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Name != "v1alpha1" {
		t.Fatalf("versions = %+v, want one v1alpha1", crd.Spec.Versions)
	}
	if len(crd.Spec.Versions[0].AdditionalPrinterColumns) != 1 {
		t.Error("the printer columns did not reach the published document")
	}
	if crd.Spec.Versions[0].Subresources != nil {
		t.Error("a status subresource was described for a projection that has none")
	}

	// With the subresource, the document has to say so.
	res.StatusStorage = &fakeStorage{}
	if crd := syntheticCRD(res); crd.Spec.Versions[0].Subresources == nil {
		t.Error("the status subresource was not described")
	}
}

func TestOpenAPIV3Path(t *testing.T) {
	got := openAPIV3Path(schema.GroupVersion{Group: "store.example.com", Version: "v1alpha1"})
	if want := "apis/store.example.com/v1alpha1"; got != want {
		t.Errorf("openAPIV3Path() = %q, want %q", got, want)
	}
}
