package projection

import (
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

// A schema that is not structural cannot be validated, defaulted or pruned
// against, so it is refused rather than served with those switched off.
//
// Pruning is the half that loses data instead of checking it. The Structural
// built from a schema describing a sub-object inside allOf has no properties
// there, so the write is pruned of the very fields the schema describes, and
// then answered 201.
func TestASchemaThatIsNotStructuralIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema *apiextensionsv1.JSONSchemaProps
	}{
		{"a sub-object described inside allOf", &apiextensionsv1.JSONSchemaProps{
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"spec": {
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"details": {
							Type: "object",
							AllOf: []apiextensionsv1.JSONSchemaProps{{
								Type: "object",
								Properties: map[string]apiextensionsv1.JSONSchemaProps{
									"a": {Type: "string"},
								},
							}},
						},
					},
				},
			},
		}},
		{"a root with no type", &apiextensionsv1.JSONSchemaProps{
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"spec": {Type: "object"},
			},
		}},
		{"a root that is not an object", &apiextensionsv1.JSONSchemaProps{
			Type: "string",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := crispv1alpha1.ProjectedResource{
				Group: "store.example.com", Version: "v1", Kind: "Order",
				Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
				Schema: tc.schema,
			}
			if _, _, err := newSchemaValidator(res); err == nil {
				t.Fatal("a schema that is not structural was accepted")
			}
		})
	}
}

// An ordinary schema still compiles, and carries a structural half for the
// rules, defaulting and pruning to be defined against.
func TestAnOrdinarySchemaStillCompiles(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
		Schema: &apiextensionsv1.JSONSchemaProps{
			Type: "object",
			Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"spec": {
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"customer": {Type: "string"},
					},
				},
			},
		},
	}

	validator, structural, err := newSchemaValidator(res)
	if err != nil {
		t.Fatalf("newSchemaValidator() refused an ordinary schema: %v", err)
	}
	if validator == nil {
		t.Error("no validator was compiled")
	}
	if structural == nil {
		t.Error("no structural schema was compiled, so rules would not be evaluated")
	}
}

// And this repository's own examples pass, which is what stops the check above
// being stricter than the projections it ships alongside.
func TestTheShippedExamplesHaveStructuralSchemas(t *testing.T) {
	projections, err := projection.LoadPath(filepath.Join("..", "..", "..", "examples"))
	if err != nil {
		t.Fatalf("loading examples/: %v", err)
	}
	if len(projections) == 0 {
		t.Fatal("no projections were loaded from examples/")
	}

	for i := range projections {
		p := &projections[i]
		res := p.Spec.Resource
		if res.Schema == nil {
			continue
		}
		t.Run(p.Name, func(t *testing.T) {
			if _, _, err := newSchemaValidator(res); err != nil {
				t.Errorf("%s: %v", p.Name, err)
			}
			// Every additional version carries its own schema, and each is
			// compiled separately.
			for _, version := range res.Versions {
				if version.Schema == nil {
					continue
				}
				perVersion := res
				perVersion.Version = version.Name
				perVersion.Schema = version.Schema
				if _, _, err := newSchemaValidator(perVersion); err != nil {
					t.Errorf("%s version %s: %v", p.Name, version.Name, err)
				}
			}
		})
	}
}
