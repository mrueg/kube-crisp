package dynamic

import (
	"context"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

func borrowingProjection() *crispv1alpha1.CustomResourceProjection {
	p := testProjection()
	p.Spec.Resource.Schema = nil
	p.Spec.Resource.SchemaFrom = &crispv1alpha1.CRDReference{Name: "orders.example.com"}
	return p
}

func shape(properties ...string) *apiextensionsv1.JSONSchemaProps {
	spec := apiextensionsv1.JSONSchemaProps{Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{}}
	for _, name := range properties {
		spec.Properties[name] = apiextensionsv1.JSONSchemaProps{Type: "string"}
	}
	return &apiextensionsv1.JSONSchemaProps{
		Type:       "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{"spec": spec},
	}
}

// A borrowed schema is not in the projection's spec and can change without the
// projection changing — exactly like the connection string, which the
// fingerprint already covers. Without it the storage is kept across a CRD
// edit, so the projection goes on validating and explaining against the shape
// it read when it first compiled.
func TestEditingTheBorrowedCRDRebuildsTheStorage(t *testing.T) {
	compiler := newTestCompiler(t)
	p := borrowingProjection()
	ctx := context.Background()

	compiler.Schemas = borrowedSchema{schema: shape("customer")}
	before, err := compiler.Prepare(ctx, p)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	compiler.Schemas = borrowedSchema{schema: shape("customer", "total")}
	after, err := compiler.Prepare(ctx, p)
	if err != nil {
		t.Fatalf("Prepare() after the CRD changed returned error: %v", err)
	}

	if before.Fingerprint == after.Fingerprint {
		t.Error("the fingerprint did not move when the borrowed schema did, so the storage is " +
			"kept and the projection keeps serving the old shape")
	}
}

// The same CRD twice must not rebuild anything: a rebuild empties the watch
// cache and makes every watcher relist.
func TestAnUnchangedBorrowedCRDKeepsTheStorage(t *testing.T) {
	compiler := newTestCompiler(t)
	compiler.Schemas = borrowedSchema{schema: shape("customer")}

	p := borrowingProjection()
	ctx := context.Background()

	before, err := compiler.Prepare(ctx, p)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	after, err := compiler.Prepare(ctx, p)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	if before.Fingerprint != after.Fingerprint {
		t.Error("an unchanged projection fingerprinted differently twice, so every sync would " +
			"rebuild the storage and every watcher would relist")
	}
}

// What Prepare resolved is what the compile serves, so a CRD edited between the
// two cannot fingerprint one shape and serve another.
func TestTheCompileServesTheSchemaThatWasFingerprinted(t *testing.T) {
	compiler := newTestCompiler(t)
	compiler.Schemas = borrowedSchema{schema: shape("customer")}

	p := borrowingProjection()
	ctx := context.Background()

	prepared, err := compiler.Prepare(ctx, p)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	// The CRD is edited after Prepare and before the compile.
	compiler.Schemas = borrowedSchema{schema: shape("customer", "total")}

	resources, err := compiler.CompileWith(ctx, p, prepared)
	if err != nil {
		t.Fatalf("CompileWith() returned error: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("compiled %d resources, want 1", len(resources))
	}

	served := resources[0].Schema
	if served == nil {
		t.Fatal("the compiled resource has no schema")
	}
	if _, unexpected := served.Properties["spec"].Properties["total"]; unexpected {
		t.Error("the compile served a schema the fingerprint never saw")
	}
}
