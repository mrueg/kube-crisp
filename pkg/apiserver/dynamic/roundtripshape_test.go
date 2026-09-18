package dynamic

import (
	"context"
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// TestVersionsMustReadAColumnTheSameWay is the other half of the round-trip
// rule. Covering the same columns was compared by name alone, so two versions
// that read total_cents as an integer and as a string passed as equivalent,
// and so did one that read status as a label where the other read it as a
// field. Each writes the column differently from the version a client read it
// through, which is not a round trip.
func TestVersionsMustReadAColumnTheSameWay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mapping *crispv1alpha1.Mapping
		column  string
	}{
		{
			name: "a different type",
			mapping: &crispv1alpha1.Mapping{
				Name:      "id",
				Namespace: "tenant",
				Fields: []crispv1alpha1.FieldMapping{
					{Column: "customer", Path: "spec.customer"},
					{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeString},
				},
			},
			column: "total_cents",
		},
		{
			name: "a label where the primary has a field",
			mapping: &crispv1alpha1.Mapping{
				Name:      "id",
				Namespace: "tenant",
				Labels:    map[string]string{"store.example.com/customer": "customer"},
				Fields: []crispv1alpha1.FieldMapping{
					{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeInteger},
				},
			},
			column: "customer",
		},
		{
			name: "an annotation where the primary has a field",
			mapping: &crispv1alpha1.Mapping{
				Name:        "id",
				Namespace:   "tenant",
				Annotations: map[string]string{"store.example.com/customer": "customer"},
				Fields: []crispv1alpha1.FieldMapping{
					{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeInteger},
				},
			},
			column: "customer",
		},
		{
			name: "omitEmpty on one side only",
			mapping: &crispv1alpha1.Mapping{
				Name:      "id",
				Namespace: "tenant",
				Fields: []crispv1alpha1.FieldMapping{
					{Column: "customer", Path: "spec.customer", OmitEmpty: true},
					{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeInteger},
				},
			},
			column: "customer",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection := multiVersionProjection()
			projection.Spec.Resource.Versions[0].Mapping = tc.mapping

			_, err := newTestCompiler(t).Compile(context.Background(), projection)
			if err == nil {
				t.Fatal("versions reading one column differently were accepted")
			}
			for _, want := range []string{"v1beta1", tc.column, "conversion: None"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// TestAColumnMappedTwiceRoundTrips pins what the comparison is of: a column
// mapped twice in both versions, the same two ways, is the same column read
// the same way, wherever each version puts it.
func TestAColumnMappedTwiceRoundTrips(t *testing.T) {
	projection := multiVersionProjection()
	twice := func(path string) *crispv1alpha1.Mapping {
		return &crispv1alpha1.Mapping{
			Name:      "id",
			Namespace: "tenant",
			Labels:    map[string]string{"store.example.com/customer": "customer"},
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "customer", Path: path},
				{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeInteger},
			},
		}
	}
	projection.Spec.Mapping = *twice("spec.customer")
	projection.Spec.Resource.Versions[0].Mapping = twice("spec.buyer")

	if _, err := newTestCompiler(t).Compile(context.Background(), projection); err != nil {
		t.Fatalf("Compile() refused versions that both map a column as a label and a field: %v", err)
	}
}

// TestALabelAndAnAnnotationAreNotTheSameReading: both are read as strings and
// used to be reported alike, which let a version that promoted a column to a
// label pass against one that read it as an annotation. They are written
// differently — from different maps on the object — so they are told apart.
func TestALabelAndAnAnnotationAreNotTheSameReading(t *testing.T) {
	projection := multiVersionProjection()
	projection.Spec.Mapping.Labels = map[string]string{"store.example.com/region": "region"}
	projection.Spec.Resource.Versions[0].Mapping.Annotations = map[string]string{"store.example.com/region": "region"}

	_, err := newTestCompiler(t).Compile(context.Background(), projection)
	if err == nil {
		t.Fatal("a column read as a label in one version and an annotation in the other was accepted")
	}
	if !strings.Contains(err.Error(), "region") {
		t.Errorf("error %q does not name the column", err)
	}
}
