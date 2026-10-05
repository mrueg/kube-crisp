package projection

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// TestNewMapperRefusesAnIdentityColumnMappedAgain covers a column that carries
// an object's identity and is also read as a label, an annotation or a field.
// A write binds the column from both, and whichever is bound last decides which
// row the statement touches: a label example.com/tenant=globex on acme/order-1
// wrote into globex's row, and the same object without the label wrote NULL.
func TestNewMapperRefusesAnIdentityColumnMappedAgain(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mapping crispv1alpha1.Mapping
		want    string
	}{
		{
			name: "namespace as a label",
			mapping: crispv1alpha1.Mapping{
				Name: "id", Namespace: "tenant",
				Labels: map[string]string{"example.com/tenant": "tenant"},
			},
			want: `label "example.com/tenant"`,
		},
		{
			name: "namespace as an annotation",
			mapping: crispv1alpha1.Mapping{
				Name: "id", Namespace: "tenant",
				Annotations: map[string]string{"example.com/tenant": "tenant"},
			},
			want: `annotation "example.com/tenant"`,
		},
		{
			name: "name as a field",
			mapping: crispv1alpha1.Mapping{
				Name: "id", Namespace: "tenant",
				Fields: []crispv1alpha1.FieldMapping{{Column: "id", Path: "spec.id"}},
			},
			want: "field spec.id",
		},
		{
			name: "a name part as a label",
			mapping: crispv1alpha1.Mapping{
				NameColumns: []string{"region", "order_no"}, Namespace: "tenant",
				Labels: map[string]string{"example.com/region": "region"},
			},
			want: `label "example.com/region"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMapper(testResource(), tc.mapping)
			if err == nil {
				t.Fatal("NewMapper() accepted an identity column mapped a second time")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("NewMapper() error = %q, want it to name %s", err, tc.want)
			}
		})
	}
}

// TestParamsBindsIdentityLast is the same rule from the other side, for a
// Mapper that did not come through NewMapper: whatever else reads the column,
// the row a write addresses is the one the request path named.
func TestParamsBindsIdentityLast(t *testing.T) {
	m := &Mapper{
		mapping: crispv1alpha1.Mapping{
			Name: "id", Namespace: "tenant",
			Labels: map[string]string{"example.com/tenant": "tenant"},
			Fields: []crispv1alpha1.FieldMapping{{Column: "id", Path: "spec.id"}},
		},
		namespaced: true,
		fields:     []mappedField{{column: "id", path: []string{"spec", "id"}}},
	}

	obj := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{
			"name": "order-1", "namespace": "acme",
			"labels": map[string]any{"example.com/tenant": "globex"},
		},
		"spec": map[string]any{"id": "order-2"},
	}}

	args, err := m.Params(obj)
	if err != nil {
		t.Fatalf("Params() returned error: %v", err)
	}
	if got := args["tenant"]; got != "acme" {
		t.Errorf("tenant bound as %v, want the object's namespace \"acme\"", got)
	}
	if got := args["id"]; got != "order-1" {
		t.Errorf("id bound as %v, want the object's name \"order-1\"", got)
	}
}
