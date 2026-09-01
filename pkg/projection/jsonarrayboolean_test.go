package projection

import (
	"encoding/json"
	"math"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestBooleanFromAJSONAggregateNumber is the shape resultFormat: JSONArray
// produces. The aggregate is decoded with encoding/json, so every number in it
// is a float64 — including a boolean column, because MySQL's BOOLEAN is
// TINYINT(1) and JSON_ARRAYAGG renders it as 0 or 1, as does SQLite's
// json_group_array. The integer and number field types already handled float64;
// boolean did not, so every row carrying a flag was refused.
func TestBooleanFromAJSONAggregateNumber(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  any
		want bool
	}{
		{"one", float64(1), true},
		{"zero", float64(0), false},
		{"a count other than one, as an int64 column would read", float64(3), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := coerce(tc.raw, crispv1alpha1.FieldTypeBoolean)
			if err != nil {
				t.Fatalf("coerce(%v, boolean) returned error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("coerce(%v, boolean) = %#v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestANumberThatIsNotAFlagIsRefused is the other half of the decision. Reading
// every non-zero as true is the tempting shortcut: it would make a measurement
// column mapped as a boolean by mistake read as a confident `true` on every
// row, with nothing anywhere to notice. Refusing names the column and the
// value, and the row is skipped or the read fails according to
// onUnmappableRow — which is the behaviour the projection asked for.
func TestANumberThatIsNotAFlagIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  float64
	}{
		{"a fraction", 2.5},
		{"NaN", math.NaN()},
		{"an infinity", math.Inf(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := coerce(tc.raw, crispv1alpha1.FieldTypeBoolean); err == nil {
				t.Errorf("coerce(%v, boolean) = %#v, want an error", tc.raw, got)
			}
		})
	}
}

// TestAJSONAggregateRowWithAFlagMaps takes a row the way scanJSONArray hands it
// over — straight out of encoding/json — and maps it, which is how the missing
// branch showed up: not as a wrong value but as a row that stopped existing.
func TestAJSONAggregateRowWithAFlagMaps(t *testing.T) {
	var decoded []map[string]any
	if err := json.Unmarshal([]byte(`[{"id":"order-1","paid":1,"qty":3}]`), &decoded); err != nil {
		t.Fatalf("decoding the aggregate: %v", err)
	}

	mapper, err := NewMapper(
		crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
			Scope: crispv1alpha1.ClusterScoped,
		},
		crispv1alpha1.Mapping{
			Name: "id",
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "qty", Path: "spec.qty", Type: crispv1alpha1.FieldTypeInteger},
				{Column: "paid", Path: "spec.paid", Type: crispv1alpha1.FieldTypeBoolean},
			},
		},
	)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj, err := mapper.Row(crispsql.Row(decoded[0]))
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	spec, ok := obj.Object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec is %T, want a map", obj.Object["spec"])
	}
	if got := spec["paid"]; got != true {
		t.Errorf("spec.paid = %#v, want true", got)
	}
	if got := spec["qty"]; got != int64(3) {
		t.Errorf("spec.qty = %#v, want int64(3)", got)
	}
}
