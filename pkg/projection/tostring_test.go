package projection

import (
	"math"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestStringTypesFromEveryDriver is the numeric-column table's other half.
// Every mapped identity field — name, namespace, uid, resourceVersion — and
// every label and annotation is rendered through this conversion, so a width it
// does not know is not one field going missing but every row failing to map,
// and with the default onUnmappableRow the collection reads empty.
func TestStringTypesFromEveryDriver(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  any
		want string
	}{
		{"string", "order-1", "order-1"},
		{"bytes, pgx text and jsonb", []byte("order-1"), "order-1"},
		{"int64", int64(42), "42"},
		{"int32", int32(42), "42"},
		{"int", 42, "42"},
		{"uint64, MySQL BIGINT UNSIGNED over the text protocol", uint64(42), "42"},
		{"uint64 past int64", uint64(math.MaxUint64), "18446744073709551615"},
		{"uint32", uint32(42), "42"},
		{"uint", uint(42), "42"},
		{"float64", 1.5, "1.5"},
		{"float32, MySQL FLOAT over the text protocol", float32(0.1), "0.1"},
		{"bool", true, "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := coerce(tc.raw, crispv1alpha1.FieldTypeString)
			if err != nil {
				t.Fatalf("coerce(%T) returned error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("coerce(%v) = %#v, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestAnOversizedUnsignedIntegerCarriesAsAString is the remedy the overflow
// error names, taken at its word. A value past int64 cannot be a JSON number,
// the error says to map the column as type: string, and that has to lead
// somewhere: before, it produced "cannot convert uint64 to string" and the row
// was refused either way.
func TestAnOversizedUnsignedIntegerCarriesAsAString(t *testing.T) {
	if _, err := coerce(uint64(math.MaxUint64), crispv1alpha1.FieldTypeInteger); err == nil {
		t.Fatal("a value past int64 was accepted as an integer field")
	}

	got, err := coerce(uint64(math.MaxUint64), crispv1alpha1.FieldTypeString)
	if err != nil {
		t.Fatalf("the remedy the overflow error names does not work: %v", err)
	}
	if want := "18446744073709551615"; got != want {
		t.Errorf("coerce() = %#v, want %q: the value has to survive exactly", got, want)
	}
}

// TestARowKeyedByAnUnsignedBigIntMaps is the shape this was found in. `id
// BIGINT UNSIGNED` is the idiomatic MySQL primary key, and go-sql-driver's text
// protocol — what a list query with no bind parameters gets — returns uint64
// for it whatever its magnitude. mapping.name reads it, so every row of such a
// table was unmappable and the collection appeared empty.
func TestARowKeyedByAnUnsignedBigIntMaps(t *testing.T) {
	mapper, err := NewMapper(
		crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
			Scope: crispv1alpha1.ClusterScoped,
		},
		crispv1alpha1.Mapping{Name: "id", ResourceVersion: "version"},
	)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj, err := mapper.Row(crispsql.Row{"id": uint64(1042), "version": uint64(7)})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if got, want := obj.GetName(), "1042"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	if got, want := obj.GetResourceVersion(), "7"; got != want {
		t.Errorf("resourceVersion = %q, want %q", got, want)
	}
}

// TestAnUnsizedUnsignedIntegerIsStillRangeChecked keeps the integer field's one
// error in one place: a uint is no wider than a uint64, so it has to be refused
// on exactly the same values and with the same advice.
func TestAnUnsizedUnsignedIntegerIsStillRangeChecked(t *testing.T) {
	got, err := coerce(uint(42), crispv1alpha1.FieldTypeInteger)
	if err != nil {
		t.Fatalf("coerce(uint) returned error: %v", err)
	}
	if got != int64(42) {
		t.Errorf("coerce(uint(42)) = %#v, want int64(42)", got)
	}

	if math.MaxUint > math.MaxInt64 {
		if _, err := coerce(uint(math.MaxUint), crispv1alpha1.FieldTypeInteger); err == nil {
			t.Error("a uint past int64 was accepted as an integer field")
		}
	}
}
