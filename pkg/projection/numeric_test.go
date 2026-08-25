package projection

import (
	"math"
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// TestNumericTypesFromEveryDriver covers the Go types the three drivers
// actually hand back for numeric columns.
//
// A type with no branch here does not produce a wrong value — it fails to map,
// and with the default onUnmappableRow the row is dropped from every list and
// every watch. The object simply stops existing, and the only signal is a
// counter. MySQL returns uint64 or []byte for BIGINT UNSIGNED depending on
// whether prepared statements are on, so the same projection behaved
// differently depending on a flag that has nothing to do with types.
func TestNumericTypesFromEveryDriver(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  any
		kind crispv1alpha1.FieldType
		want any
	}{
		{"int64", int64(42), crispv1alpha1.FieldTypeInteger, int64(42)},
		{"uint64 within range", uint64(42), crispv1alpha1.FieldTypeInteger, int64(42)},
		{"uint32", uint32(42), crispv1alpha1.FieldTypeInteger, int64(42)},
		{"string", "42", crispv1alpha1.FieldTypeInteger, int64(42)},
		{"bytes, MySQL binary protocol", []byte("42"), crispv1alpha1.FieldTypeInteger, int64(42)},

		{"float64", 1.5, crispv1alpha1.FieldTypeNumber, 1.5},
		{"numeric as string, pgx", "1.5", crispv1alpha1.FieldTypeNumber, 1.5},
		{"decimal as bytes, MySQL", []byte("1.5"), crispv1alpha1.FieldTypeNumber, 1.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := coerce(tc.raw, tc.kind)
			if err != nil {
				t.Fatalf("CoerceValue(%T) returned error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("CoerceValue(%v) = %v (%T), want %v", tc.raw, got, got, tc.want)
			}
		})
	}
}

// TestIntegerOverflowSaysWhatToDo. A BIGINT UNSIGNED past what an int64 holds
// cannot become a JSON number at all, so the row has to be refused — but the
// error has to name the way out, or the only symptom is a row that vanished.
func TestIntegerOverflowSaysWhatToDo(t *testing.T) {
	_, err := coerce(uint64(math.MaxUint64), crispv1alpha1.FieldTypeInteger)
	if err == nil {
		t.Fatal("a value past int64 was accepted as an integer field")
	}
	if !strings.Contains(err.Error(), "type: string") {
		t.Errorf("the error does not say how to carry the value: %v", err)
	}
}
