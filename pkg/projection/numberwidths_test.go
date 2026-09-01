package projection

import (
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// A column mapped as number has to take every width the same column mapped as
// integer takes.
//
// Which Go type a value arrives as is the driver's decision, not the
// projection's: the same column comes back as int64 or int or uint depending on
// the driver, the protocol, and whether prepared statements are on. A width
// accepted for one declared type and refused for the other makes the mapping,
// rather than the data, decide whether the row exists at all — and under the
// default onUnmappableRow the row simply stops being served.
func TestNumberTakesEveryWidthIntegerTakes(t *testing.T) {
	// The integer widths only. float32 is not among them — the integer branch
	// takes float64 and refuses float32 — so it is checked separately below,
	// where it belongs.
	for name, raw := range map[string]any{
		"int64":  int64(42),
		"int32":  int32(42),
		"int":    int(42),
		"uint64": uint64(42),
		"uint32": uint32(42),
		"uint":   uint(42),
	} {
		t.Run(name, func(t *testing.T) {
			asInteger, integerErr := coerce(raw, crispv1alpha1.FieldTypeInteger)
			asNumber, numberErr := coerce(raw, crispv1alpha1.FieldTypeNumber)

			if integerErr != nil {
				t.Fatalf("integer refused %s: %v", name, integerErr)
			}
			if numberErr != nil {
				t.Fatalf("integer accepted %s and number refused it: %v", name, numberErr)
			}

			got, ok := asNumber.(float64)
			if !ok {
				t.Fatalf("number produced %T, want float64", asNumber)
			}
			if got != 42 {
				t.Errorf("number produced %v, want 42", got)
			}
			if asInteger.(int64) != 42 {
				t.Errorf("integer produced %v, want 42", asInteger)
			}
		})
	}
}

// And a type neither takes is still refused, so this widened the set rather
// than opening it.
func TestNumberStillRefusesWhatIsNotANumber(t *testing.T) {
	for name, raw := range map[string]any{
		"bool":   true,
		"struct": struct{}{},
		"map":    map[string]any{},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := coerce(raw, crispv1alpha1.FieldTypeNumber); err == nil {
				t.Errorf("number accepted a %s", name)
			}
		})
	}
}

// The float widths are number's own: a number field is a float64, and these
// reach it without going near the integer branch.
func TestNumberTakesTheFloatWidths(t *testing.T) {
	for name, raw := range map[string]any{
		"float64": float64(42.5),
		"float32": float32(42.5),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := coerce(raw, crispv1alpha1.FieldTypeNumber)
			if err != nil {
				t.Fatalf("number refused %s: %v", name, err)
			}
			if got.(float64) != 42.5 {
				t.Errorf("number produced %v, want 42.5", got)
			}
		})
	}
}
