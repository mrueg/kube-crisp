package projection

import (
	"testing"
)

// A keyset continue token carries the last row's key back to the database, and
// it goes through JSON to get there. JSON has one number type, so an id that
// does not fit a float64 comes back rounded — and the next page starts from a
// key that was never in the table. CockroachDB's unique_rowid() produces ids in
// exactly that range.
func TestAContinueTokenKeepsALargeIntegerKey(t *testing.T) {
	for _, want := range []int64{
		1234567890123456789,
		1 << 53,
		(1 << 53) + 1,
		9223372036854775807,
		-9223372036854775808,
	} {
		token, err := decodeContinue(encodeContinue(continueToken{After: want, Consumed: 10}))
		if err != nil {
			t.Fatalf("decodeContinue() returned error: %v", err)
		}
		got, ok := token.After.(int64)
		if !ok {
			t.Fatalf("the key came back as %T, want int64", token.After)
		}
		if got != want {
			t.Errorf("the key came back as %d, want %d", got, want)
		}
	}
}

// A key that is genuinely fractional still has to survive.
func TestAContinueTokenKeepsAFractionalKey(t *testing.T) {
	token, err := decodeContinue(encodeContinue(continueToken{After: 1.5}))
	if err != nil {
		t.Fatalf("decodeContinue() returned error: %v", err)
	}
	if got, ok := token.After.(float64); !ok || got != 1.5 {
		t.Errorf("the key came back as %#v, want 1.5", token.After)
	}
}
