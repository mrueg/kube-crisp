package projection

import (
	"testing"
	"time"
)

// The reference recommends a timestamp column for resourceVersion — "updated_at
// = clock_timestamp()" — and the mapper renders one with time.RFC3339Nano,
// which strips trailing zeros from the fraction. So the strings are
// variable-length and do not sort as text.
//
// Compared byte-wise, the watch's high-water mark stalled on the lexically
// largest row rather than the newest, and every later poll re-read the same
// rows from the same point while logging that the column was not moving
// forward. A client that had seen a later version could also be served a
// strictly older object out of the cache, because the floor it asserted was
// judged the same way.
func TestATimestampVersionIsComparedAsAnInstant(t *testing.T) {
	base := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	var (
		whole = base.Format(time.RFC3339Nano)                             // …:00Z
		tenth = base.Add(100 * time.Millisecond).Format(time.RFC3339Nano) // …:00.1Z
		micro = base.Add(123456 * time.Microsecond).Format(time.RFC3339Nano)
	)

	// The three lexical inversions, stated as what they are.
	if tenth <= micro {
		t.Fatalf("%q is no longer lexically above %q; this test has lost its point", tenth, micro)
	}
	if !movesForward(tenth, micro) {
		t.Errorf("%s was not treated as newer than %s", micro, tenth)
	}
	if movesForward(micro, tenth) {
		t.Errorf("%s was treated as newer than %s", tenth, micro)
	}
	if !movesForward(whole, tenth) {
		t.Errorf("%s was not treated as newer than %s", tenth, whole)
	}
	if movesForward(tenth, whole) {
		t.Errorf("%s was treated as newer than %s", whole, tenth)
	}
}

// A microsecond epoch column orders numerically and not lexically either, which
// is the case the numeric branch was already there for.
func TestANumericVersionIsComparedAsANumber(t *testing.T) {
	if !movesForward("9", "10") {
		t.Error("10 was not treated as newer than 9")
	}
	if movesForward("10", "9") {
		t.Error("9 was treated as newer than 10")
	}
}

// A fixed-width version does sort as text, and that is the property being
// relied on. Length is what says so.
func TestAFixedWidthVersionIsComparedAsText(t *testing.T) {
	if !movesForward("01HQ8Z0000", "01HQ8Z0001") {
		t.Error("a later fixed-width version was not treated as newer")
	}
	if movesForward("01HQ8Z0001", "01HQ8Z0000") {
		t.Error("an earlier fixed-width version was treated as newer")
	}
}

// And a pair it cannot place is answered no rather than guessed at. The
// watermark then does not advance and the cache is not trusted, so the cost is
// a re-read rather than a row nobody is told about.
func TestAVersionThatCannotBePlacedDoesNotMoveTheWatermark(t *testing.T) {
	if _, ok := compareVersions("v2-alpha", "v10"); ok {
		t.Fatal("two versions of different shape and length were reported comparable")
	}
	if movesForward("v2-alpha", "v10") {
		t.Error("an incomparable version advanced the watermark")
	}
	if movesForward("v10", "v2-alpha") {
		t.Error("an incomparable version advanced the watermark in the other direction")
	}

	// An empty watermark still starts anywhere, and an empty version is not a
	// version.
	if !movesForward("", "anything") {
		t.Error("the first version read did not set the watermark")
	}
	if movesForward("anything", "") {
		t.Error("an empty version moved the watermark")
	}
}
