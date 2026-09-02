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

// The shape a PostgreSQL projection actually stores. "updated_at =
// clock_timestamp()::text" is what the reference recommends and what this
// repository's own e2e projections write: a space rather than a T, a two-digit
// offset, and a trimmed fraction.
//
// This one is not mis-ordered by a byte compare -- "+" sorts below both "." and
// the digits, so the offset happens to rescue it. What it is, is
// variable-length, which is the only thing a byte compare can be trusted on. So
// requiring equal length without also reading these as timestamps left every
// such version incomparable, the ring scan found no event newer than the
// client, and a watch was admitted and replayed nothing.
//
// Caught by the cluster e2e rather than by a unit test, because nothing here
// used the format the e2e projections write.
func TestAPostgresTimestampVersionIsComparedAsAnInstant(t *testing.T) {
	var (
		earlier = "2026-09-02 15:34:39.1+00"
		later   = "2026-09-02 15:34:39.123456+00"
	)
	if len(earlier) == len(later) {
		t.Fatalf("%q and %q are the same length; this test has lost its point", earlier, later)
	}
	if !movesForward(earlier, later) {
		t.Errorf("%s was not treated as newer than %s", later, earlier)
	}
	if movesForward(later, earlier) {
		t.Errorf("%s was treated as newer than %s", earlier, later)
	}

	// A whole second against a fraction of one, which differ in length too.
	if !movesForward("2026-09-02 15:34:39+00", "2026-09-02 15:34:39.5+00") {
		t.Error("a fraction of a second was not treated as newer than the whole second")
	}

	// And across the forms, since a projection can be read by one driver and
	// its version written by another.
	if _, ok := compareVersions("2026-09-02 15:34:39.5+00", "2026-09-02T15:34:39.6Z"); !ok {
		t.Error("two timestamps in different renderings were reported incomparable")
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
