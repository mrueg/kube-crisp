package projection

import (
	"math"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestPageSizeAsksForOneMoreRowThanThePage covers what the extra row is for:
// without it the last page and a full one look alike, and a client is told it
// has seen the whole collection one page early.
func TestPageSizeAsksForOneMoreRowThanThePage(t *testing.T) {
	for _, limit := range []int64{1, 5, 500, math.MaxInt64 - 1} {
		if got, want := pageSize(limit), limit+1; got != want {
			t.Errorf("pageSize(%d) = %d, want %d", limit, got, want)
		}
	}
}

// TestPageSizeDoesNotWrapAtTheLargestLimit is the bug. ListOptions puts no
// bound on limit, so a client may send MaxInt64 — and one more than that is a
// negative bind value, which PostgreSQL refuses outright and SQLite reads as no
// limit at all. Either way the request was answered with something other than
// the page it asked for.
func TestPageSizeDoesNotWrapAtTheLargestLimit(t *testing.T) {
	if got := pageSize(math.MaxInt64); got <= 0 {
		t.Errorf("pageSize(MaxInt64) = %d, which is not a number of rows", got)
	}
}

// TestALimitLargerThanTheCollectionStillReadsIt, so the clamp cannot be
// satisfied by refusing an oversized limit: what a client asked for is still
// what it gets.
func TestALimitLargerThanTheCollectionStillReadsIt(t *testing.T) {
	const rows = 25
	store := newPagedStorage(t, seqPagedSpec(), rows)

	list, err := store.List(namespacedContext("acme"),
		&metainternalversion.ListOptions{Limit: math.MaxInt64})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	page := list.(*unstructured.UnstructuredList)
	if got := len(page.Items); got != rows {
		t.Errorf("List() returned %d items, want all %d", got, rows)
	}
	if token := page.GetContinue(); token != "" {
		t.Errorf("a limit past the collection reported another page: %q", token)
	}
}
