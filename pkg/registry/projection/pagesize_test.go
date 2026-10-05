package projection

import (
	"math"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
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

// TestAPageLargerThanMaxRowsIsShortenedRatherThanRefused is a regression test.
//
// A page asks for one row more than its limit, and the statement refuses any
// result set larger than its maxRows — so a limit at or above maxRows, over a
// table larger than that, read more rows than the statement allows and the
// list answered 500 where it should have answered a page. kubectl's default
// --chunk-size of 500 is safe only because it is below the default maxRows;
// --chunk-size=5000 against the default was not. A server may return fewer
// items than the limit as long as it hands back a continue token, so that is
// what it does now, and the walk still reaches every row exactly once.
func TestAPageLargerThanMaxRowsIsShortenedRatherThanRefused(t *testing.T) {
	const (
		rows    = 25
		maxRows = 10
	)
	for name, spec := range map[string]crispv1alpha1.CustomResourceProjectionSpec{
		"keyset": seqPagedSpec(),
		"offset": offsetPagedSpec(),
	} {
		t.Run(name, func(t *testing.T) {
			spec.Queries.List.MaxRows = ptr(int32(maxRows))
			store := newPagedStorage(t, spec, rows)

			for _, limit := range []int64{maxRows, maxRows + 5, math.MaxInt64} {
				result, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{Limit: limit})
				if err != nil {
					t.Fatalf("List(limit=%d) returned error: %v", limit, err)
				}
				page := result.(*unstructured.UnstructuredList)
				if got := len(page.Items); got == 0 || got >= maxRows {
					t.Errorf("List(limit=%d) returned %d items, want a page shorter than maxRows (%d)",
						limit, got, maxRows)
				}
				if page.GetContinue() == "" {
					t.Errorf("List(limit=%d) returned a shortened page with no continue token", limit)
				}

				seen := walk(t, store, limit)
				if len(seen) != rows {
					t.Fatalf("paging with limit=%d returned %d objects, want %d", limit, len(seen), rows)
				}
				unique := map[string]bool{}
				for _, name := range seen {
					if unique[name] {
						t.Errorf("paging with limit=%d returned %q more than once", limit, name)
					}
					unique[name] = true
				}
			}
		})
	}
}
