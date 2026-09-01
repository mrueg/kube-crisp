package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// offsetPagedSpec is seqPagedSpec's other half: the same collection paged by
// skipping rows rather than by resuming after a key.
func offsetPagedSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at, seq
		      FROM orders
		      WHERE tenant = :namespace
		      ORDER BY seq
		      LIMIT :limit OFFSET COALESCE(:offset, 0)`,
	}
	return spec
}

// resumeWithoutALimit reads a page, then asks for the remainder with the token
// alone, and returns both halves.
func resumeWithoutALimit(
	t *testing.T, store *REST, pageSize int64,
) (page, remainder []unstructured.Unstructured, remainderToken string) {
	t.Helper()

	ctx := namespacedContext("acme")
	first, err := store.List(ctx, &metainternalversion.ListOptions{Limit: pageSize})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	firstList := first.(*unstructured.UnstructuredList)
	if firstList.GetContinue() == "" {
		t.Fatal("the first page carried no continue token")
	}

	second, err := store.List(ctx, &metainternalversion.ListOptions{Continue: firstList.GetContinue()})
	if err != nil {
		t.Fatalf("List() returned error resuming: %v", err)
	}
	secondList := second.(*unstructured.UnstructuredList)
	return firstList.Items, secondList.Items, secondList.GetContinue()
}

// assertResumedAfterThePage checks the two halves are one collection read in
// two pieces: nothing repeated, nothing missing, and nothing left to fetch.
func assertResumedAfterThePage(t *testing.T, page, remainder []unstructured.Unstructured, token string, rows int) {
	t.Helper()

	if got, want := len(page)+len(remainder), rows; got != want {
		t.Fatalf("the two reads returned %d objects between them, want %d", got, want)
	}
	seen := map[string]bool{}
	for i := range page {
		seen[page[i].GetName()] = true
	}
	for i := range remainder {
		if name := remainder[i].GetName(); seen[name] {
			t.Errorf("object %q came back on both reads: the continue token was ignored", name)
		}
	}
	if token != "" {
		t.Errorf("resuming without a limit returned a continue token %q, but it returned everything left", token)
	}
}

// TestListResumesFromAKeysetTokenWithoutALimit is the silent duplication this
// fixes. ValidateListOptions accepts a continue token with no limit — only
// pairing continue with resourceVersionMatch is rejected — and the etcd store
// answers it by returning the rest of the collection. Here the token was only
// decoded when a limit came with it, so the request read as an ordinary
// unpaged list and served page one over again: every item the client already
// held, delivered a second time with nothing to say it had happened.
func TestListResumesFromAKeysetTokenWithoutALimit(t *testing.T) {
	const rows = 25
	store := newPagedStorage(t, seqPagedSpec(), rows)

	page, remainder, token := resumeWithoutALimit(t, store, 10)
	assertResumedAfterThePage(t, page, remainder, token, rows)
}

// TestListResumesFromAnOffsetTokenWithoutALimit is the same for a projection
// that pages by skipping rows: the offset the token carries has to be applied
// even though no limit came with it.
func TestListResumesFromAnOffsetTokenWithoutALimit(t *testing.T) {
	const rows = 25
	store := newPagedStorage(t, offsetPagedSpec(), rows)

	page, remainder, token := resumeWithoutALimit(t, store, 10)
	assertResumedAfterThePage(t, page, remainder, token, rows)
}

// TestAContinueTokenWithoutALimitIsStillValidated is the other consequence of
// reading the token: one the server did not issue is now rejected, rather than
// quietly ignored and answered with the whole collection from the start.
func TestAContinueTokenWithoutALimitIsStillValidated(t *testing.T) {
	store := newPagedStorage(t, seqPagedSpec(), 5)

	_, err := store.List(namespacedContext("acme"),
		&metainternalversion.ListOptions{Continue: "not-a-token"})
	if !errors.IsBadRequest(err) {
		t.Fatalf("List() returned %v, want a bad-request error", err)
	}
}

// TestAContinueTokenIsIgnoredWhenTheQueryCannotSkipRows keeps the existing
// contract for a projection that declares neither :after nor :offset. Paging
// is not offered there at all — a limit is ignored for the same reason — so a
// token cannot have come from this server, and the collection is returned
// whole rather than an error being raised over a parameter that means nothing
// to this query.
func TestAContinueTokenIsIgnoredWhenTheQueryCannotSkipRows(t *testing.T) {
	store := newTestREST(t)

	list, err := store.List(namespacedContext("acme"),
		&metainternalversion.ListOptions{Continue: "not-a-token"})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got, want := len(list.(*unstructured.UnstructuredList).Items), 2; got != want {
		t.Errorf("List() returned %d items, want all %d", got, want)
	}
}
