package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// pagedBulkDeleteSpec pages by an ascending sequence column and can remove the
// whole tenant in one statement. That statement has no way to express a page,
// which is the point: a limited request must not reach it.
func pagedBulkDeleteSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := seqPagedSpec()
	spec.Queries.Delete = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace AND id = :name`,
	}
	spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace`,
	}
	return spec
}

// newPagedWritable is newPagedStorage's writable half.
func newPagedWritable(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec, rows int) *WritableREST {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             "sqlite",
		DSN:                timeOrderedDB(t, rows),
		PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable
}

// TestDeleteCollectionWithALimitRemovesOnlyThePageItReports is the data-loss
// bug this guards: the collection was listed with the limit applied, the
// response reported that page and admission was shown that page, and then a
// bulk statement that knows nothing about limits emptied the table. The client
// was told three objects had gone while ten had.
func TestDeleteCollectionWithALimitRemovesOnlyThePageItReports(t *testing.T) {
	const rows = 10
	store := newPagedWritable(t, pagedBulkDeleteSpec(), rows)
	ctx := namespacedContext("acme")

	deleted, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{},
		&metainternalversion.ListOptions{Limit: 3})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	reported := deleted.(*unstructured.UnstructuredList).Items
	if got, want := len(reported), 3; got != want {
		t.Fatalf("DeleteCollection() reported %d objects, want %d", got, want)
	}

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	survivors := remaining.(*unstructured.UnstructuredList).Items
	if got, want := len(survivors), rows-len(reported); got != want {
		t.Fatalf("%d objects remain, want %d: the limit did not bound the delete", got, want)
	}

	// And the survivors are the rows outside the page, not an arbitrary
	// subset: what was reported deleted really is what went.
	left := map[string]bool{}
	for i := range survivors {
		left[survivors[i].GetName()] = true
	}
	for i := range reported {
		if name := reported[i].GetName(); left[name] {
			t.Errorf("object %q was reported deleted but is still there", name)
		}
	}
}

// TestDeleteCollectionWithAContinueTokenRemovesOnlyThatPage is the same
// request seen from the middle. The rows before the token were never listed
// and never validated, and a statement that cannot be told where the page
// begins would take them too.
func TestDeleteCollectionWithAContinueTokenRemovesOnlyThatPage(t *testing.T) {
	const rows = 10
	store := newPagedWritable(t, pagedBulkDeleteSpec(), rows)
	ctx := namespacedContext("acme")

	first, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 4})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	token := first.(*unstructured.UnstructuredList).GetContinue()
	if token == "" {
		t.Fatalf("the first page carried no continue token")
	}

	deleted, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{},
		&metainternalversion.ListOptions{Limit: 4, Continue: token})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	if got, want := len(deleted.(*unstructured.UnstructuredList).Items), 4; got != want {
		t.Fatalf("DeleteCollection() reported %d objects, want %d", got, want)
	}

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got, want := len(remaining.(*unstructured.UnstructuredList).Items), rows-4; got != want {
		t.Fatalf("%d objects remain, want %d: the page was not what was deleted", got, want)
	}
}

// TestDeleteCollectionWithALimitNeedsASingleObjectDelete is the corner the
// fallback leaves: with nothing but a bulk statement there is no way to remove
// exactly one page, and refusing is the only answer that does not delete rows
// the client never asked about. The same is already true of a selector the
// statement cannot see.
func TestDeleteCollectionWithALimitNeedsASingleObjectDelete(t *testing.T) {
	const rows = 10
	spec := pagedBulkDeleteSpec()
	spec.Queries.Delete = nil

	store := newPagedWritable(t, spec, rows)
	ctx := namespacedContext("acme")

	_, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{},
		&metainternalversion.ListOptions{Limit: 3})
	if !errors.IsMethodNotSupported(err) {
		t.Fatalf("DeleteCollection() returned %v, want a method-not-supported error", err)
	}

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got := len(remaining.(*unstructured.UnstructuredList).Items); got != rows {
		t.Errorf("%d objects remain, want %d: the rejected request still deleted", got, rows)
	}
}

// TestDeleteCollectionWithoutALimitStillUsesOneStatement keeps the fast path.
// An unpaged `delete --all` is exactly what the bulk statement expresses, and
// narrowing the guard past that would cost a round trip per row.
func TestDeleteCollectionWithoutALimitStillUsesOneStatement(t *testing.T) {
	const rows = 10
	spec := pagedBulkDeleteSpec()
	spec.Queries.Delete = nil

	store := newPagedWritable(t, spec, rows)
	ctx := namespacedContext("acme")

	deleted, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{},
		&metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	if got, want := len(deleted.(*unstructured.UnstructuredList).Items), rows; got != want {
		t.Fatalf("DeleteCollection() reported %d objects, want %d", got, want)
	}

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got := len(remaining.(*unstructured.UnstructuredList).Items); got != 0 {
		t.Errorf("%d objects survived the collection delete", got)
	}
}
