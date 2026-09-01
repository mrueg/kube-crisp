package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// bulkDeleteSpec is the writable fixture with a statement that removes the
// whole tenant at once.
func bulkDeleteSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace`,
	}
	return spec
}

// TestDeleteCollectionReportsAForeignKeyViolationAsAConflict is the single
// delete's bug, reached through the collection instead. Delete was taught to
// translate its driver errors; the bulk statement next to it still wrapped
// every one of them in a 500, so the same refusal over the same fixture
// answered 409 or "internal error occurred" depending only on whether the
// client had asked for one object or for all of them.
func TestDeleteCollectionReportsAForeignKeyViolationAsAConflict(t *testing.T) {
	store := newReferencedStorage(t, bulkDeleteSpec())

	_, err := store.DeleteCollection(namespacedContext("acme"), nil,
		&metav1.DeleteOptions{}, &metainternalversion.ListOptions{})
	if err == nil {
		t.Fatal("DeleteCollection() removed a row another table references")
	}
	if !errors.IsConflict(err) {
		t.Fatalf("DeleteCollection() error = %v (%T), want Conflict, as Delete gives", err, err)
	}
}

// TestTheBulkCollectionStatementIsShedAtTheConcurrencyLimit is the hole the
// single delete's own comment predicted — "kubectl delete --all is the request
// most likely to find it" — left open one path over. The per-object fallback
// acquires a slot for every object it removes, so the same verb respected the
// projection's limit or ignored it entirely depending on which path the
// request happened to take.
//
// Exercised at the statement rather than through DeleteCollection, because the
// collection is listed first and that read is shed at the limit before the
// statement is ever reached: the request answers 429 either way, and only this
// seam can tell whether the write itself was bounded.
func TestTheBulkCollectionStatementIsShedAtTheConcurrencyLimit(t *testing.T) {
	spec := bulkDeleteSpec()
	spec.DataSource.MaxConcurrentQueries = ptr(int32(1))

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	release, err := store.limiter.Acquire(t.Context())
	if err != nil {
		t.Fatalf("taking the projection's only slot: %v", err)
	}

	err = store.deleteInBulk(ctx, "acme", &metainternalversion.ListOptions{}, 0)
	release()
	if !errors.IsTooManyRequests(err) {
		t.Fatalf("the bulk statement returned %v with the only slot held, want TooManyRequests", err)
	}

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got, want := len(remaining.(*unstructured.UnstructuredList).Items), 2; got != want {
		t.Errorf("%d objects remain, want %d: a shed request still emptied the collection", got, want)
	}
}

// TestDeleteCollectionStillRemovesTheCollection is the plain path, so nothing
// above can be satisfied by refusing to delete anything.
func TestDeleteCollectionStillRemovesTheCollection(t *testing.T) {
	store := newStorage(t, bulkDeleteSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	deleted, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{},
		&metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	if got, want := len(deleted.(*unstructured.UnstructuredList).Items), 2; got != want {
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
