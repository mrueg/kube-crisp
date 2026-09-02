package projection

import (
	"database/sql"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// Every other narrowing of a collection delete is checked against what the
// statement can see: a label selector needs :labelSelector declared, and a limit
// or a continue token is refused outright. The namespace — the one narrowing
// that is not optional — was not checked.
//
// The reads defend themselves twice, dropping rows whose mapped namespace
// differs from the request's and warning about them. A bulk delete has no
// second pass, because the rows are gone. So a statement that cannot be told
// which namespace let a caller holding deletecollection in one namespace empty
// every tenant's rows, while the response listed only their own.
func TestABulkDeleteThatCannotSeeTheNamespaceIsNotUsed(t *testing.T) {
	spec := writableSpec()
	spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders`,
	}

	storage, path := newStorageWithDB(t, spec)
	store := storage.(*WritableREST)

	deleted, err := store.DeleteCollection(namespacedContext("acme"), nil,
		&metav1.DeleteOptions{}, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	if got, want := len(deleted.(*unstructured.UnstructuredList).Items), 2; got != want {
		t.Fatalf("DeleteCollection() reported %d objects, want %d", got, want)
	}

	// globex is another tenant and asked for nothing.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}
	defer db.Close()

	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM orders WHERE tenant = 'globex'`).Scan(&remaining); err != nil {
		t.Fatalf("counting the other tenant's rows: %v", err)
	}
	if remaining == 0 {
		t.Error("a collection delete in one namespace removed another tenant's rows")
	}
}

// And a statement that does bind it still takes the fast path, or the check has
// turned every collection delete into a row at a time.
func TestABulkDeleteThatBindsTheNamespaceIsStillUsed(t *testing.T) {
	spec := writableSpec()
	spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace`,
	}

	store := newStorage(t, spec).(*WritableREST)
	if !store.canDeleteInBulk(&metainternalversion.ListOptions{}) {
		t.Error("a statement binding :namespace was not used for a collection delete")
	}
}

// A cluster-scoped projection has no namespace to bind, so nothing is required
// of it.
func TestAClusterScopedBulkDeleteNeedsNoNamespace(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Scope = crispv1alpha1.ClusterScoped
	spec.Mapping.Namespace = ""
	spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders`,
	}

	store := newStorage(t, spec).(*WritableREST)
	if !store.canDeleteInBulk(&metainternalversion.ListOptions{}) {
		t.Error("a cluster-scoped projection was refused the bulk path")
	}
}
