package projection

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// unmappableSpec projects a name column that is empty for one row, which is a
// row that cannot become an object: nothing to call it.
func unmappableSpec(policy crispv1alpha1.UnmappableRowPolicy) crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT '' AS id, tenant, customer FROM orders WHERE tenant = :namespace`,
	}
	spec.Queries.Get = nil
	spec.Mapping.OnUnmappableRow = policy
	return spec
}

// TestUnmappableRowsAreSkippedByDefault keeps the default what it was: one bad
// row does not stop a client seeing the rest of the table.
func TestUnmappableRowsAreSkippedByDefault(t *testing.T) {
	store, ok := newStorage(t, unmappableSpec("")).(*REST)
	if !ok {
		t.Fatal("expected a read-only projection")
	}

	obj, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v, want the unmappable rows skipped", err)
	}
	list, ok := obj.(*unstructured.UnstructuredList)
	if !ok {
		t.Fatalf("List() returned %T, want an UnstructuredList", obj)
	}
	if len(list.Items) != 0 {
		t.Errorf("%d items came back, want none — every row is unmappable here", len(list.Items))
	}
}

// TestUnmappableRowsCanFailTheRead covers the case the default is wrong for.
//
// A collection that silently omits rows is one a client cannot tell from a
// collection that genuinely has fewer, so a controller reconciling towards it
// deletes what it cannot see. Fail is for a projection where that is the
// greater risk.
func TestUnmappableRowsCanFailTheRead(t *testing.T) {
	store, ok := newStorage(t, unmappableSpec(crispv1alpha1.UnmappableRowFail)).(*REST)
	if !ok {
		t.Fatal("expected a read-only projection")
	}

	_, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err == nil {
		t.Fatal("a row that cannot be mapped was skipped though onUnmappableRow is Fail, so a " +
			"client is handed a collection that is quietly missing rows")
	}
	if !errors.IsInternalError(err) {
		t.Errorf("List() returned %v, want an InternalError", err)
	}
	// The message has to name the setting, or the failure looks like a bug in
	// the projection rather than the policy it asked for.
	if msg := err.Error(); !strings.Contains(msg, "onUnmappableRow") {
		t.Errorf("the error does not mention onUnmappableRow: %s", msg)
	}
}
