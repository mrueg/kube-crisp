package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// A multi-statement write reports the rows its own statement touched. Summing
// the transaction lets a prelude that always writes something stand in for an
// update that matched nothing.
func TestMultiStatementUpdateReportsItsOwnRowCount(t *testing.T) {
	spec := writableSpec()
	spec.Queries.Update = &crispv1alpha1.Query{
		Statements: []string{
			// A bookkeeping statement of the kind a prelude exists for: it
			// always touches a row.
			`UPDATE orders SET updated_at = updated_at WHERE tenant = :namespace AND id = :name`,
			// Guarded in SQL on something the server cannot check for
			// itself: only a shipped order may be updated.
			`UPDATE orders
			   SET customer = :customer, status = :status, total_cents = :total_cents,
			       updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
			 WHERE tenant = :namespace AND id = :name AND status = 'shipped'`,
		},
	}

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	// order-1002 is pending, so the guarded statement matches nothing. The
	// prelude still touches a row. No resourceVersion, so nothing is asserted
	// and the write is the only thing that can report the miss.
	incoming := newOrder("order-1002", "grace", 8888)
	incoming.SetResourceVersion("")

	_, _, err := store.Update(ctx, "order-1002",
		rest.DefaultUpdatedObjectInfo(incoming), nil, nil, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("Update() reported success for a statement that matched no rows; the prelude's " +
			"row count stood in for it")
	}
	if !errors.IsNotFound(err) {
		t.Fatalf("Update() error = %v, want NotFound", err)
	}
}
