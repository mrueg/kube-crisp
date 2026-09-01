package projection

import (
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// TestAListQueryCanBindADeclaredJSONValue is the failure as a projection
// author meets it: not a wrong answer but a query that cannot run at all,
// reported in the driver's words about a type nobody wrote down.
//
// A parameter declared `from: Value` with `type: json` was resolved by the row
// conversion, which decodes JSON into a Go map — and database/sql refuses to
// bind one, so every request through the query failed with "unsupported type
// map[string]interface {}".
func TestAListQueryCanBindADeclaredJSONValue(t *testing.T) {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders
		      WHERE tenant = :namespace AND status = json_extract(:filter, '$.status')
		      ORDER BY id`,
		Parameters: []crispv1alpha1.QueryParameter{
			{
				Name:  "filter",
				From:  crispv1alpha1.ParameterSourceValue,
				Type:  crispv1alpha1.FieldTypeJSON,
				Value: `{"status":"shipped"}`,
			},
		},
	}

	store := newStorage(t, spec).(*REST)

	list, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := list.(*unstructured.UnstructuredList).Items
	if got, want := len(items), 1; got != want {
		t.Fatalf("List() returned %d items, want %d", got, want)
	}
	if got, want := items[0].GetName(), "order-1001"; got != want {
		t.Errorf("List() returned %q, want %q: the literal did not reach the query", got, want)
	}
}
