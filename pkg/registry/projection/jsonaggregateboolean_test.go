package projection

import (
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// TestListJSONAggregationCarriesBooleans is the failure the way an operator
// meets it: not a wrong value, but a collection that is simply empty.
//
// resultFormat: JSONArray decodes the aggregate with encoding/json, so every
// number in a row arrives as a float64 — and a boolean is a number by then,
// since SQLite's json_group_array renders one as 0 or 1 and MySQL's
// JSON_ARRAYAGG does the same with TINYINT(1). The boolean field type had no
// float64 branch, so every row carrying a flag failed to map, and under the
// default onUnmappableRow: Skip the whole list came back with nothing in it
// and a warning.
func TestListJSONAggregationCarriesBooleans(t *testing.T) {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		ResultFormat: crispv1alpha1.ResultFormatJSONArray,
		SQL: `SELECT json_group_array(json_object(
		          'id', id, 'tenant', tenant, 'customer', customer, 'status', status,
		          'total_cents', total_cents, 'line_items', json(line_items),
		          'updated_at', updated_at, 'paid', total_cents > 0))
		      FROM (SELECT * FROM orders WHERE tenant = :namespace ORDER BY id)`,
	}
	spec.Mapping.Fields = append(spec.Mapping.Fields, crispv1alpha1.FieldMapping{
		Column: "paid", Path: "spec.paid", Type: crispv1alpha1.FieldTypeBoolean,
	})

	store := newStorage(t, spec).(*REST)

	list, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := list.(*unstructured.UnstructuredList)
	if got, want := len(items.Items), 2; got != want {
		t.Fatalf("List() returned %d items, want %d: rows with a flag were dropped", got, want)
	}

	paid, found, err := unstructured.NestedBool(items.Items[0].Object, "spec", "paid")
	if err != nil || !found {
		t.Fatalf("spec.paid missing: %v", err)
	}
	if !paid {
		t.Errorf("spec.paid = false, want true")
	}
}
