package projection

import (
	"fmt"
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

func ordersSpec() crispv1alpha1.CustomResourceProjectionSpec {
	return crispv1alpha1.CustomResourceProjectionSpec{
		DataSource: crispv1alpha1.DataSource{Driver: "postgres"},
		Queries: crispv1alpha1.Queries{
			List: crispv1alpha1.Query{
				SQL: "SELECT id, tenant, customer, status, total_cents, updated_at FROM orders WHERE tenant = :namespace",
			},
			Create: &crispv1alpha1.Query{
				Statements: []string{
					"INSERT INTO order_events (id, tenant, event) VALUES (:id, :tenant, 'created')",
					"INSERT INTO orders (id, tenant, customer) VALUES (:id, :tenant, :customer) RETURNING id",
				},
			},
		},
		Watch: &crispv1alpha1.WatchSpec{
			DeletedQuery: &crispv1alpha1.Query{
				SQL: "SELECT id, tenant FROM order_tombstones WHERE deleted_at > :since",
			},
		},
		Mapping: crispv1alpha1.Mapping{
			Name:            "id",
			Namespace:       "tenant",
			ResourceVersion: "updated_at",
			Labels:          map[string]string{"store.example.com/status": "status"},
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "customer", Path: "spec.customer"},
				{Column: "total_cents", Path: "spec.totalCents", Type: crispv1alpha1.FieldTypeInteger},
			},
		},
	}
}

// TestRequiredSchemaNamesEveryTable covers what makes this worth reporting: the
// tables are spread across the queries, and a transactional write reaches ones
// no single-statement query does.
func TestRequiredSchemaNamesEveryTable(t *testing.T) {
	got := RequiredSchema(ordersSpec())
	if got == nil {
		t.Fatal("RequiredSchema() returned nothing for a projection that reads three tables")
	}

	want := "order_events,order_tombstones,orders"
	if strings.Join(got.Tables, ",") != want {
		t.Errorf("tables = %v, want %v", got.Tables, strings.Split(want, ","))
	}
}

// TestRequiredSchemaGathersTheMappedColumns is the other half: the columns are
// spread across a dozen mapping fields, so what a projection needs is nowhere
// written down.
func TestRequiredSchemaGathersTheMappedColumns(t *testing.T) {
	got := RequiredSchema(ordersSpec())

	var rendered []string
	for _, column := range got.Columns {
		rendered = append(rendered, fmt.Sprintf("%s:%s:%s", column.Name, column.Type, column.UsedFor))
	}
	want := []string{
		"customer:string:field",
		"id:string:identity",
		"status:string:label",
		"tenant:string:identity",
		"total_cents:integer:field",
		"updated_at:string:metadata",
	}
	if strings.Join(rendered, ",") != strings.Join(want, ",") {
		t.Errorf("columns\n got: %v\nwant: %v", rendered, want)
	}
}

// TestRequiredSchemaReportsIdentityOverField: a column can be both, and the
// half that cannot be dropped is the one worth reporting.
func TestRequiredSchemaReportsIdentityOverField(t *testing.T) {
	spec := ordersSpec()
	spec.Mapping.Fields = append(spec.Mapping.Fields,
		crispv1alpha1.FieldMapping{Column: "id", Path: "spec.orderID"})

	for _, column := range RequiredSchema(spec).Columns {
		if column.Name != "id" {
			continue
		}
		if column.UsedFor != usedForIdentity {
			t.Errorf("id is reported as %q, want %q — it is what names the object", column.UsedFor, usedForIdentity)
		}
		return
	}
	t.Fatal("id is not reported at all")
}

// TestRequiredSchemaIsStable keeps a status that is rebuilt on every sync from
// reshuffling. The columns come out of a map, so without an order they would
// never compare equal to themselves and every sync would write again.
func TestRequiredSchemaIsStable(t *testing.T) {
	first := RequiredSchema(ordersSpec())
	for i := 0; i < 50; i++ {
		again := RequiredSchema(ordersSpec())
		if strings.Join(again.Tables, ",") != strings.Join(first.Tables, ",") {
			t.Fatalf("tables differ between calls: %v then %v", first.Tables, again.Tables)
		}
		for j := range again.Columns {
			if again.Columns[j] != first.Columns[j] {
				t.Fatalf("column %d differs between calls: %+v then %+v", j, first.Columns[j], again.Columns[j])
			}
		}
	}
}

// TestRequiredSchemaOfAProjectionThatMapsNothing returns nothing rather than an
// empty object, so the field is absent from the status instead of present and
// saying nothing.
func TestRequiredSchemaOfAProjectionThatMapsNothing(t *testing.T) {
	spec := crispv1alpha1.CustomResourceProjectionSpec{
		DataSource: crispv1alpha1.DataSource{Driver: "sqlite"},
		Queries:    crispv1alpha1.Queries{List: crispv1alpha1.Query{SQL: "SELECT 1"}},
	}
	if got := RequiredSchema(spec); got != nil {
		t.Errorf("RequiredSchema() = %+v, want nil when there is nothing to report", got)
	}
}
