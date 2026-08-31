package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

// requiredSchemaFor is what the controller stores, reached the same way it
// reaches it.
func requiredSchemaFor(t *testing.T, p crispv1alpha1.CustomResourceProjection) *crispv1alpha1.RequiredSchema {
	t.Helper()
	required := projection.RequiredSchema(p.Spec)
	if required == nil {
		t.Fatal("the projection requires nothing, so the comparison would be vacuous")
	}
	return required
}

// ordersProjection reads one table and maps an identity column, a labelled
// column and a typed field, so the checklist has one of each kind.
const ordersProjection = `
apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: orders
spec:
  dataSource:
    driver: postgres
    secretRef: {name: orders-db, namespace: kube-crisp}
  resource:
    group: store.example.com
    version: v1alpha1
    kind: Order
    plural: orders
    scope: Cluster
  queries:
    list:
      sql: |
        SELECT id, customer, total_cents,
               (extract(epoch FROM o.updated_at) * 1000000)::bigint::text AS version
        FROM orders o
  mapping:
    name: id
    resourceVersion: version
    labels:
      store.example.com/customer: customer
    fields:
      - {column: total_cents, path: spec.totalCents, type: integer}
`

func writeOrders(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "orders.yaml"), []byte(ordersProjection), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func runSchema(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := NewCommandSchema(&out, &errOut)
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

// TestSchemaFromManifests is the mode the status field cannot serve: a
// projection that has never reached a cluster has no status to read.
func TestSchemaFromManifests(t *testing.T) {
	stdout, _, err := runSchema(t, "-f", writeOrders(t))
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"orders.store.example.com/v1alpha1",
		"tables: orders",
		"id", "identity",
		"customer", "label",
		"total_cents", "integer",
		"version", "metadata",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output does not mention %q:\n%s", want, stdout)
		}
	}
}

// TestSchemaDoesNotReportAColumnAsATable covers the bug this command found.
// EXTRACT spells an argument separator FROM, and reading it as a clause named
// o.updated_at as a table — in every projection deriving a resourceVersion
// from a timestamp, which is the documented idiom.
func TestSchemaDoesNotReportAColumnAsATable(t *testing.T) {
	stdout, _, err := runSchema(t, "-f", writeOrders(t))
	if err != nil {
		t.Fatal(err)
	}

	tables := ""
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, "tables:") {
			tables = line
		}
	}
	if tables == "" {
		t.Fatalf("no tables line:\n%s", stdout)
	}
	if strings.Contains(tables, "updated_at") {
		t.Fatalf("a column is reported as a table: %s", tables)
	}
	if !strings.Contains(tables, "orders") {
		t.Fatalf("the real table is missing: %s", tables)
	}
}

// TestSchemaJSONCarriesTheSameAnswer, for handing to a schema tool rather than
// to a person.
func TestSchemaJSONCarriesTheSameAnswer(t *testing.T) {
	stdout, _, err := runSchema(t, "-f", writeOrders(t), "-o", "json")
	if err != nil {
		t.Fatal(err)
	}

	var got []projectionSchema
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, stdout)
	}
	if len(got) != 1 {
		t.Fatalf("got %d projection(s), want 1", len(got))
	}
	if strings.Join(got[0].Tables, ",") != "orders" {
		t.Fatalf("tables = %v, want just orders", got[0].Tables)
	}

	byName := map[string]crispv1alpha1.RequiredColumn{}
	for _, column := range got[0].Columns {
		byName[column.Name] = column
	}
	if got := byName["id"]; got.UsedFor != "identity" {
		t.Errorf("id usedFor = %q, want identity", got.UsedFor)
	}
	if got := byName["total_cents"]; got.Type != crispv1alpha1.FieldTypeInteger {
		t.Errorf("total_cents type = %q, want integer", got.Type)
	}
}

// TestSchemaMatchesWhatTheControllerWouldStore. The command derives the answer
// instead of reading status.requiredSchema, which is only correct while the two
// come from the same place.
func TestSchemaMatchesWhatTheControllerWouldStore(t *testing.T) {
	projections, err := loadFiles([]string{writeOrders(t)})
	if err != nil {
		t.Fatal(err)
	}

	got := requirements(projections)
	if len(got) != 1 {
		t.Fatalf("got %d projection(s), want 1", len(got))
	}

	// projection.RequiredSchema is what pkg/controller/projection assigns to
	// status.requiredSchema.
	want := requiredSchemaFor(t, projections[0])
	if strings.Join(got[0].Tables, ",") != strings.Join(want.Tables, ",") {
		t.Errorf("tables = %v, controller would store %v", got[0].Tables, want.Tables)
	}
	if len(got[0].Columns) != len(want.Columns) {
		t.Fatalf("got %d column(s), controller would store %d", len(got[0].Columns), len(want.Columns))
	}
	for i := range got[0].Columns {
		if got[0].Columns[i] != want.Columns[i] {
			t.Errorf("column %d = %+v, controller would store %+v", i, got[0].Columns[i], want.Columns[i])
		}
	}
}

// TestSchemaRejectsAnUnsupportedFormat fails before reading anything.
func TestSchemaRejectsAnUnsupportedFormat(t *testing.T) {
	if _, _, err := runSchema(t, "-f", writeOrders(t), "-o", "yaml"); err == nil {
		t.Fatal("expected an error")
	}
}

// TestSchemaFilenamesAndNamesAreExclusive, as everywhere else in the plugin.
func TestSchemaFilenamesAndNamesAreExclusive(t *testing.T) {
	_, _, err := runSchema(t, "-f", writeOrders(t), "orders")
	if err == nil || !strings.Contains(err.Error(), "cannot combine") {
		t.Fatalf("got %v, want a refusal to combine -f with names", err)
	}
}
