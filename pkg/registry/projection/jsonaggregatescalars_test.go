package projection

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestAJSONAggregateAgreesWithRowsOnAJSONColumn covers a json column whose
// document is a scalar. The aggregate embeds the column's value as JSON, so a
// document holding the string "hello" arrives already decoded, as the Go
// string hello — and it was then parsed as JSON a second time. "hello" made
// the row unmappable, and "123" and "true" came out as a number and a boolean,
// which a read-modify-write then stored over the string. The same row read
// through the Rows format, as a get does, was the string it is, so which
// result format a projection chose changed what its objects said.
func TestAJSONAggregateAgreesWithRowsOnAJSONColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scalars.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	docs := map[string]string{
		"hello":  `"hello"`,
		"digits": `"123"`,
		"flag":   `"true"`,
		"number": `123`,
		"object": `{"a":1,"b":["x"]}`,
		"array":  `[1,"two",true]`,
	}
	if _, err := db.Exec(`CREATE TABLE orders (
		id TEXT PRIMARY KEY, tenant TEXT NOT NULL, attrs TEXT NOT NULL, updated_at TEXT NOT NULL)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	for id, doc := range docs {
		if _, err := db.Exec(`INSERT INTO orders VALUES (?, 'acme', ?, '1')`, id, doc); err != nil {
			t.Fatalf("seeding %s: %v", id, err)
		}
	}

	spec := testSpec()
	spec.Queries = crispv1alpha1.Queries{
		List: crispv1alpha1.Query{
			ResultFormat: crispv1alpha1.ResultFormatJSONArray,
			SQL: `SELECT json_group_array(json_object(
			          'id', id, 'tenant', tenant, 'attrs', json(attrs), 'updated_at', updated_at))
			      FROM (SELECT * FROM orders WHERE tenant = :namespace ORDER BY id)`,
		},
		Get: &crispv1alpha1.Query{
			SQL: `SELECT id, tenant, attrs, updated_at FROM orders WHERE tenant = :namespace AND id = :name`,
		},
	}
	spec.Mapping = crispv1alpha1.Mapping{
		Name:            "id",
		Namespace:       "tenant",
		ResourceVersion: "updated_at",
		OnUnmappableRow: crispv1alpha1.UnmappableRowFail,
		Fields: []crispv1alpha1.FieldMapping{
			{Column: "attrs", Path: "spec.attrs", Type: crispv1alpha1.FieldTypeJSON},
		},
	}

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: path})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	store := storages.read
	ctx := namespacedContext("acme")

	list, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := list.(*unstructured.UnstructuredList).Items
	if len(items) != len(docs) {
		t.Fatalf("List() returned %d items, want %d", len(items), len(docs))
	}

	want := map[string]any{
		"hello":  "hello",
		"digits": "123",
		"flag":   "true",
		"number": int64(123),
		"object": map[string]any{"a": int64(1), "b": []any{"x"}},
		"array":  []any{int64(1), "two", true},
	}
	for i := range items {
		name := items[i].GetName()
		listed := items[i].Object["spec"].(map[string]any)["attrs"]
		if !reflect.DeepEqual(listed, want[name]) {
			t.Errorf("listed %s: spec.attrs = %#v, want %#v", name, listed, want[name])
		}

		fetched, err := store.Get(ctx, name, &metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Get(%q) returned error: %v", name, err)
		}
		got := fetched.(*unstructured.Unstructured).Object["spec"].(map[string]any)["attrs"]
		if !reflect.DeepEqual(got, listed) {
			t.Errorf("%s: a get says spec.attrs = %#v and a list says %#v", name, got, listed)
		}
	}
}
