package projection

import (
	"database/sql"
	"path/filepath"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// The id every row of this table carries is one a float64 cannot hold: it is
// one past 2^53, which is the smallest integer JSON decoding rounds, and it is
// the range CockroachDB's unique_rowid() lives in.
const largeID = 9007199254740993

// largeIDDB keys a table by such an id, with the same value beside it as the
// integer, the string and the untyped JSON a mapping can read it through.
func largeIDDB(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "large.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	for _, s := range []string{
		`CREATE TABLE orders (
			id INTEGER PRIMARY KEY, tenant TEXT NOT NULL, qty INTEGER NOT NULL,
			ref INTEGER NOT NULL, attrs TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`INSERT INTO orders VALUES (9007199254740993, 'acme', 9007199254740993, 9007199254740993, '{"parent":9007199254740993}', '1')`,
		// Two past the first: the id a rounded key would resume after lies
		// between them, so a page that starts from it repeats the first row.
		`INSERT INTO orders VALUES (9007199254740995, 'acme', 1, 1, '{}', '1')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	return path
}

func largeIDSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.Queries = crispv1alpha1.Queries{
		List: crispv1alpha1.Query{
			ResultFormat: crispv1alpha1.ResultFormatJSONArray,
			SQL: `SELECT json_group_array(json_object(
			          'id', id, 'tenant', tenant, 'qty', qty, 'ref', ref,
			          'attrs', json(attrs), 'updated_at', updated_at))
			      FROM (SELECT * FROM orders
			            WHERE tenant = :namespace AND (:after IS NULL OR id > :after)
			            ORDER BY id LIMIT :limit)`,
			KeysetColumn: "id",
		},
		Get: &crispv1alpha1.Query{
			SQL: `SELECT id, tenant, qty, ref, attrs, updated_at
			      FROM orders WHERE tenant = :namespace AND id = :name`,
		},
		Update: &crispv1alpha1.Query{
			SQL: `UPDATE orders
			      SET qty = :qty, ref = :ref, attrs = :attrs,
			          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
			      WHERE tenant = :namespace AND id = :name
			        AND (:resourceVersion IS NULL OR updated_at = :resourceVersion)`,
		},
	}
	spec.Mapping = crispv1alpha1.Mapping{
		Name:            "id",
		Namespace:       "tenant",
		ResourceVersion: "updated_at",
		Fields: []crispv1alpha1.FieldMapping{
			{Column: "qty", Path: "spec.qty", Type: crispv1alpha1.FieldTypeInteger},
			{Column: "ref", Path: "spec.ref", Type: crispv1alpha1.FieldTypeString},
			{Column: "attrs", Path: "spec.attrs", Type: crispv1alpha1.FieldTypeJSON},
		},
	}
	return spec
}

// TestAJSONAggregateKeepsALargeIntegerThroughTheAPI is the loss the way a
// client meets it. resultFormat: JSONArray decoded the aggregate with
// json.Unmarshal, so every number in it was a float64 before any field type
// saw it — and the row keyed by 9007199254740993 was served as an object named
// "9007199254740992", with spec.qty rounded the same way. A GET by that name,
// and the UPDATE behind every kubectl edit, then bound an id that is not in
// the table.
func TestAJSONAggregateKeepsALargeIntegerThroughTheAPI(t *testing.T) {
	path := largeIDDB(t)
	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver: "sqlite", DSN: path, PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", largeIDSpec(), pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	store := storages.writable
	ctx := namespacedContext("acme")

	list, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := list.(*unstructured.UnstructuredList).Items
	if len(items) != 2 {
		t.Fatalf("List() returned %d items, want 2", len(items))
	}
	const name = "9007199254740993"
	if got := items[0].GetName(); got != name {
		t.Fatalf("List() named the object %q, want %q", got, name)
	}
	assertLargeID(t, &items[0], largeID)

	// The key a page resumes after comes out of the same aggregate. Rounded,
	// it would name an id between the two rows, and the second page would
	// serve the first row again.
	first, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List(limit 1) returned error: %v", err)
	}
	token := first.(*unstructured.UnstructuredList).GetContinue()
	if token == "" {
		t.Fatal("the first page carries no continue token")
	}
	second, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 1, Continue: token})
	if err != nil {
		t.Fatalf("List(continue) returned error: %v", err)
	}
	page := second.(*unstructured.UnstructuredList).Items
	if len(page) != 1 || page[0].GetName() != "9007199254740995" {
		t.Errorf("the second page holds %v, want the row after %s", pageNames(page), name)
	}

	// The name the list handed out has to reach the row it came from.
	fetched, err := store.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get(%q) returned error: %v: the list named a row that is not in the table", name, err)
	}
	assertLargeID(t, fetched.(*unstructured.Unstructured), largeID)

	// And a write binds the digits back exactly, so the update reaches that
	// row and stores what the client sent.
	updated := fetched.(*unstructured.Unstructured).DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, int64(largeID+2), "spec", "qty"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(updated.Object, "9007199254740995", "spec", "ref"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(updated.Object, int64(largeID+2), "spec", "attrs", "parent"); err != nil {
		t.Fatal(err)
	}
	result, _, err := store.Update(ctx, name, rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	assertLargeID(t, result.(*unstructured.Unstructured), largeID+2)

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()
	var qty, ref int64
	var attrs string
	if err := db.QueryRow(`SELECT qty, ref, attrs FROM orders WHERE id = 9007199254740993`).Scan(&qty, &ref, &attrs); err != nil {
		t.Fatalf("reading the row back: %v", err)
	}
	if qty != largeID+2 || ref != largeID+2 {
		t.Errorf("the row holds qty %d and ref %d, want %d: the write bound rounded values", qty, ref, largeID+2)
	}
	if want := `{"parent":9007199254740995}`; attrs != want {
		t.Errorf("the row holds attrs %s, want %s", attrs, want)
	}
}

func pageNames(items []unstructured.Unstructured) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.GetName())
	}
	return out
}

func assertLargeID(t *testing.T, obj *unstructured.Unstructured, want int64) {
	t.Helper()
	spec, _ := obj.Object["spec"].(map[string]any)
	if got := spec["qty"]; got != want {
		t.Errorf("spec.qty = %#v (%T), want %d", got, got, want)
	}
	if got := spec["ref"]; got != "9007199254740993" && got != "9007199254740995" {
		t.Errorf("spec.ref = %#v, want the exact digits", got)
	}
	attrs, _ := spec["attrs"].(map[string]any)
	if got := attrs["parent"]; got != want {
		t.Errorf("spec.attrs.parent = %#v (%T), want %d", got, got, want)
	}
}
