package projection

import (
	"database/sql"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// newUIDStorage is the orders fixture with a uid column that the update
// statement writes, which is the shape mapping.uid is recommended for.
func newUIDStorage(t *testing.T) *WritableREST {
	t.Helper()

	path := filepath.Join(t.TempDir(), "uid.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	for _, stmt := range []string{
		`CREATE TABLE orders (
			id TEXT PRIMARY KEY, tenant TEXT NOT NULL, uid TEXT NOT NULL,
			customer TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`INSERT INTO orders VALUES ('order-1001','acme','11111111-1111-1111-1111-111111111111','ada','1')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	_ = db.Close()

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: path, PreparedStatements: true})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	spec := crispv1alpha1.CustomResourceProjectionSpec{
		DataSource: crispv1alpha1.DataSource{Driver: "sqlite"},
		Resource: crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1",
			Kind: "Order", Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
		},
		Queries: crispv1alpha1.Queries{
			List: crispv1alpha1.Query{
				SQL: `SELECT id, tenant, uid, customer, updated_at FROM orders WHERE tenant = :namespace ORDER BY id`,
			},
			Get: &crispv1alpha1.Query{
				SQL: `SELECT id, tenant, uid, customer, updated_at FROM orders WHERE tenant = :namespace AND id = :name`,
			},
			Update: &crispv1alpha1.Query{
				SQL: `UPDATE orders SET customer = :customer, uid = :uid,
				          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
				      WHERE tenant = :namespace AND id = :name`,
			},
		},
		Mapping: crispv1alpha1.Mapping{
			Name: "id", Namespace: "tenant", UID: "uid", ResourceVersion: "updated_at",
			Fields: []crispv1alpha1.FieldMapping{{Column: "customer", Path: "spec.customer"}},
		},
	}

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable
}

// metadata.uid is what ownerReferences and the garbage collector resolve
// against, so a client must not be able to rewrite it through an update.
func TestUpdateCannotRewriteTheUID(t *testing.T) {
	store := newUIDStorage(t)
	ctx := namespacedContext("acme")

	before, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	original := before.(*unstructured.Unstructured).GetUID()

	incoming := before.DeepCopyObject().(*unstructured.Unstructured)
	incoming.SetUID("22222222-2222-2222-2222-222222222222")
	_ = unstructured.SetNestedField(incoming.Object, "grace", "spec", "customer")

	_, _, err = store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(incoming), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Update() error = %v, want Invalid: metadata.uid is immutable", err)
	}

	after, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := after.(*unstructured.Unstructured).GetUID(); got != original {
		t.Errorf("metadata.uid is now %q, was %q", got, original)
	}
}

// A patch sends no uid, and that is not an assertion that it should change.
func TestUpdateWithoutAUIDKeepsTheOneThatIsThere(t *testing.T) {
	store := newUIDStorage(t)
	ctx := namespacedContext("acme")

	before, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	original := before.(*unstructured.Unstructured).GetUID()

	incoming := before.DeepCopyObject().(*unstructured.Unstructured)
	incoming.SetUID("")
	_ = unstructured.SetNestedField(incoming.Object, "grace", "spec", "customer")

	result, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(incoming), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	if got := result.(*unstructured.Unstructured).GetUID(); got != original {
		t.Errorf("metadata.uid = %q after an update that sent none, want %q", got, original)
	}
}
