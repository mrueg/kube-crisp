package projection

import (
	"database/sql"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// newReferencedStorage is an orders table something else points at, so deleting
// a row the database will not let go of can be asked for.
func newReferencedStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) *WritableREST {
	t.Helper()

	path := filepath.Join(t.TempDir(), "referenced.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	for _, stmt := range []string{
		`PRAGMA foreign_keys = ON`,
		`CREATE TABLE orders (
			id TEXT PRIMARY KEY, tenant TEXT NOT NULL, customer TEXT NOT NULL,
			status TEXT NOT NULL, total_cents INTEGER NOT NULL,
			line_items TEXT NOT NULL, updated_at TEXT NOT NULL)`,
		`CREATE TABLE shipments (
			id TEXT PRIMARY KEY,
			order_id TEXT NOT NULL REFERENCES orders (id))`,
		`INSERT INTO orders VALUES ('order-1001','acme','ada','shipped',4999,'[]','1')`,
		`INSERT INTO shipments VALUES ('ship-1','order-1001')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	_ = db.Close()

	// Foreign keys are off per connection in SQLite, so the pool has to ask.
	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver: "sqlite", DSN: path + "?_pragma=foreign_keys(1)", PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable
}

// A delete the database refuses because something references the row is a
// conflict, not an internal error. Every other write path translates its driver
// errors; this one wrapped them all in a 500, so a row with a child answered
// "internal error occurred" and told the client nothing it could act on.
func TestDeleteReportsAForeignKeyViolationAsAConflict(t *testing.T) {
	store := newReferencedStorage(t, writableSpec())

	_, _, err := store.Delete(namespacedContext("acme"), "order-1001", nil, &metav1.DeleteOptions{})
	if err == nil {
		t.Fatal("Delete() removed a row another table references")
	}
	if !errors.IsConflict(err) {
		t.Fatalf("Delete() error = %v (%T), want Conflict", err, err)
	}
}

// The concurrency limit binds deletes too. Without it, the one bound on how
// much work a projection can have in flight had a verb-shaped hole in it — and
// kubectl delete --all is the request most likely to find it.
func TestDeleteIsShedWhenTheProjectionIsAtItsLimit(t *testing.T) {
	spec := writableSpec()
	spec.DataSource.MaxConcurrentQueries = ptr(int32(1))

	store := newStorage(t, spec).(*WritableREST)

	release, err := store.limiter.Acquire(t.Context())
	if err != nil {
		t.Fatalf("taking the slot: %v", err)
	}
	defer release()

	_, _, err = store.Delete(namespacedContext("acme"), "order-1001", nil, &metav1.DeleteOptions{})
	if !errors.IsTooManyRequests(err) {
		t.Fatalf("Delete() error = %v, want TooManyRequests", err)
	}
}
