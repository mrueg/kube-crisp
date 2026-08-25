package projection

import (
	"database/sql"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// restUpdate wraps an object as the update the REST layer expects.
func restUpdate(obj *unstructured.Unstructured) rest.UpdatedObjectInfo {
	return rest.DefaultUpdatedObjectInfo(obj)
}

// setStoredVersion moves a row's version column behind the server's back, so a
// statement-level precondition can be made to fail.
func setStoredVersion(path, id, version string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`UPDATE orders SET updated_at = ? WHERE id = ?`, version, id)
	return err
}

// twoTableSpec projects a kind whose writes touch a second table, which is the
// case a single statement cannot serve: the order and its audit row have to
// land together or not at all.
func twoTableSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Queries.Create = &crispv1alpha1.Query{
		Statements: []string{
			`INSERT INTO order_events (id, tenant, event) VALUES (:id, :tenant, 'created')`,
			`INSERT INTO orders (id, tenant, customer, status, total_cents, line_items, updated_at)
			 VALUES (:id, :tenant, :customer, :status, :total_cents, :line_items,
			         CAST((SELECT COALESCE(MAX(CAST(updated_at AS INTEGER)), 0) + 1 FROM orders) AS TEXT))`,
		},
	}
	spec.Queries.Delete = &crispv1alpha1.Query{
		Statements: []string{
			`INSERT INTO order_events (id, tenant, event) VALUES (:name, :namespace, 'deleted')`,
			`DELETE FROM orders WHERE tenant = :namespace AND id = :name`,
		},
	}
	return spec
}

// newTwoTableStorage seeds the extra table the transactional fixture writes to.
func newTwoTableStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) (*WritableREST, string) {
	t.Helper()

	path := newTestDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE order_events (
		id TEXT NOT NULL, tenant TEXT NOT NULL, event TEXT NOT NULL)`); err != nil {
		t.Fatalf("creating the events table: %v", err)
	}
	_ = db.Close()

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: path, PreparedStatements: true})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable, path
}

// countRows reads a table straight from the file, behind the server's back.
func countRows(t *testing.T, path, query string) int {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("counting with %q: %v", query, err)
	}
	return count
}

func TestTransactionalCreateWritesBothTables(t *testing.T) {
	store, path := newTwoTableStorage(t, twoTableSpec())
	ctx := namespacedContext("acme")

	if _, err := store.Create(ctx, newOrder("order-tx-1", "ada", 10), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	if got := countRows(t, path, `SELECT COUNT(*) FROM orders WHERE id = 'order-tx-1'`); got != 1 {
		t.Errorf("orders holds %d rows for the created object, want 1", got)
	}
	if got := countRows(t, path, `SELECT COUNT(*) FROM order_events WHERE id = 'order-tx-1' AND event = 'created'`); got != 1 {
		t.Errorf("order_events holds %d rows for the created object, want 1", got)
	}

	// The object is readable afterwards, so the transaction committed rather
	// than merely not erroring.
	if _, err := store.Get(ctx, "order-tx-1", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() after a transactional create returned error: %v", err)
	}
}

// TestTransactionalCreateRollsBack is the reason the feature exists: the first
// statement must not survive the second one failing.
func TestTransactionalCreateRollsBack(t *testing.T) {
	store, path := newTwoTableStorage(t, twoTableSpec())
	ctx := namespacedContext("acme")

	// order-1001 is already there, so the insert into orders violates the
	// primary key while the audit row has already been written.
	_, err := store.Create(ctx, newOrder("order-1001", "ada", 10), nil, &metav1.CreateOptions{})
	if !errors.IsAlreadyExists(err) {
		t.Fatalf("Create() error = %v, want AlreadyExists", err)
	}

	if got := countRows(t, path, `SELECT COUNT(*) FROM order_events WHERE id = 'order-1001'`); got != 0 {
		t.Errorf("order_events holds %d rows after the transaction failed, want 0", got)
	}
	if got := countRows(t, path, `SELECT COUNT(*) FROM orders WHERE id = 'order-1001'`); got != 1 {
		t.Errorf("orders holds %d rows for the pre-existing object, want the original 1", got)
	}
}

func TestTransactionalDelete(t *testing.T) {
	store, path := newTwoTableStorage(t, twoTableSpec())
	ctx := namespacedContext("acme")

	if _, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	if got := countRows(t, path, `SELECT COUNT(*) FROM orders WHERE id = 'order-1001'`); got != 0 {
		t.Errorf("the row survived the delete (%d rows)", got)
	}
	if got := countRows(t, path, `SELECT COUNT(*) FROM order_events WHERE id = 'order-1001' AND event = 'deleted'`); got != 1 {
		t.Errorf("order_events holds %d rows for the delete, want 1", got)
	}
}

// TestTransactionalDeleteOfMissingObjectRollsBack covers the subtle case: the
// delete matches nothing, so the audit row it wrote first must go too.
func TestTransactionalDeleteOfMissingObjectRollsBack(t *testing.T) {
	store, path := newTwoTableStorage(t, twoTableSpec())
	ctx := namespacedContext("acme")

	if _, _, err := store.Delete(ctx, "order-absent", nil, &metav1.DeleteOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Delete() error = %v, want NotFound", err)
	}
	if got := countRows(t, path, `SELECT COUNT(*) FROM order_events WHERE id = 'order-absent'`); got != 0 {
		t.Errorf("order_events holds %d rows for an object that was never deleted, want 0", got)
	}
}

// TestTransactionalUpdateReturnsTheRow checks that the last statement's result
// still answers the request.
func TestTransactionalUpdateReturnsTheRow(t *testing.T) {
	spec := twoTableSpec()
	spec.Queries.Update = &crispv1alpha1.Query{
		Statements: []string{
			`INSERT INTO order_events (id, tenant, event) VALUES (:name, :namespace, 'updated')`,
			`UPDATE orders
			 SET customer = :customer, status = :status, total_cents = :total_cents,
			     updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
			 WHERE tenant = :namespace AND id = :name
			 RETURNING id, tenant, customer, status, total_cents, line_items, updated_at`,
		},
	}

	store, path := newTwoTableStorage(t, spec)
	ctx := namespacedContext("acme")

	updated := newOrder("order-1001", "grace", 77)
	updated.SetResourceVersion("1")

	result, _, err := store.Update(ctx, "order-1001", restUpdate(updated), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}

	obj := result.(*unstructured.Unstructured)
	if customer, _, _ := unstructured.NestedString(obj.Object, "spec", "customer"); customer != "grace" {
		t.Errorf("the response carries customer %q, want the written %q", customer, "grace")
	}
	if obj.GetResourceVersion() != "2" {
		t.Errorf("resourceVersion = %q, want the database's %q", obj.GetResourceVersion(), "2")
	}
	if got := countRows(t, path, `SELECT COUNT(*) FROM order_events WHERE event = 'updated'`); got != 1 {
		t.Errorf("order_events holds %d update rows, want 1", got)
	}
}

func TestTransactionalPreconditionStillApplies(t *testing.T) {
	spec := twoTableSpec()
	spec.Queries.Update = &crispv1alpha1.Query{
		Statements: []string{
			`INSERT INTO order_events (id, tenant, event) VALUES (:name, :namespace, 'updated')`,
			`UPDATE orders SET customer = :customer, updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
			 WHERE tenant = :namespace AND id = :name AND updated_at = :resourceVersion`,
		},
	}

	store, path := newTwoTableStorage(t, spec)
	ctx := namespacedContext("acme")

	stale := newOrder("order-1001", "grace", 77)
	stale.SetResourceVersion("1")
	// Reads see version 1, so the API-level check passes; the statement's own
	// precondition is what rejects this, and the audit row must not survive it.
	if err := setStoredVersion(path, "order-1001", "5"); err != nil {
		t.Fatalf("moving the row's version: %v", err)
	}

	_, _, err := store.Update(ctx, "order-1001", restUpdate(stale), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsConflict(err) {
		t.Fatalf("Update() error = %v, want Conflict", err)
	}
	if got := countRows(t, path, `SELECT COUNT(*) FROM order_events`); got != 0 {
		t.Errorf("order_events holds %d rows after a rejected update, want 0", got)
	}
}

func TestStatementsRejectedForReads(t *testing.T) {
	spec := writableSpec()
	spec.Queries.List = crispv1alpha1.Query{Statements: []string{"SELECT 1", "SELECT 2"}}

	_, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "only supported for writes") {
		t.Fatalf("New() error = %v, want a rejection of statements on a read", err)
	}
}

func TestStatementsAndSQLAreExclusive(t *testing.T) {
	spec := writableSpec()
	spec.Queries.Create = &crispv1alpha1.Query{
		SQL:        "INSERT INTO orders DEFAULT VALUES",
		Statements: []string{"INSERT INTO orders DEFAULT VALUES"},
	}

	_, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("New() error = %v, want a rejection of sql alongside statements", err)
	}
}

func TestQueryWithoutAnyStatementIsRejected(t *testing.T) {
	spec := writableSpec()
	spec.Queries.Create = &crispv1alpha1.Query{}

	_, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "is required") {
		t.Fatalf("New() error = %v, want a rejection of an empty query", err)
	}
}

// TestOnlyTheLastStatementMayReturnRows rejects a mistake whose symptom would
// otherwise be silence: rows returned by an earlier statement go nowhere.
func TestOnlyTheLastStatementMayReturnRows(t *testing.T) {
	spec := writableSpec()
	spec.Queries.Create = &crispv1alpha1.Query{
		Statements: []string{
			`INSERT INTO orders (id, tenant, customer, status, total_cents, line_items, updated_at)
			 VALUES (:id, :tenant, :customer, :status, :total_cents, :line_items, '1') RETURNING id`,
			`INSERT INTO order_events (id, tenant, event) VALUES (:id, :tenant, 'created')`,
		},
	}

	_, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "only the last statement may return rows") {
		t.Fatalf("New() error = %v, want a rejection of a returning prelude", err)
	}
}

// TestEveryWriteVerbAcceptsStatements guards the list against a verb being
// added and forgotten: a write that cannot be a transaction is a write whose
// projection has to choose between atomicity and using it at all.
func TestEveryWriteVerbAcceptsStatements(t *testing.T) {
	writes := map[string]func(*crispv1alpha1.Queries, *crispv1alpha1.Query){
		"create":           func(q *crispv1alpha1.Queries, v *crispv1alpha1.Query) { q.Create = v },
		"update":           func(q *crispv1alpha1.Queries, v *crispv1alpha1.Query) { q.Update = v },
		"updateStatus":     func(q *crispv1alpha1.Queries, v *crispv1alpha1.Query) { q.UpdateStatus = v },
		"delete":           func(q *crispv1alpha1.Queries, v *crispv1alpha1.Query) { q.Delete = v },
		"deleteCollection": func(q *crispv1alpha1.Queries, v *crispv1alpha1.Query) { q.DeleteCollection = v },
		"markDeleted":      func(q *crispv1alpha1.Queries, v *crispv1alpha1.Query) { q.MarkDeleted = v },
	}

	for verb, set := range writes {
		t.Run(verb, func(t *testing.T) {
			spec := writableSpec()
			set(&spec.Queries, &crispv1alpha1.Query{Statements: []string{
				`UPDATE orders SET customer = customer WHERE id = :name`,
				`UPDATE orders SET status = status WHERE id = :name`,
			}})

			if _, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil); err != nil &&
				strings.Contains(err.Error(), "only supported for writes") {
				t.Fatalf("%s rejected statements: %v", verb, err)
			}
		})
	}
}
