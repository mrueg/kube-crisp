package projection

import (
	"database/sql"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// softDeleteSpec projects a table that marks rows as going away instead of
// removing them, and keeps a generation counter alongside the version.
func softDeleteSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()

	read := `SELECT id, tenant, customer, status, total_cents, line_items, updated_at, deleted_at, generation
	         FROM orders WHERE tenant = :namespace`
	spec.Queries.List.SQL = read + " ORDER BY id"
	spec.Queries.Get.SQL = read + " AND id = :name"
	spec.Queries.Create = &crispv1alpha1.Query{
		SQL: `INSERT INTO orders (id, tenant, customer, status, total_cents, line_items, updated_at, generation)
		      VALUES (:id, :tenant, :customer, :status, :total_cents, :line_items,
		              CAST((SELECT COALESCE(MAX(CAST(updated_at AS INTEGER)), 0) + 1 FROM orders) AS TEXT), 1)`,
	}
	spec.Queries.Update = &crispv1alpha1.Query{
		// The generation advances with the spec, which is what makes it worth
		// comparing against an observedGeneration.
		SQL: `UPDATE orders
		      SET customer = :customer, status = :status, total_cents = :total_cents,
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT),
		          generation = generation + 1
		      WHERE tenant = :namespace AND id = :name`,
	}
	spec.Queries.Delete = &crispv1alpha1.Query{
		SQL: `UPDATE orders
		      SET deleted_at = '2026-08-21T10:00:00Z',
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
		      WHERE tenant = :namespace AND id = :name AND deleted_at IS NULL`,
	}

	spec.Mapping.DeletionTimestamp = "deleted_at"
	spec.Mapping.Generation = "generation"
	return spec
}

func newSoftDeleteStorage(t *testing.T) (*WritableREST, string) {
	t.Helper()

	path := newTestDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	for _, stmt := range []string{
		`ALTER TABLE orders ADD COLUMN deleted_at TEXT`,
		`ALTER TABLE orders ADD COLUMN generation INTEGER NOT NULL DEFAULT 1`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("extending the table: %v", err)
		}
	}
	_ = db.Close()

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: path, PreparedStatements: true})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", softDeleteSpec(), pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable, path
}

func TestGenerationIsReadFromTheRow(t *testing.T) {
	store, _ := newSoftDeleteStorage(t)
	ctx := namespacedContext("acme")

	obj, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := obj.(*unstructured.Unstructured).GetGeneration(); got != 1 {
		t.Errorf("metadata.generation = %d, want 1", got)
	}
}

// TestGenerationAdvancesWithTheSpec is the property a controller depends on:
// generation moving is how it knows the spec it reconciled is out of date.
func TestGenerationAdvancesWithTheSpec(t *testing.T) {
	store, _ := newSoftDeleteStorage(t)
	ctx := namespacedContext("acme")

	updated := newOrder("order-1001", "grace", 77)
	updated.SetResourceVersion("1")
	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}

	obj, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := obj.(*unstructured.Unstructured).GetGeneration(); got != 2 {
		t.Errorf("metadata.generation = %d after an update, want 2", got)
	}
}

// TestGenerationIsNotClientSettable keeps the counter meaningful: only the
// database moves it.
func TestGenerationIsNotClientSettable(t *testing.T) {
	store, _ := newSoftDeleteStorage(t)
	ctx := namespacedContext("acme")

	updated := newOrder("order-1001", "grace", 77)
	updated.SetResourceVersion("1")
	updated.SetGeneration(4096)

	result, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	if got := result.(*unstructured.Unstructured).GetGeneration(); got != 2 {
		t.Errorf("metadata.generation = %d, want the database's 2 rather than the client's 4096", got)
	}
}

// TestSoftDeleteMarksTheObjectTerminating covers the read side: a row that has
// been marked as going away is served with a deletionTimestamp, which is how
// every client recognises a terminating object.
func TestSoftDeleteMarksTheObjectTerminating(t *testing.T) {
	store, _ := newSoftDeleteStorage(t)
	ctx := namespacedContext("acme")

	before, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if !before.(*unstructured.Unstructured).GetDeletionTimestamp().IsZero() {
		t.Fatal("the object is terminating before anything deleted it")
	}

	if _, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	after, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after the delete returned error: %v", err)
	}
	if after.(*unstructured.Unstructured).GetDeletionTimestamp().IsZero() {
		t.Error("the object carries no deletionTimestamp after a soft delete")
	}

	// It is still listed. A terminating object is visible until it is gone,
	// exactly as a custom resource with a finalizer is.
	list, err := store.List(ctx, nil)
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	var found bool
	for _, item := range list.(*unstructured.UnstructuredList).Items {
		if item.GetName() == "order-1001" {
			found = true
			if item.GetDeletionTimestamp().IsZero() {
				t.Error("the listed object carries no deletionTimestamp")
			}
		}
	}
	if !found {
		t.Error("a terminating object disappeared from the collection")
	}
}

// TestDeletingATerminatingObjectIsNotAnError is the contract clients rely on:
// asking again for something already on its way out succeeds, and does not
// restart the clock.
func TestDeletingATerminatingObjectIsNotAnError(t *testing.T) {
	store, path := newSoftDeleteStorage(t)
	ctx := namespacedContext("acme")

	if _, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("first Delete() returned error: %v", err)
	}
	first, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	version := first.(*unstructured.Unstructured).GetResourceVersion()

	obj, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("second Delete() returned error: %v", err)
	}
	if obj.(*unstructured.Unstructured).GetDeletionTimestamp().IsZero() {
		t.Error("the second delete answered with an object that is not terminating")
	}

	after, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := after.(*unstructured.Unstructured).GetResourceVersion(); got != version {
		t.Errorf("resourceVersion moved from %q to %q, so the second delete wrote to the row", version, got)
	}
	if got := countSoftDeleted(t, path); got != 1 {
		t.Errorf("%d rows are marked deleted, want 1", got)
	}
}

func TestDeletingAMissingObjectIsStillNotFound(t *testing.T) {
	store, _ := newSoftDeleteStorage(t)

	if _, _, err := store.Delete(namespacedContext("acme"), "order-absent", nil, &metav1.DeleteOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Delete() error = %v, want NotFound", err)
	}
}

// TestMappingRejectsAMissingColumn keeps the failure loud: a mapping that names
// a column the query does not return is a configuration mistake, not an object
// with a missing field.
func TestMappingRejectsAMissingColumn(t *testing.T) {
	spec := softDeleteSpec()
	spec.Mapping.Generation = "no_such_column"

	store, _ := newSoftDeleteStorage(t)
	storages, err := New("orders", spec, store.pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	_, err = storages.writable.Get(namespacedContext("acme"), "order-1001", &metav1.GetOptions{})
	if err == nil {
		t.Fatal("a mapping naming an absent column produced an object anyway")
	}
}

func countSoftDeleted(t *testing.T, path string) int {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM orders WHERE deleted_at IS NOT NULL`).Scan(&count); err != nil {
		t.Fatalf("counting: %v", err)
	}
	return count
}

// TestDestroyStopsPollingAndDisconnectsWatchers is what the teardown in the
// router exists to reach.
//
// A projection that is replaced or deleted has its storage destroyed after the
// rebuild routes past it. If that did not stop the poll, the retired storage
// would keep querying its table on a timer forever, with nobody reading the
// result and nothing reporting it — the query load of a projection that no
// longer exists.
func TestDestroyStopsPollingAndDisconnectsWatchers(t *testing.T) {
	store := newTestREST(t)
	ctx := namespacedContext("acme")

	watcher, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	if got := store.watch.group.size(); got != 1 {
		t.Fatalf("%d caches are being polled, want 1 once a watcher has arrived", got)
	}

	store.Destroy()

	if got := store.watch.group.size(); got != 0 {
		t.Errorf("%d caches are still being polled after the storage was destroyed", got)
	}

	// The watcher's channel closes, so a client is told the stream ended rather
	// than being left holding one that will never produce another event.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for range watcher.ResultChan() {
		}
	}()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Error("the watcher was left connected to destroyed storage")
	}
}

// TestDestroyIsSafeWithoutWatchers: most projections are never watched, and a
// rebuild retires them the same way.
func TestDestroyIsSafeWithoutWatchers(t *testing.T) {
	store := newTestREST(t)

	store.Destroy()
	// Twice, since a resource can appear in more than one retired set when a
	// projection is replaced and then removed.
	store.Destroy()
}
