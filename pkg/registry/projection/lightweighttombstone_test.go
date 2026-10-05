package projection

import (
	"database/sql"
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// tombstoneStorage builds a watched projection whose deletions are read from a
// tombstone table by deletedSQL, and hands back a function that deletes a row
// the way a tombstone-keeping writer does.
func tombstoneStorage(t *testing.T, deletedSQL string) (*WritableREST, func(id, tenant string)) {
	t.Helper()

	path := newTestDB(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE order_tombstones (
		id TEXT NOT NULL,
		tenant TEXT NOT NULL,
		customer TEXT NOT NULL,
		status TEXT NOT NULL,
		total_cents INTEGER NOT NULL,
		line_items TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		deleted_at TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("creating the tombstone table: %v", err)
	}

	spec := incrementalSpec()
	spec.Watch.FullResyncInterval = &metav1.Duration{Duration: time.Hour}
	spec.Watch.DeletedQuery = &crispv1alpha1.Query{SQL: deletedSQL}

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: path, PreparedStatements: true})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	remove := func(id, tenant string) {
		t.Helper()
		if _, err := db.Exec(`INSERT INTO order_tombstones
			SELECT id, tenant, customer, status, total_cents, line_items, updated_at, '100'
			FROM orders WHERE id = ? AND tenant = ?`, id, tenant); err != nil {
			t.Fatalf("writing the tombstone: %v", err)
		}
		if _, err := db.Exec(`DELETE FROM orders WHERE id = ? AND tenant = ?`, id, tenant); err != nil {
			t.Fatalf("deleting the row: %v", err)
		}
	}
	return storages.writable, remove
}

// nextDeletion waits for a Deleted event and returns the object it carries.
func nextDeletion(t *testing.T, w watch.Interface) *unstructured.Unstructured {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			if event.Type == watch.Deleted {
				return event.Object.(*unstructured.Unstructured)
			}
		case <-deadline:
			t.Fatal("no deletion arrived")
			return nil
		}
	}
}

// TestAnIdentityOnlyTombstoneKeepsTheCacheWhole covers a projection whose
// deletedQuery returns only the identity columns.
//
// That is the documented minimum, and the cache is meant to keep whole objects
// for it, since nothing else can describe a deleted row. It was made lightweight
// on the mere presence of a deletedQuery, so it kept only keys and versions and
// a deletion carried a trimmed entry: no spec, no status, nothing for a field
// selector to match.
func TestAnIdentityOnlyTombstoneKeepsTheCacheWhole(t *testing.T) {
	store, remove := tombstoneStorage(t, `SELECT id, tenant FROM order_tombstones
		WHERE (:since IS NULL OR CAST(deleted_at AS INTEGER) > CAST(:since AS INTEGER))`)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	nextEvent(t, w)
	nextEvent(t, w)

	remove("order-1002", "acme")
	gone := nextDeletion(t, w)
	if gone.GetName() != "order-1002" {
		t.Fatalf("deleted %q, want order-1002", gone.GetName())
	}
	if customer, _, _ := unstructured.NestedString(gone.Object, "spec", "customer"); customer != "grace" {
		t.Errorf("the deletion carried spec.customer = %q, want grace; with a tombstone that "+
			"cannot describe the row the cache has to", customer)
	}
}

// TestATombstoneThatMapsInFullLightensTheCache is the other half: once a
// tombstone has been seen to carry every mapped column, the cache gives up the
// objects it no longer needs, and a deletion is described by the tombstone.
func TestATombstoneThatMapsInFullLightensTheCache(t *testing.T) {
	store, remove := tombstoneStorage(t, `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		FROM order_tombstones
		WHERE (:since IS NULL OR CAST(deleted_at AS INTEGER) > CAST(:since AS INTEGER))`)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	nextEvent(t, w)
	nextEvent(t, w)

	remove("order-1002", "acme")
	gone := nextDeletion(t, w)
	if customer, _, _ := unstructured.NestedString(gone.Object, "spec", "customer"); customer != "grace" {
		t.Errorf("the deletion carried spec.customer = %q, want grace from the tombstone", customer)
	}

	store.watch.mu.Lock()
	held := store.watch.items["acme/order-1001"]
	lightweight := store.watch.lightweight
	store.watch.mu.Unlock()
	if !lightweight {
		t.Fatal("a tombstone carrying every mapped column was seen and the cache stayed whole")
	}
	if _, found, _ := unstructured.NestedString(held.Object, "spec", "customer"); found {
		t.Error("the cache went lightweight and still holds the body of a row it had before")
	}
}
