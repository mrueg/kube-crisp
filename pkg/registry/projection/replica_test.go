package projection

import (
	"database/sql"
	"path/filepath"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/component-base/metrics/testutil"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// twoDatabases builds a projection over a primary and a stand-in replica.
//
// The two are separate databases that replicate nothing between them, which is
// a cruder version of replication lag than any real replica has — and exactly
// what makes it unambiguous which one answered a read.
func twoDatabases(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) (*WritableREST, string, string) {
	t.Helper()

	open := func(path string) *crispsql.Pool {
		pool, err := crispsql.Open(crispsql.PoolOptions{
			Driver: "sqlite", DSN: path, PreparedStatements: true, Name: path,
		})
		if err != nil {
			t.Fatalf("opening pool: %v", err)
		}
		t.Cleanup(func() { _ = pool.Close() })
		return pool
	}

	primaryPath, replicaPath := newTestDB(t), newTestDB(t)
	storages, err := New("orders", spec, open(primaryPath), open(replicaPath), nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	if storages.writable == nil {
		t.Fatalf("expected a writable projection, got %T", storages.Resource)
	}
	return storages.writable, primaryPath, replicaPath
}

// setCustomer changes a row behind the projection's back.
func setCustomer(t *testing.T, path, id, customer string) {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE orders SET customer = ? WHERE id = ?`, customer, id); err != nil {
		t.Fatalf("updating %s: %v", path, err)
	}
}

// customerIn reads spec.customer out of a returned object.
func customerIn(t *testing.T, obj any) string {
	t.Helper()

	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("expected an unstructured object, got %T", obj)
	}
	value, _, _ := unstructured.NestedString(u.Object, "spec", "customer")
	return value
}

// TestReadsGoToTheReplica: reads are almost all of what a projected kind does,
// and sending them to a replica is the whole point of naming one.
func TestReadsGoToTheReplica(t *testing.T) {
	store, primary, replica := twoDatabases(t, writableSpec())
	ctx := namespacedContext("acme")

	setCustomer(t, primary, "order-1001", "primary-says")
	setCustomer(t, replica, "order-1001", "replica-says")

	got, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if customer := customerIn(t, got); customer != "replica-says" {
		t.Errorf("Get() read %q, want replica-says; the read did not go to the replica", customer)
	}

	list, err := store.List(ctx, nil)
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	for _, item := range list.(*unstructured.UnstructuredList).Items {
		if item.GetName() != "order-1001" {
			continue
		}
		if customer := customerIn(t, &item); customer != "replica-says" {
			t.Errorf("List() read %q, want replica-says", customer)
		}
	}
}

// TestWritesAndTheirBaseGoToThePrimary is the half that cannot tolerate lag.
//
// A resourceVersion checked against a lagging replica is checked against a
// version the row may already have left behind, and the untouched half of a
// merged object would be written back from state the primary has moved past. So
// the write, and the read it is based on, both go to the primary.
func TestWritesAndTheirBaseGoToThePrimary(t *testing.T) {
	store, primary, replica := twoDatabases(t, writableSpec())
	ctx := namespacedContext("acme")

	setCustomer(t, primary, "order-1001", "primary-says")
	setCustomer(t, replica, "order-1001", "replica-says")

	// The write's base is read fresh, which means the primary.
	base, err := store.read(ctx, "order-1001", fresh, "")
	if err != nil {
		t.Fatalf("reading the write base: %v", err)
	}
	if customer := customerIn(t, base); customer != "primary-says" {
		t.Errorf("the write base read %q, want primary-says", customer)
	}

	// And the write itself lands on the primary.
	updated := newOrder("order-1001", "written", 4242)
	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}

	db, err := sql.Open("sqlite", primary)
	if err != nil {
		t.Fatalf("opening the primary: %v", err)
	}
	defer db.Close()

	var customer string
	if err := db.QueryRow(`SELECT customer FROM orders WHERE id = 'order-1001'`).Scan(&customer); err != nil {
		t.Fatalf("reading the primary back: %v", err)
	}
	if customer != "written" {
		t.Errorf("the primary holds %q after the write, want written", customer)
	}

	// The replica is untouched: nothing writes there.
	replicaDB, err := sql.Open("sqlite", replica)
	if err != nil {
		t.Fatalf("opening the replica: %v", err)
	}
	defer replicaDB.Close()
	if err := replicaDB.QueryRow(`SELECT customer FROM orders WHERE id = 'order-1001'`).Scan(&customer); err != nil {
		t.Fatalf("reading the replica back: %v", err)
	}
	if customer != "replica-says" {
		t.Errorf("the replica holds %q; a write reached it", customer)
	}
}

// TestWithoutAReplicaEverythingUsesThePrimary keeps the ordinary case honest.
func TestWithoutAReplicaEverythingUsesThePrimary(t *testing.T) {
	store := newWritableREST(t)

	if got := store.poolFor(shared); got != store.pool {
		t.Error("a read went somewhere other than the primary with no replica configured")
	}
	if got := store.poolFor(fresh); got != store.pool {
		t.Error("a fresh read went somewhere other than the primary")
	}
}

// openPool is twoDatabases' pool constructor, for tests that need the two
// databases to differ in more than their contents.
func openPool(t *testing.T, opts crispsql.PoolOptions) *crispsql.Pool {
	t.Helper()

	pool, err := crispsql.Open(opts)
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// TestReadsFallBackToThePrimaryWhenTheReplicaIsUnreachable: a replica exists to
// spare the primary load, so a replica that has gone away should cost latency,
// not availability. Without the fallback every read 503s while a perfectly
// healthy primary sits idle.
func TestReadsFallBackToThePrimaryWhenTheReplicaIsUnreachable(t *testing.T) {
	crispmetrics.ReplicaFallbacks.Reset()

	primary := openPool(t, crispsql.PoolOptions{
		Driver: "sqlite", DSN: newTestDB(t), PreparedStatements: true, Name: "primary",
	})
	// Nothing is listening on this port, so the read fails while connecting —
	// before any statement is parsed, which is why the driver difference does
	// not matter here.
	replica := openPool(t, crispsql.PoolOptions{
		Driver: "postgres", DSN: "postgres://u:p@127.0.0.1:1/none?connect_timeout=1", Name: "replica",
	})

	storages, err := New("orders", testSpec(), primary, replica, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	store, ok := storages.read, storages.read != nil
	if !ok {
		t.Fatalf("expected a read-only projection, got %T", storages.Resource)
	}

	list, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() failed with an unreachable replica and a healthy primary: %v", err)
	}
	if got := len(list.(*unstructured.UnstructuredList).Items); got != 2 {
		t.Errorf("the primary answered with %d objects, want the fixture's 2", got)
	}

	fallbacks, err := testutil.GetCounterMetricValue(
		crispmetrics.ReplicaFallbacks.WithLabelValues("orders", "orders.store.example.com"))
	if err != nil {
		t.Fatalf("reading the fallback counter: %v", err)
	}
	if fallbacks != 1 {
		t.Errorf("the fallback was counted %v times, want 1", fallbacks)
	}

	// And the replica is left alone for a moment, so an outage costs one failed
	// read per interval rather than one per request.
	if got := store.poolFor(shared); got != primary {
		t.Errorf("the next read was routed to %s, want the primary while the replica is in cooldown", got.Name())
	}
	store.replicaDownUntil.Store(0)
	if got := store.poolFor(shared); got != replica {
		t.Errorf("the replica was not tried again after the cooldown, got %s", got.Name())
	}
}

// TestAReplicaThatRejectsAStatementDoesNotFallBack draws the boundary. Only
// reachability falls back: a statement the replica refused would be refused by
// the primary too, and retrying it there would double the load and still fail.
func TestAReplicaThatRejectsAStatementDoesNotFallBack(t *testing.T) {
	crispmetrics.ReplicaFallbacks.Reset()

	primary := openPool(t, crispsql.PoolOptions{
		Driver: "sqlite", DSN: newTestDB(t), PreparedStatements: true, Name: "primary",
	})
	// Reachable, and empty: the table the statement names is not there.
	replica := openPool(t, crispsql.PoolOptions{
		Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "empty.db"), PreparedStatements: true, Name: "replica",
	})

	storages, err := New("orders", testSpec(), primary, replica, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	store := storages.read

	if _, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{}); err == nil {
		t.Fatal("a statement the replica rejected was quietly retried on the primary")
	}

	fallbacks, err := testutil.GetCounterMetricValue(
		crispmetrics.ReplicaFallbacks.WithLabelValues("orders", "orders.store.example.com"))
	if err != nil {
		t.Fatalf("reading the fallback counter: %v", err)
	}
	if fallbacks != 0 {
		t.Errorf("the fallback fired %v times for a rejected statement, want 0", fallbacks)
	}
	if store.replicaDownUntil.Load() != 0 {
		t.Error("a rejected statement put the replica into cooldown")
	}
}
