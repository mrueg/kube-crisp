package projection

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/watch"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/audit"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/component-base/metrics/testutil"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// newTestDB creates a SQLite database holding the demo orders table.
func newTestDB(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "orders.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE orders (
			id TEXT PRIMARY KEY,
			tenant TEXT NOT NULL,
			customer TEXT NOT NULL,
			status TEXT NOT NULL,
			total_cents INTEGER NOT NULL,
			line_items TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`INSERT INTO orders VALUES ('order-1001','acme','ada','shipped',4999,'[{"sku":"widget","qty":2}]','1')`,
		`INSERT INTO orders VALUES ('order-1002','acme','grace','pending',1250,'[]','2')`,
		`INSERT INTO orders VALUES ('order-1003','globex','alan','pending',9900,'[]','3')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seeding database: %v", err)
		}
	}
	return path
}

func testSpec() crispv1alpha1.CustomResourceProjectionSpec {
	return crispv1alpha1.CustomResourceProjectionSpec{
		DataSource: crispv1alpha1.DataSource{Driver: "sqlite"},
		Resource: crispv1alpha1.ProjectedResource{
			Group:   "store.example.com",
			Version: "v1alpha1",
			Kind:    "Order",
			Plural:  "orders",
			Scope:   crispv1alpha1.NamespaceScoped,
		},
		Queries: crispv1alpha1.Queries{
			List: crispv1alpha1.Query{
				SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
				      FROM orders WHERE tenant = :namespace ORDER BY id`,
			},
			Get: &crispv1alpha1.Query{
				SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
				      FROM orders WHERE tenant = :namespace AND id = :name`,
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
				{Column: "line_items", Path: "spec.lineItems", Type: crispv1alpha1.FieldTypeJSON},
				{Column: "status", Path: "status.phase"},
			},
		},
	}
}

// writableSpec adds the write queries to the read-only fixture.
func writableSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.Queries.Create = &crispv1alpha1.Query{
		// The database assigns the version, exactly as the update below does.
		// A client-supplied version would not be monotonic, and incremental
		// watch polling reads strictly forward from the highest one seen.
		SQL: `INSERT INTO orders (id, tenant, customer, status, total_cents, line_items, updated_at)
		      VALUES (:id, :tenant, :customer, :status, :total_cents, :line_items,
		              CAST((SELECT COALESCE(MAX(CAST(updated_at AS INTEGER)), 0) + 1 FROM orders) AS TEXT))`,
	}
	spec.Queries.Update = &crispv1alpha1.Query{
		// The recommended shape: the database assigns the new version, and the
		// client's resourceVersion is only a precondition in the WHERE clause.
		// updated_at is the mapped resourceVersion, so every write has to move
		// it, which is also what makes a change visible to watchers.
		SQL: `UPDATE orders
		      SET customer = :customer, status = :status, total_cents = :total_cents,
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
		      WHERE tenant = :namespace AND id = :name
		        AND (:resourceVersion IS NULL OR updated_at = :resourceVersion)`,
	}
	spec.Queries.Delete = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace AND id = :name`,
	}
	return spec
}

func newStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) rest.Storage {
	t.Helper()

	storage, _ := newStorageWithDB(t, spec)
	return storage
}

// newStorageWithDB also hands back the database path, for tests that need to
// write to it behind the server's back.
func newStorageWithDB(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) (rest.Storage, string) {
	t.Helper()

	path := newTestDB(t)
	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             "sqlite",
		DSN:                path,
		PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	// The implementation, not the composed type Storages.Resource holds. What
	// that one advertises is the subject of verbs_test.go; every other test
	// here is about behaviour, and calls methods a projection with a narrower
	// set of queries would not offer.
	if storages.writable != nil {
		return storages.writable, path
	}
	return storages.read, path
}

// newTestPoolFor opens a pool against a fresh database for one spec.
func newTestPoolFor(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) *crispsql.Pool {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             spec.DataSource.Driver,
		DSN:                newTestDB(t),
		PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func newTestREST(t *testing.T) *REST {
	t.Helper()

	store := newStorage(t, testSpec())
	r, ok := store.(*REST)
	if !ok {
		t.Fatalf("read-only projection produced %T, want *REST", store)
	}
	return r
}

func newWritableREST(t *testing.T) *WritableREST {
	t.Helper()

	store := newStorage(t, writableSpec())
	w, ok := store.(*WritableREST)
	if !ok {
		t.Fatalf("writable projection produced %T, want *WritableREST", store)
	}
	return w
}

func namespacedContext(namespace string) context.Context {
	return genericapirequest.WithNamespace(genericapirequest.NewContext(), namespace)
}

func TestRESTListScopesRowsToNamespace(t *testing.T) {
	store := newTestREST(t)

	obj, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	list, ok := obj.(*unstructured.UnstructuredList)
	if !ok {
		t.Fatalf("List() returned %T, want *unstructured.UnstructuredList", obj)
	}
	if got, want := len(list.Items), 2; got != want {
		t.Fatalf("List() returned %d items, want %d", got, want)
	}
	if got, want := list.Items[0].GetName(), "order-1001"; got != want {
		t.Errorf("first item = %q, want %q", got, want)
	}
	if got, want := list.GetKind(), "OrderList"; got != want {
		t.Errorf("list kind = %q, want %q", got, want)
	}

	// The globex row belongs to another namespace and must not appear.
	for _, item := range list.Items {
		if item.GetNamespace() != "acme" {
			t.Errorf("item %s leaked from namespace %s", item.GetName(), item.GetNamespace())
		}
	}
}

func TestRESTGet(t *testing.T) {
	store := newTestREST(t)

	obj, err := store.Get(namespacedContext("acme"), "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	order := obj.(*unstructured.Unstructured)
	if got, want := order.GetAPIVersion(), "store.example.com/v1alpha1"; got != want {
		t.Errorf("apiVersion = %q, want %q", got, want)
	}

	customer, found, err := unstructured.NestedString(order.Object, "spec", "customer")
	if err != nil || !found {
		t.Fatalf("spec.customer not found: %v", err)
	}
	if customer != "ada" {
		t.Errorf("spec.customer = %q, want %q", customer, "ada")
	}

	total, found, err := unstructured.NestedInt64(order.Object, "spec", "totalCents")
	if err != nil || !found {
		t.Fatalf("spec.totalCents not found: %v", err)
	}
	if total != 4999 {
		t.Errorf("spec.totalCents = %d, want 4999", total)
	}
}

func TestRESTGetAcrossNamespacesIsNotFound(t *testing.T) {
	store := newTestREST(t)

	// order-1003 exists, but in the globex tenant.
	_, err := store.Get(namespacedContext("acme"), "order-1003", &metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Fatalf("Get() error = %v, want NotFound", err)
	}
}

func TestRESTLabelSelectorFiltersResults(t *testing.T) {
	store := newTestREST(t)

	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{"store.example.com/status": "pending"},
	})
	if err != nil {
		t.Fatalf("building selector: %v", err)
	}

	obj, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{LabelSelector: selector})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	list := obj.(*unstructured.UnstructuredList)
	if got, want := len(list.Items), 1; got != want {
		t.Fatalf("List() returned %d items, want %d", got, want)
	}
	if got, want := list.Items[0].GetName(), "order-1002"; got != want {
		t.Errorf("item = %q, want %q", got, want)
	}
}

func newOrder(name, customer string, total int64) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"customer":   customer,
			"totalCents": total,
			"lineItems":  []any{map[string]any{"sku": "widget", "qty": int64(1)}},
		},
		"status": map[string]any{"phase": "pending"},
	}}
	obj.SetName(name)
	obj.SetNamespace("acme")
	obj.SetResourceVersion("1")
	return obj
}

func TestWritableRESTCreate(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	created, err := store.Create(ctx, newOrder("order-2001", "hopper", 777), nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	obj := created.(*unstructured.Unstructured)
	if got, want := obj.GetName(), "order-2001"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	customer, _, _ := unstructured.NestedString(obj.Object, "spec", "customer")
	if customer != "hopper" {
		t.Errorf("spec.customer = %q, want %q", customer, "hopper")
	}

	// The row must really be in the database, not just echoed back.
	fetched, err := store.Get(ctx, "order-2001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after create: %v", err)
	}
	total, _, _ := unstructured.NestedInt64(fetched.(*unstructured.Unstructured).Object, "spec", "totalCents")
	if total != 777 {
		t.Errorf("spec.totalCents = %d, want 777", total)
	}
}

func TestWritableRESTCreateDuplicateIsAlreadyExists(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	_, err := store.Create(ctx, newOrder("order-1001", "ada", 1), nil, &metav1.CreateOptions{})
	if !errors.IsAlreadyExists(err) {
		t.Fatalf("Create() error = %v, want AlreadyExists", err)
	}
}

func TestWritableRESTCreateRejectsForeignNamespace(t *testing.T) {
	store := newWritableREST(t)

	obj := newOrder("order-3001", "ada", 1)
	obj.SetNamespace("globex")

	_, err := store.Create(namespacedContext("acme"), obj, nil, &metav1.CreateOptions{})
	if !errors.IsBadRequest(err) {
		t.Fatalf("Create() error = %v, want BadRequest", err)
	}
}

func TestWritableRESTUpdate(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	updated := newOrder("order-1001", "grace", 8888)
	result, created, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	if created {
		t.Error("Update() reported the object as created; projections never create on update")
	}

	customer, _, _ := unstructured.NestedString(result.(*unstructured.Unstructured).Object, "spec", "customer")
	if customer != "grace" {
		t.Errorf("spec.customer = %q, want %q", customer, "grace")
	}
}

func TestWritableRESTUpdateMissingIsNotFound(t *testing.T) {
	store := newWritableREST(t)

	_, _, err := store.Update(namespacedContext("acme"), "order-9999",
		rest.DefaultUpdatedObjectInfo(newOrder("order-9999", "ada", 1)), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsNotFound(err) {
		t.Fatalf("Update() error = %v, want NotFound", err)
	}
}

func TestWritableRESTDelete(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	deleted, immediate, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}
	if !immediate {
		t.Error("Delete() should report immediate deletion")
	}
	if got, want := deleted.(*unstructured.Unstructured).GetName(), "order-1001"; got != want {
		t.Errorf("deleted object = %q, want %q", got, want)
	}

	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Get() after delete: error = %v, want NotFound", err)
	}
}

func TestWritableRESTDeleteMissingIsNotFound(t *testing.T) {
	store := newWritableREST(t)

	_, _, err := store.Delete(namespacedContext("acme"), "order-9999", nil, &metav1.DeleteOptions{})
	if !errors.IsNotFound(err) {
		t.Fatalf("Delete() error = %v, want NotFound", err)
	}
}

// TestReadOnlyProjectionRejectsWrites documents the contract: a projection
// without write queries is served by read-only storage, so the endpoint
// installer never advertises the write verbs at all.
func TestReadOnlyProjectionRejectsWrites(t *testing.T) {
	store := newStorage(t, testSpec())

	if _, ok := store.(rest.Creater); ok {
		t.Error("read-only projection implements rest.Creater")
	}
	if _, ok := store.(rest.GracefulDeleter); ok {
		t.Error("read-only projection implements rest.GracefulDeleter")
	}
}

// TestPartiallyWritableProjectionRejectsMissingVerb covers a projection that
// defines only some write queries.
func TestPartiallyWritableProjectionRejectsMissingVerb(t *testing.T) {
	spec := testSpec()
	spec.Queries.Create = &crispv1alpha1.Query{
		SQL: `INSERT INTO orders (id, tenant, customer, status, total_cents, line_items, updated_at)
		      VALUES (:id, :tenant, :customer, :status, :total_cents, :line_items, :updated_at)`,
	}

	store := newStorage(t, spec).(*WritableREST)

	_, _, err := store.Delete(namespacedContext("acme"), "order-1001", nil, &metav1.DeleteOptions{})
	if !errors.IsMethodNotSupported(err) {
		t.Fatalf("Delete() error = %v, want MethodNotSupported", err)
	}
}

// TestListPaginationUsesContinueTokens checks that a limited list reports more
// pages rather than silently truncating the collection.
func TestListPaginationUsesContinueTokens(t *testing.T) {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders WHERE tenant = :namespace ORDER BY id
		      LIMIT COALESCE(:limit, 1000) OFFSET COALESCE(:offset, 0)`,
	}

	store := newStorage(t, spec).(*REST)
	ctx := namespacedContext("acme")

	first, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	firstList := first.(*unstructured.UnstructuredList)
	if got, want := len(firstList.Items), 1; got != want {
		t.Fatalf("first page held %d items, want %d", got, want)
	}
	if firstList.GetContinue() == "" {
		t.Fatal("first page has no continue token, so the client would stop early")
	}

	second, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 1, Continue: firstList.GetContinue()})
	if err != nil {
		t.Fatalf("List() second page returned error: %v", err)
	}
	secondList := second.(*unstructured.UnstructuredList)
	if got, want := len(secondList.Items), 1; got != want {
		t.Fatalf("second page held %d items, want %d", got, want)
	}
	if firstList.Items[0].GetName() == secondList.Items[0].GetName() {
		t.Error("second page repeated the first page's object")
	}
	if secondList.GetContinue() != "" {
		t.Error("second page should be the last page")
	}
}

// TestListWithoutOffsetIgnoresLimit is the other half of the contract: a
// projection whose query cannot skip rows must return everything rather than
// truncate silently.
func TestListWithoutOffsetIgnoresLimit(t *testing.T) {
	store := newTestREST(t)

	list, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := list.(*unstructured.UnstructuredList)
	if got, want := len(items.Items), 2; got != want {
		t.Fatalf("List() returned %d items, want all %d", got, want)
	}
	if items.GetContinue() != "" {
		t.Error("a non-pageable projection must not hand out continue tokens")
	}
}

// TestListJSONAggregation covers the json_agg path, where the database
// assembles the documents and the server decodes one value instead of scanning
// every column of every row.
func TestListJSONAggregation(t *testing.T) {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		ResultFormat: crispv1alpha1.ResultFormatJSONArray,
		SQL: `SELECT json_group_array(json_object(
		          'id', id, 'tenant', tenant, 'customer', customer, 'status', status,
		          'total_cents', total_cents, 'line_items', json(line_items), 'updated_at', updated_at))
		      FROM (SELECT * FROM orders WHERE tenant = :namespace ORDER BY id)`,
	}

	store := newStorage(t, spec).(*REST)

	list, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	items := list.(*unstructured.UnstructuredList)
	if got, want := len(items.Items), 2; got != want {
		t.Fatalf("List() returned %d items, want %d", got, want)
	}
	if got, want := items.Items[0].GetName(), "order-1001"; got != want {
		t.Errorf("first item = %q, want %q", got, want)
	}

	// Values must be mapped exactly as they are on the row-scanning path,
	// including nested JSON columns and integer typing.
	total, found, err := unstructured.NestedInt64(items.Items[0].Object, "spec", "totalCents")
	if err != nil || !found {
		t.Fatalf("spec.totalCents missing: %v", err)
	}
	if total != 4999 {
		t.Errorf("spec.totalCents = %d, want 4999", total)
	}

	lineItems, found, err := unstructured.NestedSlice(items.Items[0].Object, "spec", "lineItems")
	if err != nil || !found {
		t.Fatalf("spec.lineItems missing: %v", err)
	}
	if len(lineItems) != 1 {
		t.Fatalf("spec.lineItems held %d entries, want 1", len(lineItems))
	}
	if qty := lineItems[0].(map[string]any)["qty"]; qty != int64(2) {
		t.Errorf("lineItems[0].qty = %#v, want int64(2)", qty)
	}
}

// watchableSpec makes the fixture watchable: the list query must accept a NULL
// namespace so one poll covers every namespace, and the interval is short so
// tests do not wait on the default.
func watchableSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders WHERE (:namespace IS NULL OR tenant = :namespace) ORDER BY id`,
	}
	spec.Watch = &crispv1alpha1.WatchSpec{
		PollInterval: &metav1.Duration{Duration: 25 * time.Millisecond},
	}
	return spec
}

// nextEvent waits for one event, failing the test rather than hanging.
func nextEvent(t *testing.T, w watch.Interface) watch.Event {
	t.Helper()

	select {
	case event, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("watch channel closed unexpectedly")
		}
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a watch event")
		return watch.Event{}
	}
}

func TestWatchDeliversInitialState(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	// A watch without a resource version replays the current contents, which
	// is what lets an informer build a complete store.
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		event := nextEvent(t, w)
		if event.Type != watch.Added {
			t.Fatalf("event %d had type %q, want %q", i, event.Type, watch.Added)
		}
		seen[event.Object.(*unstructured.Unstructured).GetName()] = true
	}
	if !seen["order-1001"] || !seen["order-1002"] {
		t.Errorf("initial events covered %v, want both acme orders", seen)
	}
}

func TestWatchObservesCreateUpdateDelete(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	// Drain the initial state.
	nextEvent(t, w)
	nextEvent(t, w)

	if _, err := store.Create(ctx, newOrder("order-4001", "hopper", 100), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	added := nextEvent(t, w)
	if added.Type != watch.Added {
		t.Fatalf("event type = %q, want %q", added.Type, watch.Added)
	}
	if got, want := added.Object.(*unstructured.Unstructured).GetName(), "order-4001"; got != want {
		t.Errorf("added object = %q, want %q", got, want)
	}

	// A real client sends back the version it read; the database moves it.
	updated := newOrder("order-4001", "margaret", 200)
	updated.SetResourceVersion(added.Object.(*unstructured.Unstructured).GetResourceVersion())
	if _, _, err := store.Update(ctx, "order-4001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	modified := nextEvent(t, w)
	if modified.Type != watch.Modified {
		t.Fatalf("event type = %q, want %q", modified.Type, watch.Modified)
	}
	customer, _, _ := unstructured.NestedString(modified.Object.(*unstructured.Unstructured).Object, "spec", "customer")
	if customer != "margaret" {
		t.Errorf("modified spec.customer = %q, want %q", customer, "margaret")
	}

	if _, _, err := store.Delete(ctx, "order-4001", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}
	deleted := nextEvent(t, w)
	if deleted.Type != watch.Deleted {
		t.Fatalf("event type = %q, want %q", deleted.Type, watch.Deleted)
	}
}

// TestWatchFiltersByNamespace checks that a namespaced watch does not leak
// events from other tenants, even though one poll covers every namespace.
func TestWatchFiltersByNamespace(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)

	w, err := store.Watch(namespacedContext("globex"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	// globex holds exactly one seeded row.
	initial := nextEvent(t, w)
	if got, want := initial.Object.(*unstructured.Unstructured).GetName(), "order-1003"; got != want {
		t.Fatalf("initial object = %q, want %q", got, want)
	}

	if _, err := store.Create(namespacedContext("acme"), newOrder("order-5001", "ada", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	select {
	case event := <-w.ResultChan():
		t.Fatalf("globex watcher received an acme event: %+v", event.Object)
	case <-time.After(500 * time.Millisecond):
		// No cross-namespace leakage, which is what we want.
	}
}

// TestWatchResumesFromListResourceVersion covers the informer sequence: LIST,
// then WATCH from the version the list reported, which must not replay the
// collection.
func TestWatchResumesFromListResourceVersion(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	// Start and stop a watch so the cache has a snapshot to report.
	primer, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	nextEvent(t, primer)
	nextEvent(t, primer)

	list, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	version := list.(*unstructured.UnstructuredList).GetResourceVersion()
	if version == "" {
		t.Fatal("List() reported no resource version, so a watch cannot resume from it")
	}

	resumed, err := store.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: version})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer resumed.Stop()
	primer.Stop()

	select {
	case event := <-resumed.ResultChan():
		t.Fatalf("resumed watch replayed the collection: %+v", event.Object)
	case <-time.After(300 * time.Millisecond):
		// Nothing replayed, which is correct.
	}
}

// TestWatchSendsInitialEventsBookmark covers the WatchList sequence a modern
// client-go informer uses: it streams the collection over the watch and waits
// for a bookmark marking the end of the initial set before reporting synced.
func TestWatchSendsInitialEventsBookmark(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)

	sendInitialEvents := true
	w, err := store.Watch(namespacedContext("acme"), &metainternalversion.ListOptions{
		SendInitialEvents:   &sendInitialEvents,
		AllowWatchBookmarks: true,
	})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	var added int
	for {
		event := nextEvent(t, w)
		if event.Type == watch.Added {
			added++
			continue
		}
		if event.Type != watch.Bookmark {
			t.Fatalf("unexpected event type %q", event.Type)
		}

		obj := event.Object.(*unstructured.Unstructured)
		if obj.GetAnnotations()[metav1.InitialEventsAnnotationKey] != "true" {
			t.Errorf("bookmark is missing the %s annotation", metav1.InitialEventsAnnotationKey)
		}
		if obj.GetResourceVersion() == "" {
			t.Error("bookmark carries no resource version")
		}
		break
	}

	if added != 2 {
		t.Errorf("received %d initial events, want 2", added)
	}
}

func TestWritableRESTUpdateRejectsStaleResourceVersion(t *testing.T) {
	store := newWritableREST(t)

	stale := newOrder("order-1001", "grace", 1)
	stale.SetResourceVersion("stale")

	_, _, err := store.Update(namespacedContext("acme"), "order-1001",
		rest.DefaultUpdatedObjectInfo(stale), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsConflict(err) {
		t.Fatalf("Update() error = %v, want Conflict", err)
	}
}

func TestWritableRESTUpdateWithoutResourceVersionIsUnconditional(t *testing.T) {
	store := newWritableREST(t)

	unconditional := newOrder("order-1001", "grace", 1)
	unconditional.SetResourceVersion("")

	if _, _, err := store.Update(namespacedContext("acme"), "order-1001",
		rest.DefaultUpdatedObjectInfo(unconditional), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() without a resourceVersion returned error: %v", err)
	}
}

func TestWritableRESTDeletePreconditions(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	stale := "stale"
	_, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{ResourceVersion: &stale},
	})
	if !errors.IsConflict(err) {
		t.Fatalf("Delete() with a stale precondition: error = %v, want Conflict", err)
	}

	// The object must still be there.
	current, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after the rejected delete: %v", err)
	}

	matching := current.(*unstructured.Unstructured).GetResourceVersion()
	if _, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{ResourceVersion: &matching},
	}); err != nil {
		t.Fatalf("Delete() with a matching precondition returned error: %v", err)
	}
}

func TestListFieldSelector(t *testing.T) {
	store := newTestREST(t)
	ctx := namespacedContext("acme")

	list, err := store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", "order-1002"),
	})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	items := list.(*unstructured.UnstructuredList)
	if got, want := len(items.Items), 1; got != want {
		t.Fatalf("List() returned %d items, want %d", got, want)
	}
	if got, want := items.Items[0].GetName(), "order-1002"; got != want {
		t.Errorf("item = %q, want %q", got, want)
	}
}

// TestListRejectsUnsupportedFieldSelector is the point of the feature: a
// selector kube-crisp cannot honour must fail, not quietly return everything.
func TestListRejectsUnsupportedFieldSelector(t *testing.T) {
	store := newTestREST(t)

	_, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.customer", "ada"),
	})
	if !errors.IsBadRequest(err) {
		t.Fatalf("List() error = %v, want BadRequest", err)
	}
}

func TestPrinterColumns(t *testing.T) {
	spec := testSpec()
	spec.Resource.AdditionalPrinterColumns = []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Customer", Type: "string", JSONPath: ".spec.customer"},
		{Name: "Phase", Type: "string", JSONPath: ".status.phase"},
	}

	store := newStorage(t, spec).(*REST)
	ctx := namespacedContext("acme")

	list, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	table, err := store.ConvertToTable(ctx, list, nil)
	if err != nil {
		t.Fatalf("ConvertToTable() returned error: %v", err)
	}

	var headers []string
	for _, column := range table.ColumnDefinitions {
		headers = append(headers, column.Name)
	}
	want := []string{"Name", "Customer", "Phase"}
	if !reflect.DeepEqual(headers, want) {
		t.Fatalf("columns = %v, want %v", headers, want)
	}

	if len(table.Rows) != 2 {
		t.Fatalf("table held %d rows, want 2", len(table.Rows))
	}
	// Row cells follow the column order: name, then each JSONPath result.
	if got, want := table.Rows[0].Cells[1], "ada"; got != want {
		t.Errorf("customer cell = %v, want %v", got, want)
	}
	if got, want := table.Rows[0].Cells[2], "shipped"; got != want {
		t.Errorf("phase cell = %v, want %v", got, want)
	}
}

// pageableSpec adds keyset paging and a count query to the fixture.
func pageableSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders
		      WHERE tenant = :namespace AND (:after IS NULL OR id > :after)
		      ORDER BY id
		      LIMIT COALESCE(:limit, 1000)`,
	}
	spec.Queries.Count = &crispv1alpha1.Query{
		SQL: `SELECT COUNT(*) FROM orders WHERE tenant = :namespace`,
	}
	return spec
}

func TestKeysetPaginationIsStableAndCounts(t *testing.T) {
	store := newStorage(t, pageableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	first, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	firstPage := first.(*unstructured.UnstructuredList)
	if got, want := len(firstPage.Items), 1; got != want {
		t.Fatalf("first page held %d items, want %d", got, want)
	}
	if firstPage.GetContinue() == "" {
		t.Fatal("first page carries no continue token")
	}
	if remaining := firstPage.GetRemainingItemCount(); remaining == nil || *remaining != 1 {
		t.Errorf("remainingItemCount = %v, want 1", remaining)
	}

	// An object inserted before the second page must not shift the window: a
	// keyset resumes after the last key, not after a row count.
	if _, err := store.Create(ctx, newOrder("order-0001", "early", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	second, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 1, Continue: firstPage.GetContinue()})
	if err != nil {
		t.Fatalf("List() second page returned error: %v", err)
	}
	secondPage := second.(*unstructured.UnstructuredList)
	if got, want := len(secondPage.Items), 1; got != want {
		t.Fatalf("second page held %d items, want %d", got, want)
	}
	if got, want := secondPage.Items[0].GetName(), "order-1002"; got != want {
		t.Errorf("second page held %q, want %q: the insert shifted the window", got, want)
	}
}

func TestDeleteCollection(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	deleted, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{}, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	if got, want := len(deleted.(*unstructured.UnstructuredList).Items), 2; got != want {
		t.Fatalf("DeleteCollection() reported %d objects, want %d", got, want)
	}

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got := len(remaining.(*unstructured.UnstructuredList).Items); got != 0 {
		t.Errorf("%d objects survived the collection delete", got)
	}

	// The other tenant must be untouched.
	globex, err := store.List(namespacedContext("globex"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got, want := len(globex.(*unstructured.UnstructuredList).Items), 1; got != want {
		t.Errorf("the other tenant holds %d objects, want %d", got, want)
	}
}

// TestDeleteCollectionWithSelector checks the fallback: a bulk statement that
// cannot see the selector must not be used, or it would delete more than was
// asked for.
func TestDeleteCollectionWithSelector(t *testing.T) {
	spec := writableSpec()
	spec.Queries.DeleteCollection = &crispv1alpha1.Query{
		SQL: `DELETE FROM orders WHERE tenant = :namespace`,
	}

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{"store.example.com/status": "pending"},
	})
	if err != nil {
		t.Fatalf("building the selector: %v", err)
	}

	deleted, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{},
		&metainternalversion.ListOptions{LabelSelector: selector})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	if got, want := len(deleted.(*unstructured.UnstructuredList).Items), 1; got != want {
		t.Fatalf("DeleteCollection() removed %d objects, want %d", got, want)
	}

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := remaining.(*unstructured.UnstructuredList).Items
	if got, want := len(items), 1; got != want {
		t.Fatalf("%d objects remain, want %d: the selector was ignored", got, want)
	}
	if got, want := items[0].GetName(), "order-1001"; got != want {
		t.Errorf("the surviving object is %q, want %q", got, want)
	}
}

// statusSpec enables the status subresource on the fixture.
func statusSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Resource.Subresources = &crispv1alpha1.ProjectedSubresources{
		Status: &crispv1alpha1.ProjectedStatusSubresource{},
	}
	spec.Queries.UpdateStatus = &crispv1alpha1.Query{
		SQL: `UPDATE orders SET status = :status,
		             updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
		      WHERE tenant = :namespace AND id = :name`,
	}
	return spec
}

func TestStatusSubresourceSplitsSpecAndStatus(t *testing.T) {
	storages, err := New("orders", statusSpec(), newTestPoolFor(t, statusSpec()), nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if storages.Status == nil {
		t.Fatal("enabling the status subresource produced no status storage")
	}

	main := storages.writable
	status := storages.Status.(*StatusREST)
	ctx := namespacedContext("acme")

	// A write to the main resource may not change status.
	spec := newOrder("order-1001", "hopper", 42)
	if err := unstructured.SetNestedField(spec.Object, "cancelled", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	updated, _, err := main.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(spec), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	phase, _, _ := unstructured.NestedString(updated.(*unstructured.Unstructured).Object, "status", "phase")
	if phase != "shipped" {
		t.Errorf("status.phase = %q, want the stored %q: a spec write changed status", phase, "shipped")
	}
	customer, _, _ := unstructured.NestedString(updated.(*unstructured.Unstructured).Object, "spec", "customer")
	if customer != "hopper" {
		t.Errorf("spec.customer = %q, want %q", customer, "hopper")
	}

	// A write to status may not change anything else.
	current, err := main.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	statusWrite := current.(*unstructured.Unstructured).DeepCopy()
	if err := unstructured.SetNestedField(statusWrite.Object, "cancelled", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	if err := unstructured.SetNestedField(statusWrite.Object, "someone-else", "spec", "customer"); err != nil {
		t.Fatalf("setting spec.customer: %v", err)
	}

	result, _, err := status.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(statusWrite), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("status Update() returned error: %v", err)
	}
	phase, _, _ = unstructured.NestedString(result.(*unstructured.Unstructured).Object, "status", "phase")
	if phase != "cancelled" {
		t.Errorf("status.phase = %q, want %q", phase, "cancelled")
	}
	customer, _, _ = unstructured.NestedString(result.(*unstructured.Unstructured).Object, "spec", "customer")
	if customer != "hopper" {
		t.Errorf("spec.customer = %q, want the stored %q: a status write changed spec", customer, "hopper")
	}
}

func TestCreateWithGenerateName(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	obj := newOrder("", "ada", 1)
	obj.SetGenerateName("order-gen-")

	created, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	name := created.(*unstructured.Unstructured).GetName()
	if !strings.HasPrefix(name, "order-gen-") || name == "order-gen-" {
		t.Fatalf("generated name = %q, want a suffixed %q", name, "order-gen-")
	}

	// A second create with the same prefix must not collide.
	other, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("second Create() returned error: %v", err)
	}
	if other.(*unstructured.Unstructured).GetName() == name {
		t.Error("generateName produced the same name twice")
	}
}

func TestCELValidationRules(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Schema = &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"totalCents": {
						Type:   "integer",
						Format: "int64",
						XValidations: apiextensionsv1.ValidationRules{
							{Rule: "self >= 0", Message: "totalCents must not be negative"},
						},
					},
					"customer":  {Type: "string"},
					"lineItems": {Type: "array", Items: &apiextensionsv1.JSONSchemaPropsOrArray{Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr(true)}}},
				},
			},
			"status": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{"phase": {Type: "string"}}},
		},
	}

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	valid := newOrder("order-cel-ok", "ada", 5)
	if _, err := store.Create(ctx, valid, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() with a valid object returned error: %v", err)
	}

	invalid := newOrder("order-cel-bad", "ada", -5)
	_, err := store.Create(ctx, invalid, nil, &metav1.CreateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Create() error = %v, want Invalid from the validation rule", err)
	}
	if !strings.Contains(err.Error(), "totalCents must not be negative") {
		t.Errorf("error %q does not carry the rule's message", err)
	}
}

func ptr[T any](v T) *T { return &v }

// incrementalSpec polls only the rows that moved, with a short full resync so
// the test does not wait a minute to see a deletion.
func incrementalSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := watchableSpec()
	spec.Watch.FullResyncInterval = &metav1.Duration{Duration: 300 * time.Millisecond}
	spec.Watch.Query = &crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders
		      WHERE (:since IS NULL OR CAST(updated_at AS INTEGER) > CAST(:since AS INTEGER))
		      ORDER BY CAST(updated_at AS INTEGER) ASC`,
	}
	return spec
}

// TestIncrementalWatchObservesChanges is the scalability path: most polls read
// only what changed, and the periodic full read is what still catches
// deletions.
func TestIncrementalWatchObservesChanges(t *testing.T) {
	store := newStorage(t, incrementalSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	// Initial state.
	nextEvent(t, w)
	nextEvent(t, w)

	created, err := store.Create(ctx, newOrder("order-6001", "hopper", 100), nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	added := nextEvent(t, w)
	if added.Type != watch.Added {
		t.Fatalf("event type = %q, want %q", added.Type, watch.Added)
	}
	if got, want := added.Object.(*unstructured.Unstructured).GetName(), "order-6001"; got != want {
		t.Fatalf("added object = %q, want %q", got, want)
	}

	updated := newOrder("order-6001", "margaret", 200)
	updated.SetResourceVersion(created.(*unstructured.Unstructured).GetResourceVersion())
	if _, _, err := store.Update(ctx, "order-6001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}
	if event := nextEvent(t, w); event.Type != watch.Modified {
		t.Fatalf("event type = %q, want %q", event.Type, watch.Modified)
	}

	// A deletion is invisible to an incremental read, so this also proves the
	// full resync still runs.
	if _, _, err := store.Delete(ctx, "order-6001", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}
	deleted := nextEvent(t, w)
	if deleted.Type != watch.Deleted {
		t.Fatalf("event type = %q, want %q", deleted.Type, watch.Deleted)
	}
	if got, want := deleted.Object.(*unstructured.Unstructured).GetName(), "order-6001"; got != want {
		t.Errorf("deleted object = %q, want %q", got, want)
	}
}

// TestIncrementalWatchReadsOnlyWhatChanged checks the point of the feature: a
// steady state costs a query that returns nothing, not a full table scan.
func TestIncrementalWatchReadsOnlyWhatChanged(t *testing.T) {
	spec := incrementalSpec()
	// Long enough that no full resync happens during the measurement.
	spec.Watch.FullResyncInterval = &metav1.Duration{Duration: time.Hour}

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	nextEvent(t, w)
	nextEvent(t, w)

	// Several poll intervals with no changes must produce no events.
	select {
	case event := <-w.ResultChan():
		t.Fatalf("an idle projection emitted %s for %v", event.Type, event.Object)
	case <-time.After(300 * time.Millisecond):
	}

	if _, err := store.Create(ctx, newOrder("order-6002", "ada", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	if event := nextEvent(t, w); event.Type != watch.Added {
		t.Fatalf("event type = %q, want %q", event.Type, watch.Added)
	}
}

// TestIncrementalWatchReportsMissedChanges covers what the load-time check
// cannot: another writer inserting a row with a stale version. An incremental
// poll reads forward and can never return it, so the resync has to find it, and
// the server has to say so rather than looking healthy.
func TestIncrementalWatchReportsMissedChanges(t *testing.T) {
	crispmetrics.WatchMissedEvents.Reset()

	spec := incrementalSpec()
	spec.Watch.FullResyncInterval = &metav1.Duration{Duration: 300 * time.Millisecond}

	storage, path := newStorageWithDB(t, spec)
	store := storage.(*WritableREST)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	nextEvent(t, w)
	nextEvent(t, w)

	// A row written behind the server's back, with a version below everything
	// already seen.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening the database directly: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(
		`INSERT INTO orders VALUES ('order-stale', 'acme', 'mallory', 'pending', 1, '[]', '0')`,
	); err != nil {
		t.Fatalf("inserting the stale row: %v", err)
	}

	// The resync still surfaces it, so watchers stay correct.
	added := nextEvent(t, w)
	if added.Type != watch.Added {
		t.Fatalf("event type = %q, want %q", added.Type, watch.Added)
	}
	if got, want := added.Object.(*unstructured.Unstructured).GetName(), "order-stale"; got != want {
		t.Fatalf("added object = %q, want %q", got, want)
	}

	missed, err := testutil.GetCounterMetricValue(
		crispmetrics.WatchMissedEvents.WithLabelValues("orders.store.example.com"))
	if err != nil {
		t.Fatalf("reading the missed-events counter: %v", err)
	}
	if missed < 1 {
		t.Errorf("missed-events counter = %v, want at least 1: a stale write went unreported", missed)
	}
}

// TestWritesAreAudited covers the trail a reviewer asks for: which projection
// answered, against which database, with which statement — and never the values
// bound into it.
func TestWritesAreAudited(t *testing.T) {
	store := newWritableREST(t)
	ctx := audit.WithAuditContext(namespacedContext("acme"))

	if _, err := store.Create(ctx, newOrder("order-audit", "ada", 4242), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	annotations := audit.AuditContextFrom(ctx).GetEventAnnotations()
	for key, want := range map[string]string{
		auditProjection: "orders",
		auditResource:   "orders.store.example.com",
		auditVerb:       "create",
	} {
		if got := annotations[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}

	statement := annotations[auditStatement]
	if !strings.Contains(strings.ToUpper(statement), "INSERT INTO ORDERS") {
		t.Errorf("the audited statement does not look like the insert: %q", statement)
	}
	if strings.Contains(statement, "\n") {
		t.Errorf("the audited statement is not on one line: %q", statement)
	}

	// The values a client submitted are its own data and must not be copied
	// into the audit trail.
	for _, value := range []string{"ada", "4242", "order-audit"} {
		if strings.Contains(statement, value) {
			t.Errorf("the audited statement leaks the bound value %q: %s", value, statement)
		}
	}
	if annotations[auditDataSource] == "" {
		t.Error("no data source was recorded")
	}
}

func TestDeleteIsAudited(t *testing.T) {
	store := newWritableREST(t)
	ctx := audit.WithAuditContext(namespacedContext("acme"))

	if _, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	annotations := audit.AuditContextFrom(ctx).GetEventAnnotations()
	if got, want := annotations[auditVerb], "delete"; got != want {
		t.Errorf("verb = %q, want %q", got, want)
	}
	if got, want := annotations[auditRows], "1"; got != want {
		t.Errorf("rows = %q, want %q", got, want)
	}
}

// TestUnreachableDatabaseIsServiceUnavailable: a database that cannot be
// reached is a 503 the client should retry, not a 500 and not a disappearing
// API. The resource stays installed either way.
func TestUnreachableDatabaseIsServiceUnavailable(t *testing.T) {
	store := newTestREST(t)

	for _, err := range []error{
		&net.OpError{Op: "dial", Net: "tcp", Err: goerrors.New("connection refused")},
		driver.ErrBadConn,
		fmt.Errorf("dial tcp 10.0.0.1:5432: connect: connection refused"),
	} {
		status := store.queryError(err, "listing")
		if !errors.IsServiceUnavailable(status) {
			t.Errorf("queryError(%v) = %v, want ServiceUnavailable", err, status)
		}
	}

	// A statement the database rejected is the projection's fault, and
	// retrying will not help.
	rejected := store.queryError(fmt.Errorf("syntax error at or near \"SELCT\""), "listing")
	if errors.IsServiceUnavailable(rejected) {
		t.Error("a rejected statement was reported as a temporary outage")
	}
	if !errors.IsInternalError(rejected) {
		t.Errorf("a rejected statement returned %v, want InternalError", rejected)
	}
}

func TestUnreachableDatabaseOnWriteIsServiceUnavailable(t *testing.T) {
	gr := schema.GroupResource{Group: "store.example.com", Resource: "orders"}

	status := translateWriteError(
		fmt.Errorf("dial tcp: connection refused"), gr, "order-1", "create")
	if !errors.IsServiceUnavailable(status) {
		t.Errorf("translateWriteError() = %v, want ServiceUnavailable", status)
	}
}

// TestWatchRejectsStaleResourceVersion covers the contract a client relies on:
// a projection keeps no history, so resuming from a version it can no longer
// serve must be refused with 410 rather than answered with a replay the client
// would mistake for a resumption.
func TestWatchRejectsStaleResourceVersion(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	_, err := store.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: "17"})
	if !errors.IsResourceExpired(err) {
		t.Fatalf("Watch() from an unknown version = %v, want ResourceExpired", err)
	}
}

func TestWatchAcceptsTheCurrentResourceVersion(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	// Prime the cache so it has a version to report.
	primer, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	nextEvent(t, primer)
	nextEvent(t, primer)
	defer primer.Stop()

	list, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	version := list.(*unstructured.UnstructuredList).GetResourceVersion()

	resumed, err := store.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: version})
	if err != nil {
		t.Fatalf("Watch() from the current version returned error: %v", err)
	}
	defer resumed.Stop()

	select {
	case event := <-resumed.ResultChan():
		t.Fatalf("a resumed watch replayed %v", event.Object)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestListResourceVersionMatch(t *testing.T) {
	store := newTestREST(t)
	ctx := namespacedContext("acme")

	// Reads go to the database, so "not older than" is always satisfiable.
	if _, err := store.List(ctx, &metainternalversion.ListOptions{
		ResourceVersion:      "1",
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
	}); err != nil {
		t.Fatalf("List() with NotOlderThan returned error: %v", err)
	}

	// An exact past version cannot be reconstructed from a table's current
	// contents, so it is refused rather than answered with something else.
	_, err := store.List(ctx, &metainternalversion.ListOptions{
		ResourceVersion:      "17",
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
	})
	if !errors.IsResourceExpired(err) {
		t.Fatalf("List() with Exact = %v, want ResourceExpired", err)
	}

	_, err = store.List(ctx, &metainternalversion.ListOptions{
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
	})
	if !errors.IsBadRequest(err) {
		t.Fatalf("List() with Exact and no version = %v, want BadRequest", err)
	}

	_, err = store.List(ctx, &metainternalversion.ListOptions{
		ResourceVersionMatch: "Whenever",
	})
	if !errors.IsBadRequest(err) {
		t.Fatalf("List() with an unknown match = %v, want BadRequest", err)
	}
}

// TestCacheIsBypassedForNewerResourceVersion: a cached page that predates the
// version the client insists on must not be served.
func TestCacheIsBypassedForNewerResourceVersion(t *testing.T) {
	spec := watchableSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	if _, err := store.List(ctx, &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("priming the cache: %v", err)
	}

	// A version far ahead of anything cached forces a fresh read rather than a
	// stale hit.
	list, err := store.List(ctx, &metainternalversion.ListOptions{
		ResourceVersion:      "999999",
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
	})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got := len(list.(*unstructured.UnstructuredList).Items); got != 2 {
		t.Fatalf("List() returned %d items, want 2", got)
	}
}

// selectableSpec declares a field beyond name and namespace, with the column
// that holds it so the query can filter in the database.
func selectableSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Resource.SelectableFields = []crispv1alpha1.SelectableField{
		{JSONPath: ".spec.customer", Column: "customer"},
		{JSONPath: ".status.phase"},
	}
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders
		      WHERE tenant = :namespace AND (:customer IS NULL OR customer = :customer)
		      ORDER BY id`,
	}
	return spec
}

func TestSelectableFields(t *testing.T) {
	store := newStorage(t, selectableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	// Pushed into the query through the declared column.
	list, err := store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.customer", "ada"),
	})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := list.(*unstructured.UnstructuredList)
	if got, want := len(items.Items), 1; got != want {
		t.Fatalf("selector returned %d items, want %d", got, want)
	}
	if got, want := items.Items[0].GetName(), "order-1001"; got != want {
		t.Errorf("item = %q, want %q", got, want)
	}

	// Declared without a column: filtered after mapping, same answer.
	list, err = store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("status.phase", "pending"),
	})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items = list.(*unstructured.UnstructuredList)
	if got, want := len(items.Items), 1; got != want {
		t.Fatalf("selector returned %d items, want %d", got, want)
	}
	if got, want := items.Items[0].GetName(), "order-1002"; got != want {
		t.Errorf("item = %q, want %q", got, want)
	}
}

// TestUndeclaredFieldSelectorIsStillRejected keeps the contract: only what a
// projection declares can be selected on.
func TestUndeclaredFieldSelectorIsStillRejected(t *testing.T) {
	store := newStorage(t, selectableSpec()).(*WritableREST)

	_, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.totalCents", "1"),
	})
	if !errors.IsBadRequest(err) {
		t.Fatalf("List() error = %v, want BadRequest", err)
	}
	if !strings.Contains(err.Error(), "spec.customer") {
		t.Errorf("error %q does not list what can be selected on", err)
	}
}

// TestWatchResumesFromHistory is the point of keeping recent changes: a client
// that reconnects is handed what it missed instead of being told to start over.
func TestWatchResumesFromHistory(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	first, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	nextEvent(t, first)
	nextEvent(t, first)

	list, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	resumeFrom := list.(*unstructured.UnstructuredList).GetResourceVersion()

	// The client disconnects, and misses a change.
	first.Stop()
	if _, err := store.Create(ctx, newOrder("order-7001", "hopper", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	// Reconnecting from where it left off must deliver the change, not a
	// replay of the whole collection and not a 410.
	resumed, err := store.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: resumeFrom})
	if err != nil {
		t.Fatalf("Watch() from a recent version returned error: %v", err)
	}
	defer resumed.Stop()

	event := nextEvent(t, resumed)
	if event.Type != watch.Added {
		t.Fatalf("event type = %q, want %q", event.Type, watch.Added)
	}
	if got, want := event.Object.(*unstructured.Unstructured).GetName(), "order-7001"; got != want {
		t.Fatalf("resumed watch delivered %q, want the missed %q", got, want)
	}

	select {
	case extra := <-resumed.ResultChan():
		t.Errorf("the resumed watch also replayed %v", extra.Object)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestWatchWithoutHistoryStillRejectsAResume covers the opt-out: with no ring
// there is nothing to replay, so 410 remains the honest answer.
func TestWatchWithoutHistoryStillRejectsAResume(t *testing.T) {
	spec := watchableSpec()
	spec.Watch.HistorySize = ptr(int32(0))

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	primer, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	nextEvent(t, primer)
	nextEvent(t, primer)

	list, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	version := list.(*unstructured.UnstructuredList).GetResourceVersion()
	primer.Stop()

	if _, err := store.Create(ctx, newOrder("order-7002", "ada", 1), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	if _, err := store.Watch(ctx, &metainternalversion.ListOptions{ResourceVersion: version}); !errors.IsResourceExpired(err) {
		t.Fatalf("Watch() error = %v, want ResourceExpired", err)
	}
}

// schemaWithDefaults describes the fixture kind and gives spec.customer a
// default, so a write that omits it still produces a complete row.
func schemaWithDefaults() *apiextensionsv1.JSONSchemaProps {
	return &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"customer":   {Type: "string", Default: &apiextensionsv1.JSON{Raw: []byte(`"unassigned"`)}},
					"totalCents": {Type: "integer", Format: "int64", Default: &apiextensionsv1.JSON{Raw: []byte(`0`)}},
					"lineItems": {
						Type:    "array",
						Default: &apiextensionsv1.JSON{Raw: []byte(`[]`)},
						Items: &apiextensionsv1.JSONSchemaPropsOrArray{
							Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr(true)},
						},
					},
				},
			},
			"status": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"phase": {Type: "string", Default: &apiextensionsv1.JSON{Raw: []byte(`"pending"`)}},
			}},
		},
	}
}

// TestSchemaDefaultsAreAppliedOnCreate covers the CRD behaviour a projection
// should match: a field the client left out arrives with the schema's default.
func TestSchemaDefaultsAreAppliedOnCreate(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Schema = schemaWithDefaults()

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	// The empty objects are deliberate: defaulting descends into what is
	// present, exactly as it does for a custom resource, so an absent status
	// stays absent rather than being conjured from its children's defaults.
	sparse := &unstructured.Unstructured{Object: map[string]any{
		"spec":   map[string]any{},
		"status": map[string]any{},
	}}
	sparse.SetName("order-default-1")
	sparse.SetNamespace("acme")

	if _, err := store.Create(ctx, sparse, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	// Read it back rather than trusting the response: the point is that the
	// defaulted values reached the database.
	obj, err := store.Get(ctx, "order-default-1", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	got := obj.(*unstructured.Unstructured)
	if customer, _, _ := unstructured.NestedString(got.Object, "spec", "customer"); customer != "unassigned" {
		t.Errorf("spec.customer = %q, want the default %q", customer, "unassigned")
	}
	if phase, _, _ := unstructured.NestedString(got.Object, "status", "phase"); phase != "pending" {
		t.Errorf("status.phase = %q, want the default %q", phase, "pending")
	}
	if total, _, _ := unstructured.NestedInt64(got.Object, "spec", "totalCents"); total != 0 {
		t.Errorf("spec.totalCents = %d, want the default 0", total)
	}
}

// TestSchemaDefaultsDoNotRewriteSuppliedValues guards the obvious mistake of
// defaulting over what the client actually sent.
func TestSchemaDefaultsDoNotRewriteSuppliedValues(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Schema = schemaWithDefaults()

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	created, err := store.Create(ctx, newOrder("order-default-2", "ada", 42), nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	obj := created.(*unstructured.Unstructured)
	if customer, _, _ := unstructured.NestedString(obj.Object, "spec", "customer"); customer != "ada" {
		t.Errorf("spec.customer = %q, want the supplied %q", customer, "ada")
	}
	if total, _, _ := unstructured.NestedInt64(obj.Object, "spec", "totalCents"); total != 42 {
		t.Errorf("spec.totalCents = %d, want the supplied 42", total)
	}
}

// TestReadsAreNotDefaulted states the boundary deliberately: a projection whose
// premise is that the rows are the truth must not invent values on the way out.
func TestReadsAreNotDefaulted(t *testing.T) {
	spec := testSpec()
	spec.Resource.Schema = &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
				"customer":   {Type: "string"},
				"totalCents": {Type: "integer", Format: "int64"},
				"lineItems": {Type: "array", Items: &apiextensionsv1.JSONSchemaPropsOrArray{
					Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr(true)},
				}},
				"region": {Type: "string", Default: &apiextensionsv1.JSON{Raw: []byte(`"eu"`)}},
			}},
			"status": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{"phase": {Type: "string"}}},
		},
	}

	store := newStorage(t, spec).(*REST)

	obj, err := store.Get(namespacedContext("acme"), "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if _, found, _ := unstructured.NestedString(obj.(*unstructured.Unstructured).Object, "spec", "region"); found {
		t.Error("a read invented spec.region from the schema default; no column maps to it")
	}
}

// schemaWithConstraint bounds spec.customer, which the seeded rows violate.
func schemaWithConstraint() *apiextensionsv1.JSONSchemaProps {
	return &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"customer":   {Type: "string", MaxLength: ptr(int64(2))},
					"totalCents": {Type: "integer", Format: "int64"},
					"lineItems": {Type: "array", Items: &apiextensionsv1.JSONSchemaPropsOrArray{
						Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr(true)},
					}},
				},
			},
			"status": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{"phase": {Type: "string"}}},
		},
	}
}

// TestValidationRatchetingAllowsUntouchedInvalidFields is the case a projection
// meets constantly: the schema is tightened after the fact, and rows that
// predate it are already invalid. An update that does not touch the offending
// field must still go through, or those rows become unmanageable.
func TestValidationRatchetingAllowsUntouchedInvalidFields(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Schema = schemaWithConstraint()

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	// order-1001 is seeded with customer "ada", three characters, which the
	// schema now forbids.
	existing, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	updated := existing.(*unstructured.Unstructured).DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, int64(123), "spec", "totalCents"); err != nil {
		t.Fatalf("preparing the update: %v", err)
	}

	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() rejected a change that left the invalid field alone: %v", err)
	}
}

// TestValidationRatchetingStillRejectsChangedInvalidFields is the other half:
// ratcheting forgives what was already there, not what the client writes now.
func TestValidationRatchetingStillRejectsChangedInvalidFields(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Schema = schemaWithConstraint()

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	existing, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	updated := existing.(*unstructured.Unstructured).DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, "grace", "spec", "customer"); err != nil {
		t.Fatalf("preparing the update: %v", err)
	}

	_, _, err = store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Update() error = %v, want Invalid for a newly written value that breaks the schema", err)
	}
	if !strings.Contains(err.Error(), "customer") {
		t.Errorf("error %q does not name the offending field", err)
	}
}

// TestValidationRatchetingAppliesToCELRules covers the same rule for
// x-kubernetes-validations, which have their own ratcheting switch.
func TestValidationRatchetingAppliesToCELRules(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Schema = &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"customer": {
						Type:         "string",
						XValidations: apiextensionsv1.ValidationRules{{Rule: "self == 'grace'", Message: "customer must be grace"}},
					},
					"totalCents": {Type: "integer", Format: "int64"},
					"lineItems": {Type: "array", Items: &apiextensionsv1.JSONSchemaPropsOrArray{
						Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr(true)},
					}},
				},
			},
			"status": {Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{"phase": {Type: "string"}}},
		},
	}

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	existing, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	// The seeded customer is "ada", which the rule rejects; changing only the
	// total must still be allowed.
	untouched := existing.(*unstructured.Unstructured).DeepCopy()
	if err := unstructured.SetNestedField(untouched.Object, int64(31), "spec", "totalCents"); err != nil {
		t.Fatalf("preparing the update: %v", err)
	}
	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(untouched), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() rejected a change that left the rule-violating field alone: %v", err)
	}

	// Writing the field is still checked.
	current, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	changed := current.(*unstructured.Unstructured).DeepCopy()
	if err := unstructured.SetNestedField(changed.Object, "hopper", "spec", "customer"); err != nil {
		t.Fatalf("preparing the update: %v", err)
	}
	_, _, err = store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(changed), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Update() error = %v, want Invalid from the validation rule", err)
	}
}

// TestCreateIsNotRatcheted states the boundary: there is no previous object to
// forgive, so a create is held to the whole schema.
func TestCreateIsNotRatcheted(t *testing.T) {
	spec := writableSpec()
	spec.Resource.Schema = schemaWithConstraint()

	store := newStorage(t, spec).(*WritableREST)

	_, err := store.Create(namespacedContext("acme"), newOrder("order-ratchet-1", "hopper", 5), nil, &metav1.CreateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Create() error = %v, want Invalid", err)
	}
}

// TestWriteBaseIsNotServedFromTheCache is a regression test.
//
// A write reads the stored object to check the client's resourceVersion
// against it and to merge the half of the object the request does not own.
// That read used to go through the read cache, so with cacheTTL set it could be
// answered with an object up to a whole TTL old — and the conflict check then
// compared the client's version against a version the row no longer had.
//
// The projection here deliberately does not bind :resourceVersion in its UPDATE.
// That is the weaker of the two shapes the README describes, where the check is
// a read followed by a write rather than one atomic statement — and it is
// exactly the shape a stale read silently defeats.
func TestWriteBaseIsNotServedFromTheCache(t *testing.T) {
	spec := writableSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Hour}
	spec.Queries.Update = &crispv1alpha1.Query{
		SQL: `UPDATE orders
		      SET customer = :customer, status = :status, total_cents = :total_cents,
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
		      WHERE tenant = :namespace AND id = :name`,
	}

	store, path := newStorageWithDB(t, spec)
	writable := store.(*WritableREST)
	ctx := namespacedContext("acme")

	// Prime the cache with the row as it is now, and keep the version it had.
	primed, err := writable.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("priming the cache: %v", err)
	}
	wasCurrent := primed.(*unstructured.Unstructured).GetResourceVersion()
	if wasCurrent == "" {
		t.Fatal("the projection reports no resourceVersion, so this test proves nothing")
	}

	// Move the row on behind kube-crisp's back, the way another writer would.
	db, openErr := sql.Open("sqlite", path)
	if openErr != nil {
		t.Fatalf("opening the database: %v", openErr)
	}
	defer db.Close()
	if _, execErr := db.Exec(
		`UPDATE orders SET updated_at = CAST(CAST(updated_at AS INTEGER) + 100 AS TEXT) WHERE id = 'order-1001'`,
	); execErr != nil {
		t.Fatalf("moving the row on: %v", execErr)
	}

	// The client asserts exactly the version the cache is still holding. Served
	// from the cache the write is accepted, because the cached object still has
	// that version; read fresh it conflicts, because the row does not.
	stale := newOrder("order-1001", "grace", 8888)
	stale.SetResourceVersion(wasCurrent)

	_, _, updateErr := writable.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(stale), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsConflict(updateErr) {
		t.Fatalf("Update() error = %v, want Conflict; the precondition was checked against a cached object", updateErr)
	}
}

// managedFieldsSpec maps metadata.managedFields onto a column, which is what
// lets server-side apply detect a conflict: an object rebuilt from a row
// otherwise carries no record of who owns which field.
func managedFieldsSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Mapping.ManagedFields = "managed_fields"
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at, managed_fields
		      FROM orders WHERE tenant = :namespace ORDER BY id`,
	}
	spec.Queries.Get = &crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at, managed_fields
		      FROM orders WHERE tenant = :namespace AND id = :name`,
	}
	spec.Queries.Update = &crispv1alpha1.Query{
		SQL: `UPDATE orders
		      SET customer = :customer, status = :status, total_cents = :total_cents,
		          managed_fields = :managed_fields,
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
		      WHERE tenant = :namespace AND id = :name
		        AND (:resourceVersion IS NULL OR updated_at = :resourceVersion)`,
	}
	return spec
}

// TestManagedFieldsSurviveAWriteAndRead is the whole point of mapping the
// column: field management's record of who owns what has to come back on the
// next read, or every apply looks like the first one.
func TestManagedFieldsSurviveAWriteAndRead(t *testing.T) {
	store, path := newStorageWithDB(t, managedFieldsSpec())
	writable := store.(*WritableREST)
	ctx := namespacedContext("acme")

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening the database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE orders ADD COLUMN managed_fields TEXT`); err != nil {
		t.Fatalf("adding the column: %v", err)
	}

	current, err := writable.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	updated := current.(*unstructured.Unstructured).DeepCopy()
	updated.SetManagedFields([]metav1.ManagedFieldsEntry{{
		Manager:    "shipping-controller",
		Operation:  metav1.ManagedFieldsOperationApply,
		APIVersion: "store.example.com/v1alpha1",
		FieldsType: "FieldsV1",
		FieldsV1:   metav1.NewFieldsV1(`{"f:spec":{"f:customer":{}}}`),
	}})
	if err := unstructured.SetNestedField(updated.Object, "grace", "spec", "customer"); err != nil {
		t.Fatalf("building the update: %v", err)
	}

	if _, _, err := writable.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(updated), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}

	// Read back through a path that has no cached copy to fall back on.
	after, err := writable.read(ctx, "order-1001", fresh, "")
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}

	managed := after.(*unstructured.Unstructured).GetManagedFields()
	if len(managed) != 1 {
		t.Fatalf("read back %d managedFields entries, want 1; ownership did not survive the round trip", len(managed))
	}
	if managed[0].Manager != "shipping-controller" {
		t.Errorf("manager = %q, want %q", managed[0].Manager, "shipping-controller")
	}
	if managed[0].FieldsV1 == nil || !strings.Contains(managed[0].FieldsV1.GetRawString(), "f:customer") {
		t.Errorf("the owned field set did not survive: %+v", managed[0].FieldsV1)
	}
}

// TestManagedFieldsAreNotRequired: a projection that does not map the column
// still serves, it just cannot detect an apply conflict.
func TestManagedFieldsAreNotRequired(t *testing.T) {
	store := newWritableREST(t)

	obj, err := store.Get(namespacedContext("acme"), "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := obj.(*unstructured.Unstructured).GetManagedFields(); len(got) != 0 {
		t.Errorf("an unmapped projection reported managedFields: %+v", got)
	}
}

// pushdownSpec filters in the database on a declared field and on a mapped
// label, using every parameter the push-down offers.
func pushdownSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := writableSpec()
	spec.Resource.SelectableFields = []crispv1alpha1.SelectableField{
		{JSONPath: ".spec.customer", Column: "customer"},
	}
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders
		      WHERE tenant = :namespace
		        AND (:customer IS NULL OR customer = :customer)
		        AND (:customer_not IS NULL OR customer <> :customer_not)
		        AND (:label_status IS NULL OR status = :label_status)
		        AND (:label_status_not IS NULL OR status <> :label_status_not)
		        AND (:label_status_in IS NULL
		             OR status IN (SELECT value FROM json_each(:label_status_in)))
		      ORDER BY id`,
	}
	return spec
}

// TestSelectorsAreFilteredInTheDatabase: a selective list should be a query the
// database can answer, not a full read followed by a filter in Go. What comes
// back is filtered again after mapping either way, so this is about how many
// rows crossed the wire.
func TestSelectorsAreFilteredInTheDatabase(t *testing.T) {
	store := newStorage(t, pushdownSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	names := func(obj runtime.Object) []string {
		var out []string
		for _, item := range obj.(*unstructured.UnstructuredList).Items {
			out = append(out, item.GetName())
		}
		sort.Strings(out)
		return out
	}

	all, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if len(names(all)) < 2 {
		t.Fatalf("the fixture holds %d objects; this test needs at least 2", len(names(all)))
	}

	// A field selector, pushed down as :customer.
	got, err := store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.customer", "ada"),
	})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	for _, item := range got.(*unstructured.UnstructuredList).Items {
		if customer, _, _ := unstructured.NestedString(item.Object, "spec", "customer"); customer != "ada" {
			t.Errorf("field selector returned %q", customer)
		}
	}

	// The other half of the field selector grammar, pushed down as
	// :customer_not.
	got, err = store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermNotEqualSelector("spec.customer", "ada"),
	})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	for _, item := range got.(*unstructured.UnstructuredList).Items {
		if customer, _, _ := unstructured.NestedString(item.Object, "spec", "customer"); customer == "ada" {
			t.Error("!= selector returned the value it excluded")
		}
	}
}

// TestLabelSelectorOperatorsArePushedDown checks the label parameters carry the
// operator the client used, not just equality.
func TestLabelSelectorOperatorsArePushedDown(t *testing.T) {
	store := newStorage(t, pushdownSpec()).(*WritableREST)

	for _, tc := range []struct {
		name     string
		selector string
		bound    string
		want     any
	}{
		{"equality", "store.example.com/status=shipped", "label_status", "shipped"},
		{"inequality", "store.example.com/status!=shipped", "label_status_not", "shipped"},
		{"set membership", "store.example.com/status in (shipped,pending)", "label_status_in", `["shipped","pending"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector, err := labels.Parse(tc.selector)
			if err != nil {
				t.Fatalf("parsing %q: %v", tc.selector, err)
			}

			args := map[string]any{}
			store.bindLabelSelector(args, selector)

			got := args[tc.bound]
			if tc.bound == "label_status_in" {
				// The set has no order; compare as a set.
				var decoded []string
				if err := json.Unmarshal([]byte(got.(string)), &decoded); err != nil {
					t.Fatalf("decoding %v: %v", got, err)
				}
				sort.Strings(decoded)
				encoded, _ := json.Marshal(decoded)
				var wantDecoded []string
				_ = json.Unmarshal([]byte(tc.want.(string)), &wantDecoded)
				sort.Strings(wantDecoded)
				wantEncoded, _ := json.Marshal(wantDecoded)
				if string(encoded) != string(wantEncoded) {
					t.Errorf("%s = %s, want %s", tc.bound, encoded, wantEncoded)
				}
				return
			}
			if got != tc.want {
				t.Errorf("%s = %v, want %v", tc.bound, got, tc.want)
			}
		})
	}

	// A label with no column of its own is not pushed down, and must not
	// invent a parameter.
	args := map[string]any{}
	selector, _ := labels.Parse("unmapped=x")
	store.bindLabelSelector(args, selector)
	if _, bound := args["label_unmapped"]; bound {
		t.Error("an unmapped label produced a bind parameter")
	}
}

// TestDeleteCollectionDoesNotRereadWhatItJustListed covers the round trips a
// collection delete costs.
//
// The one-at-a-time path exists for requests a single statement cannot express,
// and it already has the objects: it listed them, fresh, to report what it
// removed. Reading each row back before deleting it doubled the cost of the
// whole operation for nothing.
func TestDeleteCollectionDoesNotRereadWhatItJustListed(t *testing.T) {
	crispmetrics.QueryDuration.Reset()

	// No deleteCollection query, so every object goes through the individual
	// path — which is the one being measured.
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	deleted, err := store.DeleteCollection(ctx, nil, &metav1.DeleteOptions{}, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("DeleteCollection() returned error: %v", err)
	}
	if got, want := len(deleted.(*unstructured.UnstructuredList).Items), 2; got != want {
		t.Fatalf("DeleteCollection() reported %d objects, want %d", got, want)
	}

	const metric = "kube_crisp_query_duration_seconds"
	testutil.AssertHistogramTotalCount(t, metric,
		map[string]string{"verb": "get", "result": crispmetrics.ResultSuccess}, 0)

	remaining, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got := len(remaining.(*unstructured.UnstructuredList).Items); got != 0 {
		t.Errorf("%d objects survived the collection delete", got)
	}
}

// TestDeleteCollectionStillReadsWhenAPreconditionIsAttached is the other half:
// a precondition asserts something about the object as it is now, so the copy
// the list produced cannot answer it and the read has to happen.
func TestDeleteCollectionStillReadsWhenAPreconditionIsAttached(t *testing.T) {
	crispmetrics.QueryDuration.Reset()

	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	existing, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	uid := existing.(*unstructured.Unstructured).GetUID()

	crispmetrics.QueryDuration.Reset()
	options := &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}

	// One object matches the precondition and the other does not, so this fails
	// — the point here is only that the objects were re-read to find out.
	_, _ = store.DeleteCollection(ctx, nil, options, &metainternalversion.ListOptions{})

	count, err := testutil.GetHistogramMetricValue(
		crispmetrics.QueryDuration.WithLabelValues(
			"orders", "orders.store.example.com", "get", crispmetrics.ResultSuccess))
	if err != nil {
		t.Fatalf("reading the get histogram: %v", err)
	}
	if count == 0 {
		t.Error("no object was re-read, so the precondition was decided on a copy taken before it")
	}
}

// TestNameSelectorIsAnsweredByTheDatabase covers the pushdown for the one
// selector every resource accepts.
//
// A list statement that references :name can answer "the object called x" with
// the lookup a get would do. Without the binding :name was always NULL on a
// list, so such a statement returned nothing and the selector only ever worked
// as a filter over a full scan.
func TestNameSelectorIsAnsweredByTheDatabase(t *testing.T) {
	spec := testSpec()
	// The statement narrows itself when the selector supplies a name, and lists
	// everything when it does not — the shape a projection actually writes.
	spec.Queries.List.SQL = `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
	                         FROM orders
	                         WHERE tenant = :namespace
	                           AND (:name IS NULL OR id = :name)
	                           AND (:name_not IS NULL OR id <> :name_not)
	                         ORDER BY id`

	store := newStorage(t, spec).(*REST)
	ctx := namespacedContext("acme")

	all, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	total := len(all.(*unstructured.UnstructuredList).Items)
	if total < 2 {
		t.Fatalf("the fixture holds %d objects in acme, want at least 2", total)
	}

	selected, err := store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", "order-1001"),
	})
	if err != nil {
		t.Fatalf("List() with a name selector returned error: %v", err)
	}
	items := selected.(*unstructured.UnstructuredList).Items
	if len(items) != 1 || items[0].GetName() != "order-1001" {
		t.Fatalf("the name selector produced %d objects (%v), want just order-1001",
			len(items), objectNames(items))
	}

	// The negative form too, since the statement binds both.
	excluded, err := store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermNotEqualSelector("metadata.name", "order-1001"),
	})
	if err != nil {
		t.Fatalf("List() with a != name selector returned error: %v", err)
	}
	remaining := excluded.(*unstructured.UnstructuredList).Items
	if len(remaining) != total-1 {
		t.Errorf("the != selector produced %d objects (%v), want %d", len(remaining), objectNames(remaining), total-1)
	}
	for _, item := range remaining {
		if item.GetName() == "order-1001" {
			t.Error("the excluded object came back")
		}
	}
}

// TestNameSelectorStillWorksWithoutPushdown: a statement that ignores :name is
// unchanged by the binding, because everything is filtered again after mapping.
func TestNameSelectorStillWorksWithoutPushdown(t *testing.T) {
	// testSpec's list statement references neither :name nor :name_not.
	store := newStorage(t, testSpec()).(*REST)
	ctx := namespacedContext("acme")

	selected, err := store.List(ctx, &metainternalversion.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", "order-1001"),
	})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	items := selected.(*unstructured.UnstructuredList).Items
	if len(items) != 1 || items[0].GetName() != "order-1001" {
		t.Errorf("produced %d objects (%v), want just order-1001", len(items), objectNames(items))
	}
}

// TestNamespaceSelectorOnlyNarrowsAClusterWideRead: a namespaced request has
// its namespace from the path, and a selector naming a different one has to
// match nothing rather than send the query somewhere else.
func TestNamespaceSelectorOnlyNarrowsAClusterWideRead(t *testing.T) {
	store := newStorage(t, testSpec()).(*REST)

	args := map[string]any{"namespace": "acme"}
	store.bindIdentitySelector(args, fields.OneTermEqualSelector("metadata.namespace", "globex"), "acme")
	if args["namespace"] != "acme" {
		t.Errorf("a namespaced read was rebound to %v, want it left at acme", args["namespace"])
	}

	clusterWide := map[string]any{"namespace": nil}
	store.bindIdentitySelector(clusterWide, fields.OneTermEqualSelector("metadata.namespace", "globex"), "")
	if clusterWide["namespace"] != "globex" {
		t.Errorf("a cluster-wide read bound namespace %v, want globex", clusterWide["namespace"])
	}
}

func objectNames(items []unstructured.Unstructured) []string {
	out := make([]string, 0, len(items))
	for i := range items {
		out = append(out, items[i].GetName())
	}
	return out
}

// TestTimeoutDoesNotAskEveryClientToRetry: RetryAfterSeconds on an error over
// 500 is an instruction, not a hint — client-go retries such a response ten
// times by default, at the interval the header names. That is what should
// happen to a request shed at the concurrency limit, which costs nothing, and
// to a refused connection, which costs nearly nothing. A timeout is the
// opposite: each attempt runs the query for its whole budget first, so eleven
// attempts spend eleven budgets to return the same error.
//
// Measured before this was fixed: one LIST against a table slower than its
// timeout became 11 queries and 15.6s of client wait.
func TestTimeoutDoesNotAskEveryClientToRetry(t *testing.T) {
	store := newTestREST(t)

	timeout := store.queryError(context.DeadlineExceeded, "listing")
	if !errors.IsTimeout(timeout) {
		t.Fatalf("queryError(DeadlineExceeded) = %v, want Timeout", timeout)
	}
	if got := retryAfterOf(t, timeout); got != 0 {
		t.Errorf("a timeout advertised Retry-After: %ds, so every client-go client "+
			"would run the slow query %d more times; want none", got, defaultClientGoRetries)
	}

	// The two that should still ask, so this cannot be "fixed" by removing
	// Retry-After everywhere: both are rejected before the database is asked
	// to do anything, which is what makes retrying them cheap.
	shed := store.queryError(crispsql.ErrTooBusy, "listing")
	if !errors.IsTooManyRequests(shed) {
		t.Fatalf("queryError(ErrTooBusy) = %v, want TooManyRequests", shed)
	}
	if got := retryAfterOf(t, shed); got == 0 {
		t.Error("a shed request no longer asks the client to come back, so it just fails")
	}

	down := store.queryError(&net.OpError{Op: "dial", Net: "tcp", Err: goerrors.New("connection refused")}, "listing")
	if !errors.IsServiceUnavailable(down) {
		t.Fatalf("queryError(connection refused) = %v, want ServiceUnavailable", down)
	}
	if got := retryAfterOf(t, down); got == 0 {
		t.Error("an unreachable database no longer asks the client to come back, " +
			"so a restart looks like a hard failure rather than a hiccup")
	}
}

// client-go's rest.Request retries this many times when a response over 500
// carries a Retry-After. Named here so the cost in the test above is the real
// one rather than a number in a comment.
const defaultClientGoRetries = 10

func retryAfterOf(t *testing.T, err error) int32 {
	t.Helper()

	var status errors.APIStatus
	if !goerrors.As(err, &status) {
		t.Fatalf("%v carries no status", err)
	}
	details := status.Status().Details
	if details == nil {
		return 0
	}
	return details.RetryAfterSeconds
}
