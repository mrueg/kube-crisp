package projection

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// seededStorage serves spec over a fresh database built from the statements,
// for the fixtures here whose table shape the shared orders one cannot take.
func seededStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec, statements ...string) (*WritableREST, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "owned.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	for _, stmt := range statements {
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

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable, path
}

// queryOne runs a statement against the database behind the server's back and
// scans its single value.
func queryOne(t *testing.T, path, query string, into any) {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()
	if err := db.QueryRow(query).Scan(into); err != nil {
		t.Fatalf("querying: %v", err)
	}
}

// A name the mapper refuses on the way back is a row the API can then neither
// get nor delete, so it has to be refused on the way in, before the insert.
func TestCreateWithAnInvalidNameIsInvalidAndInsertsNothing(t *testing.T) {
	store, path := newStorageWithDB(t, writableSpec())
	ctx := namespacedContext("acme")

	_, err := store.(*WritableREST).Create(ctx, newOrder("Bad_Name", "ada", 1), nil, &metav1.CreateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Create() error = %v, want Invalid", err)
	}

	var rows int
	queryOne(t, path, `SELECT COUNT(*) FROM orders WHERE id = 'Bad_Name'`, &rows)
	if rows != 0 {
		t.Error("the row was inserted despite the invalid name")
	}
}

// A generated name is checked the same way, through a candidate the generator
// produces, so a prefix that cannot become a valid name is refused up front.
func TestCreateWithAnInvalidGenerateNameIsInvalid(t *testing.T) {
	store := newWritableREST(t)

	obj := newOrder("", "ada", 1)
	obj.SetGenerateName("Bad_Prefix-")

	_, err := store.Create(namespacedContext("acme"), obj, nil, &metav1.CreateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Create() error = %v, want Invalid", err)
	}
}

// The mapper refuses a row whose label value is not one the API allows, so a
// value accepted on write and rejected on every read is the same trap one
// level down.
func TestCreateWithAnInvalidLabelValueIsInvalid(t *testing.T) {
	store := newWritableREST(t)

	obj := newOrder("order-5001", "ada", 1)
	obj.SetLabels(map[string]string{"store.example.com/status": "not a label value!"})

	_, err := store.Create(namespacedContext("acme"), obj, nil, &metav1.CreateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Create() error = %v, want Invalid", err)
	}
}

// The same rule holds on update: an object that reads back cannot be edited
// into one that does not.
func TestUpdateWithAnInvalidLabelValueIsInvalid(t *testing.T) {
	store := newWritableREST(t)

	obj := newOrder("order-1001", "ada", 1)
	obj.SetLabels(map[string]string{"store.example.com/status": "not a label value!"})

	_, _, err := store.Update(namespacedContext("acme"), "order-1001",
		rest.DefaultUpdatedObjectInfo(obj), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsInvalid(err) {
		t.Fatalf("Update() error = %v, want Invalid", err)
	}
}

// A version names a stored object, and a create has none yet.
func TestCreateWithAResourceVersionIsBadRequest(t *testing.T) {
	store := newWritableREST(t)

	obj := newOrder("order-5002", "ada", 1)
	obj.SetResourceVersion("7")

	_, err := store.Create(namespacedContext("acme"), obj, nil, &metav1.CreateOptions{})
	if !errors.IsBadRequest(err) {
		t.Fatalf("Create() error = %v, want BadRequest", err)
	}
}

// ownerReferences and the garbage collector resolve against the uid, so a
// creator that could choose one could have a new object stand in for a deleted
// owner. Where a column holds the uid, the server mints it.
func TestCreateMintsTheUIDWhereAColumnHoldsIt(t *testing.T) {
	spec := uidSpec()
	spec.Queries.Create = &crispv1alpha1.Query{
		SQL: `INSERT INTO orders (id, tenant, uid, customer, updated_at) VALUES (:id, :tenant, :uid, :customer, '1')`,
	}
	store := newUIDStorage(t, spec)
	ctx := namespacedContext("acme")

	obj := newOrder("order-5003", "ada", 1)
	obj.SetUID("11111111-1111-1111-1111-111111111111")

	created, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	got := created.(*unstructured.Unstructured).GetUID()
	if got == obj.GetUID() {
		t.Fatalf("metadata.uid = %q: the client's uid was stored", got)
	}
	if _, err := uuid.Parse(string(got)); err != nil {
		t.Fatalf("metadata.uid = %q, want a freshly minted UUID: %v", got, err)
	}

	fetched, err := store.Get(ctx, "order-5003", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if fetched.(*unstructured.Unstructured).GetUID() != got {
		t.Errorf("Get() uid = %q, want the minted %q", fetched.(*unstructured.Unstructured).GetUID(), got)
	}
}

// Without a uid column nothing is bound for it, and the uid every read derives
// from the row is the one the create answers with, whatever the client sent.
func TestCreateDerivesTheUIDWhereNoColumnHoldsIt(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	obj := newOrder("order-5004", "ada", 1)
	obj.SetUID("11111111-1111-1111-1111-111111111111")

	created, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	got := created.(*unstructured.Unstructured).GetUID()
	if got == "" || got == obj.GetUID() {
		t.Fatalf("metadata.uid = %q, want one derived from the row", got)
	}

	fetched, err := store.Get(ctx, "order-5004", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if fetched.(*unstructured.Unstructured).GetUID() != got {
		t.Errorf("Get() uid = %q, want the same derived %q", fetched.(*unstructured.Unstructured).GetUID(), got)
	}
}

// creationTimestampSpec reads the row's created_at, which the database stamps.
func creationTimestampSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := uidSpec()
	spec.Queries.List.SQL = `SELECT id, tenant, uid, customer, created_at, updated_at FROM orders WHERE tenant = :namespace ORDER BY id`
	spec.Queries.Get.SQL = `SELECT id, tenant, uid, customer, created_at, updated_at FROM orders WHERE tenant = :namespace AND id = :name`
	spec.Queries.Create = &crispv1alpha1.Query{
		SQL: `INSERT INTO orders (id, tenant, uid, customer, updated_at) VALUES (:id, :tenant, :uid, :customer, '1')`,
	}
	spec.Mapping.CreationTimestamp = "created_at"
	return spec
}

const stampedByTheDatabase = "2026-01-01T00:00:00Z"

func newCreationTimestampStorage(t *testing.T) *WritableREST {
	t.Helper()

	store, _ := seededStorage(t, creationTimestampSpec(),
		`CREATE TABLE orders (
			id TEXT PRIMARY KEY, tenant TEXT NOT NULL, uid TEXT NOT NULL, customer TEXT NOT NULL,
			created_at TEXT NOT NULL DEFAULT '`+stampedByTheDatabase+`', updated_at TEXT NOT NULL)`)
	return store
}

// The database stamps creationTimestamp; the client's copy is not a request.
func TestCreationTimestampIsServerOwned(t *testing.T) {
	store := newCreationTimestampStorage(t)
	ctx := namespacedContext("acme")

	obj := newOrder("order-5005", "ada", 1)
	if err := unstructured.SetNestedField(obj.Object, "2000-01-01T00:00:00Z", "metadata", "creationTimestamp"); err != nil {
		t.Fatalf("setting creationTimestamp: %v", err)
	}

	// A dry run answers with what would be stored, and that is not the
	// client's timestamp either.
	dry, err := store.Create(ctx, obj.DeepCopy(), nil, &metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		t.Fatalf("dry-run Create() returned error: %v", err)
	}
	if ts := dry.(*unstructured.Unstructured).GetCreationTimestamp(); ts.Year() == 2000 {
		t.Errorf("dry-run creationTimestamp = %v: the client's value was kept", ts)
	}

	created, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	got, _, _ := unstructured.NestedString(created.(*unstructured.Unstructured).Object, "metadata", "creationTimestamp")
	if got != stampedByTheDatabase {
		t.Errorf("metadata.creationTimestamp = %q, want the database's %q", got, stampedByTheDatabase)
	}
}

// statusCreateSpec enables the status subresource over a table whose status
// column may be NULL, which is what a create then has to leave it as.
func statusCreateSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := uidSpec()
	spec.Resource.Subresources = &crispv1alpha1.ProjectedSubresources{Status: &crispv1alpha1.ProjectedStatusSubresource{}}
	spec.Queries.List.SQL = `SELECT id, tenant, uid, customer, status, updated_at FROM orders WHERE tenant = :namespace ORDER BY id`
	spec.Queries.Get.SQL = `SELECT id, tenant, uid, customer, status, updated_at FROM orders WHERE tenant = :namespace AND id = :name`
	spec.Queries.Create = &crispv1alpha1.Query{
		SQL: `INSERT INTO orders (id, tenant, uid, customer, status, updated_at) VALUES (:id, :tenant, :uid, :customer, :status, '1')`,
	}
	spec.Queries.Update.SQL = `UPDATE orders SET customer = :customer, status = :status,
	                                  updated_at = CAST(CAST(updated_at AS INTEGER) + 1 AS TEXT)
	                           WHERE tenant = :namespace AND id = :name`
	spec.Mapping.Fields = append(spec.Mapping.Fields,
		crispv1alpha1.FieldMapping{Column: "status", Path: "status.phase", OmitEmpty: true})
	return spec
}

func newStatusCreateStorage(t *testing.T) (*WritableREST, string) {
	t.Helper()

	return seededStorage(t, statusCreateSpec(),
		`CREATE TABLE orders (
			id TEXT PRIMARY KEY, tenant TEXT NOT NULL, uid TEXT NOT NULL, customer TEXT NOT NULL,
			status TEXT, updated_at TEXT NOT NULL)`)
}

// With the status subresource enabled, status is the controller's to write
// through /status. A creator with no access there must not get to fill it in
// on the way in, so a create binds NULL for the columns behind it.
func TestCreateUnderTheStatusSubresourceDropsStatus(t *testing.T) {
	store, path := newStatusCreateStorage(t)
	ctx := namespacedContext("acme")

	obj := newOrder("order-5006", "ada", 1)
	if err := unstructured.SetNestedField(obj.Object, "approved", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}

	dry, err := store.Create(ctx, obj.DeepCopy(), nil, &metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		t.Fatalf("dry-run Create() returned error: %v", err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(dry.(*unstructured.Unstructured).Object, "status"); found {
		t.Error("a dry run answered with the status the client sent")
	}

	created, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	if phase, found, _ := unstructured.NestedString(created.(*unstructured.Unstructured).Object, "status", "phase"); found {
		t.Errorf("status.phase = %q after a create, want none", phase)
	}

	var stored sql.NullString
	queryOne(t, path, `SELECT status FROM orders WHERE id = 'order-5006'`, &stored)
	if stored.Valid {
		t.Errorf("status column = %q, want NULL", stored.String)
	}
}

// The client's status is dropped before the schema's defaults are applied, so
// a status field with a default starts at the default rather than at nothing.
// That is what a NOT NULL column behind a status field relies on: the insert
// binds the column either way, and cannot tell a dropped value from an absent
// one.
func TestCreateUnderTheStatusSubresourceStartsStatusAtItsDefault(t *testing.T) {
	yes := true
	spec := statusCreateSpec()
	spec.Resource.Schema = &apiextensionsv1.JSONSchemaProps{
		Type:                   "object",
		XPreserveUnknownFields: &yes,
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"status": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"phase": {Type: "string", Default: &apiextensionsv1.JSON{Raw: []byte(`"pending"`)}},
				},
			},
		},
	}
	store, path := seededStorage(t, spec,
		`CREATE TABLE orders (
			id TEXT PRIMARY KEY, tenant TEXT NOT NULL, uid TEXT NOT NULL, customer TEXT NOT NULL,
			status TEXT NOT NULL, updated_at TEXT NOT NULL)`)
	ctx := namespacedContext("acme")

	obj := newOrder("order-5008", "ada", 1)
	if err := unstructured.SetNestedField(obj.Object, "approved", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	created, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	if phase, _, _ := unstructured.NestedString(created.(*unstructured.Unstructured).Object, "status", "phase"); phase != "pending" {
		t.Errorf("status.phase = %q after a create, want the default %q", phase, "pending")
	}

	var stored string
	queryOne(t, path, `SELECT status FROM orders WHERE id = 'order-5008'`, &stored)
	if stored != "pending" {
		t.Errorf("status column = %q, want the default %q", stored, "pending")
	}
}

// Without the subresource there is no split, and status is written like any
// other field.
func TestCreateWithoutTheStatusSubresourceWritesStatus(t *testing.T) {
	spec := statusCreateSpec()
	spec.Resource.Subresources = nil
	store, path := seededStorage(t, spec,
		`CREATE TABLE orders (
			id TEXT PRIMARY KEY, tenant TEXT NOT NULL, uid TEXT NOT NULL, customer TEXT NOT NULL,
			status TEXT, updated_at TEXT NOT NULL)`)

	obj := newOrder("order-5007", "ada", 1)
	if err := unstructured.SetNestedField(obj.Object, "approved", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	created, err := store.Create(namespacedContext("acme"), obj, nil, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	if phase, _, _ := unstructured.NestedString(created.(*unstructured.Unstructured).Object, "status", "phase"); phase != "approved" {
		t.Errorf("status.phase = %q, want %q", phase, "approved")
	}

	var stored string
	queryOne(t, path, `SELECT status FROM orders WHERE id = 'order-5007'`, &stored)
	if stored != "approved" {
		t.Errorf("status column = %q, want %q", stored, "approved")
	}
}
