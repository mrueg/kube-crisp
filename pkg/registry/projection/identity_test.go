package projection

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/warning"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// compositeDB holds a table keyed by two columns, which is the case a single
// name column cannot describe.
func compositeDB(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "shipments.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE shipments (
			region   TEXT NOT NULL,
			order_no TEXT NOT NULL,
			tenant   TEXT NOT NULL,
			carrier  TEXT NOT NULL,
			seq      INTEGER NOT NULL,
			PRIMARY KEY (region, order_no))`,
		`INSERT INTO shipments VALUES ('eu','1042','acme','dhl',1)`,
		`INSERT INTO shipments VALUES ('us','1042','acme','ups',2)`,
		`INSERT INTO shipments VALUES ('eu','2001','acme','dpd',3)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	return path
}

func compositeSpec() crispv1alpha1.CustomResourceProjectionSpec {
	return crispv1alpha1.CustomResourceProjectionSpec{
		DataSource: crispv1alpha1.DataSource{Driver: "sqlite"},
		Resource: crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1", Kind: "Shipment",
			Plural: "shipments", Scope: crispv1alpha1.NamespaceScoped,
		},
		Queries: crispv1alpha1.Queries{
			List: crispv1alpha1.Query{
				SQL: `SELECT region, order_no, tenant, carrier, seq FROM shipments
				      WHERE tenant = :namespace ORDER BY seq`,
			},
			Get: &crispv1alpha1.Query{
				SQL: `SELECT region, order_no, tenant, carrier, seq FROM shipments
				      WHERE tenant = :namespace AND region = :region AND order_no = :order_no`,
			},
			Create: &crispv1alpha1.Query{
				SQL: `INSERT INTO shipments (region, order_no, tenant, carrier, seq)
				      VALUES (:region, :order_no, :tenant, :carrier,
				              (SELECT COALESCE(MAX(seq), 0) + 1 FROM shipments))`,
			},
			Delete: &crispv1alpha1.Query{
				SQL: `DELETE FROM shipments WHERE tenant = :namespace AND region = :region AND order_no = :order_no`,
			},
		},
		Mapping: crispv1alpha1.Mapping{
			NameColumns:     []string{"region", "order_no"},
			Namespace:       "tenant",
			ResourceVersion: "seq",
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "carrier", Path: "spec.carrier"},
			},
		},
	}
}

func newCompositeStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) *WritableREST {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: compositeDB(t), PreparedStatements: true})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("shipments", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.writable
}

func TestCompositeIdentityBuildsTheName(t *testing.T) {
	store := newCompositeStorage(t, compositeSpec())

	result, err := store.List(namespacedContext("acme"), nil)
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	var names []string
	for _, item := range result.(*unstructured.UnstructuredList).Items {
		names = append(names, item.GetName())
	}
	want := []string{"eu-1042", "us-1042", "eu-2001"}
	if len(names) != len(want) {
		t.Fatalf("listed %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Errorf("object %d is named %q, want %q", i, names[i], want[i])
		}
	}
}

// TestCompositeIdentityRoundTrips is what makes the name usable: it has to
// split back into the columns it was built from.
func TestCompositeIdentityRoundTrips(t *testing.T) {
	store := newCompositeStorage(t, compositeSpec())
	ctx := namespacedContext("acme")

	obj, err := store.Get(ctx, "us-1042", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if carrier, _, _ := unstructured.NestedString(obj.(*unstructured.Unstructured).Object, "spec", "carrier"); carrier != "ups" {
		t.Errorf("spec.carrier = %q, want the us row's %q", carrier, "ups")
	}

	// The other half of the composite key is a different object.
	other, err := store.Get(ctx, "eu-1042", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if carrier, _, _ := unstructured.NestedString(other.(*unstructured.Unstructured).Object, "spec", "carrier"); carrier != "dhl" {
		t.Errorf("spec.carrier = %q, want the eu row's %q", carrier, "dhl")
	}
}

func TestCompositeIdentityWrites(t *testing.T) {
	store := newCompositeStorage(t, compositeSpec())
	ctx := namespacedContext("acme")

	obj := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"carrier": "fedex"},
	}}
	obj.SetName("ap-3003")
	obj.SetNamespace("acme")

	if _, err := store.Create(ctx, obj, nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}

	created, err := store.Get(ctx, "ap-3003", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after create returned error: %v", err)
	}
	if carrier, _, _ := unstructured.NestedString(created.(*unstructured.Unstructured).Object, "spec", "carrier"); carrier != "fedex" {
		t.Errorf("spec.carrier = %q, want %q", carrier, "fedex")
	}

	if _, _, err := store.Delete(ctx, "ap-3003", nil, &metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}
	if _, err := store.Get(ctx, "ap-3003", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Errorf("Get() after delete error = %v, want NotFound", err)
	}
}

// TestNameThatCannotSplitIsNotFound: a name that cannot have come from these
// columns names no row, and saying NotFound beats binding nonsense.
func TestNameThatCannotSplitIsNotFound(t *testing.T) {
	store := newCompositeStorage(t, compositeSpec())

	if _, err := store.Get(namespacedContext("acme"), "just-one-too-many-parts", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Get() error = %v, want NotFound", err)
	}
	if _, err := store.Get(namespacedContext("acme"), "nodashes", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Get() error = %v, want NotFound", err)
	}
}

// TestAmbiguousCompositeNameIsRejected is the correctness case: if a value
// carries the separator, two different rows would produce the same name.
func TestAmbiguousCompositeNameIsRejected(t *testing.T) {
	spec := compositeSpec()
	store := newCompositeStorage(t, spec)

	// "eu-1" + "042" and "eu" + "1-042" both read as "eu-1-042".
	if _, err := store.Create(namespacedContext("acme"), func() *unstructured.Unstructured {
		obj := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"carrier": "dhl"}}}
		obj.SetName("eu-1-042")
		obj.SetNamespace("acme")
		return obj
	}(), nil, &metav1.CreateOptions{}); err == nil {
		t.Fatal("a name with too many parts was accepted")
	}

	// And a row whose column already holds the separator is never served under
	// a name that cannot be resolved back to it. It is left out of the
	// collection, and the client is told so rather than being handed a shorter
	// list with no explanation.
	poisoned := compositeSpec()
	poisoned.Mapping.NameSeparator = "0"
	poisonedStore := newCompositeStorage(t, poisoned)

	recorder := &recordingWarnings{}
	ctx := warning.WithWarningRecorder(namespacedContext("acme"), recorder)

	list, err := poisonedStore.List(ctx, nil)
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	for _, item := range list.(*unstructured.UnstructuredList).Items {
		if strings.Contains(item.GetName(), "0") {
			t.Errorf("row served under the ambiguous name %q, which does not resolve back to one row", item.GetName())
		}
	}
	if len(recorder.messages) == 0 {
		t.Error("rows were dropped from the collection without telling the client")
	}
}

// recordingWarnings collects the warnings a request would carry back.
type recordingWarnings struct {
	messages []string
}

func (r *recordingWarnings) AddWarning(_, text string) {
	r.messages = append(r.messages, text)
}

func TestGenerateNameIsRejectedForCompositeIdentity(t *testing.T) {
	store := newCompositeStorage(t, compositeSpec())

	obj := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"carrier": "dhl"}}}
	obj.SetGenerateName("shipment-")
	obj.SetNamespace("acme")

	_, err := store.Create(namespacedContext("acme"), obj, nil, &metav1.CreateOptions{})
	if err == nil || !strings.Contains(err.Error(), "generateName is not supported") {
		t.Fatalf("Create() error = %v, want a rejection of generateName", err)
	}
}

func TestCompositeIdentityRequiresAKeysetColumn(t *testing.T) {
	spec := compositeSpec()
	spec.Queries.List.SQL = `SELECT region, order_no, tenant, carrier, seq FROM shipments
	                         WHERE tenant = :namespace AND (:after IS NULL OR seq > :after)
	                         ORDER BY seq LIMIT :limit`

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: compositeDB(t)})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	if _, err := New("shipments", spec, pool, nil, nil); err == nil || !strings.Contains(err.Error(), "keysetColumn is required") {
		t.Fatalf("New() error = %v, want a demand for an explicit keysetColumn", err)
	}
}

func TestNameAndNameColumnsAreExclusive(t *testing.T) {
	spec := compositeSpec()
	spec.Mapping.Name = "region"

	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: compositeDB(t)})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	if _, err := New("shipments", spec, pool, nil, nil); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("New() error = %v, want a rejection of both name and nameColumns", err)
	}
}
