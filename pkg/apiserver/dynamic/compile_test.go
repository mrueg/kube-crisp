package dynamic

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
	"github.com/mrueg/kube-crisp/pkg/projection"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"

	_ "modernc.org/sqlite"
)

type fixedResolver struct{ dsn string }

func (r fixedResolver) Resolve(context.Context, crispv1alpha1.DataSource) (string, error) {
	return r.dsn, nil
}

// borrowedSchema stands in for a CustomResourceDefinition lookup.
type borrowedSchema struct {
	schema *apiextensionsv1.JSONSchemaProps
	err    error
}

func (b borrowedSchema) Resolve(context.Context, crispv1alpha1.CRDReference) (*apiextensionsv1.JSONSchemaProps, error) {
	return b.schema, b.err
}

func newTestCompiler(t *testing.T) *Compiler {
	t.Helper()

	path := filepath.Join(t.TempDir(), "orders.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE orders (id TEXT PRIMARY KEY, tenant TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '1')`); err != nil {
		t.Fatalf("creating the table: %v", err)
	}

	pools := crispsql.NewPoolCache()
	t.Cleanup(pools.Close)

	return &Compiler{Pools: pools, Resolver: fixedResolver{dsn: path}}
}

func testProjection() *crispv1alpha1.CustomResourceProjection {
	return &crispv1alpha1.CustomResourceProjection{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: crispv1alpha1.CustomResourceProjectionSpec{
			DataSource: crispv1alpha1.DataSource{Driver: "sqlite"},
			Resource: crispv1alpha1.ProjectedResource{
				Group:   "store.example.com",
				Version: "v1alpha1",
				Kind:    "Order",
				Plural:  "orders",
				Scope:   crispv1alpha1.NamespaceScoped,
				Schema:  &apiextensionsv1.JSONSchemaProps{Type: "object"},
			},
			Queries: crispv1alpha1.Queries{
				List: crispv1alpha1.Query{SQL: "SELECT id, tenant FROM orders WHERE tenant = :namespace"},
			},
			Mapping: crispv1alpha1.Mapping{Name: "id", Namespace: "tenant"},
		},
	}
}

func TestCompileOneVersion(t *testing.T) {
	compiler := newTestCompiler(t)

	resources, err := compiler.Compile(context.Background(), testProjection())
	if err != nil {
		t.Fatalf("Compile() returned error: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("compiled %d resources, want 1", len(resources))
	}

	res := resources[0]
	if got, want := res.Path(), "/apis/store.example.com/v1alpha1/orders"; got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
	if got, want := res.Singular, "order"; got != want {
		t.Errorf("singular = %q, want %q", got, want)
	}
	if got, want := res.ListKind, "OrderList"; got != want {
		t.Errorf("list kind = %q, want %q", got, want)
	}
	if !res.DataSourceReady {
		t.Error("a reachable database was reported as unreachable")
	}
	if res.PoolKey == "" {
		t.Error("no pool key was recorded, so the pool can never be released")
	}

	// The kind has to reach the scheme the router builds, or the endpoint
	// installer cannot encode a response for it. Registration happens per
	// rebuild rather than on a shared scheme, so this is what the router does.
	apiScheme, _ := crispscheme.New()
	registerKinds(apiScheme, []Resource{res})
	if !apiScheme.Recognizes(res.GroupVersion().WithKind("Order")) {
		t.Error("the projected kind was not registered with the scheme")
	}
}

// TestCompileEveryServedVersion covers multi-version projections: one storage
// per version, sharing a pool, each with its own mapping.
func TestCompileEveryServedVersion(t *testing.T) {
	compiler := newTestCompiler(t)

	p := testProjection()
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{
		{
			Name:   "v1beta1",
			Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
			Mapping: &crispv1alpha1.Mapping{
				Name:      "id",
				Namespace: "tenant",
			},
		},
		{
			Name:   "v1",
			Served: ptr(false),
			Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
		},
	}

	resources, err := compiler.Compile(context.Background(), p)
	if err != nil {
		t.Fatalf("Compile() returned error: %v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("compiled %d resources, want 2: the unserved version must be skipped", len(resources))
	}

	versions := []string{resources[0].Version, resources[1].Version}
	if versions[0] != "v1alpha1" || versions[1] != "v1beta1" {
		t.Errorf("versions = %v, want [v1alpha1 v1beta1]", versions)
	}
	if resources[0].PoolKey != resources[1].PoolKey {
		t.Error("two versions of one projection opened different pools")
	}
}

func TestCompileRejectsAVersionWithoutASchema(t *testing.T) {
	compiler := newTestCompiler(t)

	p := testProjection()
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{Name: "v1beta1"}}

	if _, err := compiler.Compile(context.Background(), p); err == nil {
		t.Fatal("a version with neither schema nor schemaFrom was accepted")
	}
}

func TestCompileBorrowsASchema(t *testing.T) {
	compiler := newTestCompiler(t)
	compiler.Schemas = borrowedSchema{schema: &apiextensionsv1.JSONSchemaProps{
		Type:       "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{"spec": {Type: "object"}},
	}}

	p := testProjection()
	p.Spec.Resource.Schema = nil
	p.Spec.Resource.SchemaFrom = &crispv1alpha1.CRDReference{Name: "orders.acme.example.com"}

	resources, err := compiler.Compile(context.Background(), p)
	if err != nil {
		t.Fatalf("Compile() returned error: %v", err)
	}
	if resources[0].Schema == nil {
		t.Fatal("the borrowed schema did not reach the resource")
	}
	if _, found := resources[0].Schema.Properties["spec"]; !found {
		t.Error("the borrowed schema is not the one the resolver returned")
	}
}

// TestCompileWithoutASchemaResolver: schemaFrom without a cluster connection is
// refused rather than quietly serving an unvalidated resource.
func TestCompileWithoutASchemaResolver(t *testing.T) {
	compiler := newTestCompiler(t)

	p := testProjection()
	p.Spec.Resource.Schema = nil
	p.Spec.Resource.SchemaFrom = &crispv1alpha1.CRDReference{Name: "orders.acme.example.com"}

	_, err := compiler.Compile(context.Background(), p)
	if err == nil {
		t.Fatal("schemaFrom was accepted with no way to resolve it")
	}
	if !strings.Contains(err.Error(), "cluster connection") {
		t.Errorf("error %q does not explain what is missing", err)
	}
}

// TestCompileCapsConnections: no projection may raise the number of
// connections opened against a database past the server's ceiling.
func TestCompileCapsConnections(t *testing.T) {
	compiler := newTestCompiler(t)
	compiler.MaxOpenConns = 3

	p := testProjection()
	p.Spec.DataSource.MaxOpenConns = ptr(int32(50))

	if _, err := compiler.Compile(context.Background(), p); err != nil {
		t.Fatalf("Compile() returned error: %v", err)
	}
	if got := compiler.Pools.Len(); got != 1 {
		t.Fatalf("%d pools were opened, want 1", got)
	}
}

// TestCompileSurvivesAnUnreachableDatabase: the resource is still installed,
// and answers 503 until the database comes back.
func TestCompileSurvivesAnUnreachableDatabase(t *testing.T) {
	compiler := newTestCompiler(t)
	compiler.Resolver = fixedResolver{dsn: "/nonexistent-directory/orders.db"}

	resources, err := compiler.Compile(context.Background(), testProjection())
	if err != nil {
		t.Fatalf("Compile() returned error: %v", err)
	}
	if len(resources) != 1 {
		t.Fatalf("compiled %d resources, want 1", len(resources))
	}
	if resources[0].DataSourceReady {
		t.Error("an unreachable database was reported as ready")
	}
	if resources[0].DataSourceError == nil {
		t.Error("nothing recorded why the database could not be reached")
	}
}

func TestCompileRejectsAnInvalidProjection(t *testing.T) {
	compiler := newTestCompiler(t)

	p := testProjection()
	p.Spec.Mapping.Namespace = ""

	if _, err := compiler.Compile(context.Background(), p); err == nil {
		t.Fatal("a namespaced projection with no namespace column was accepted")
	}
}

func TestPoolsAreSharedAcrossProjections(t *testing.T) {
	compiler := newTestCompiler(t)

	first := testProjection()
	second := testProjection()
	second.Name = "orders-copy"
	second.Spec.Resource.Plural = "ordercopies"
	second.Spec.Resource.Kind = "OrderCopy"
	// A different Secret, the same database.
	second.Spec.DataSource.SecretRef.Name = "another-secret"

	for _, p := range []*crispv1alpha1.CustomResourceProjection{first, second} {
		if _, err := compiler.Compile(context.Background(), p); err != nil {
			t.Fatalf("Compile(%s) returned error: %v", p.Name, err)
		}
	}

	if got := compiler.Pools.Len(); got != 1 {
		t.Errorf("%d pools for one database, want 1: a per-database limit would mean nothing", got)
	}
}

func TestSingularAndListKindDefaults(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{Kind: "Order"}
	if got, want := Singular(res), "order"; got != want {
		t.Errorf("Singular() = %q, want %q", got, want)
	}
	if got, want := listKind(res), "OrderList"; got != want {
		t.Errorf("listKind() = %q, want %q", got, want)
	}

	res.Singular, res.ListKind = "bestellung", "Bestellungen"
	if got, want := Singular(res), "bestellung"; got != want {
		t.Errorf("Singular() = %q, want the declared %q", got, want)
	}
	if got, want := listKind(res), "Bestellungen"; got != want {
		t.Errorf("listKind() = %q, want the declared %q", got, want)
	}
}

func ptr[T any](v T) *T { return &v }

var _ projection.SchemaResolver = borrowedSchema{}

// multiVersionProjection serves one kind at two versions, mapping the same
// columns to different places — the reason to add a version at all.
func multiVersionProjection() *crispv1alpha1.CustomResourceProjection {
	p := testProjection()
	p.Spec.Mapping = crispv1alpha1.Mapping{
		Name:      "id",
		Namespace: "tenant",
		Fields: []crispv1alpha1.FieldMapping{
			{Column: "customer", Path: "spec.customer"},
			{Column: "total_cents", Path: "spec.totalCents", Type: crispv1alpha1.FieldTypeInteger},
		},
	}
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{
		Name:   "v1beta1",
		Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"},
		Mapping: &crispv1alpha1.Mapping{
			Name:      "id",
			Namespace: "tenant",
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "customer", Path: "spec.customer"},
				{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeInteger},
			},
		},
	}}
	return p
}

// TestVersionsMustMapTheSameColumns is what a client can rely on when a kind is
// served at two versions: there is no conversion between them, so they have to
// be views of the same columns or a write through one silently drops what the
// other shows.
func TestVersionsMustMapTheSameColumns(t *testing.T) {
	projection := multiVersionProjection()
	projection.Spec.Resource.Versions[0].Mapping = &crispv1alpha1.Mapping{
		Name:      "id",
		Namespace: "tenant",
		Fields: []crispv1alpha1.FieldMapping{
			// The primary version also maps total_cents; this one forgets it.
			{Column: "customer", Path: "spec.customer"},
		},
	}

	_, err := newTestCompiler(t).Compile(context.Background(), projection)
	if err == nil {
		t.Fatal("versions mapping different columns were accepted")
	}
	if !strings.Contains(err.Error(), "total_cents") {
		t.Errorf("error %q does not name the column that would be dropped", err)
	}
	if !strings.Contains(err.Error(), "conversion: None") {
		t.Errorf("error %q does not say how to allow it deliberately", err)
	}
}

// TestConversionNoneAllowsDifferentColumns keeps the deliberate case possible:
// a version that exposes less, said out loud.
func TestConversionNoneAllowsDifferentColumns(t *testing.T) {
	projection := multiVersionProjection()
	projection.Spec.Resource.Conversion = crispv1alpha1.ConversionNone
	projection.Spec.Resource.Versions[0].Mapping = &crispv1alpha1.Mapping{
		Name:      "id",
		Namespace: "tenant",
		Fields:    []crispv1alpha1.FieldMapping{{Column: "customer", Path: "spec.customer"}},
	}

	resources, err := newTestCompiler(t).Compile(context.Background(), projection)
	if err != nil {
		t.Fatalf("Compile() returned error: %v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("compiled %d versions, want 2", len(resources))
	}
}

// TestVersionsSharingOneMappingRoundTrip is the ordinary case: a version added
// for its schema alone inherits the mapping and cannot diverge.
func TestVersionsSharingOneMappingRoundTrip(t *testing.T) {
	projection := multiVersionProjection()
	projection.Spec.Resource.Versions[0].Mapping = nil

	if _, err := newTestCompiler(t).Compile(context.Background(), projection); err != nil {
		t.Fatalf("Compile() returned error: %v", err)
	}
}

// TestDifferentPathsRoundTrip states the boundary precisely: the versions have
// to cover the same columns, not put them in the same place. Mapping a column
// somewhere else is the whole reason to add a version.
func TestDifferentPathsRoundTrip(t *testing.T) {
	projection := multiVersionProjection()
	projection.Spec.Resource.Versions[0].Mapping = &crispv1alpha1.Mapping{
		Name:      "id",
		Namespace: "tenant",
		Fields: []crispv1alpha1.FieldMapping{
			{Column: "customer", Path: "spec.buyer"},
			{Column: "total_cents", Path: "spec.amount.cents", Type: crispv1alpha1.FieldTypeInteger},
		},
	}

	if _, err := newTestCompiler(t).Compile(context.Background(), projection); err != nil {
		t.Fatalf("Compile() rejected versions that map the same columns to different paths: %v", err)
	}
}
