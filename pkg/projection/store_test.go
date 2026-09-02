package projection

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

func incrementalProjection() *crispv1alpha1.CustomResourceProjection {
	return &crispv1alpha1.CustomResourceProjection{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: crispv1alpha1.CustomResourceProjectionSpec{
			DataSource: crispv1alpha1.DataSource{Driver: "postgres"},
			Resource: crispv1alpha1.ProjectedResource{
				Group:   "store.example.com",
				Version: "v1alpha1",
				Kind:    "Order",
				Plural:  "orders",
				Scope:   crispv1alpha1.NamespaceScoped,
				Schema:  &apiextensionsv1.JSONSchemaProps{Type: "object"},
			},
			Queries: crispv1alpha1.Queries{
				List: crispv1alpha1.Query{SQL: "SELECT id, tenant, updated_at FROM orders WHERE tenant = :namespace"},
			},
			Mapping: crispv1alpha1.Mapping{
				Name:            "id",
				Namespace:       "tenant",
				ResourceVersion: "updated_at",
			},
			Watch: &crispv1alpha1.WatchSpec{
				Query: &crispv1alpha1.Query{
					SQL: "SELECT id, tenant, updated_at FROM orders WHERE (:since::text IS NULL OR updated_at > :since) ORDER BY updated_at",
				},
			},
		},
	}
}

// TestValidateRejectsClientAssignedVersion covers the mistake that silently
// breaks incremental polling: writing the client's resourceVersion into the
// column the poll reads forward from.
func TestValidateRejectsClientAssignedVersion(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*crispv1alpha1.CustomResourceProjection)
	}{
		{
			name: "create",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Queries.Create = &crispv1alpha1.Query{
					SQL: "INSERT INTO orders (id, tenant, updated_at) VALUES (:id, :tenant, :updated_at)",
				}
			},
		},
		{
			name: "update",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Queries.Update = &crispv1alpha1.Query{
					SQL: "UPDATE orders SET updated_at = :updated_at WHERE id = :name",
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := incrementalProjection()
			tc.apply(p)

			err := Validate(p)
			if err == nil {
				t.Fatal("a projection writing a client-supplied resourceVersion was accepted")
			}
			if !strings.Contains(err.Error(), "updated_at") || !strings.Contains(err.Error(), tc.name) {
				t.Errorf("error %q does not name the column and the query it came from", err)
			}
		})
	}
}

// TestValidateAcceptsDatabaseAssignedVersion is the other half: the same
// projection is fine when the database owns the column and the client's value
// is only a precondition.
func TestValidateAcceptsDatabaseAssignedVersion(t *testing.T) {
	p := incrementalProjection()
	p.Spec.Queries.Update = &crispv1alpha1.Query{
		SQL: `UPDATE orders SET customer = :customer, updated_at = clock_timestamp()
		      WHERE id = :name AND (:resourceVersion::text IS NULL OR updated_at = :resourceVersion)`,
	}

	if err := Validate(p); err != nil {
		t.Fatalf("Validate() returned error: %v", err)
	}
}

// TestValidateIgnoresVersionWritesWithoutIncrementalWatch keeps the check
// scoped: a projection that polls fully has no monotonicity requirement.
func TestValidateIgnoresVersionWritesWithoutIncrementalWatch(t *testing.T) {
	p := incrementalProjection()
	p.Spec.Watch.Query = nil
	p.Spec.Queries.Create = &crispv1alpha1.Query{
		SQL: "INSERT INTO orders (id, tenant, updated_at) VALUES (:id, :tenant, :updated_at)",
	}

	if err := Validate(p); err != nil {
		t.Fatalf("Validate() returned error: %v", err)
	}
}

func writeManifest(t *testing.T, dir, name, content string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
}

const validManifest = `apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: orders
spec:
  dataSource:
    driver: sqlite
    secretRef: {name: orders-db, namespace: kube-crisp}
  resource:
    group: store.example.com
    version: v1alpha1
    kind: Order
    plural: orders
    scope: Namespaced
    schema:
      type: object
  queries:
    list:
      sql: SELECT id, tenant FROM orders WHERE tenant = :namespace
  mapping:
    name: id
    namespace: tenant
`

// TestLoadDirReadsOnlyProjections: a directory may hold Secrets, ConfigMaps, or
// anything else, and a loader that choked on them would make the simple case
// awkward.
func TestLoadDirReadsOnlyProjections(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "orders.yaml", validManifest)
	writeManifest(t, dir, "secret.yaml", `apiVersion: v1
kind: Secret
metadata:
  name: orders-db
type: Opaque
stringData:
  dsn: file:test.db
`)
	writeManifest(t, dir, "notes.txt", "not a manifest at all")

	projections, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() returned error: %v", err)
	}
	if len(projections) != 1 {
		t.Fatalf("loaded %d projections, want 1", len(projections))
	}
	if got, want := projections[0].Name, "orders"; got != want {
		t.Errorf("projection = %q, want %q", got, want)
	}
}

func TestLoadDirReadsMultipleDocuments(t *testing.T) {
	dir := t.TempDir()
	second := strings.Replace(validManifest, "name: orders", "name: orders-two", 1)
	second = strings.Replace(second, "plural: orders", "plural: ordertwos", 1)
	writeManifest(t, dir, "both.yaml", validManifest+"\n---\n"+second)

	projections, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir() returned error: %v", err)
	}
	if len(projections) != 2 {
		t.Fatalf("loaded %d projections, want 2", len(projections))
	}
}

func TestLoadDirRejectsAnInvalidProjection(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "orders.yaml", strings.Replace(validManifest, "    namespace: tenant\n", "", 1))

	if _, err := LoadDir(dir); err == nil {
		t.Fatal("a namespaced projection with no namespace column was loaded")
	}
}

func TestLoadDirRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	writeManifest(t, dir, "orders.yaml", validManifest+"  notAField: surprise\n")

	if _, err := LoadDir(dir); err == nil {
		t.Fatal("a manifest with an unknown field was accepted")
	}
}

func TestLoadDirOnAMissingDirectory(t *testing.T) {
	if _, err := LoadDir(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("LoadDir() succeeded on a directory that does not exist")
	}
}

// TestValidateRejects walks the checks a projection has to pass before
// anything tries to serve it.
func TestValidateRejects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*crispv1alpha1.CustomResourceProjection)
		wants string
	}{
		{"no group", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Group = ""
		}, "group is required"},
		{"no plural", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Plural = ""
		}, "plural is required"},
		{"upper-case plural", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Plural = "Orders"
		}, "lowercase"},
		// A generated role names the group verbatim, so a projection claiming
		// one Kubernetes owns is a grant on the cluster's own resources. See
		// reservedgroup.go.
		{"a group Kubernetes owns", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Group = "rbac.authorization.k8s.io"
			p.Spec.Resource.Plural = "clusterroles"
		}, "reserved for Kubernetes"},
		{"a built-in group Kubernetes owns", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Group = "apps"
		}, "which Kubernetes owns"},
		{"unknown scope", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Scope = "Galactic"
		}, "Namespaced or Cluster"},
		{"no schema", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Schema = nil
		}, "schema or spec.resource.schemaFrom"},
		{"both schemas", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.SchemaFrom = &crispv1alpha1.CRDReference{Name: "orders.acme.example.com"}
		}, "mutually exclusive"},
		{"no list query", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Queries.List.SQL = "  "
		}, "queries.list.sql is required"},
		{"duplicate version", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{
				{Name: "v1alpha1", Schema: &apiextensionsv1.JSONSchemaProps{Type: "object"}},
			}
		}, "declared twice"},
		{"version without a schema", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{Name: "v1beta1"}}
		}, "schema or schemaFrom"},
		{"version with a broken mapping", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{
				Name:    "v1beta1",
				Schema:  &apiextensionsv1.JSONSchemaProps{Type: "object"},
				Mapping: &crispv1alpha1.Mapping{Name: "id"},
			}}
		}, "namespace is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := incrementalProjection()
			p.Spec.Watch = nil
			tc.apply(p)

			err := Validate(p)
			if err == nil {
				t.Fatal("Validate() accepted it")
			}
			if !strings.Contains(err.Error(), tc.wants) {
				t.Errorf("error %q does not mention %q", err, tc.wants)
			}
		})
	}
}

func TestPoolKeyIdentifiesTheDatabase(t *testing.T) {
	source := func(secret string) crispv1alpha1.DataSource {
		return crispv1alpha1.DataSource{
			Driver:    "postgres",
			SecretRef: crispv1alpha1.SecretReference{Namespace: "kube-crisp", Name: secret},
		}
	}

	const dsn = "postgres://user:pass@db:5432/store"

	// Same connection string through different Secrets: one pool, or a
	// per-database connection limit would mean nothing.
	if PoolKey(source("a"), dsn) != PoolKey(source("b"), dsn) {
		t.Error("the same database through two Secrets produced two pool keys")
	}

	// A rotated credential is a different pool, which is what makes rotation
	// take effect.
	if PoolKey(source("a"), dsn) == PoolKey(source("a"), dsn+"?sslmode=require") {
		t.Error("a changed connection string reused the pool")
	}

	// And the key carries no credentials, since it is logged.
	if strings.Contains(PoolKey(source("a"), dsn), "pass") {
		t.Errorf("the pool key leaks the connection string: %s", PoolKey(source("a"), dsn))
	}
}

// TestValidateRejectsAClientVersionWrittenByATransaction closes the gap the
// single-statement form of this check left open.
//
// A write expressed as spec.queries.create.statements leaves sql empty, so a
// check reading only that field passed every multi-statement write — which is
// exactly the shape where the version is written by one statement and the row
// returned by another.
func TestValidateRejectsAClientVersionWrittenByATransaction(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(p *crispv1alpha1.CustomResourceProjection)
	}{
		{
			name: "create",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Queries.Create = &crispv1alpha1.Query{
					Statements: []string{
						"INSERT INTO order_audit (id, at) VALUES (:id, now())",
						"INSERT INTO orders (id, tenant, updated_at) VALUES (:id, :tenant, :updated_at) RETURNING id, tenant, updated_at",
					},
				}
			},
		},
		{
			name: "updateStatus",
			apply: func(p *crispv1alpha1.CustomResourceProjection) {
				p.Spec.Queries.UpdateStatus = &crispv1alpha1.Query{
					Statements: []string{
						"UPDATE orders SET updated_at = :updated_at WHERE id = :name",
						"SELECT id, tenant, updated_at FROM orders WHERE id = :name",
					},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := incrementalProjection()
			tc.apply(p)

			err := Validate(p)
			if err == nil {
				t.Fatal("a transactional write of a client-supplied resourceVersion was accepted")
			}
			if !strings.Contains(err.Error(), "updated_at") || !strings.Contains(err.Error(), tc.name) {
				t.Errorf("error %q does not name the column and the query it came from", err)
			}
		})
	}
}

// TestValidateAcceptsATransactionThatLetsTheDatabaseAssignTheVersion is the
// other half, so the check above is not simply rejecting every transaction.
func TestValidateAcceptsATransactionThatLetsTheDatabaseAssignTheVersion(t *testing.T) {
	p := incrementalProjection()
	p.Spec.Queries.Create = &crispv1alpha1.Query{
		Statements: []string{
			"INSERT INTO order_audit (id, at) VALUES (:id, now())",
			"INSERT INTO orders (id, tenant, updated_at) VALUES (:id, :tenant, clock_timestamp()) RETURNING id, tenant, updated_at",
		},
	}

	if err := Validate(p); err != nil {
		t.Fatalf("Validate() returned error: %v", err)
	}
}

// TestValidateRejectsAStatementTimeoutTheDriverCannotHonour: silently doing
// nothing is the outcome worth avoiding, since the setting exists precisely to
// bound a query that would otherwise run away.
func TestValidateRejectsAStatementTimeoutTheDriverCannotHonour(t *testing.T) {
	enabled := true

	for _, driver := range []string{"mysql", "sqlite"} {
		t.Run(driver, func(t *testing.T) {
			p := incrementalProjection()
			p.Spec.DataSource.Driver = driver
			p.Spec.DataSource.StatementTimeout = &enabled

			err := Validate(p)
			if err == nil {
				t.Fatalf("a statement timeout was accepted on %s", driver)
			}
			if !strings.Contains(err.Error(), "statementTimeout") || !strings.Contains(err.Error(), driver) {
				t.Errorf("error %q does not name the setting and the driver", err)
			}
		})
	}

	postgres := incrementalProjection()
	postgres.Spec.DataSource.StatementTimeout = &enabled
	if err := Validate(postgres); err != nil {
		t.Errorf("Validate() rejected a statement timeout on postgres: %v", err)
	}
}

// TestPoolKeyIsTheDataSourceAlone: one database is one pool, whatever the
// projections reaching it disagree about.
//
// The key used to carry the prepared-statement and statement-timeout settings,
// so that projections disagreeing about either got pools of their own. That
// turned one database into as many as four pools, each bounded separately by
// MaxOpenConns — which made --max-open-conns-per-datasource a limit on a pool
// rather than on a database, twice over in the e2e cluster alone. Both settings
// now travel on the statement, where neither was ever a property of the
// connection.
func TestPoolKeyIsTheDataSourceAlone(t *testing.T) {
	const dsn = "postgres://user:pass@db.example:5432/store"
	enabled, disabled := true, false

	base := crispv1alpha1.DataSource{Driver: "postgres"}
	timed := base
	timed.StatementTimeout = &enabled
	untimed := base
	untimed.StatementTimeout = &disabled
	unprepared := base
	unprepared.PreparedStatements = &disabled

	for _, tc := range []struct {
		name string
		ds   crispv1alpha1.DataSource
	}{
		{"a statement timeout", timed},
		{"no statement timeout", untimed},
		{"no prepared statements", unprepared},
	} {
		if PoolKey(tc.ds, dsn) != PoolKey(base, dsn) {
			t.Errorf("%s produced a pool of its own for the same database", tc.name)
		}
	}

	// The connection string is what separates two pools, which is what makes
	// credential rotation take effect.
	if PoolKey(timed, dsn) == PoolKey(timed, dsn+"?sslmode=require") {
		t.Error("a changed connection string did not produce a new pool")
	}
	// And the driver, since the same string means different things to each.
	mysql := base
	mysql.Driver = "mysql"
	if PoolKey(mysql, dsn) == PoolKey(base, dsn) {
		t.Error("two drivers share a pool key")
	}
}

// TestValidateRejectsNotifyItCannotHonour is the same set of rules as the CRD's,
// enforced where a projection is compiled — the CRD says them earlier, this says
// them for a projection that reached the apiserver some other way.
func TestValidateRejectsNotifyItCannotHonour(t *testing.T) {
	for _, tc := range []struct {
		name  string
		want  string
		apply func(p *crispv1alpha1.CustomResourceProjection)
	}{
		{"a driver with no notifications", "cannot", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.DataSource.Driver = "mysql"
			p.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders_changed"}
		}},
		{"watching turned off", "disabled", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders_changed"}
			p.Spec.Watch.Disabled = true
		}},
		{"a channel that is not an identifier", "notify channel", func(p *crispv1alpha1.CustomResourceProjection) {
			p.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders;DROP"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := incrementalProjection()
			tc.apply(p)

			err := Validate(p)
			if err == nil {
				t.Fatal("a notify setting that cannot be honoured was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}

	valid := incrementalProjection()
	valid.Spec.Watch.Notify = &crispv1alpha1.NotifySpec{Channel: "orders_changed"}
	if err := Validate(valid); err != nil {
		t.Errorf("Validate() rejected a valid notify setting: %v", err)
	}
}

// TestValidateRejectsAnUnknownDriver: the registry is what a driver name is
// checked against now, and the error has to name the alternatives — "unsupported
// driver" on its own sends someone to the source.
func TestValidateRejectsAnUnknownDriver(t *testing.T) {
	p := incrementalProjection()
	p.Spec.DataSource.Driver = "postgress"

	err := Validate(p)
	if err == nil {
		t.Fatal("an unregistered driver was accepted")
	}
	for _, want := range []string{"postgress", "postgres", "mysql", "sqlite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
