package projection

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// sharedIDDB holds one order id in two tenants, which is what an identity of
// (tenant, id) allows and what a statement keyed on the id alone gets wrong.
func sharedIDDB(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "orders.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE orders (
			id TEXT NOT NULL, tenant TEXT NOT NULL, customer TEXT NOT NULL,
			status TEXT NOT NULL, total_cents INTEGER NOT NULL,
			line_items TEXT NOT NULL, updated_at TEXT NOT NULL,
			PRIMARY KEY (tenant, id))`,
		`INSERT INTO orders VALUES ('order-1','acme','ada','pending',100,'[]','1')`,
		`INSERT INTO orders VALUES ('order-1','globex','alan','pending',200,'[]','2')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seeding database: %v", err)
		}
	}
	return path
}

func newSharedIDStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec) (*WritableREST, *sql.DB) {
	t.Helper()

	path := sharedIDDB(t)
	pool, err := crispsql.Open(crispsql.PoolOptions{Driver: "sqlite", DSN: path, PreparedStatements: true})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return storages.writable, db
}

func customerOf(t *testing.T, db *sql.DB, tenant string) string {
	t.Helper()
	var customer string
	if err := db.QueryRow(`SELECT customer FROM orders WHERE tenant = ? AND id = 'order-1'`, tenant).
		Scan(&customer); err != nil {
		t.Fatalf("reading %s's order-1: %v", tenant, err)
	}
	return customer
}

// The update every case below gets wrong: keyed on the id alone.
const updateByIDAlone = `UPDATE orders SET customer = :customer,
		          updated_at = CAST(CAST(updated_at AS INTEGER) + 10 AS TEXT)
		      WHERE id = :name`

const returningOrder = ` RETURNING id, tenant, customer, status, total_cents, line_items, updated_at`

// TestAWriteThatReachesAnotherTenantsRowIsRefused is a regression test.
//
// The reads refuse a row from another namespace; a single-object write did not
// look. An update keyed on the id alone, where the identity is (tenant, id),
// rewrote every tenant's order-1, and with RETURNING answered from whichever
// row came first — globex's, to an acme caller, as often as not. It is a 500
// now. As a transaction it also leaves nothing written; as a single statement
// the write has already committed, and the error says so.
func TestAWriteThatReachesAnotherTenantsRowIsRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		query      crispv1alpha1.Query
		rolledBack bool
	}{
		"single statement with RETURNING": {
			query: crispv1alpha1.Query{SQL: updateByIDAlone + returningOrder},
		},
		"single statement without RETURNING": {
			query: crispv1alpha1.Query{SQL: updateByIDAlone},
		},
		"transaction with RETURNING": {
			query: crispv1alpha1.Query{Statements: []string{
				`UPDATE orders SET status = status WHERE tenant = :namespace AND id = :name`,
				updateByIDAlone + returningOrder,
			}},
			rolledBack: true,
		},
		"transaction without RETURNING": {
			query: crispv1alpha1.Query{Statements: []string{
				`UPDATE orders SET status = status WHERE tenant = :namespace AND id = :name`,
				updateByIDAlone,
			}},
			rolledBack: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			spec := writableSpec()
			spec.Queries.Update = &tc.query
			store, db := newSharedIDStorage(t, spec)

			order := newOrder("order-1", "mallory", 100)
			result, _, err := store.Update(namespacedContext("acme"), "order-1",
				restUpdate(order), nil, nil, false, &metav1.UpdateOptions{})
			if !errors.IsInternalError(err) {
				t.Fatalf("Update() = %v, %v; want an internal error, since the statement reached another tenant's row",
					result, err)
			}
			if result != nil {
				t.Errorf("Update() answered with an object alongside the error: %v", result)
			}

			if tc.rolledBack {
				if got := customerOf(t, db, "globex"); got != "alan" {
					t.Errorf("globex's order-1 has customer %q, want %q: the transaction was not rolled back", got, "alan")
				}
				if got := customerOf(t, db, "acme"); got != "ada" {
					t.Errorf("acme's order-1 has customer %q, want %q: the transaction was not rolled back", got, "ada")
				}
			} else if !strings.Contains(err.Error(), "not been rolled back") {
				t.Errorf("Update() error %q does not say the write it refused has already landed", err)
			}
		})
	}
}

// TestADeleteThatReachesAnotherTenantsRowIsRefused is the same for a delete,
// which checked only for a statement that removed nothing.
func TestADeleteThatReachesAnotherTenantsRowIsRefused(t *testing.T) {
	spec := writableSpec()
	spec.Queries.Delete = &crispv1alpha1.Query{Statements: []string{
		`UPDATE orders SET status = status WHERE tenant = :namespace AND id = :name`,
		`DELETE FROM orders WHERE id = :name`,
	}}
	store, db := newSharedIDStorage(t, spec)

	_, _, err := store.Delete(namespacedContext("acme"), "order-1", nil, &metav1.DeleteOptions{})
	if !errors.IsInternalError(err) {
		t.Fatalf("Delete() error = %v, want an internal error", err)
	}
	for _, tenant := range []string{"acme", "globex"} {
		customerOf(t, db, tenant) // fails the test if the row is gone
	}
}

// TestAWriteMustReturnTheRowItWasAskedFor covers a statement that reached one
// row, but not the one the request named.
func TestAWriteMustReturnTheRowItWasAskedFor(t *testing.T) {
	store, _ := newSharedIDStorage(t, writableSpec())

	row := func(tenant, id string) crispsql.Row {
		return crispsql.Row{
			"id": id, "tenant": tenant, "customer": "ada", "status": "pending",
			"total_cents": int64(1), "line_items": "[]", "updated_at": "1",
		}
	}
	for name, tc := range map[string]struct {
		verb   string
		row    crispsql.Row
		refuse bool
	}{
		"the row asked for":                      {"update", row("acme", "order-1"), false},
		"another namespace":                      {"update", row("globex", "order-1"), true},
		"another name":                           {"update", row("acme", "order-2"), true},
		"another namespace, on a create":         {"create", row("globex", "order-1"), true},
		"a name the database chose, on a create": {"create", row("acme", "order-77"), false},
	} {
		t.Run(name, func(t *testing.T) {
			err := store.oneRow(tc.verb, "acme", "order-1")([]crispsql.Row{tc.row}, 1)
			if tc.refuse && err == nil {
				t.Errorf("a %s that returned %s/%s was not refused", tc.verb, tc.row["tenant"], tc.row["id"])
			}
			if !tc.refuse && err != nil {
				t.Errorf("a %s that returned %s/%s was refused: %v", tc.verb, tc.row["tenant"], tc.row["id"], err)
			}
		})
	}
}
