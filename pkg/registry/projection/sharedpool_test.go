package projection

import (
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestProjectionsSharingAPoolKeepTheirOwnSettings is what makes one pool per
// database safe.
//
// The prepared-statement and statement-timeout settings used to live on the
// pool, and the pool key carried them so that two projections could never
// disagree — at the price of one database becoming as many as four pools, each
// bounded separately by MaxOpenConns. They are on the statement now, so the
// pool can be shared. This checks that sharing it does not also share the
// settings, which is the thing that would have gone wrong.
func TestProjectionsSharingAPoolKeepTheirOwnSettings(t *testing.T) {
	pool := newTestPoolFor(t, testSpec())
	no, yes := false, true

	build := func(t *testing.T, apply func(*crispv1alpha1.CustomResourceProjectionSpec)) *REST {
		t.Helper()
		spec := testSpec()
		apply(&spec)
		storages, err := New("orders", spec, pool, nil, nil)
		if err != nil {
			t.Fatalf("New() returned error: %v", err)
		}
		return storages.read
	}

	prepared := build(t, func(s *crispv1alpha1.CustomResourceProjectionSpec) {
		s.DataSource.PreparedStatements = &yes
	})
	unprepared := build(t, func(s *crispv1alpha1.CustomResourceProjectionSpec) {
		s.DataSource.PreparedStatements = &no
	})

	if !prepared.list.statement.Prepared {
		t.Error("a projection asking for prepared statements did not get them")
	}
	if unprepared.list.statement.Prepared {
		t.Error("a projection that turned prepared statements off got them anyway, from the pool it shares")
	}

	// Both still answer, which is the point of the setting being per statement
	// rather than per connection.
	for name, store := range map[string]*REST{"prepared": prepared, "unprepared": unprepared} {
		list, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
		if err != nil {
			t.Fatalf("%s: List() returned error: %v", name, err)
		}
		if _, err := store.Get(namespacedContext("acme"), "order-1001", &metav1.GetOptions{}); err != nil {
			t.Fatalf("%s: Get() returned error: %v", name, err)
		}
		_ = list
	}
}

// TestStatementTimeoutIsPerStatementNotPerPool covers the other half of the
// same change, for the setting that made the split look necessary.
//
// It is applied with SET LOCAL inside the transaction that runs the query, so
// it dies with that transaction and was never carried by the connection — which
// is exactly why two projections that disagree can share one.
func TestStatementTimeoutIsPerStatementNotPerPool(t *testing.T) {
	pool := newTestPoolFor(t, testSpec())
	yes := true

	spec := testSpec()
	spec.DataSource.StatementTimeout = &yes
	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	// SQLite cannot be asked to bound a statement, so asking for it means
	// nothing and the statement must not claim otherwise — a claim would put
	// every query into a transaction for no benefit.
	if crispsql.SupportsStatementTimeout("sqlite") {
		t.Skip("this driver can enforce a statement timeout; the assertion below is for one that cannot")
	}
	if storages.read.list.statement.EnforceTimeout {
		t.Error("a SQLite projection asked the database to enforce a timeout it cannot enforce")
	}

	// The prelude statements of a transactional write get the same treatment as
	// the one that returns rows; a timeout covering only the last statement
	// would leave the others unbounded.
	writable := testSpec()
	writable.DataSource.StatementTimeout = &yes
	writable.Queries.Create = &crispv1alpha1.Query{
		Statements: []string{
			"INSERT INTO order_events (id, tenant, event) VALUES (:id, :tenant, 'created')",
			`INSERT INTO orders (id, tenant, customer, status, total_cents, line_items, updated_at)
			 VALUES (:id, :tenant, :customer, :status, :total_cents, :line_items, '1')`,
		},
	}
	built, err := New("orders", writable, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error for the transactional write: %v", err)
	}
	statements := built.writable.create.all()
	if len(statements) != 2 {
		t.Fatalf("the create compiled to %d statements, want 2", len(statements))
	}
	for i, statement := range statements {
		if statement.EnforceTimeout != statements[0].EnforceTimeout {
			t.Errorf("statement %d disagrees with the first about enforcing the timeout", i)
		}
	}
}
