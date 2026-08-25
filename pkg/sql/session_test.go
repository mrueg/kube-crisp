package sql

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestValidateSessionVariableName pins the rule down, since the name is the one
// part of a session variable that goes into the statement text rather than
// being bound. It is hand-scanned rather than matched against a regular
// expression, so the edges are worth stating.
func TestValidateSessionVariableName(t *testing.T) {
	valid := []string{
		"app",
		"app.tenant",
		"_leading",
		"a.b.c",
		"app.tenant_id",
		"a1.b2",
	}
	for _, name := range valid {
		if err := ValidateSessionVariableName(name); err != nil {
			t.Errorf("ValidateSessionVariableName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{
		"",
		".",
		".leading",
		"trailing.",
		"a..b",
		"1leading",
		"a.1b",
		"has space",
		"has-dash",
		"quote'; DROP TABLE orders; --",
		"semi;colon",
		"a\nb",
		"unicodé",
	}
	for _, name := range invalid {
		if err := ValidateSessionVariableName(name); err == nil {
			t.Errorf("ValidateSessionVariableName(%q) = nil, want an error", name)
		}
	}
}

// TestSupportsSessionVariables: SQLite has no session state, so a projection
// asking for it there is a mistake worth reporting at load time rather than a
// silent no-op at query time.
func TestSupportsSessionVariables(t *testing.T) {
	for driver, want := range map[string]bool{
		"postgres": true,
		"mysql":    true,
		"sqlite":   false,
		"":         false,
	} {
		if got := SupportsSessionVariables(driver); got != want {
			t.Errorf("SupportsSessionVariables(%q) = %v, want %v", driver, got, want)
		}
	}
}

// TestMySQLVariableName: a MySQL user variable cannot hold a dot, so the dotted
// name PostgreSQL uses to namespace a setting has to be flattened.
func TestMySQLVariableName(t *testing.T) {
	for in, want := range map[string]string{
		"app.tenant":   "app_tenant",
		"app":          "app",
		"a.b.c":        "a_b_c",
		"already_flat": "already_flat",
	} {
		if got := mysqlVariableName(in); got != want {
			t.Errorf("mysqlVariableName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestClearSessionIsANoOpOffMySQL: PostgreSQL scopes a setting to the
// transaction, so there is nothing to undo; only MySQL's connection-scoped user
// variables outlive it.
func TestClearSessionIsANoOpOffMySQL(t *testing.T) {
	pool := newTestPool(t, true)

	// A nil transaction would panic if this tried to use one, which is the
	// point: on SQLite it must not.
	if err := pool.clearSession(context.Background(), nil, []SessionVariable{
		{Name: "app.tenant", Value: "acme"},
	}); err != nil {
		t.Errorf("clearSession() on sqlite returned error: %v", err)
	}
}

// TestRewrittenHandlesTheFixedStatements: the internal helper rewrites the
// constant statements applySession uses, and falls back to the original text
// for a driver it cannot rewrite for rather than returning nothing.
func TestRewrittenHandlesTheFixedStatements(t *testing.T) {
	pool := newTestPool(t, true)

	if got := pool.rewritten("SELECT set_config(:name, :value, true)"); got == "" {
		t.Error("rewritten() returned nothing")
	}

	unknown := &Pool{driver: "oracle"}
	const stmt = "SELECT :name"
	if got := unknown.rewritten(stmt); got != stmt {
		t.Errorf("rewritten() for an unsupported driver = %q, want the statement unchanged", got)
	}
}

// TestStatementTimeoutIsPostgresOnly: MySQL's max_execution_time covers
// read-only SELECTs rather than every statement, and SQLite has nothing, so
// asking for it there is refused rather than silently doing nothing.
func TestStatementTimeoutIsPostgresOnly(t *testing.T) {
	for driver, want := range map[string]bool{
		"postgres": true,
		"mysql":    false,
		"sqlite":   false,
		"nonesuch": false,
	} {
		if got := SupportsStatementTimeout(driver); got != want {
			t.Errorf("SupportsStatementTimeout(%q) = %v, want %v", driver, got, want)
		}
	}
}

// TestStatementTimeoutIsInertOnADriverThatCannotSetIt: a pool asked for one on
// SQLite must not pay for a transaction per query and get nothing for it.
func TestStatementTimeoutIsInertOnADriverThatCannotSetIt(t *testing.T) {
	pool, err := Open(PoolOptions{
		Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "inert.db"), StatementTimeout: true,
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	defer func() { _ = pool.Close() }()

	if pool.statementTimeout {
		t.Error("SQLite pool kept the statement timeout, so every query would run in a transaction for nothing")
	}
	if got := clientDeadline(pool.EnforceTimeoutOn(true), time.Second); got != time.Second {
		t.Errorf("clientDeadline = %v, want the query's own %v when nothing enforces it at the database", got, time.Second)
	}
}

// TestClientDeadlineOutlastsTheDatabases is the property that makes the feature
// work at all.
//
// The two deadlines otherwise race, and this server's usually wins — so the
// caller is told "context deadline exceeded" and whether the database stopped
// working depends on a cancellation landing behind it. Waiting a little longer
// is what lets the database's own answer arrive.
func TestClientDeadlineOutlastsTheDatabases(t *testing.T) {
	pool, err := Open(PoolOptions{
		Driver: "postgres", DSN: "postgres://u:p@127.0.0.1:1/none", StatementTimeout: true,
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	defer func() { _ = pool.Close() }()

	for _, tc := range []struct {
		timeout time.Duration
		want    time.Duration
	}{
		// A tenth, floored so a short query still has room for the round trip.
		{timeout: 50 * time.Millisecond, want: 150 * time.Millisecond},
		{timeout: time.Second, want: 1100 * time.Millisecond},
		// And capped, so a long query does not add a long tail on top.
		{timeout: time.Minute, want: 61 * time.Second},
	} {
		if got := clientDeadline(true, tc.timeout); got != tc.want {
			t.Errorf("clientDeadline(%v) = %v, want %v", tc.timeout, got, tc.want)
		}
		if clientDeadline(true, tc.timeout) <= tc.timeout {
			t.Errorf("clientDeadline(%v) does not outlast the database's deadline", tc.timeout)
		}
	}

	// Nothing to outlast when there is no timeout to enforce.
	if got := clientDeadline(true, 0); got != 0 {
		t.Errorf("clientDeadline(0) = %v, want 0", got)
	}
}

// TestIsStatementTimeout separates the database giving up on a statement from
// the client giving up on waiting for one. Both mean "did not finish in time" to
// a caller, so both are answered as a timeout — but only the first says the work
// actually stopped, and only PostgreSQL reports it.
func TestIsStatementTimeout(t *testing.T) {
	if !IsStatementTimeout(&pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"}) {
		t.Error("a PostgreSQL query_canceled was not recognised")
	}
	// A constraint violation is not a timeout, and must not be answered as one.
	if IsStatementTimeout(&pgconn.PgError{Code: "23505"}) {
		t.Error("a unique violation was read as a timeout")
	}
	if IsStatementTimeout(errors.New("timeout: context deadline exceeded")) {
		t.Error("a client-side deadline was read as the database having stopped")
	}
	if IsStatementTimeout(nil) {
		t.Error("no error was read as a timeout")
	}
}
