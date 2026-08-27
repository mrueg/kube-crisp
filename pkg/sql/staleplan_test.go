package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// stalePlanConn models what PostgreSQL does after a migration: a statement
// prepared before the schema changed is refused, while the same SQL prepared
// afresh runs. The unprepared path is Queryer/Execer, which is what
// database/sql uses when it is not handed a statement.
type stalePlanConn struct{}

// staleFrom counts calls; once armed, every prepared statement fails.
var (
	staleArmed   atomic.Bool
	staleQueries atomic.Int64
	staleExecs   atomic.Int64
)

var errStalePlan = errors.New(`ERROR: cached plan must not change result type (SQLSTATE 0A000)`)

type stalePlanDriver struct{}

func (stalePlanDriver) Open(string) (driver.Conn, error) { return &stalePlanConn{}, nil }

func (c *stalePlanConn) Prepare(query string) (driver.Stmt, error) {
	return &stalePlanStmt{query: query}, nil
}
func (c *stalePlanConn) Close() error              { return nil }
func (c *stalePlanConn) Begin() (driver.Tx, error) { return nil, errors.New("no transactions") }

// QueryContext and ExecContext are the unprepared path: a fresh plan every
// time, so they always work.
func (c *stalePlanConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	staleQueries.Add(1)
	return &emptyRows{}, nil
}

func (c *stalePlanConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	staleExecs.Add(1)
	return driver.RowsAffected(1), nil
}

type stalePlanStmt struct{ query string }

func (s *stalePlanStmt) Close() error  { return nil }
func (s *stalePlanStmt) NumInput() int { return -1 }

func (s *stalePlanStmt) Exec([]driver.Value) (driver.Result, error) {
	if staleArmed.Load() {
		return nil, errStalePlan
	}
	return driver.RowsAffected(1), nil
}

func (s *stalePlanStmt) Query([]driver.Value) (driver.Rows, error) {
	if staleArmed.Load() {
		return nil, errStalePlan
	}
	return &emptyRows{}, nil
}

type emptyRows struct{}

func (r *emptyRows) Columns() []string              { return []string{"id"} }
func (r *emptyRows) Close() error                   { return nil }
func (r *emptyRows) Next(dest []driver.Value) error { return io.EOF }

func init() {
	sql.Register("stale-plan-test", stalePlanDriver{})
	if err := Register(Driver{
		Name:         "stalePlanTest",
		SQLDriver:    "stale-plan-test",
		Placeholders: PlaceholderQuestion,
	}); err != nil {
		panic(err)
	}
}

// A migration must not take a projection down and leave it down.
//
// PostgreSQL refuses a prepared statement whose table has gained or lost a
// column, and the statement stays in the cache — so without evicting it, every
// request through the projection fails identically until the process restarts.
func TestAPreparedStatementIsRePreparedWhenTheSchemaChanges(t *testing.T) {
	t.Cleanup(func() { staleArmed.Store(false) })
	staleArmed.Store(false)

	pool, err := Open(PoolOptions{Driver: "stalePlanTest", DSN: "x", PreparedStatements: true})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	ctx := context.Background()

	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the first query returned error: %v", err)
	}
	if pool.PreparedCount() != 1 {
		t.Fatalf("the statement was not cached; there is nothing to go stale")
	}

	// The migration lands.
	staleArmed.Store(true)

	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the query after the schema change returned error: %v", err)
	}
	if pool.PreparedCount() != 0 {
		t.Error("the statement bound to the old schema is still cached, so every request " +
			"through it fails until the process restarts")
	}
}

// The same for a write, which must not be replayed by the retry: the error is
// raised while the plan is revalidated, before anything is written.
func TestAPreparedExecIsRePreparedWhenTheSchemaChanges(t *testing.T) {
	t.Cleanup(func() { staleArmed.Store(false) })
	staleArmed.Store(false)

	pool, err := Open(PoolOptions{Driver: "stalePlanTest", DSN: "x", PreparedStatements: true})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	stmt, err := pool.Prepare("UPDATE orders SET status = 'shipped'", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	ctx := context.Background()

	if _, err := pool.Exec(ctx, stmt, nil); err != nil {
		t.Fatalf("the first exec returned error: %v", err)
	}

	before := staleExecs.Load()
	staleArmed.Store(true)

	if _, err := pool.Exec(ctx, stmt, nil); err != nil {
		t.Fatalf("the exec after the schema change returned error: %v", err)
	}
	if pool.PreparedCount() != 0 {
		t.Error("the statement bound to the old schema is still cached")
	}
	if got := staleExecs.Load() - before; got != 1 {
		t.Errorf("the retry ran the write %d times, want exactly 1", got)
	}
}
