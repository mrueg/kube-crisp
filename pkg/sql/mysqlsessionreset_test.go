package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// A database/sql driver standing in for go-sql-driver/mysql where it matters
// here: user variables live on the connection, and the connection implements
// SessionResetter and Validator, so database/sql keeps it after a rollback
// caused by a cancelled context — and its session reset leaves user variables
// exactly as they were, as the real driver's does.
//
// It understands four statements: SET @name = ? and SET @name = NULL, which
// set and clear a variable on the connection; SELECT @name, which reads one
// back; and CANCEL, which runs mysqlSessionFake.onCancel and then fails the way
// a statement interrupted by its context does.
type mysqlSessionFakeDriver struct{}

var mysqlSessionFake struct {
	sync.Mutex
	onCancel  func()
	failClear bool
	closed    int
}

func resetMySQLSessionFake() {
	mysqlSessionFake.Lock()
	defer mysqlSessionFake.Unlock()
	mysqlSessionFake.onCancel = nil
	mysqlSessionFake.failClear = false
	mysqlSessionFake.closed = 0
}

func (mysqlSessionFakeDriver) Open(string) (driver.Conn, error) {
	return &mysqlSessionFakeConn{vars: map[string]string{}}, nil
}

type mysqlSessionFakeConn struct {
	vars map[string]string
}

func (*mysqlSessionFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("mysql-session-fake prepares nothing")
}

func (c *mysqlSessionFakeConn) Close() error {
	mysqlSessionFake.Lock()
	defer mysqlSessionFake.Unlock()
	mysqlSessionFake.closed++
	return nil
}

func (*mysqlSessionFakeConn) Begin() (driver.Tx, error) { return mysqlSessionFakeTx{}, nil }

// BeginTx so that a read-only transaction can be asked for, as QueryAllWith does.
func (*mysqlSessionFakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return mysqlSessionFakeTx{}, nil
}

// What go-sql-driver/mysql's ResetSession amounts to for this purpose: it
// checks the connection is still usable and does not touch user variables.
func (*mysqlSessionFakeConn) ResetSession(context.Context) error { return nil }
func (*mysqlSessionFakeConn) IsValid() bool                      { return true }

func (c *mysqlSessionFakeConn) ExecContext(
	_ context.Context, query string, args []driver.NamedValue,
) (driver.Result, error) {
	switch {
	case query == "CANCEL":
		return nil, c.cancel()
	case strings.HasPrefix(query, "SET @") && strings.HasSuffix(query, " = NULL"):
		mysqlSessionFake.Lock()
		fail := mysqlSessionFake.failClear
		mysqlSessionFake.Unlock()
		if fail {
			return nil, errors.New("connection lost while clearing")
		}
		delete(c.vars, strings.TrimSuffix(strings.TrimPrefix(query, "SET @"), " = NULL"))
	case strings.HasPrefix(query, "SET @") && strings.HasSuffix(query, " = ?"):
		c.vars[strings.TrimSuffix(strings.TrimPrefix(query, "SET @"), " = ?")] = args[0].Value.(string)
	default:
		return nil, errors.New("mysql-session-fake cannot run " + query)
	}
	return driver.RowsAffected(0), nil
}

func (c *mysqlSessionFakeConn) QueryContext(
	_ context.Context, query string, _ []driver.NamedValue,
) (driver.Rows, error) {
	switch {
	case query == "CANCEL":
		return nil, c.cancel()
	case strings.HasPrefix(query, "SELECT @"):
		value, set := c.vars[strings.TrimPrefix(query, "SELECT @")]
		row := []driver.Value{nil}
		if set {
			row[0] = value
		}
		return &mysqlSessionFakeRows{row: row}, nil
	}
	return nil, errors.New("mysql-session-fake cannot run " + query)
}

// cancel cancels the request's context and answers the way a driver does once
// that happens — but after the statement has finished, so nothing is running
// on the connection when database/sql rolls the transaction back and keeps it.
func (*mysqlSessionFakeConn) cancel() error {
	mysqlSessionFake.Lock()
	onCancel := mysqlSessionFake.onCancel
	mysqlSessionFake.Unlock()
	if onCancel != nil {
		onCancel()
	}
	return context.Canceled
}

type mysqlSessionFakeRows struct {
	row  []driver.Value
	done bool
}

func (*mysqlSessionFakeRows) Columns() []string { return []string{"value"} }
func (*mysqlSessionFakeRows) Close() error      { return nil }
func (r *mysqlSessionFakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, r.row)
	return nil
}

type mysqlSessionFakeTx struct{}

func (mysqlSessionFakeTx) Commit() error   { return nil }
func (mysqlSessionFakeTx) Rollback() error { return nil }

func init() {
	sql.Register("mysql-session-fake", mysqlSessionFakeDriver{})
}

// newMySQLSessionFakePool is a MySQL-dialect pool over one connection, so
// whatever a request leaves on it is what the next request gets.
func newMySQLSessionFakePool(t *testing.T) *Pool {
	t.Helper()
	resetMySQLSessionFake()

	db, err := sql.Open("mysql-session-fake", "")
	if err != nil {
		t.Fatalf("opening the fake: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &Pool{db: db, driver: "mysql"}
}

// tenantOnNextRequest is what the next request on the pool would read as
// @app_tenant: a projection that sets no variables of its own, sharing the
// pool.
func tenantOnNextRequest(t *testing.T, pool *Pool) any {
	t.Helper()
	var tenant any
	if err := pool.db.QueryRowContext(context.Background(), "SELECT @app_tenant").Scan(&tenant); err != nil {
		t.Fatalf("reading @app_tenant on the next request: %v", err)
	}
	return tenant
}

var acmeSession = []SessionVariable{{Name: "app.tenant", Value: "acme"}}

// A request cancelled while nothing is running on its connection must not hand
// the caller's identity to the next request on that connection.
//
// database/sql rolls the transaction back itself when the context ends, and
// because go-sql-driver/mysql can reset a session it keeps the connection
// rather than discarding it — but that reset does not touch user variables. The
// clear that should have caught it ran on the transaction with the cancelled
// context, failed with the transaction already over, and was thrown away; the
// connection went back to the pool with @app_tenant still set, and the next
// request on it — possibly another projection's, one that sets no variables —
// ran as the previous caller.
func TestCancelledRequestDoesNotLeaveTheTenantOnTheConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, *Pool) error
	}{
		{name: "read", run: func(ctx context.Context, pool *Pool) error {
			_, err := pool.QueryAllWith(ctx, acmeSession,
				[]*Statement{{SQL: "CANCEL", MaxRows: 10, Timeout: time.Minute, ReturnsRows: true}}, nil)
			return err
		}},
		{name: "write", run: func(ctx context.Context, pool *Pool) error {
			_, _, err := pool.TransactWith(ctx, acmeSession,
				[]*Statement{{SQL: "CANCEL", Timeout: time.Minute}}, nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMySQLSessionFakePool(t)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mysqlSessionFake.Lock()
			mysqlSessionFake.onCancel = cancel
			mysqlSessionFake.Unlock()

			if err := tc.run(ctx, pool); err == nil {
				t.Fatal("the cancelled request succeeded")
			}

			if tenant := tenantOnNextRequest(t, pool); tenant != nil {
				t.Errorf("the next request on the connection read @app_tenant = %v, want NULL", tenant)
			}
		})
	}
}

// A connection whose variables could not be cleared is not returned to the
// pool.
//
// There is no second way to put the variable back, and the connection is
// shared with every projection reaching the same database — so the one thing
// still in this server's power is to make sure nothing ever runs on it again.
func TestUnclearableConnectionIsDiscarded(t *testing.T) {
	pool := newMySQLSessionFakePool(t)
	mysqlSessionFake.Lock()
	mysqlSessionFake.failClear = true
	mysqlSessionFake.Unlock()

	_, _ = pool.QueryAllWith(context.Background(), acmeSession,
		[]*Statement{{SQL: "SELECT @app_tenant", MaxRows: 10, Timeout: time.Minute, ReturnsRows: true}}, nil)

	mysqlSessionFake.Lock()
	closed := mysqlSessionFake.closed
	mysqlSessionFake.Unlock()
	if closed == 0 {
		t.Error("the connection that could not be cleared was kept")
	}
	if tenant := tenantOnNextRequest(t, pool); tenant != nil {
		t.Errorf("the next request on the pool read @app_tenant = %v, want NULL", tenant)
	}
}
