package sql

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestPool(t *testing.T, prepared bool) *Pool {
	t.Helper()

	pool, err := Open(PoolOptions{
		Driver:             "sqlite",
		DSN:                filepath.Join(t.TempDir(), "test.db"),
		PreparedStatements: prepared,
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	stmt, err := pool.Prepare("CREATE TABLE items (id TEXT PRIMARY KEY, qty INTEGER)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Exec(context.Background(), stmt, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	return pool
}

func TestPoolExecAndQuery(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, true)

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, :qty)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	affected, err := pool.Exec(ctx, insert, map[string]any{"id": "a", "qty": 3})
	if err != nil {
		t.Fatalf("Exec() returned error: %v", err)
	}
	if affected != 1 {
		t.Errorf("Exec() affected %d rows, want 1", affected)
	}

	query, err := pool.Prepare("SELECT id, qty FROM items WHERE id = :id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	rows, err := pool.Query(ctx, query, map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Query() returned %d rows, want 1", len(rows))
	}
	if rows[0]["qty"] != int64(3) {
		t.Errorf("qty = %#v, want int64(3)", rows[0]["qty"])
	}
}

// TestPoolCachesPreparedStatements checks that repeated use of one statement
// prepares it once rather than on every call.
func TestPoolCachesPreparedStatements(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, true)

	query, err := pool.Prepare("SELECT id FROM items WHERE id = :id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := pool.Query(ctx, query, map[string]any{"id": "missing"}); err != nil {
			t.Fatalf("Query() returned error: %v", err)
		}
	}

	// One statement for the CREATE TABLE in the fixture, one for this query.
	if got, want := pool.PreparedCount(), 2; got != want {
		t.Errorf("cached %d prepared statements, want %d", got, want)
	}
}

func TestPoolWithoutPreparedStatements(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, false)

	query, err := pool.Prepare("SELECT id FROM items WHERE id = :id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Query(ctx, query, map[string]any{"id": "missing"}); err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if got := pool.PreparedCount(); got != 0 {
		t.Errorf("cached %d prepared statements with caching disabled, want 0", got)
	}
}

// TestIsUniqueViolation checks the classification a client depends on: a
// duplicate write has to read as AlreadyExists whichever driver reported it.
func TestIsUniqueViolation(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, true)

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, :qty)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	args := map[string]any{"id": "duplicate", "qty": 1}
	if _, err := pool.Exec(ctx, insert, args); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	_, err = pool.Exec(ctx, insert, args)
	if err == nil {
		t.Fatal("the duplicate insert succeeded")
	}
	if !IsUniqueViolation(err) {
		t.Errorf("IsUniqueViolation(%v) = false, want true", err)
	}
	if IsForeignKeyViolation(err) {
		t.Errorf("IsForeignKeyViolation(%v) = true, want false", err)
	}
}

// TestErrorClassificationFallsBackToText covers drivers and wrappers that do
// not expose a typed error.
func TestErrorClassificationFallsBackToText(t *testing.T) {
	if !IsUniqueViolation(fmt.Errorf("Error 1062 (23000): Duplicate entry 'x' for key 'PRIMARY'")) {
		t.Error("a MySQL-style duplicate message was not classified")
	}
	if !IsForeignKeyViolation(fmt.Errorf("insert violates foreign key constraint")) {
		t.Error("a foreign key message was not classified")
	}
	if IsUniqueViolation(nil) || IsForeignKeyViolation(nil) {
		t.Error("a nil error was classified as a violation")
	}
}

func TestSQLiteGetsABusyTimeout(t *testing.T) {
	const path = "/var/lib/store.db"

	got := sqliteBusyTimeout(path)
	if !strings.Contains(got, "busy_timeout") {
		t.Errorf("sqliteBusyTimeout() = %q, want a busy timeout", got)
	}

	// An explicit setting is left alone.
	explicit := path + "?_pragma=busy_timeout(100)"
	if got := sqliteBusyTimeout(explicit); got != explicit {
		t.Errorf("sqliteBusyTimeout() = %q, want the DSN unchanged", got)
	}

	// Existing parameters are preserved.
	if got := sqliteBusyTimeout(path + "?mode=ro"); !strings.Contains(got, "mode=ro") ||
		!strings.Contains(got, "busy_timeout") {
		t.Errorf("sqliteBusyTimeout() = %q, want both settings", got)
	}

	// It reaches a pool only through the driver that declares it. Another
	// driver may adapt its own connection strings — MySQL asks for matched
	// rows — but never with this.
	for _, driver := range []string{"postgres", "mysql"} {
		d, ok := Lookup(driver)
		if !ok {
			t.Fatalf("the %s driver is not registered", driver)
		}
		if d.PrepareDSN == nil {
			continue
		}
		if got := d.PrepareDSN(path); strings.Contains(got, "busy_timeout") {
			t.Errorf("the %s driver rewrote a connection string with SQLite's pragma: %q", driver, got)
		}
	}
}

// TestQueryRowsDoNotAliasEachOther pins the property that lets scanRows reuse
// one set of scan destinations for a whole result set: every row has to come
// back holding its own values, not a view onto the destinations the next row
// will overwrite.
func TestQueryRowsDoNotAliasEachOther(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, true)

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, :qty)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	for i := range 3 {
		args := map[string]any{"id": fmt.Sprintf("item-%d", i), "qty": int64(i * 10)}
		if _, err := pool.Exec(ctx, insert, args); err != nil {
			t.Fatalf("inserting: %v", err)
		}
	}

	list, err := pool.Prepare("SELECT id, qty FROM items ORDER BY id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	rows, err := pool.Query(ctx, list, nil)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("read %d rows, want 3", len(rows))
	}

	for i, row := range rows {
		id, ok := row["id"].(string)
		if !ok {
			t.Fatalf("row %d holds id as %T", i, row["id"])
		}
		if want := fmt.Sprintf("item-%d", i); id != want {
			t.Errorf("row %d has id %q, want %q — rows are sharing their scan destinations", i, id, want)
		}
		if qty := row["qty"]; qty != int64(i*10) {
			t.Errorf("row %d has qty %v, want %d", i, qty, i*10)
		}
	}

	// The maps themselves have to be distinct too, not one map handed out three
	// times.
	rows[0]["id"] = "rewritten"
	if rows[1]["id"] == "rewritten" {
		t.Error("writing to one row changed another, so they share a map")
	}
}

// BenchmarkScanRows guards the allocation profile of the read path. Scanning is
// where a large list spends most of its allocations, so a regression here is a
// regression in every projection over a table of any size.
func BenchmarkScanRows(b *testing.B) {
	const rows = 2000

	pool, err := Open(PoolOptions{Driver: "sqlite", DSN: filepath.Join(b.TempDir(), "bench.db")})
	if err != nil {
		b.Fatalf("Open() returned error: %v", err)
	}
	b.Cleanup(func() { _ = pool.Close() })

	ctx := context.Background()
	create, err := pool.Prepare(
		"CREATE TABLE wide (id INTEGER PRIMARY KEY, a TEXT, c TEXT, d TEXT, e TEXT, f INTEGER)", time.Second, 10)
	if err != nil {
		b.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Exec(ctx, create, nil); err != nil {
		b.Fatalf("creating table: %v", err)
	}

	insert, err := pool.Prepare(
		"INSERT INTO wide VALUES (:id, :a, :c, :d, :e, :f)", time.Second, 10)
	if err != nil {
		b.Fatalf("Prepare() returned error: %v", err)
	}
	for i := range rows {
		args := map[string]any{
			"id": int64(i), "a": "alpha", "c": "charlie",
			"d": "delta", "e": "echo", "f": int64(i),
		}
		if _, err := pool.Exec(ctx, insert, args); err != nil {
			b.Fatalf("inserting: %v", err)
		}
	}

	list, err := pool.Prepare("SELECT * FROM wide", time.Second, rows+1)
	if err != nil {
		b.Fatalf("Prepare() returned error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := pool.Query(ctx, list, nil)
		if err != nil {
			b.Fatalf("Query() returned error: %v", err)
		}
		if len(out) != rows {
			b.Fatalf("read %d rows, want %d", len(out), rows)
		}
	}
}

// TestCheckAsksTheDatabaseWhetherAStatementCanRun covers the question that
// decides whether a projection is servable: not "does this parse here" but
// "does the database know these names".
//
// Without it a projection that has outlived its schema compiles, reports Ready,
// appears in discovery, and fails every request with a 500.
func TestCheckAsksTheDatabaseWhetherAStatementCanRun(t *testing.T) {
	pool := newTestPool(t, false)
	ctx := context.Background()

	// The table newTestPool creates, with the columns it has.
	if err := pool.Check(ctx, "SELECT id, qty FROM items WHERE id = :name"); err != nil {
		t.Errorf("a statement the database can run was rejected: %v", err)
	}

	// The failures this exists to catch, one per way a schema moves.
	for _, tc := range []struct {
		name      string
		statement string
	}{
		{"a column that does not exist", "SELECT id, no_such_column FROM items"},
		{"a table that does not exist", "SELECT id FROM no_such_table"},
		{"a statement that does not parse", "SELCT id FROM items"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := pool.Check(ctx, tc.statement); err == nil {
				t.Errorf("Check(%q) accepted a statement the database cannot run", tc.statement)
			}
		})
	}
}

// TestCheckDoesNotRunTheStatement, since checking a delete must not delete
// anything. Preparing parses and resolves names; it does not execute.
func TestCheckDoesNotRunTheStatement(t *testing.T) {
	pool := newTestPool(t, false)
	ctx := context.Background()

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:name, 1)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Exec(ctx, insert, map[string]any{"name": "item-1"}); err != nil {
		t.Fatalf("seeding a row: %v", err)
	}

	before := countItems(t, pool)
	if before == 0 {
		t.Fatal("the fixture holds no rows, so a delete could not be seen to have run")
	}

	if err := pool.Check(ctx, "DELETE FROM items"); err != nil {
		t.Fatalf("checking a delete: %v", err)
	}

	if after := countItems(t, pool); after != before {
		t.Errorf("checking a DELETE removed rows: %d became %d", before, after)
	}
}

func countItems(t *testing.T, pool *Pool) int {
	t.Helper()

	stmt, err := pool.Prepare("SELECT count(*) AS n FROM items", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	rows, err := pool.Query(context.Background(), stmt, nil)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("count returned %d rows, want 1", len(rows))
	}
	switch n := rows[0]["n"].(type) {
	case int64:
		return int(n)
	default:
		t.Fatalf("count returned %T, want int64", rows[0]["n"])
		return 0
	}
}
