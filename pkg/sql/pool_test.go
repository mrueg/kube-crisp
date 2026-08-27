package sql

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestLimiterSheds is the load-shedding contract: a projection at its limit
// rejects rather than queueing, because a client that waits out its own timeout
// learns nothing while a fast rejection lets it back off.
func TestLimiterSheds(t *testing.T) {
	limiter := NewLimiter(1)
	if got := limiter.Limit(); got != 1 {
		t.Fatalf("Limit() = %d, want 1", got)
	}

	release, err := limiter.Acquire(context.Background())
	if err != nil {
		t.Fatalf("the first Acquire() failed: %v", err)
	}

	// The second waits acquireTimeout and is then shed.
	start := time.Now()
	if _, err := limiter.Acquire(context.Background()); !errors.Is(err, ErrTooBusy) {
		t.Fatalf("Acquire() at the limit = %v, want ErrTooBusy", err)
	}
	if waited := time.Since(start); waited < acquireTimeout/2 {
		t.Errorf("Acquire() gave up after %v; it should wait about %v first", waited, acquireTimeout)
	}

	// Releasing lets the next one through.
	release()
	if _, err := limiter.Acquire(context.Background()); err != nil {
		t.Errorf("Acquire() after a release failed: %v", err)
	}
}

// TestLimiterHonoursTheCaller: a client that has already given up should not
// hold a slot open waiting for one.
func TestLimiterHonoursTheCaller(t *testing.T) {
	limiter := NewLimiter(1)
	if _, err := limiter.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := limiter.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Acquire() with a cancelled caller = %v, want context.Canceled", err)
	}
}

// TestLimiterUnlimited: zero means no limit, and a nil limiter is inert so the
// read paths do not have to ask whether one is configured.
func TestLimiterUnlimited(t *testing.T) {
	for name, limiter := range map[string]*Limiter{
		"zero":     NewLimiter(0),
		"negative": NewLimiter(-1),
		"nil":      nil,
	} {
		t.Run(name, func(t *testing.T) {
			if got := limiter.Limit(); got != 0 {
				t.Errorf("Limit() = %d, want 0", got)
			}
			for i := 0; i < 100; i++ {
				if _, err := limiter.Acquire(context.Background()); err != nil {
					t.Fatalf("Acquire() %d failed: %v", i, err)
				}
			}
		})
	}
}

// TestPoolCacheSharesAndReleases covers the lifecycle that keeps a deleted
// projection from holding its connections open forever.
func TestPoolCacheSharesAndReleases(t *testing.T) {
	cache := NewPoolCache()
	t.Cleanup(cache.Close)

	opened := 0
	open := func(name string) func() (*Pool, error) {
		return func() (*Pool, error) {
			opened++
			return Open(PoolOptions{
				Driver: "sqlite",
				DSN:    filepath.Join(t.TempDir(), name+".db"),
				Name:   name,
			})
		}
	}

	first, err := cache.Get("a", open("a"))
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	// The same key is the same pool: projections reaching one database share
	// its connections rather than each opening their own.
	again, err := cache.Get("a", open("a"))
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if again != first {
		t.Error("the same key produced two pools")
	}
	if opened != 1 {
		t.Errorf("opened %d pools for one key, want 1", opened)
	}

	if _, err := cache.Get("b", open("b")); err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}

	// RetainOnly is what releases the pools of projections that are gone.
	if evicted := cache.RetainOnly(map[string]struct{}{"a": {}}); evicted != 1 {
		t.Errorf("RetainOnly() evicted %d, want 1", evicted)
	}
	if got := cache.Len(); got != 1 {
		t.Errorf("Len() = %d after retaining one, want 1", got)
	}

	cache.Evict("a")
	if got := cache.Len(); got != 0 {
		t.Errorf("Len() = %d after evicting the last, want 0", got)
	}

	// Evicting something that is not there is not an error.
	cache.Evict("gone")
}

// TestPoolCacheReportsOpenFailures: a pool that cannot be opened must not be
// cached, or the failure would be remembered instead of retried.
func TestPoolCacheReportsOpenFailures(t *testing.T) {
	cache := NewPoolCache()
	t.Cleanup(cache.Close)

	if _, err := cache.Get("bad", func() (*Pool, error) {
		return nil, errors.New("nope")
	}); err == nil {
		t.Fatal("Get() hid an open failure")
	}
	if got := cache.Len(); got != 0 {
		t.Errorf("Len() = %d after a failed open, want 0", got)
	}
}

// TestPoolIdentity covers what a pool tells metrics and audit records about
// itself — never its credentials.
func TestPoolIdentity(t *testing.T) {
	named, err := Open(PoolOptions{Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "n.db"), Name: "orders"})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = named.Close() })

	if got := named.Name(); got != "orders" {
		t.Errorf("Name() = %q, want orders", got)
	}
	if got := named.Driver(); got != "sqlite" {
		t.Errorf("Driver() = %q, want sqlite", got)
	}
	if err := named.Ping(context.Background()); err != nil {
		t.Errorf("Ping() returned error: %v", err)
	}

	// Without a name the driver stands in, so a metric is never unlabelled.
	unnamed, err := Open(PoolOptions{Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "u.db")})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = unnamed.Close() })
	if got := unnamed.Name(); got != "sqlite" {
		t.Errorf("Name() = %q with no name set, want the driver", got)
	}
}

// TestOpenRejectsAnUnknownDriver, rather than failing later on the first query.
func TestOpenRejectsAnUnknownDriver(t *testing.T) {
	if _, err := Open(PoolOptions{Driver: "oracle", DSN: "x"}); err == nil {
		t.Error("Open() accepted an unsupported driver")
	}
}

// TestPingIdleExercisesEveryHeldConnection: one ping only borrows one
// connection, so the rest stay cold — and a firewall that drops idle sockets
// takes them with nobody noticing until a request does.
//
// What it exercises is what the pool is holding, not what it is allowed to
// hold. Pinging up to MaxIdleConns would open connections in order to keep them
// warm, which on a server nobody is using means the keep-alive is the only
// thing that ever wanted them — and since idle capacity now follows the pool
// size, that would be the whole pool.
func TestPingIdleExercisesEveryHeldConnection(t *testing.T) {
	pool, err := Open(PoolOptions{
		Driver:       "sqlite",
		DSN:          filepath.Join(t.TempDir(), "warm.db"),
		MaxIdleConns: 3,
		MaxOpenConns: 5,
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	// Nothing held yet: one ping, which opens the one connection a later
	// request would otherwise have opened itself.
	pool.pingIdle(context.Background())
	if got := pool.db.Stats().OpenConnections; got < 1 {
		t.Errorf("the pool holds %d connections after a keep-alive on a cold pool, want at least 1", got)
	}

	// Three held: all three exercised, and no fourth opened to be warmed.
	conns := make([]*sql.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		conn, err := pool.db.Conn(t.Context())
		if err != nil {
			t.Fatalf("borrowing connection %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	if got := pool.db.Stats().Idle; got != 3 {
		t.Fatalf("the pool holds %d idle connections, want 3", got)
	}

	pool.pingIdle(context.Background())
	pool.reportStats()

	stats := pool.db.Stats()
	if stats.OpenConnections != 3 {
		t.Errorf("the pool holds %d connections after a keep-alive, want the 3 it was holding", stats.OpenConnections)
	}
	if stats.MaxIdleClosed != 0 {
		t.Errorf("the keep-alive closed %d connections; it should exercise them, not churn them", stats.MaxIdleClosed)
	}
}

// TestTransactIsAllOrNothing is what lets a projected kind span more than one
// table: a create that inserts an order and its line items has to be both or
// neither, or the API reports a complete object that is not one.
func TestTransactIsAllOrNothing(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, :qty)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	// The same primary key twice: the second statement is bound to fail.
	if _, _, err := pool.Transact(ctx, []*Statement{insert, insert},
		map[string]any{"id": "dup", "qty": int64(1)}); err == nil {
		t.Fatal("a transaction whose second statement failed reported success")
	}

	count, err := pool.Prepare("SELECT COUNT(*) AS n FROM items WHERE id = :id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	rows, err := pool.Query(ctx, count, map[string]any{"id": "dup"})
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if got := rows[0]["n"]; got != int64(0) {
		t.Errorf("the failed transaction left %v rows behind; it should have rolled back", got)
	}
}

// TestTransactReturnsTheLastStatement: only the last result can be the object a
// client is answered with, and only its row count says whether the write
// matched.
//
// The counts used to be summed. A write decides "matched nothing" from this
// number, so a bookkeeping statement that always touches a row stood in for an
// update that matched none, and a guarded update that changed nothing was
// answered 200. The rest of the transaction is still visible, on its own spans.
func TestTransactReturnsTheLastStatement(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	first, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:a, 1)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	last, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:b, 2) RETURNING id, qty", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	last.ReturnsRows = true

	rows, affected, err := pool.TransactWith(ctx, nil, []*Statement{first, last},
		map[string]any{"a": "one", "b": "two"})
	if err != nil {
		t.Fatalf("TransactWith() returned error: %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != "two" {
		t.Errorf("TransactWith() returned %v, want the last statement's row", rows)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1 (the last statement's row, not the transaction's total)", affected)
	}
}

// TestTransactRejectsAnEmptySequence rather than committing nothing quietly.
func TestTransactRejectsAnEmptySequence(t *testing.T) {
	pool := newTestPool(t, true)
	if _, _, err := pool.TransactWith(context.Background(), nil, nil, nil); err == nil {
		t.Error("TransactWith() accepted no statements")
	}
}

// TestQueryAllWithReadsOneMoment is what makes a paged list agree with its
// count: run separately, rows inserted between them leave the client holding a
// page that the remainingItemCount cannot account for.
func TestQueryAllWithReadsOneMoment(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, :qty)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := pool.Exec(ctx, insert, map[string]any{"id": id, "qty": int64(1)}); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}

	list, err := pool.Prepare("SELECT id FROM items ORDER BY id LIMIT :limit", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	count, err := pool.Prepare("SELECT COUNT(*) AS n FROM items", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	results, err := pool.QueryAllWith(ctx, nil, []*Statement{list, count}, map[string]any{"limit": int64(2)})
	if err != nil {
		t.Fatalf("QueryAllWith() returned error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("QueryAllWith() returned %d result sets, want 2", len(results))
	}
	if len(results[0]) != 2 {
		t.Errorf("the page holds %d rows, want 2", len(results[0]))
	}
	if got := results[1][0]["n"]; got != int64(3) {
		t.Errorf("the count reported %v, want 3", got)
	}

	// Nothing to run is not an error; it is simply no result sets.
	if got, err := pool.QueryAllWith(ctx, nil, nil, nil); err != nil || got != nil {
		t.Errorf("QueryAllWith() with no statements = %v, %v; want nil, nil", got, err)
	}
}

// TestJSONAggregationDecodesOneValue covers the json_agg path, where the
// database assembles the documents and the server decodes one value instead of
// scanning every column of every row.
func TestJSONAggregationDecodesOneValue(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	stmt, err := pool.Prepare(`SELECT '[{"id":"a","qty":1},{"id":"b","qty":2}]' AS rows`, time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	stmt.Format = FormatJSONArray

	rows, err := pool.Query(ctx, stmt, nil)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("decoded %d rows, want 2", len(rows))
	}
	if rows[0]["id"] != "a" || rows[1]["id"] != "b" {
		t.Errorf("decoded %v", rows)
	}

	// An empty aggregate is no rows rather than an error: a query that matched
	// nothing is an ordinary answer.
	empty, err := pool.Prepare(`SELECT NULL AS rows WHERE 1 = 0`, time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	empty.Format = FormatJSONArray
	if got, err := pool.Query(ctx, empty, nil); err != nil || len(got) != 0 {
		t.Errorf("an empty aggregate returned %v, %v", got, err)
	}
}

// TestMaxRowsIsEnforced: a collection larger than the projection allows is an
// error rather than a truncation, because a client told it saw everything
// cannot tell that it did not.
func TestMaxRowsIsEnforced(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, 1)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if _, err := pool.Exec(ctx, insert, map[string]any{"id": id}); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}

	stmt, err := pool.Prepare("SELECT id FROM items", time.Second, 2)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Query(ctx, stmt, nil); err == nil {
		t.Error("a result set past maxRows was returned rather than refused")
	}
}

// TestIsUnavailableSeparatesOutageFromRefusal decides what a client is told: an
// unreachable database is a 503 worth retrying, a rejected statement is the
// projection's fault and retrying will not help.
func TestIsUnavailableSeparatesOutageFromRefusal(t *testing.T) {
	unavailable := []error{
		&net.OpError{Op: "dial", Err: errors.New("connection refused")},
		&net.DNSError{Err: "no such host", Name: "db"},
		errors.New("dial tcp 10.0.0.1:5432: connect: connection refused"),
		errors.New("the database system is starting up"),
		errors.New("too many connections"),
		errors.New("server closed the connection unexpectedly"),
	}
	for _, err := range unavailable {
		if !IsUnavailable(err) {
			t.Errorf("IsUnavailable(%v) = false, want true", err)
		}
	}

	refusals := []error{
		nil,
		errors.New(`syntax error at or near "SELCT"`),
		errors.New(`column "nope" does not exist`),
	}
	for _, err := range refusals {
		if IsUnavailable(err) {
			t.Errorf("IsUnavailable(%v) = true, want false", err)
		}
	}
}

// TestConcurrentPreparedStatementUse: the statement cache is shared, so the
// race between two goroutines wanting the same statement has to end with one
// statement rather than two, or a leak.
func TestConcurrentPreparedStatementUse(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	stmt, err := pool.Prepare("SELECT :n AS n", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	// The fixture's CREATE TABLE is cached too, so what matters is the growth.
	before := pool.PreparedCount()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := pool.Query(ctx, stmt, map[string]any{"n": int64(1)}); err != nil {
				t.Errorf("Query() returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := pool.PreparedCount() - before; got != 1 {
		t.Errorf("sixteen concurrent uses of one statement added %d cache entries, want 1", got)
	}
}
