// Package sql owns connection pooling, parameter binding, and row retrieval
// for projections. It deliberately knows nothing about Kubernetes types.
package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/utils/lru"

	// Registered database/sql drivers.
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// Defaults applied when a projection leaves pool settings unset.
const (
	DefaultMaxOpenConns    = 10
	DefaultConnMaxLifetime = 30 * time.Minute
	DefaultQueryTimeout    = 10 * time.Second
	// DefaultMaxPreparedStatements bounds the per-pool statement cache. Each
	// entry costs a statement on every connection the pool holds, so an
	// unbounded cache is unbounded state in the database as well as here.
	DefaultMaxPreparedStatements = 256
	DefaultMaxRows               = 5000
	// DefaultMaxBytes bounds a result set by size as well as by row count.
	//
	// maxRows alone does not bound memory: one row can carry a megabyte of
	// JSON or text, and a JSON-aggregated read returns the whole collection as
	// a single row, where maxRows never applies at all. Generous enough that an
	// ordinary list never meets it, and low enough that one projection cannot
	// take a server every other projection shares.
	DefaultMaxBytes  = 64 << 20
	DefaultKeepAlive = 30 * time.Second
	// DefaultStatsInterval is how often pool statistics are republished. It is
	// deliberately not DefaultKeepAlive: the two used to share a ticker, so
	// keepAliveInterval: 0 — a supported setting, for a database behind a proxy
	// that objects to being pinged — silently took every pool metric with it.
	DefaultStatsInterval = 15 * time.Second
)

// ResultFormat describes how a statement returns its rows.
type ResultFormat string

const (
	// FormatRows is the ordinary one-object-per-row result set.
	FormatRows ResultFormat = "rows"
	// FormatJSONArray is a single row holding a single JSON array column, as
	// produced by PostgreSQL's json_agg.
	FormatJSONArray ResultFormat = "json"
)

// PoolOptions configures a connection pool.
type PoolOptions struct {
	Driver          string
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// PreparedStatements caches a prepared statement per distinct SQL text.
	PreparedStatements bool

	// StatementTimeout asks the database to abort a statement that outruns the
	// query's timeout, rather than only stopping this server waiting for it.
	//
	// It costs a transaction per query — that is the only scope a setting can
	// be confined to — and a transaction does not use the statement cache. It
	// is therefore opt-in rather than the default.
	StatementTimeout bool

	// MaxPreparedStatements bounds that cache. Zero takes the default.
	MaxPreparedStatements int

	// KeepAliveInterval pings the pool on this interval to keep connections
	// warm. Zero disables it.
	KeepAliveInterval time.Duration

	// Name labels this pool's metrics.
	Name string
}

// Pool is a driver-aware connection pool for one data source.
//
// Connections are held open for reuse and, unless disabled, each distinct
// statement is prepared once per connection so that repeated requests skip
// parsing and planning.
type Pool struct {
	db     *sql.DB
	name   string
	driver string

	// dsn is kept so that a notification listener can open a connection of its
	// own. It carries credentials and is never logged; the pool's own label is
	// a hash of it.
	dsn string

	// prepare enables the statement cache; stmts is it, bounded and evicting
	// least-recently-used. Pools are shared by every projection reaching the
	// same database, and a projection whose SQL changes leaves its old
	// statements behind, so this grows without a bound of its own.
	prepare bool
	stmts   *lru.Cache

	// statementTimeout pushes each query's deadline to the database, which
	// forces every query into a transaction. See PoolOptions.StatementTimeout.
	statementTimeout bool

	// keepAlive pings the pool's idle connections on this interval. Zero turns
	// the pings off and nothing else — statistics are published on a ticker of
	// their own, because a pool nobody pings is exactly the one worth being
	// able to see.
	keepAlive time.Duration
	stop      chan struct{}
	stopOnce  sync.Once
}

// Limiter bounds how many queries one projection has in flight.
//
// It is per projection rather than per pool because pools are shared by every
// projection reaching the same database: a limit on the pool would be set by
// whichever projection opened it first. Connections to the database are bounded
// separately, by the pool's own size.
type Limiter struct {
	slots chan struct{}
}

// NewLimiter returns a limiter for n concurrent queries. Zero or less means
// unlimited.
func NewLimiter(n int) *Limiter {
	if n <= 0 {
		return &Limiter{}
	}
	return &Limiter{slots: make(chan struct{}, n)}
}

// Limit reports how many queries may run at once, or zero when unbounded.
func (l *Limiter) Limit() int {
	if l == nil {
		return 0
	}
	return cap(l.slots)
}

// Acquire takes a slot, or reports ErrTooBusy once the wait exceeds
// acquireTimeout. Shedding beats queueing: a client that waits out its own
// timeout learns nothing, while a fast rejection lets it back off.
func (l *Limiter) Acquire(ctx context.Context) (func(), error) {
	if l == nil || l.slots == nil {
		return func() {}, nil
	}

	select {
	case l.slots <- struct{}{}:
		return func() { <-l.slots }, nil
	default:
	}

	timer := time.NewTimer(acquireTimeout)
	defer timer.Stop()

	select {
	case l.slots <- struct{}{}:
		return func() { <-l.slots }, nil
	case <-timer.C:
		return nil, ErrTooBusy
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ErrTooBusy reports that a projection is at its concurrency limit. Callers
// turn it into a 429 rather than queueing behind an overloaded database.
var ErrTooBusy = errors.New("projection is at its query concurrency limit")

// acquireTimeout is how long a request waits for a slot before being shed.
const acquireTimeout = time.Second

func (p *Pool) metricLabel() string {
	if p.name != "" {
		return p.name
	}
	return p.driver
}

// Open establishes a pool. It does not verify connectivity; call Ping for that.
func Open(opts PoolOptions) (*Pool, error) {
	driver, ok := Lookup(opts.Driver)
	if !ok {
		return nil, fmt.Errorf("unsupported driver %q; this build knows %s",
			opts.Driver, strings.Join(RegisteredDrivers(), ", "))
	}

	dsn := opts.DSN
	if driver.PrepareDSN != nil {
		dsn = driver.PrepareDSN(dsn)
	}

	db, err := sql.Open(driver.SQLDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("opening %s data source: %w", opts.Driver, err)
	}

	if opts.MaxOpenConns <= 0 {
		opts.MaxOpenConns = DefaultMaxOpenConns
	}
	// Idle capacity matches the pool, rather than the two connections this used
	// to keep.
	//
	// Under steady load the old default cost nothing: database/sql hands a
	// released connection straight to whoever is waiting for one, so the idle
	// limit is rarely reached. An API server's traffic is not steady. It
	// arrives in waves — an informer resync, a dashboard, a reconcile loop —
	// and between waves every connection past the second was closed, so the
	// next wave dialled and authenticated them all again. Measured against
	// PostgreSQL at eight concurrent queries per wave, that was 150 of every
	// 200 queries paying for a new connection, and six times the wall-clock for
	// queries short enough that connecting was the whole cost. Over TLS, which
	// that measurement did not use, it would be worse.
	//
	// The cost of the new default is idle connections held against the
	// database: up to MaxOpenConns per pool per replica, which is what
	// MaxOpenConns already permits and what ConnMaxLifetime already recycles.
	// Set maxIdleConns explicitly to trade the other way.
	if opts.MaxIdleConns <= 0 {
		opts.MaxIdleConns = opts.MaxOpenConns
	}
	if opts.ConnMaxLifetime <= 0 {
		opts.ConnMaxLifetime = DefaultConnMaxLifetime
	}

	db.SetMaxOpenConns(opts.MaxOpenConns)
	db.SetMaxIdleConns(opts.MaxIdleConns)
	db.SetConnMaxLifetime(opts.ConnMaxLifetime)
	if opts.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(opts.ConnMaxIdleTime)
	}

	if opts.MaxPreparedStatements <= 0 {
		opts.MaxPreparedStatements = DefaultMaxPreparedStatements
	}

	pool := &Pool{
		db:      db,
		dsn:     dsn,
		name:    opts.Name,
		driver:  opts.Driver,
		prepare: opts.PreparedStatements,
		// Only where it can actually be set. A driver that cannot honour it
		// would otherwise pay for a transaction per query and get nothing.
		statementTimeout: opts.StatementTimeout && SupportsStatementTimeout(opts.Driver),
		stmts: lru.NewWithEvictionFunc(opts.MaxPreparedStatements, func(_ lru.Key, value any) {
			if stmt, ok := value.(*sql.Stmt); ok {
				_ = stmt.Close()
			}
		}),
		keepAlive: opts.KeepAliveInterval,
		stop:      make(chan struct{}),
	}

	warnIfUnencrypted(driver, dsn, pool.metricLabel())
	if pool.keepAlive > 0 {
		go pool.keepWarm()
	}
	go pool.publishStats()

	// Once at startup, so a pool that is opened and never used still has a
	// series rather than appearing only after the first interval elapses.
	pool.reportStats()
	return pool, nil
}

// sqliteDefaultBusyTimeout is how long a SQLite connection waits for a lock
// before giving up.
const sqliteDefaultBusyTimeout = "5000"

// sqliteBusyTimeout gives SQLite connections a busy timeout unless the DSN
// already sets one.
//
// SQLite allows one writer at a time and returns SQLITE_BUSY immediately
// without this, so a poll running alongside a write fails rather than waiting a
// few milliseconds. It is the sqlite driver's PrepareDSN, so no other driver
// ever sees it.
func sqliteBusyTimeout(dsn string) string {
	if strings.Contains(dsn, "busy_timeout") {
		return dsn
	}

	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "_pragma=busy_timeout(" + sqliteDefaultBusyTimeout + ")"
}

// keepWarm pings the pool periodically so that idle connections are not the
// first thing a request has to pay for.
func (p *Pool) keepWarm() {
	ticker := time.NewTicker(p.keepAlive)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			p.pingIdle(ctx)
			cancel()
		}
	}
}

// pingIdle exercises the connections the pool keeps open.
//
// One ping only borrows one connection, so on a pool holding several idle ones
// the rest go untouched and are still the first thing some later request has to
// pay for — or has closed under it by a firewall that drops idle sockets. The
// pings run together so that they take different connections rather than the
// same one in turn.
func (p *Pool) pingIdle(ctx context.Context) {
	// What the pool is holding, not what it is allowed to hold. Pinging up to
	// the ceiling would open connections in order to keep them warm, which on
	// an idle server means the keep-alive is the only thing that ever wanted
	// them.
	idle := p.db.Stats().Idle
	if idle < 1 {
		idle = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < idle; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.db.PingContext(ctx); err != nil {
				klog.V(3).InfoS("data source keep-alive ping failed", "driver", p.driver, "err", err)
			}
		}()
	}
	wg.Wait()
}

// publishStats republishes pool statistics on a ticker.
//
// Separate from keepWarm because the keep-alive is optional and this is not.
// They shared a ticker until a pool configured with keepAliveInterval: 0
// stopped reporting anything at all — no open connections, no wait count, no
// prepared statements — which reads on a dashboard exactly like a pool that is
// idle rather than one that is unobserved.
func (p *Pool) publishStats() {
	ticker := time.NewTicker(DefaultStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.reportStats()
		}
	}
}

// reportStats publishes connection pool state.
func (p *Pool) reportStats() {
	label := p.name
	if label == "" {
		label = p.driver
	}

	stats := p.db.Stats()
	crispmetrics.DataSourceConnections.WithLabelValues(label, crispmetrics.ConnectionsOpen).Set(float64(stats.OpenConnections))
	crispmetrics.DataSourceConnections.WithLabelValues(label, crispmetrics.ConnectionsInUse).Set(float64(stats.InUse))
	crispmetrics.DataSourceConnections.WithLabelValues(label, crispmetrics.ConnectionsIdle).Set(float64(stats.Idle))
	crispmetrics.DataSourceConnections.WithLabelValues(label, crispmetrics.ConnectionsWaited).Set(float64(stats.WaitCount))
	crispmetrics.PreparedStatements.WithLabelValues(label).Set(float64(p.PreparedCount()))

	// How long requests have spent waiting for a connection, in total. A count
	// of waits says a pool was contended; this says what it cost, which is what
	// separates a pool that is briefly full from one that is the bottleneck.
	crispmetrics.DataSourceWaitSeconds.WithLabelValues(label).Set(stats.WaitDuration.Seconds())

	// Connections the pool threw away rather than kept, which is the number
	// that separates a pool that is the right size from one that reconnects on
	// most requests. A pool serving more concurrent queries than MaxIdleConns
	// closes the surplus the moment each query returns and dials again for the
	// next, and nothing else here would show it: open, in_use and idle all look
	// healthy while every one of those connections is new.
	crispmetrics.DataSourceConnectionsClosed.WithLabelValues(label, crispmetrics.ClosedMaxIdle).Set(float64(stats.MaxIdleClosed))
	crispmetrics.DataSourceConnectionsClosed.WithLabelValues(label, crispmetrics.ClosedMaxIdleTime).Set(float64(stats.MaxIdleTimeClosed))
	crispmetrics.DataSourceConnectionsClosed.WithLabelValues(label, crispmetrics.ClosedMaxLifetime).Set(float64(stats.MaxLifetimeClosed))
}

// Name reports the pool's label, which identifies the database without
// carrying its credentials.
func (p *Pool) Name() string { return p.metricLabel() }

// Driver reports the configured driver enum value.
func (p *Pool) Driver() string { return p.driver }

// Ping verifies connectivity.
func (p *Pool) Ping(ctx context.Context) error { return p.db.PingContext(ctx) }

// Close releases prepared statements and the pool.
func (p *Pool) Close() error {
	p.stopOnce.Do(func() { close(p.stop) })

	// Clear runs the eviction function for every entry, which closes them.
	p.stmts.Clear()

	// Otherwise the last values this pool reported stay in /metrics for the
	// life of the process: a pool that is gone goes on claiming ten open
	// connections, and an alert on pool exhaustion fires on a database nothing
	// is connected to any more.
	label := p.metricLabel()
	crispmetrics.DataSourceConnections.DeletePartialMatch(map[string]string{"datasource": label})
	crispmetrics.DataSourceConnectionsClosed.DeletePartialMatch(map[string]string{"datasource": label})
	crispmetrics.DataSourceWaitSeconds.DeleteLabelValues(label)
	crispmetrics.PreparedStatements.DeleteLabelValues(label)

	return p.db.Close()
}

// stmtFor returns the cached prepared statement for text, preparing it on
// first use. Callers fall back to an unprepared query when this fails, since a
// database that refuses to prepare should still be able to serve requests.
func (p *Pool) stmtFor(ctx context.Context, text string) (*sql.Stmt, error) {
	if cached, ok := p.stmts.Get(text); ok {
		if stmt, ok := cached.(*sql.Stmt); ok {
			return stmt, nil
		}
	}

	prepared, err := p.db.PrepareContext(ctx, text)
	if err != nil {
		return nil, err
	}

	// Add evicts the least recently used entry when the cache is full, closing
	// it through the eviction function. A racing goroutine that prepared the
	// same text loses its copy the same way, since the newer one replaces it.
	p.stmts.Add(text, prepared) //nolint:sqlclosecheck // closed by the cache's eviction function, or by Close
	return prepared, nil
}

// PreparedCount reports how many statements are currently cached, for tests
// and diagnostics.
func (p *Pool) PreparedCount() int { return p.stmts.Len() }

// Statement is a query prepared for one driver: the rewritten SQL plus the
// positional order of its bind parameters.
type Statement struct {
	SQL     string
	Params  []string
	Timeout time.Duration
	MaxRows int

	// MaxBytes caps the size of the values a result set carries, in bytes.
	MaxBytes int
	Format   ResultFormat

	// ReturnsRows records whether this statement answers with a result set, so
	// a transaction knows whether to run it as a query or for its effect. The
	// caller sets it; the pool has no opinion about what a statement means.
	ReturnsRows bool

	// Prepared and EnforceTimeout are per statement rather than per pool, so
	// that projections which disagree about them can still share one pool.
	//
	// They used to be pool fields, and the pool key carried them so the
	// disagreement could not arise — which meant one database reached with two
	// different settings got two pools, each with its own MaxOpenConns. That
	// made --max-open-conns-per-datasource a bound on a pool rather than on a
	// database, which is not what it says or what an operator sizing a database
	// would assume.
	//
	// Neither setting was ever a property of the connection. A prepared
	// statement is cached by SQL text, and the statement timeout is applied
	// with SET LOCAL inside the transaction that runs the query, so it dies
	// with that transaction rather than travelling with the connection.
	Prepared       bool
	EnforceTimeout bool
}

// Prepare rewrites a :named statement for this pool's driver.
func (p *Pool) Prepare(stmt string, timeout time.Duration, maxRows int) (*Statement, error) {
	rewritten, names, err := Rewrite(stmt, p.driver)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = DefaultQueryTimeout
	}
	if maxRows <= 0 {
		maxRows = DefaultMaxRows
	}
	return &Statement{
		SQL:      rewritten,
		Params:   names,
		Timeout:  timeout,
		MaxRows:  maxRows,
		MaxBytes: DefaultMaxBytes,
		Format:   FormatRows,
		// The pool's own settings, which are the defaults it was opened with.
		// A projection that disagrees overrides them on the statement — see
		// Statement.Prepared.
		Prepared:       p.prepare,
		EnforceTimeout: p.statementTimeout,
	}, nil
}

// EnforceTimeoutOn reports whether this driver can be asked to bound a
// statement at the database, which is what decides whether asking for it means
// anything.
func (p *Pool) EnforceTimeoutOn(want bool) bool {
	return want && SupportsStatementTimeout(p.driver)
}

// Row is one result row keyed by column name.
type Row map[string]any

// Query executes stmt, resolving each declared bind parameter from args.
// A parameter with no entry in args is passed as NULL.
func (p *Pool) Query(ctx context.Context, stmt *Statement, args map[string]any) ([]Row, error) {
	return p.QueryWith(ctx, nil, stmt, args)
}

// QueryWith runs a query with session variables applied to the connection it
// runs on.
//
// With variables to set, the query moves into a transaction: that is the only
// way a setting can be scoped to one request rather than left behind on a
// pooled connection for whoever gets it next. Without them nothing changes, and
// the prepared statement cache still applies.
func (p *Pool) QueryWith(ctx context.Context, session []SessionVariable, stmt *Statement, args map[string]any) ([]Row, error) {
	// A statement timeout is scoped the same way a session variable is, and for
	// the same reason: a transaction is the only place a setting can be confined
	// to one request rather than left on a pooled connection.
	if len(session) > 0 || stmt.EnforceTimeout {
		rows, _, err := p.transact(ctx, session, []*Statement{stmt}, args, true)
		return rows, err
	}

	ctx, cancel := context.WithTimeout(ctx, stmt.Timeout)
	defer cancel()

	ctx, span := p.startSpan(ctx, "query", stmt.SQL)

	rows, err := p.queryContext(ctx, stmt, bind(stmt, args))
	if err != nil {
		span.end(0, err)
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	span.returned()

	scanned, err := scanRows(rows, stmt)
	span.end(len(scanned), err)
	return scanned, err
}

// scanRows turns a result set into rows, honouring the statement's format and
// row cap.
func scanRows(rows *sql.Rows, stmt *Statement) ([]Row, error) {
	if stmt.Format == FormatJSONArray {
		return scanJSONArray(rows, stmt.MaxBytes)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("reading result columns: %w", err)
	}

	// One set of destinations for the whole result set rather than one per row.
	// Each row's values are copied into a map of its own immediately after the
	// scan, so nothing outlives the next call — and database/sql clones a
	// driver's []byte on the way into an any, so a text column is not a window
	// onto a buffer the driver is about to reuse either.
	//
	// This is most of the allocation a large read does: a fresh slice plus a
	// boxed any per column, per row. On five thousand ten-column rows it was
	// 185k allocations against 125k.
	values := make([]any, len(columns))
	scan := make([]any, len(columns))
	for i := range scan {
		scan[i] = &values[i]
	}

	var (
		out   []Row
		bytes int
	)
	for rows.Next() {
		if len(out) >= stmt.MaxRows {
			return nil, fmt.Errorf("result set exceeded maxRows (%d); narrow the query or raise the limit", stmt.MaxRows)
		}

		if err := rows.Scan(scan...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		row := make(Row, len(columns))
		for i, name := range columns {
			row[name] = values[i]
			bytes += valueSize(values[i])
		}

		// Counted as it accumulates rather than at the end, so a read that is
		// going to be too large stops being read rather than being measured
		// once it is already held.
		if stmt.MaxBytes > 0 && bytes > stmt.MaxBytes {
			return nil, fmt.Errorf("result set exceeded maxBytes (%d) after %d rows; "+
				"narrow the query, select fewer columns, or raise the limit", stmt.MaxBytes, len(out)+1)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return out, nil
}

// queryContext runs a statement, using the prepared statement cache when it is
// enabled and falling back to an ad-hoc query when preparing fails.
func (p *Pool) queryContext(ctx context.Context, stmt *Statement, values []any) (*sql.Rows, error) {
	if stmt.Prepared {
		// The statement is cached until it is evicted or the pool is closed,
		// which is the whole point of preparing it.
		prepared, err := p.stmtFor(ctx, stmt.SQL) //nolint:sqlclosecheck // cached statement, closed by the cache or by Pool.Close
		if err != nil {
			klog.V(3).InfoS("falling back to an unprepared query", "driver", p.driver, "err", err)
		} else {
			rows, err := prepared.QueryContext(ctx, values...)
			if !isStatementClosed(err) {
				return rows, err
			}
			// Evicted between the lookup and the call. Nothing has been read
			// yet, so running it unprepared is the same query.
			klog.V(4).InfoS("retrying an evicted prepared statement unprepared", "driver", p.driver)
		}
	}
	return p.db.QueryContext(ctx, stmt.SQL, values...)
}

// isStatementClosed reports whether a call failed because the prepared
// statement was closed underneath it.
//
// The statement cache evicts, and an eviction can land between another
// goroutine taking a statement out of the cache and calling it. database/sql
// does not export this error, so it is matched by text; a miss only costs the
// caller the real error instead of a retry.
func isStatementClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "statement is closed")
}

// Exec runs a statement that does not return rows and reports how many rows it
// affected. Statements that return rows, such as INSERT ... RETURNING, should
// go through Query instead.
func (p *Pool) Exec(ctx context.Context, stmt *Statement, args map[string]any) (int64, error) {
	return p.ExecWith(ctx, nil, stmt, args)
}

// ExecWith runs a statement with session variables applied, moving it into a
// transaction when there are any.
func (p *Pool) ExecWith(ctx context.Context, session []SessionVariable, stmt *Statement, args map[string]any) (int64, error) {
	if len(session) > 0 || stmt.EnforceTimeout {
		_, affected, err := p.transact(ctx, session, []*Statement{stmt}, args, false)
		return affected, err
	}

	ctx, cancel := context.WithTimeout(ctx, stmt.Timeout)
	defer cancel()

	ctx, span := p.startSpan(ctx, "exec", stmt.SQL)

	values := bind(stmt, args)

	var (
		result sql.Result
		err    error
	)
	if stmt.Prepared {
		prepared, prepErr := p.stmtFor(ctx, stmt.SQL) //nolint:sqlclosecheck // cached statement, closed by the cache or by Pool.Close
		if prepErr != nil {
			klog.V(3).InfoS("falling back to an unprepared exec", "driver", p.driver, "err", prepErr)
		} else {
			result, err = prepared.ExecContext(ctx, values...)
		}
		if prepErr != nil || isStatementClosed(err) {
			// Either it could not be prepared, or it was evicted between the
			// lookup and the call. Nothing has been written yet either way.
			result, err = p.db.ExecContext(ctx, stmt.SQL, values...)
		}
	} else {
		result, err = p.db.ExecContext(ctx, stmt.SQL, values...)
	}
	if err != nil {
		span.endAffected(0, err)
		return 0, fmt.Errorf("executing statement: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		// Not every driver reports this; treat it as "unknown, but succeeded".
		span.endAffected(-1, nil)
		return -1, nil
	}
	span.endAffected(affected, nil)
	return affected, nil
}

// Transact runs statements in order inside one transaction and returns the
// result of the last one.
//
// This is what lets a projected kind span more than one table. A create that
// inserts an order and its line items has to be all or nothing; run as separate
// statements it can leave a row the API will then report as a complete object
// when it is not.
//
// Only the last statement may return rows, because only its result can be the
// object the client is answered with. The others are executed for their effect,
// and their affected-row counts are summed into the count reported back.
//
// Statements inside a transaction do not use the prepared-statement cache: the
// cache is per pool, and a transaction holds one connection of its own.
func (p *Pool) Transact(ctx context.Context, stmts []*Statement, args map[string]any) ([]Row, int64, error) {
	return p.TransactWith(ctx, nil, stmts, args)
}

// QueryAllWith runs several queries inside one transaction and returns a result
// set for each.
//
// It is what makes a paged list and its count agree: run separately, rows can
// be inserted between them, and a client is told there are more objects than
// the page it is holding can account for. One transaction is one moment.
func (p *Pool) QueryAllWith(ctx context.Context, session []SessionVariable, stmts []*Statement, args map[string]any) ([][]Row, error) {
	if len(stmts) == 0 {
		return nil, nil
	}

	// The budget is the longest statement's, since they share a transaction.
	// The database is asked to enforce it if any statement in here wanted that,
	// because one deadline covers the whole transaction either way.
	timeout := stmts[0].Timeout
	enforced := stmts[0].EnforceTimeout
	for _, stmt := range stmts[1:] {
		if stmt.Timeout > timeout {
			timeout = stmt.Timeout
		}
		enforced = enforced || stmt.EnforceTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, clientDeadline(enforced, timeout))
	defer cancel()

	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		// A MySQL user variable belongs to the connection, not to the
		// transaction, so a rollback does not unset it. This covers the paths
		// that do not reach the commit; the one that does clears first, which
		// makes this a no-op on an already-finished transaction.
		_ = p.clearSession(ctx, tx, session)
		_ = tx.Rollback()
	}()

	if err := p.applyStatementTimeout(ctx, tx, enforced, timeout); err != nil {
		return nil, err
	}
	if err := p.applySession(ctx, tx, session); err != nil {
		return nil, err
	}

	out := make([][]Row, 0, len(stmts))
	for _, stmt := range stmts {
		read := func() ([]Row, error) {
			rows, err := tx.QueryContext(ctx, stmt.SQL, bind(stmt, args)...)
			if err != nil {
				return nil, fmt.Errorf("executing query: %w", err)
			}
			defer func() { _ = rows.Close() }()
			return scanRows(rows, stmt)
		}

		result, err := read()
		if err != nil {
			return nil, err
		}
		out = append(out, result)
	}

	// Before the commit, so nothing is left on a connection going back to a
	// pool every projection reaching this database shares.
	if err := p.clearSession(ctx, tx, session); err != nil {
		return nil, err
	}

	// A read-only transaction still has to be closed to release its snapshot.
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}
	return out, nil
}

// TransactWith runs a transaction with session variables applied to it first.
func (p *Pool) TransactWith(ctx context.Context, session []SessionVariable, stmts []*Statement, args map[string]any) ([]Row, int64, error) {
	if len(stmts) == 0 {
		return nil, 0, fmt.Errorf("no statements to run")
	}
	return p.transact(ctx, session, stmts, args, stmts[len(stmts)-1].ReturnsRows)
}

// transact is the shared body: set the session, run the statements, commit.
func (p *Pool) transact(
	ctx context.Context,
	session []SessionVariable,
	stmts []*Statement,
	args map[string]any,
	returnsRows bool,
) (out []Row, affected int64, err error) {
	last := stmts[len(stmts)-1]
	var enforced bool
	for _, stmt := range stmts {
		enforced = enforced || stmt.EnforceTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, clientDeadline(enforced, last.Timeout))
	defer cancel()

	// One span for the transaction and one for each statement in it. The outer
	// span is what a slow write looks like from the caller; the inner ones are
	// what says which of the tables it spans was the slow half, which is the
	// question a multi-statement create otherwise leaves unanswerable.
	ctx, span := p.startSpan(ctx, "transaction", last.SQL)
	defer func() { span.endAffected(affected, err) }()

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("beginning transaction: %w", err)
	}
	// Rollback after a successful commit is a no-op, so this needs no
	// bookkeeping to be correct. The session is cleared alongside it, because a
	// MySQL user variable belongs to the connection rather than to the
	// transaction and a rollback leaves it set.
	defer func() {
		_ = p.clearSession(ctx, tx, session)
		_ = tx.Rollback()
	}()

	// Before anything reads or writes, so a row-level security policy is in
	// force for every statement in here — and so the database's own deadline
	// covers all of them, not only the one that happens to run long.
	if err := p.applyStatementTimeout(ctx, tx, enforced, last.Timeout); err != nil {
		return nil, 0, err
	}
	if err := p.applySession(ctx, tx, session); err != nil {
		return nil, 0, err
	}

	for _, stmt := range stmts[:len(stmts)-1] {
		_, stmtSpan := p.startSpan(ctx, "exec", stmt.SQL)
		result, err := tx.ExecContext(ctx, stmt.SQL, bind(stmt, args)...)
		if err != nil {
			stmtSpan.endAffected(0, err)
			return nil, 0, fmt.Errorf("executing statement: %w", err)
		}
		count, countErr := result.RowsAffected()
		if countErr != nil {
			count = -1
		} else {
			affected += count
		}
		stmtSpan.endAffected(count, nil)
	}

	if returnsRows {
		read := func() ([]Row, error) {
			_, stmtSpan := p.startSpan(ctx, "query", last.SQL)
			rows, err := tx.QueryContext(ctx, last.SQL, bind(last, args)...)
			if err != nil {
				stmtSpan.end(0, err)
				return nil, fmt.Errorf("executing statement: %w", err)
			}
			defer func() { _ = rows.Close() }()
			stmtSpan.returned()

			scanned, err := scanRows(rows, last)
			stmtSpan.end(len(scanned), err)
			return scanned, err
		}

		out, err = read()
		if err != nil {
			return nil, 0, err
		}
		affected += int64(len(out))
	} else {
		_, stmtSpan := p.startSpan(ctx, "exec", last.SQL)
		result, err := tx.ExecContext(ctx, last.SQL, bind(last, args)...)
		if err != nil {
			stmtSpan.endAffected(0, err)
			return nil, 0, fmt.Errorf("executing statement: %w", err)
		}
		count, countErr := result.RowsAffected()
		if countErr != nil {
			// Not every driver reports this; treat it as "unknown, but
			// succeeded", exactly as Exec does.
			count = -1
		}
		stmtSpan.endAffected(count, nil)
		affected += count
	}

	// Before the commit, for the same reason as on the read path.
	if err := p.clearSession(ctx, tx, session); err != nil {
		return nil, 0, err
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("committing transaction: %w", err)
	}
	return out, affected, nil
}

// bind resolves a statement's declared parameters from args, in the order the
// driver expects them.
func bind(stmt *Statement, args map[string]any) []any {
	values := make([]any, 0, len(stmt.Params))
	for _, name := range stmt.Params {
		values = append(values, args[name])
	}
	return values
}

// scanJSONArray reads a result set holding a single JSON array column and
// turns it into rows. This is the json_agg path: the database assembles the
// documents and the server decodes one value instead of scanning every column
// of every row.
func scanJSONArray(rows *sql.Rows, maxBytes int) ([]Row, error) {
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating rows: %w", err)
		}
		return nil, nil
	}

	var raw []byte
	if err := rows.Scan(&raw); err != nil {
		return nil, fmt.Errorf("scanning JSON aggregate: %w", err)
	}

	// Checked before decoding, which is where the cost doubles: the aggregate
	// is already held once as bytes and is about to be held again as maps. This
	// is the read maxRows cannot bound at all, because the whole collection
	// arrives as one row.
	if maxBytes > 0 && len(raw) > maxBytes {
		return nil, fmt.Errorf("JSON aggregate exceeded maxBytes (%d bytes against a limit of %d); "+
			"narrow the query or raise the limit", len(raw), maxBytes)
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}

	var decoded []map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decoding JSON aggregate: %w", err)
	}

	out := make([]Row, 0, len(decoded))
	for _, item := range decoded {
		out = append(out, Row(item))
	}
	return out, rows.Err()
}

// valueSize is roughly what a scanned value costs to hold.
//
// The variable-length types are what make a result set large; everything else
// is a number, a bool, or a time, and counting those exactly would cost more
// than it measures. Deliberately an estimate — it decides when to refuse a
// read, not what to report.
func valueSize(value any) int {
	switch typed := value.(type) {
	case nil:
		return 0
	case []byte:
		return len(typed)
	case string:
		return len(typed)
	default:
		return 8
	}
}

// PoolCache keeps one pool per data source key so that many projections
// sharing a database also share connections.
type PoolCache struct {
	mu    sync.Mutex
	pools map[string]*Pool
}

// NewPoolCache returns an empty cache.
func NewPoolCache() *PoolCache {
	return &PoolCache{pools: map[string]*Pool{}}
}

// Get returns the pool for key, creating it via open if absent.
func (c *PoolCache) Get(key string, open func() (*Pool, error)) (*Pool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if p, ok := c.pools[key]; ok {
		return p, nil
	}
	p, err := open()
	if err != nil {
		return nil, err
	}
	c.pools[key] = p
	return p, nil
}

// EvictIf closes and removes the pool for key, but only if it is still the pool
// the caller was using.
//
// Evicting by key alone is unsafe from anywhere that does not hold the pool it
// means to drop: the entry may have been replaced since, and closing the
// replacement takes the database away from every projection now serving through
// it. The admission check does exactly that — it retries after a pool is closed
// underneath it, and by then the key may hold a live pool.
func (c *PoolCache) EvictIf(key string, pool *Pool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	existing, ok := c.pools[key]
	if !ok || existing != pool {
		return false
	}
	_ = existing.Close()
	delete(c.pools, key)
	return true
}

// Evict closes and removes the pool for key.
func (c *PoolCache) Evict(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if p, ok := c.pools[key]; ok {
		_ = p.Close()
		delete(c.pools, key)
	}
}

// RetainOnly closes and forgets every pool whose key is not in keep, which is
// how connections belonging to deleted projections are released.
func (c *PoolCache) RetainOnly(keep map[string]struct{}) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	var evicted int
	for key, p := range c.pools {
		if _, wanted := keep[key]; wanted {
			continue
		}
		_ = p.Close()
		delete(c.pools, key)
		evicted++
	}
	return evicted
}

// All returns every open pool, in no particular order.
func (c *PoolCache) All() []*Pool {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]*Pool, 0, len(c.pools))
	for _, p := range c.pools {
		out = append(out, p)
	}
	return out
}

// Len reports how many pools are open.
func (c *PoolCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pools)
}

// Close releases every pooled connection.
func (c *PoolCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, p := range c.pools {
		_ = p.Close()
		delete(c.pools, key)
	}
}

// Check asks the database whether it could run a statement, without running it.
//
// Preparing is the cheapest way to find out. The database parses the statement
// and resolves every name in it against the catalogue — which is exactly the
// half that goes wrong when a projection outlives the schema it was written
// against — and then answers without touching a row. A statement that prepares
// may still fail on the data; one that does not prepare cannot succeed at all.
//
// The statement is rewritten first, so what the database is asked about is the
// statement that would actually run, placeholders and all.
func (p *Pool) Check(ctx context.Context, statement string) error {
	rewritten, _, err := Rewrite(statement, p.driver)
	if err != nil {
		return err
	}

	prepared, err := p.db.PrepareContext(ctx, rewritten)
	if err != nil {
		return err
	}
	// Closing is cleanup; whether the database could parse and plan the
	// statement is the answer, and it has already been given.
	defer func() { _ = prepared.Close() }()

	return nil
}

// warnIfUnencrypted says so when a data source is reached without transport
// encryption.
//
// A warning rather than a refusal: a unix socket, a sidecar proxy, or a
// database on the same host needs no TLS, and refusing those would be refusing
// perfectly ordinary deployments. What is not defensible is saying nothing —
// every connection string in this repository's documentation asks for TLS, and
// until now nothing noticed when a real one did not, so a pasted sslmode=disable
// sent credentials and every projected row across the network in the clear with
// no signal anywhere.
//
// Logged once per pool, and pools are shared by connection string, so this is
// once per database rather than once per projection.
func warnIfUnencrypted(driver Driver, dsn, label string) {
	if driver.Encrypted == nil || driver.Encrypted(dsn) {
		return
	}

	klog.InfoS("data source connects without transport encryption; credentials and every "+
		"projected row cross this connection in the clear",
		"datasource", label, "driver", driver.Name,
		"fix", encryptionHint(driver.Name))
}

// encryptionHint names the setting for the driver, so the warning says what to
// do rather than only what is wrong.
func encryptionHint(driver string) string {
	switch driver {
	case "postgres":
		return "set sslmode=require (or verify-full) in the connection string"
	case "mysql":
		return "set tls=true in the connection string"
	default:
		return "enable TLS in the connection string"
	}
}
