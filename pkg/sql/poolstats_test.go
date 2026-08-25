package sql

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// TestIdleCapacityTracksThePool checks that a pool left to its defaults keeps
// as many idle connections as it is allowed to open.
//
// It used to keep two, whatever the pool size. Under steady load that costs
// nothing, because database/sql hands a released connection straight to a
// waiter — but an API server's traffic arrives in waves, and between waves
// every connection past the second was closed and re-dialled by the next wave.
// Measured against PostgreSQL at eight concurrent queries per wave: 150 of 200
// queries paid for a new connection, at six times the wall-clock.
func TestIdleCapacityTracksThePool(t *testing.T) {
	for _, tc := range []struct {
		name     string
		open     int
		idle     int
		wantIdle int
	}{
		{"both unset", 0, 0, DefaultMaxOpenConns},
		{"open set, idle unset", 4, 0, 4},
		{"idle set explicitly", 10, 3, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := Open(PoolOptions{
				Driver:       "sqlite",
				DSN:          filepath.Join(t.TempDir(), "idle.db"),
				MaxOpenConns: tc.open,
				MaxIdleConns: tc.idle,
			})
			if err != nil {
				t.Fatalf("opening the pool: %v", err)
			}
			t.Cleanup(func() { _ = pool.Close() })

			if got := pool.db.Stats().MaxIdleClosed; got != 0 {
				t.Fatalf("a pool that has served nothing closed %d connections", got)
			}
			// database/sql does not report MaxIdleConns, but it does clamp the
			// pool to it: opening more connections than the idle limit and
			// releasing them all is what makes it close the surplus.
			if got := idleCapacity(t, pool, tc.wantIdle); got != tc.wantIdle {
				t.Errorf("pool keeps %d idle connections, want %d", got, tc.wantIdle)
			}
		})
	}
}

// idleCapacity borrows probe+1 connections at once, releases them, and reports
// how many the pool kept.
func idleCapacity(t *testing.T, pool *Pool, probe int) int {
	t.Helper()

	conns := make([]interface{ Close() error }, 0, probe)
	for i := 0; i < probe; i++ {
		conn, err := pool.db.Conn(t.Context())
		if err != nil {
			t.Fatalf("borrowing connection %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	return pool.db.Stats().Idle
}

// TestPoolStatsArePublishedWithoutKeepAlive covers a pool configured with
// keepAliveInterval: 0.
//
// Statistics used to ride along on the keep-alive ticker, so turning the pings
// off — which is a supported thing to do, for a database behind a proxy that
// objects to them — silently took every pool metric with it. On a dashboard
// that is indistinguishable from a pool sitting idle.
func TestPoolStatsArePublishedWithoutKeepAlive(t *testing.T) {
	crispmetrics.DataSourceConnections.Reset()
	t.Cleanup(crispmetrics.DataSourceConnections.Reset)

	pool, err := Open(PoolOptions{
		Driver:            "sqlite",
		DSN:               filepath.Join(t.TempDir(), "stats.db"),
		Name:              "no-keepalive",
		KeepAliveInterval: 0,
	})
	if err != nil {
		t.Fatalf("opening the pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	// Open publishes once so the series exists immediately, rather than after
	// the first interval elapses.
	if count := testutil.CollectAndCount(crispmetrics.DataSourceConnections, "kube_crisp_datasource_connections"); count == 0 {
		t.Fatal("a pool with the keep-alive disabled published no connection metrics")
	}
}

// TestClosingAPoolRemovesItsMetrics checks that a pool takes its series with it.
//
// Nothing removed them before, so a projection that was deleted, or one whose
// DSN changed, left its last reading in /metrics for the life of the process: a
// pool that is gone goes on reporting the connections it had, and an alert on
// pool exhaustion fires on a database nothing is connected to.
func TestClosingAPoolRemovesItsMetrics(t *testing.T) {
	crispmetrics.DataSourceConnections.Reset()
	crispmetrics.DataSourceConnectionsClosed.Reset()
	crispmetrics.DataSourceWaitSeconds.Reset()
	crispmetrics.PreparedStatements.Reset()
	t.Cleanup(func() {
		crispmetrics.DataSourceConnections.Reset()
		crispmetrics.DataSourceConnectionsClosed.Reset()
		crispmetrics.DataSourceWaitSeconds.Reset()
		crispmetrics.PreparedStatements.Reset()
	})

	pool, err := Open(PoolOptions{
		Driver:            "sqlite",
		DSN:               filepath.Join(t.TempDir(), "closed.db"),
		Name:              "going-away",
		KeepAliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("opening the pool: %v", err)
	}

	before := testutil.CollectAndCount(crispmetrics.DataSourceConnections, "kube_crisp_datasource_connections")
	if before == 0 {
		t.Fatal("the pool published no connection metrics to begin with")
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("closing the pool: %v", err)
	}

	if after := testutil.CollectAndCount(crispmetrics.DataSourceConnections, "kube_crisp_datasource_connections"); after != 0 {
		t.Errorf("%d connection series survived the pool being closed", after)
	}
	if after := testutil.CollectAndCount(crispmetrics.DataSourceWaitSeconds, "kube_crisp_datasource_wait_seconds"); after != 0 {
		t.Errorf("%d wait series survived the pool being closed", after)
	}
	if after := testutil.CollectAndCount(crispmetrics.PreparedStatements, "kube_crisp_datasource_prepared_statements"); after != 0 {
		t.Errorf("%d prepared-statement series survived the pool being closed", after)
	}
}
