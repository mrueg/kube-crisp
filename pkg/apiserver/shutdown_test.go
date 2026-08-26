package apiserver

import (
	"context"
	"strings"
	"testing"
)

// TestPreShutdownLeavesTheDatabaseAlone is the ordering this depends on.
//
// Pre-shutdown hooks run before in-flight requests drain — hooks, then stop
// accepting, then wait on the request and watch wait groups, then
// InFlightRequestsDrained. Closing the pools from a hook therefore pulled the
// database out from under every request still being answered, and every watch
// poll until the last watcher went away. They got "sql: database is closed",
// which no client can act on and no retry reaches.
//
// Draining gracefully is the point of the drain, and the drain needs the
// database.
func TestPreShutdownLeavesTheDatabaseAlone(t *testing.T) {
	config := offlineConfig(t, testProjection())
	server, err := config.Complete().New()
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	// A pool the way a request in flight would be holding one. Compiling the
	// projection above opened it, so the cache has exactly one.
	pools := server.pools.All()
	if len(pools) != 1 {
		t.Fatalf("the server holds %d pools, want 1", len(pools))
	}
	pool := pools[0]
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("the pool is not usable to begin with: %v", err)
	}

	if err := server.GenericAPIServer.RunPreShutdownHooks(); err != nil {
		t.Fatalf("RunPreShutdownHooks() returned error: %v", err)
	}

	// Everything that is still draining runs against this.
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("the database was closed before in-flight requests could drain: %v", err)
	}

	// And closing it after serving still works, which is where it moved to.
	server.ClosePools()
	err = pool.Ping(context.Background())
	if err == nil {
		t.Fatal("ClosePools() left the pool open")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("closing the pools failed in an unexpected way: %v", err)
	}
}
