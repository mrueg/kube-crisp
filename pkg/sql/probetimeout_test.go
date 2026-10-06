package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// A database/sql driver whose connections never come up: Connect waits until
// it is given up on, the way a dial to a database that takes the connection
// and never answers does.
//
// Registered with database/sql and not with this package's registry, for the
// reason sessionRecorderDriver gives.
type silentDriver struct{}

func (silentDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("silent opens only through its connector")
}

func (silentDriver) OpenConnector(string) (driver.Connector, error) { return silentConnector{}, nil }

type silentConnector struct{}

func (silentConnector) Connect(ctx context.Context) (driver.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (silentConnector) Driver() driver.Driver { return silentDriver{} }

func init() {
	sql.Register("silent", silentDriver{})
}

func silentPool(t *testing.T) *Pool {
	t.Helper()
	db, err := sql.Open("silent", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Pool{db: db, driver: "postgres"}
}

// TestProbesGiveUpOnASilentDatabase: Ping and Check return once probeTimeout
// passes, even under a context with no deadline, and say the database is
// unavailable rather than that the statement is wrong.
func TestProbesGiveUpOnASilentDatabase(t *testing.T) {
	previous := probeTimeout
	probeTimeout = 50 * time.Millisecond
	t.Cleanup(func() { probeTimeout = previous })

	pool := silentPool(t)
	probes := map[string]func(context.Context) error{
		"Ping":  pool.Ping,
		"Check": func(ctx context.Context) error { return pool.Check(ctx, "SELECT 1") },
	}
	for name, probe := range probes {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- probe(context.Background()) }()

			select {
			case err := <-done:
				if !IsUnavailable(err) {
					t.Errorf("a database that never answered was not reported as unavailable: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the probe was still waiting on a database that never answers")
			}
		})
	}
}

// TestProbesReturnTheCallersCancellation: a caller that gave up is told so, not
// that the database went quiet.
func TestProbesReturnTheCallersCancellation(t *testing.T) {
	pool := silentPool(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := pool.Ping(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Ping under a cancelled context returned %v, want context.Canceled", err)
	}
	var probeErr *probeTimeoutError
	if errors.As(err, &probeErr) {
		t.Errorf("the caller's cancellation was reported as the database not answering: %v", err)
	}
}
