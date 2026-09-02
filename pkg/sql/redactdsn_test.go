package sql

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// A connection string is a credential, and it is also whatever was in the
// Secret the projection named — the resolver hands the value over without
// inspecting it. Drivers quote what they could not parse, and those errors do
// not stay in the process: they reach the DataSourceConnected condition, a 503
// body, and the log.
//
// So naming any key of a readable Secret as dsnKey, letting the parse fail, and
// reading the value back out of the projection's status turned permission to
// use a Secret into permission to read every key of it.
func TestAConnectionStringIsNotEchoedBackInAnError(t *testing.T) {
	const secret = "an-api-token-that-is-not-a-dsn"

	pool, err := Open(PoolOptions{Driver: "postgres", DSN: secret})
	if err != nil {
		// Opening is lazy for pgx, so the parse failure usually arrives at the
		// ping below. If it arrives here, it must already be redacted.
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("Open() echoed the connection string: %v", err)
		}
		return
	}
	t.Cleanup(func() { _ = pool.Close() })

	err = pool.Ping(context.Background())
	if err == nil {
		t.Skip("this build's driver accepted the string; nothing to redact")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("Ping() echoed the connection string: %v", err)
	}
	if !strings.Contains(err.Error(), "redacted") {
		t.Errorf("the error does not say something was removed: %v", err)
	}
}

// redactDSN itself, including the cases where it must do nothing.
func TestRedactDSN(t *testing.T) {
	const dsn = "postgres://user:hunter2@db:5432/store"

	got := redactDSN(dsn, errors.New("cannot parse `"+dsn+"`: bad"))
	if strings.Contains(got.Error(), "hunter2") {
		t.Errorf("redactDSN() left the credential in: %v", got)
	}
	if !strings.Contains(got.Error(), "cannot parse") {
		t.Errorf("redactDSN() removed more than the connection string: %v", got)
	}

	if redactDSN(dsn, nil) != nil {
		t.Error("redactDSN() invented an error")
	}
	unrelated := errors.New("connection refused")
	// The same error back, not a copy of it: nothing to redact must mean
	// nothing touched, so a caller's errors.Is still works.
	if !errors.Is(redactDSN(dsn, unrelated), unrelated) {
		t.Error("redactDSN() rewrote an error that did not carry the connection string")
	}
	if !errors.Is(redactDSN("", unrelated), unrelated) {
		t.Error("redactDSN() rewrote an error with no connection string to look for")
	}
}
