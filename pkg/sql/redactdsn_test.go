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

// Taking the exact connection string back out is not enough, because a driver
// does not always quote it exactly.
//
// pgx masks what it recognises as a password and quotes the rest, so the
// string in its error is no longer the one redactDSN looks for, and everything
// that is not called password -- a cloud key, a URL's user name, whatever else
// the named Secret key held -- went through whole. MySQL quotes the value of a
// parameter it could not read. What a parse failure is allowed to say is why,
// in the driver's words, and never what it was given.
func TestAParseFailureKeepsItsReasonAndNotItsInput(t *testing.T) {
	for _, tc := range []struct {
		name, driver, dsn string
		secrets           []string
		reason            string
	}{
		{
			name:    "postgres keyword/value",
			driver:  "postgres",
			dsn:     "aws_secret_access_key=SECRETKEY123 password=hunter2 port=notanumber",
			secrets: []string{"SECRETKEY123", "hunter2", "aws_secret_access_key"},
			reason:  "invalid port",
		},
		{
			name:    "postgres URL with user information",
			driver:  "postgres",
			dsn:     "postgres://admin-SECRETUSER:hunter2@db:notaport/store?aws_secret_access_key=SECRETKEY123",
			secrets: []string{"SECRETKEY123", "hunter2", "SECRETUSER"},
			reason:  "invalid port",
		},
		{
			// net/url quotes its whole input too, inside pgx's own message.
			name:    "postgres URL net/url cannot read",
			driver:  "postgres",
			dsn:     "postgres://admin-SECRETUSER:hunter2@db/store%zz?aws_secret_access_key=SECRETKEY123",
			secrets: []string{"SECRETKEY123", "hunter2", "SECRETUSER"},
			reason:  "failed to parse as URL",
		},
		{
			name:    "postgres URL that parses but does not configure",
			driver:  "postgres",
			dsn:     "postgres://admin-SECRETUSER:hunter2@db:5432/store?target_session_attrs=SECRETKEY123",
			secrets: []string{"SECRETKEY123", "hunter2", "SECRETUSER"},
			reason:  "unknown target_session_attrs value",
		},
		{
			name:    "mysql parameter value",
			driver:  "mysql",
			dsn:     "admin-SECRETUSER:hunter2@tcp(db:3306)/store?parseTime=SECRETKEY123",
			secrets: []string{"SECRETKEY123", "hunter2", "SECRETUSER"},
			reason:  "invalid bool value",
		},
		{
			name:    "mysql shape",
			driver:  "mysql",
			dsn:     "admin-SECRETUSER:hunter2@tcp(db:3306)store",
			secrets: []string{"hunter2", "SECRETUSER"},
			reason:  "missing the slash",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := Open(PoolOptions{Driver: tc.driver, DSN: tc.dsn})
			if err == nil {
				t.Cleanup(func() { _ = pool.Close() })
				err = pool.Ping(context.Background())
			}
			if err == nil {
				t.Fatal("the connection string was accepted")
			}
			for _, secret := range tc.secrets {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("the error carries %q from the connection string: %v", secret, err)
				}
			}
			if !strings.Contains(err.Error(), tc.reason) {
				t.Errorf("the error lost the driver's reason %q: %v", tc.reason, err)
			}
		})
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
