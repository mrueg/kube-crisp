package sql

import (
	"fmt"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsSerializationFailureClassifiesEveryDriver covers the one class of
// failure a database raises to say that nothing was changed and the same
// request will probably work if it is sent again. Wrapped, because the write
// path adds context on the way out and the classification has to survive it.
func TestIsSerializationFailureClassifiesEveryDriver(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			"PostgreSQL serialization_failure",
			&pgconn.PgError{Code: "40001", Message: "could not serialize access due to concurrent update"},
		},
		{
			"PostgreSQL deadlock_detected",
			&pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
		},
		{
			"MySQL deadlock",
			&mysqldriver.MySQLError{
				Number:  1213,
				Message: "Deadlock found when trying to get lock; try restarting transaction",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !IsSerializationFailure(tc.err) {
				t.Errorf("IsSerializationFailure(%v) = false, want true", tc.err)
			}
			wrapped := fmt.Errorf("executing statement: %w", tc.err)
			if !IsSerializationFailure(wrapped) {
				t.Errorf("IsSerializationFailure(%v) = false through a wrap", wrapped)
			}
		})
	}
}

// TestIsSerializationFailureLeavesTheOtherFailuresAlone. Each of these has its
// own answer already, and claiming them here would replace a 503 or a 409
// AlreadyExists with the wrong one — a lock wait timeout above all, which is
// deliberately not in this class.
func TestIsSerializationFailureLeavesTheOtherFailuresAlone(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{
			"MySQL lock wait timeout",
			&mysqldriver.MySQLError{
				Number:  1205,
				Message: "Lock wait timeout exceeded; try restarting transaction",
			},
		},
		{
			"PostgreSQL unique violation",
			&pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint"},
		},
		{
			"PostgreSQL statement timeout",
			&pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"},
		},
		{"an unreachable database", fmt.Errorf("dial tcp 10.0.0.1:5432: connection refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if IsSerializationFailure(tc.err) {
				t.Errorf("IsSerializationFailure(%v) = true, want false", tc.err)
			}
		})
	}
}

// TestSerializationFailureFallsBackToText covers drivers and wrappers that do
// not expose a typed error, the way the other classifiers do.
func TestSerializationFailureFallsBackToText(t *testing.T) {
	for _, message := range []string{
		"ERROR: could not serialize access due to read/write dependencies among transactions (SQLSTATE 40001)",
		"ERROR: deadlock detected (SQLSTATE 40P01)",
		"Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction",
		"restart transaction: TransactionRetryWithProtoRefreshError",
	} {
		if !IsSerializationFailure(fmt.Errorf("%s", message)) {
			t.Errorf("a retryable message was not classified: %s", message)
		}
	}

	if IsSerializationFailure(fmt.Errorf("duplicate key value violates unique constraint")) {
		t.Error("a unique violation was classified as a serialization failure")
	}
}
