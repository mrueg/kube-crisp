package sql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// PostgreSQL SQLSTATE codes for the constraint violations clients can act on.
const (
	pgUniqueViolation     = "23505"
	pgForeignKeyViolation = "23503"
)

// MySQL error numbers for the same conditions.
const (
	mysqlDuplicateEntry    = 1062
	mysqlForeignKeyParent  = 1451
	mysqlForeignKeyMissing = 1452
)

// PostgreSQL SQLSTATE class 40, transaction rollback: the database refusing a
// transaction it cannot serialise against a concurrent one. CockroachDB reports
// its retry errors as 40001 too.
const (
	pgSerializationFailure = "40001"
	pgDeadlockDetected     = "40P01"
)

// MySQL's number for the same: "Deadlock found when trying to get lock; try
// restarting transaction".
const mysqlDeadlock = 1213

// IsUniqueViolation reports whether a write failed because the row already
// exists, so callers can answer with AlreadyExists rather than an opaque error.
//
// Every driver phrases this differently, so it is classified by code where the
// driver offers one, with a text fallback for drivers and wrappers that do not.
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgUniqueViolation
	}

	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == mysqlDuplicateEntry
	}

	var sqliteErr *sqlitedriver.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() {
		case sqlite3.SQLITE_CONSTRAINT_UNIQUE, sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY:
			return true
		}
		return false
	}

	return containsAny(err, "duplicate key", "duplicate entry", "unique constraint", "unique violation")
}

// IsForeignKeyViolation reports whether a write failed against a foreign key.
func IsForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgForeignKeyViolation
	}

	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == mysqlForeignKeyParent || mysqlErr.Number == mysqlForeignKeyMissing
	}

	var sqliteErr *sqlitedriver.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_FOREIGNKEY
	}

	return containsAny(err, "foreign key")
}

// IsSerializationFailure reports whether the database rolled a write back
// because it could not run it alongside a concurrent one.
//
// This is the one class of failure a database raises specifically to say that
// nothing was changed and the same request will probably succeed if it is sent
// again: PostgreSQL's serialization_failure and deadlock_detected, and MySQL's
// deadlock, whose message ends "try restarting transaction". A projection using
// queries.create.statements for a check-then-insert meets it under any real
// concurrency, and so does any projection whose database runs at SERIALIZABLE.
// CockroachDB reports its retries the same way, and always at SERIALIZABLE.
//
// Deliberately not a lock wait timeout — MySQL 1205, or SQLite giving up after
// its busy timeout. Those say a lock was held too long, not that this write
// lost a race, and the difference matters twice over. They mean sustained
// contention rather than an unlucky interleaving, so telling a controller to
// come straight back makes the pile-up worse where a timeout carries
// backpressure. And MySQL rolls back only the statement on 1205 unless
// innodb_rollback_on_timeout is set, so "nothing was changed" is exactly what
// cannot be promised about a multi-statement write. They stay with the other
// timeouts.
//
// SQLite has no code in this class to report: it takes one writer at a time and
// the pool gives it a busy timeout, so what a projection meets there is the
// lock wait above.
func IsSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgSerializationFailure || pgErr.Code == pgDeadlockDetected
	}

	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == mysqlDeadlock
	}

	return containsAny(err,
		"could not serialize access",
		"deadlock detected",
		"deadlock found",
		"restart transaction",
	)
}

// pgQueryCanceled is the SQLSTATE PostgreSQL reports when it stops a statement
// itself, which is what a statement timeout produces.
const pgQueryCanceled = "57014"

// IsStatementTimeout reports whether the database gave up on a statement rather
// than the client giving up on waiting for it.
//
// With dataSource.statementTimeout the database's deadline is the one that
// fires, so this is the ordinary shape of a query that ran too long. It means
// the same thing to a client as a context deadline — come back later, or ask
// for less — so it is answered the same way, and not as an internal error.
func IsStatementTimeout(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgQueryCanceled
	}
	return false
}

// IsUnavailable reports whether a query failed because the database could not
// be reached, as opposed to rejecting the statement.
//
// The distinction decides what a client is told: an unreachable database is a
// 503 worth retrying, while a rejected statement is the projection's fault and
// retrying will not help.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) || errors.Is(err, sql.ErrTxDone) {
		return true
	}

	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return true
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}

	return containsAny(err,
		"connection refused",
		"no such host",
		"bad connection",
		"connection reset",
		"broken pipe",
		"dial tcp",
		"server closed the connection",
		"the database system is starting up",
		"too many connections",
	)
}

func containsAny(err error, needles ...string) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error())
	for _, needle := range needles {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}
