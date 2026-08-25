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
