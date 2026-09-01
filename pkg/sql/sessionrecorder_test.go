package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
)

// A database/sql driver that records what it is asked to run.
//
// Registered with database/sql and not with this package's registry: the
// session tests construct a Pool directly, so what is needed is somewhere for
// its statements to land, not a projectable driver. Registering one here would
// also put a driver with no CRD enum entry in front of every other test that
// asks the registry what it holds.
type sessionRecorderDriver struct{}

type recordedExec struct {
	sql  string
	args []driver.NamedValue
}

var (
	sessionRecordMu sync.Mutex
	sessionRecorded []recordedExec
)

// resetSessionRecorder empties the log and returns a reader for it, so a test
// never sees what an earlier one ran.
func resetSessionRecorder() func() []recordedExec {
	sessionRecordMu.Lock()
	sessionRecorded = nil
	sessionRecordMu.Unlock()

	return func() []recordedExec {
		sessionRecordMu.Lock()
		defer sessionRecordMu.Unlock()
		return append([]recordedExec(nil), sessionRecorded...)
	}
}

func (sessionRecorderDriver) Open(string) (driver.Conn, error) { return sessionRecorderConn{}, nil }

type sessionRecorderConn struct{}

// Unprepared only, so that ExecContext sees the statement and its arguments
// rather than a handle to one prepared earlier.
func (sessionRecorderConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("session-recorder prepares nothing")
}
func (sessionRecorderConn) Close() error              { return nil }
func (sessionRecorderConn) Begin() (driver.Tx, error) { return sessionRecorderTx{}, nil }

func (sessionRecorderConn) ExecContext(
	_ context.Context, query string, args []driver.NamedValue,
) (driver.Result, error) {
	sessionRecordMu.Lock()
	defer sessionRecordMu.Unlock()
	sessionRecorded = append(sessionRecorded, recordedExec{sql: query, args: args})
	return driver.RowsAffected(0), nil
}

type sessionRecorderTx struct{}

func (sessionRecorderTx) Commit() error   { return nil }
func (sessionRecorderTx) Rollback() error { return nil }

func init() {
	sql.Register("session-recorder", sessionRecorderDriver{})
}
