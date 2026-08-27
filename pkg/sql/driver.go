package sql

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// PlaceholderStyle is how a database wants bind parameters written.
type PlaceholderStyle int

const (
	// PlaceholderDollar numbers its parameters: $1, $2. PostgreSQL.
	PlaceholderDollar PlaceholderStyle = iota
	// PlaceholderQuestion is positional: ?. MySQL and SQLite.
	PlaceholderQuestion
)

// Driver describes a kind of database kube-crisp can project from.
//
// It exists so that the set is open. Everything that differs between databases
// is stated here rather than scattered through switch statements, so adding one
// is a registration rather than an edit in six places.
//
// Adding a driver means building your own binary regardless — a database/sql
// driver has to be linked in — so a build that registers one also regenerates
// the CRD, whose enum lists the drivers that build accepts.
type Driver struct {
	// Name is what a projection's dataSource.driver carries.
	Name string

	// SQLDriver is the name the database/sql driver registered itself under,
	// which is not always the same: the pgx driver answers to "pgx".
	SQLDriver string

	// Placeholders is how bind parameters are written for this database.
	Placeholders PlaceholderStyle

	// SessionVariables reports whether a setting can be scoped to one request,
	// which is what row-level security is driven through. A driver without them
	// rejects a projection that asks rather than silently doing nothing.
	SessionVariables bool

	// StatementTimeout reports whether the database can be told to abort a
	// statement that outruns its deadline, rather than only being abandoned.
	StatementTimeout bool

	// Notifications reports whether the database can push a hint that something
	// changed, which is what turns a watch from a poll into a wake-up.
	Notifications bool

	// PrepareDSN adapts a connection string before it is opened, for a default
	// that belongs to the driver rather than to the projection. Optional.
	PrepareDSN func(dsn string) string

	// Encrypted reports whether a connection string asks for transport
	// encryption. Optional; a driver that does not answer is never warned about.
	//
	// Credentials and every projected row cross this connection, so a database
	// reached over a network without it is sending both in the clear. Whether
	// that matters is the operator's call — a unix socket or a sidecar proxy
	// needs nothing — which is why this produces a warning and not a refusal.
	// What it must not do is say nothing at all, which is what happened before:
	// every example in the documentation asks for TLS and nothing noticed when
	// a connection string did not.
	Encrypted func(dsn string) bool
}

var (
	driversMu sync.RWMutex
	drivers   = map[string]Driver{}
)

// Register makes a driver available to projections.
//
// Call it before the server starts serving; a projection that names an
// unregistered driver is refused when it is compiled.
func Register(d Driver) error {
	switch {
	case d.Name == "":
		return fmt.Errorf("a driver needs a name")
	case d.SQLDriver == "":
		return fmt.Errorf("driver %q does not say which database/sql driver to open it with", d.Name)
	}

	driversMu.Lock()
	defer driversMu.Unlock()

	if _, taken := drivers[d.Name]; taken {
		return fmt.Errorf("driver %q is already registered", d.Name)
	}
	drivers[d.Name] = d
	return nil
}

// Lookup returns the driver registered under a name.
func Lookup(name string) (Driver, bool) {
	driversMu.RLock()
	defer driversMu.RUnlock()

	d, ok := drivers[name]
	return d, ok
}

// RegisteredDrivers lists what a projection may name, sorted. It is what an
// error about an unknown driver should offer instead.
func RegisteredDrivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()

	names := make([]string, 0, len(drivers))
	for name := range drivers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// SupportsSessionVariables reports whether a driver has anything to set. SQLite
// has no session state to speak of, so a projection asking for it there is a
// mistake worth reporting at load time rather than a silent no-op.
func SupportsSessionVariables(driver string) bool {
	d, ok := Lookup(driver)
	return ok && d.SessionVariables
}

// SupportsStatementTimeout reports whether a driver can be told to abort a
// statement that runs too long.
//
// PostgreSQL only, among the built-ins. MySQL's max_execution_time is
// connection-scoped and applies to read-only SELECTs, which is most of a
// projection but not all of it, and SQLite has no equivalent at all — so rather
// than half a feature that is silently absent for a write, the other two are
// refused.
func SupportsStatementTimeout(driver string) bool {
	d, ok := Lookup(driver)
	return ok && d.StatementTimeout
}

// SupportsNotifications reports whether a driver can push a change hint, which
// is what lets a watch be woken rather than having to ask on a timer.
func SupportsNotifications(driver string) bool {
	d, ok := Lookup(driver)
	return ok && d.Notifications
}

// The drivers this build ships with. A projection naming one of these needs
// nothing but a connection string.
func init() {
	for _, d := range []Driver{
		{
			Name:             "postgres",
			SQLDriver:        "pgx",
			Placeholders:     PlaceholderDollar,
			SessionVariables: true,
			StatementTimeout: true,
			Notifications:    true,
			Encrypted:        postgresEncrypted,
		},
		{
			// CockroachDB speaks the PostgreSQL wire protocol, so it is the
			// same driver and the same connection strings — but it has no
			// LISTEN/NOTIFY.
			//
			// Registered separately for exactly that. Pointed at "postgres" it
			// would be told notifications work, and a projection configuring
			// watch.notify would subscribe to something that never fires and
			// wait forever at its poll interval with nothing to say why. Named
			// here, the same projection is refused at load time.
			Name:             "cockroach",
			SQLDriver:        "pgx",
			Placeholders:     PlaceholderDollar,
			SessionVariables: true,
			StatementTimeout: true,
			Notifications:    false,
			Encrypted:        postgresEncrypted,
		},
		{
			Name:             "mysql",
			SQLDriver:        "mysql",
			Placeholders:     PlaceholderQuestion,
			SessionVariables: true,
			Encrypted:        mysqlEncrypted,
			PrepareDSN:       mysqlFoundRows,
		},
		{
			Name:         "sqlite",
			SQLDriver:    "sqlite",
			Placeholders: PlaceholderQuestion,
			PrepareDSN:   sqliteBusyTimeout,
		},
	} {
		if err := Register(d); err != nil {
			panic(fmt.Sprintf("registering the built-in %s driver: %v", d.Name, err))
		}
	}
}

// postgresEncrypted reports whether a PostgreSQL connection string asks for TLS.
//
// sslmode is the setting, and its default is the reason this is worth checking
// at all: unset means "prefer", which tries TLS and then silently continues
// without it if the server declines. A connection string that says nothing is
// therefore not a connection string that gets encryption — it is one that gets
// whatever the server offers, without telling anybody which it was.
//
// Only require, verify-ca and verify-full actually insist. prefer and allow
// fall back; disable never tries.
func postgresEncrypted(dsn string) bool {
	switch strings.ToLower(dsnParam(dsn, "sslmode")) {
	case "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

// mysqlEncrypted reports whether a MySQL connection string asks for TLS.
//
// The Go driver defaults to none, so an unset tls parameter is plaintext rather
// than a negotiation.
func mysqlEncrypted(dsn string) bool {
	switch strings.ToLower(dsnParam(dsn, "tls")) {
	case "", "false", "0", "skip-verify", "preferred":
		// skip-verify encrypts but authenticates nothing, and preferred falls
		// back — neither is a connection anybody should be told is secured.
		return false
	default:
		return true
	}
}

// dsnParam pulls one setting out of a connection string, in either of the two
// shapes these drivers accept: a URL with a query string, or the trailing
// ?key=value a DSN carries.
//
// Deliberately forgiving. It is used to decide whether to log a warning, so a
// connection string it cannot parse produces one — which is the safe direction
// for something whose job is to notice an unencrypted connection.
func dsnParam(dsn, key string) string {
	query := dsn
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		query = dsn[i+1:]
	} else if !strings.Contains(dsn, "=") {
		return ""
	}

	for _, pair := range strings.FieldsFunc(query, func(r rune) bool { return r == '&' || r == ' ' }) {
		name, value, found := strings.Cut(pair, "=")
		if found && strings.EqualFold(strings.TrimSpace(name), key) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
