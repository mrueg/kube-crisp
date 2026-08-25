package sql

import (
	"strings"
	"testing"
)

// TestBuiltInDriversAreRegistered pins what this build ships with, since the
// set is what a projection's dataSource.driver is checked against and what the
// CRD's enum has to agree with.
func TestBuiltInDriversAreRegistered(t *testing.T) {
	want := map[string]Driver{
		"postgres": {SQLDriver: "pgx", Placeholders: PlaceholderDollar,
			SessionVariables: true, StatementTimeout: true, Notifications: true},
		"mysql": {SQLDriver: "mysql", Placeholders: PlaceholderQuestion,
			SessionVariables: true},
		"sqlite": {SQLDriver: "sqlite", Placeholders: PlaceholderQuestion},
	}

	for name, expected := range want {
		got, ok := Lookup(name)
		if !ok {
			t.Errorf("the %s driver is not registered", name)
			continue
		}
		if got.SQLDriver != expected.SQLDriver {
			t.Errorf("%s opens with %q, want %q", name, got.SQLDriver, expected.SQLDriver)
		}
		if got.Placeholders != expected.Placeholders {
			t.Errorf("%s uses placeholder style %v, want %v", name, got.Placeholders, expected.Placeholders)
		}
		if got.SessionVariables != expected.SessionVariables {
			t.Errorf("%s reports session variables %v, want %v", name, got.SessionVariables, expected.SessionVariables)
		}
		if got.StatementTimeout != expected.StatementTimeout {
			t.Errorf("%s reports statement timeout %v, want %v", name, got.StatementTimeout, expected.StatementTimeout)
		}
		if got.Notifications != expected.Notifications {
			t.Errorf("%s reports notifications %v, want %v", name, got.Notifications, expected.Notifications)
		}
	}

	if _, ok := Lookup("nonesuch"); ok {
		t.Error("an unregistered driver was found")
	}
}

// TestRegisterOpensTheSet is the point of the registry: a build that links in
// another database/sql driver can serve projections against it without editing
// the switch statements that used to decide what a database could do.
func TestRegisterOpensTheSet(t *testing.T) {
	const name = "testdriver"
	t.Cleanup(func() {
		driversMu.Lock()
		delete(drivers, name)
		driversMu.Unlock()
	})

	added := Driver{
		Name: name, SQLDriver: "somedriver",
		Placeholders: PlaceholderDollar, SessionVariables: true, StatementTimeout: true,
	}
	if err := Register(added); err != nil {
		t.Fatalf("Register() returned error: %v", err)
	}

	// Everything that used to be a switch now answers for it.
	if !SupportsSessionVariables(name) {
		t.Error("the registered driver does not report session variables")
	}
	if !SupportsStatementTimeout(name) {
		t.Error("the registered driver does not report a statement timeout")
	}
	if SupportsNotifications(name) {
		t.Error("the registered driver reports notifications it did not declare")
	}

	rewritten, params, err := Rewrite("SELECT * FROM t WHERE id = :name", name)
	if err != nil {
		t.Fatalf("Rewrite() returned error: %v", err)
	}
	if !strings.Contains(rewritten, "$1") || len(params) != 1 || params[0] != "name" {
		t.Errorf("Rewrite() = %q %v, want the declared placeholder style", rewritten, params)
	}

	if err := Register(added); err == nil {
		t.Error("registering the same name twice was accepted")
	}
	if !contains(RegisteredDrivers(), name) {
		t.Errorf("RegisteredDrivers() = %v, want it to list %s", RegisteredDrivers(), name)
	}
}

// TestRegisterRejectsAnIncompleteDriver: a driver that does not say how to open
// it is one that fails at the first request instead of at registration.
func TestRegisterRejectsAnIncompleteDriver(t *testing.T) {
	if err := Register(Driver{SQLDriver: "x"}); err == nil {
		t.Error("a driver with no name was accepted")
	}
	if err := Register(Driver{Name: "nameless"}); err == nil {
		t.Error("a driver with no database/sql driver was accepted")
	}
}

// TestUnknownDriverNamesTheAlternatives, because "unsupported driver" without
// the list is a message that sends someone to the source.
func TestUnknownDriverNamesTheAlternatives(t *testing.T) {
	_, err := Open(PoolOptions{Driver: "postgress", DSN: "x"})
	if err == nil {
		t.Fatal("Open() accepted an unregistered driver")
	}
	for _, want := range []string{"postgress", "postgres", "mysql", "sqlite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// TestEncryptedReadsTheConnectionString covers what the warning is based on.
//
// The defaults are the reason this is not obvious. PostgreSQL's unset sslmode
// means "prefer", which tries TLS and continues without it if the server
// declines — so a connection string that says nothing is not one that gets
// encryption, it is one that gets whatever it is given without reporting which.
// MySQL's unset tls is plainer: no encryption at all.
func TestEncryptedReadsTheConnectionString(t *testing.T) {
	for _, tc := range []struct {
		driver string
		dsn    string
		want   bool
	}{
		{"postgres", "postgres://u:p@db:5432/store?sslmode=require", true},
		{"postgres", "postgres://u:p@db:5432/store?sslmode=verify-full", true},
		{"postgres", "postgres://u:p@db:5432/store?sslmode=disable", false},
		// Unset and prefer both fall back silently.
		{"postgres", "postgres://u:p@db:5432/store", false},
		{"postgres", "postgres://u:p@db:5432/store?sslmode=prefer", false},
		// The key=value form the driver also accepts.
		{"postgres", "host=db user=u sslmode=require", true},
		{"postgres", "host=db user=u sslmode=disable", false},

		{"mysql", "u:p@tcp(db:3306)/store?tls=true", true},
		{"mysql", "u:p@tcp(db:3306)/store?tls=custom", true},
		{"mysql", "u:p@tcp(db:3306)/store", false},
		{"mysql", "u:p@tcp(db:3306)/store?tls=false", false},
		// Encrypts, authenticates nothing; and falls back respectively.
		{"mysql", "u:p@tcp(db:3306)/store?tls=skip-verify", false},
		{"mysql", "u:p@tcp(db:3306)/store?tls=preferred", false},
	} {
		t.Run(tc.driver+" "+tc.dsn, func(t *testing.T) {
			driver, ok := Lookup(tc.driver)
			if !ok {
				t.Fatalf("driver %q is not registered", tc.driver)
			}
			if driver.Encrypted == nil {
				t.Fatalf("driver %q reports nothing about encryption", tc.driver)
			}
			if got := driver.Encrypted(tc.dsn); got != tc.want {
				t.Errorf("Encrypted(%q) = %v, want %v", tc.dsn, got, tc.want)
			}
		})
	}

	// SQLite is a local file, so there is no connection to encrypt and nothing
	// to warn about. A driver that says nothing is never warned about.
	sqlite, ok := Lookup("sqlite")
	if !ok {
		t.Fatal("the sqlite driver is not registered")
	}
	if sqlite.Encrypted != nil {
		t.Error("sqlite reports on transport encryption, which would warn about a local file")
	}
}
