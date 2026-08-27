package sql

import (
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// Everything above this package reads an affected count of zero as "nothing
// matched". MySQL counts changed rows unless asked otherwise, so an update that
// writes the values a row already holds would be reported as a miss.
func TestMySQLConnectionsCountMatchedRows(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
	}{
		{"no parameters", "user:pass@tcp(db:3306)/orders"},
		{"existing parameters", "user:pass@tcp(db:3306)/orders?parseTime=true"},
		{"a question mark in the password", "user:pa?ss@tcp(db:3306)/orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := mysqldriver.ParseDSN(mysqlFoundRows(tc.dsn))
			if err != nil {
				t.Fatalf("the driver could not parse the connection string: %v", err)
			}
			if !config.ClientFoundRows {
				t.Error("the connection counts changed rows, so an update that changes nothing " +
					"reports having matched nothing")
			}
		})
	}
}

// A connection string that already says so is left alone, whichever way it says
// it.
func TestMySQLFoundRowsRespectsAnExplicitSetting(t *testing.T) {
	for _, dsn := range []string{
		"user:pass@tcp(db:3306)/orders?clientFoundRows=false",
		"user:pass@tcp(db:3306)/orders?clientFoundRows=true",
	} {
		if got := mysqlFoundRows(dsn); got != dsn {
			t.Errorf("mysqlFoundRows(%q) = %q, want it unchanged", dsn, got)
		}
	}
}

// The registry is what actually decides this: a helper nothing is wired to
// would leave every real connection counting changed rows.
func TestTheRegisteredMySQLDriverAsksForMatchedRows(t *testing.T) {
	driver, ok := Lookup("mysql")
	if !ok {
		t.Fatal("the mysql driver is not registered")
	}
	if driver.PrepareDSN == nil {
		t.Fatal("the mysql driver adapts nothing, so clientFoundRows is never set")
	}

	config, err := mysqldriver.ParseDSN(driver.PrepareDSN("user:pass@tcp(db:3306)/orders"))
	if err != nil {
		t.Fatalf("the driver could not parse the connection string it prepared: %v", err)
	}
	if !config.ClientFoundRows {
		t.Error("the registered mysql driver does not ask for matched rows")
	}
}
