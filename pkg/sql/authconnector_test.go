package sql

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// unroutable is a port nothing listens on, so a connection attempt fails
// immediately. These tests are about what happens before the dial, and the dial
// itself is expected to fail — what they assert is that a password was asked
// for first, and that the error carries the reason when asking is what went
// wrong.
const unroutable = "127.0.0.1:1"

// The real connectors, checked for the one thing that cannot be seen from
// outside: that a password is minted per connection rather than read out of the
// connection string once.
//
// There is no database here and none is needed. A connector that asked for a
// password at build time and reused it would still connect, and every test that
// went through a real database would still pass — right up to the point,
// fifteen minutes into production, where every new connection starts failing to
// authenticate. So the assertion is on the call, not on the connection.
func TestEveryDriversConnectorMintsAPasswordBeforeItDials(t *testing.T) {
	for _, tc := range []struct {
		driver string
		dsn    string
	}{
		{"postgres", "postgres://orders@" + unroutable + "/store?sslmode=require"},
		{"cockroach", "postgres://orders@" + unroutable + "/store?sslmode=require"},
		{"mysql", "orders@tcp(" + unroutable + ")/store?tls=skip-verify"},
	} {
		t.Run(tc.driver, func(t *testing.T) {
			driver, ok := Lookup(tc.driver)
			if !ok {
				t.Fatalf("driver %q is not registered", tc.driver)
			}
			if driver.AuthConnector == nil {
				t.Fatalf("driver %q has no connector, so it cannot take a minted password", tc.driver)
			}

			var asked int
			connector, err := driver.AuthConnector(tc.dsn, CredentialsFunc(func(context.Context) (string, error) {
				asked++
				return "minted", nil
			}))
			if err != nil {
				t.Fatalf("AuthConnector() returned error: %v", err)
			}

			// Building the connector must not mint anything: a pool is opened
			// once and lives for days, so a password obtained here would be one
			// obtained days before it is used.
			if asked != 0 {
				t.Errorf("building the connector asked for %d passwords, want none until a connection is opened", asked)
			}

			// The dial fails; that is not what is being tested.
			if conn, err := connector.Connect(context.Background()); err == nil {
				_ = conn.Close()
				t.Fatal("connecting to a port nothing listens on succeeded")
			}
			if asked != 1 {
				t.Errorf("opening a connection asked for %d passwords, want exactly one minted for it", asked)
			}

			// And again, because the point of the whole arrangement is that the
			// second connection does not reuse the first one's token.
			if conn, err := connector.Connect(context.Background()); err == nil {
				_ = conn.Close()
			}
			if asked != 2 {
				t.Errorf("a second connection asked for %d passwords in total, want one each", asked)
			}
		})
	}
}

// A provider that cannot mint a token has to say so through the connection
// attempt, with its own message intact. Swallowed, it becomes an
// authentication failure that names the database and not the credential.
func TestAConnectionFailsWithTheReasonAPasswordCouldNotBeMinted(t *testing.T) {
	refused := errors.New("the instance role cannot rds-db:connect as orders")

	for _, tc := range []struct {
		driver string
		dsn    string
	}{
		{"postgres", "postgres://orders@" + unroutable + "/store?sslmode=require"},
		{"mysql", "orders@tcp(" + unroutable + ")/store?tls=skip-verify"},
	} {
		t.Run(tc.driver, func(t *testing.T) {
			driver, _ := Lookup(tc.driver)
			connector, err := driver.AuthConnector(tc.dsn, CredentialsFunc(func(context.Context) (string, error) {
				return "", refused
			}))
			if err != nil {
				t.Fatalf("AuthConnector() returned error: %v", err)
			}

			_, err = connector.Connect(context.Background())
			if err == nil {
				t.Fatal("a connection was opened without a password")
			}
			if !errors.Is(err, refused) {
				t.Errorf("Connect() returned %v, want the provider's own error", err)
			}
		})
	}
}

// A connection string the driver cannot read fails when the pool is opened,
// which is while the projection is being compiled, rather than once per
// connection attempt from inside a pool nobody is looking at.
func TestAConnectionStringThatCannotBeParsedFailsWhenTheConnectorIsBuilt(t *testing.T) {
	for _, tc := range []struct{ driver, dsn string }{
		{"postgres", "postgres://orders@db:not-a-port/store"},
		{"mysql", "this is not a mysql dsn"},
	} {
		t.Run(tc.driver, func(t *testing.T) {
			driver, _ := Lookup(tc.driver)
			_, err := driver.AuthConnector(tc.dsn, CredentialsFunc(func(context.Context) (string, error) {
				return "minted", nil
			}))
			if err == nil {
				t.Fatal("AuthConnector() accepted a connection string the driver cannot read")
			}
			if !strings.Contains(err.Error(), "connection string") {
				t.Errorf("error %q does not say what could not be read", err)
			}
		})
	}
}

// MySQL checks a minted password with the cleartext plugin, which the driver
// refuses unless it is told to allow it — so a projection would compile, report
// Ready, and fail every connection with a message about native password
// authentication that names neither the setting nor the reason.
//
// Forcing it on is only defensible because Open refuses a credential provider
// on a connection the driver does not report as encrypted, so this code is
// never reached outside TLS. The two decisions are one decision, and this test
// is here to fail if either half is removed on its own.
func TestTheMySQLConnectorAllowsTheCleartextPluginAMintedPasswordNeeds(t *testing.T) {
	connector, err := mysqlAuthConnector("orders@tcp(db:3306)/store?tls=true",
		CredentialsFunc(func(context.Context) (string, error) { return "minted", nil }))
	if err != nil {
		t.Fatalf("mysqlAuthConnector() returned error: %v", err)
	}

	mysqlConn, ok := connector.(*mysqlConnector)
	if !ok {
		t.Fatalf("mysqlAuthConnector() returned %T, want the connector that substitutes the password", connector)
	}
	if !mysqlConn.config.AllowCleartextPasswords {
		t.Error("the MySQL connector refuses the cleartext plugin, which is the only way RDS IAM tokens are checked")
	}

	// The other half: a plaintext connection with auth never gets this far.
	mysql, _ := Lookup("mysql")
	_, err = authConnector(mysql, "orders@tcp(db:3306)/store", &AuthOptions{Provider: "irrelevant"})
	if err == nil {
		t.Fatal("a minted credential was accepted on an unencrypted MySQL connection")
	}
	if !strings.Contains(err.Error(), "transport encryption") {
		t.Errorf("error %q does not name the missing encryption", err)
	}
}
