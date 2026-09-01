package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// authConnector builds the connector a pool with dataSource.auth opens through,
// or nil when there is no auth and the ordinary DSN path applies.
//
// Everything that can be wrong with an auth stanza is decided here, before a
// single connection is attempted, because this runs while the projection is
// being compiled and that is where somebody is looking. A provider that is not
// registered, a driver that cannot take a per-connection password, a connection
// string the driver cannot parse, a provider that rejects its own options: all
// of them fail the projection with a message, rather than becoming a pool that
// reports Ready and then fails every request.
func authConnector(d Driver, dsn string, auth *AuthOptions) (driver.Connector, error) {
	if auth == nil {
		return nil, nil
	}

	if d.AuthConnector == nil {
		return nil, fmt.Errorf(
			"driver %q cannot authenticate with a password obtained per connection; this build can do that for %s",
			d.Name, joinOr(driversWithAuthConnector(), "no driver at all"))
	}

	// A minted credential over an unencrypted connection is refused, where a
	// static one is only warned about.
	//
	// The difference is what the credential is. A password in a Secret is a
	// shared secret the database also holds, and an operator who puts one on a
	// plaintext connection has decided something about the network it crosses —
	// a unix socket, a sidecar, a host it never leaves. A minted credential is a
	// bearer token: it is valid for whoever holds it, for the quarter of an hour
	// it lives, and nothing else stands between it and the database. It is also
	// sent as typed, because that is what these schemes require — an RDS IAM
	// token is a signed URL checked as a cleartext password — so there is no
	// challenge-response hiding it on the wire the way there is for a password.
	//
	// It costs nothing to refuse, either, because the cloud refuses it too: RDS
	// requires TLS for IAM authentication and drops the connection itself. So
	// this is not a policy imposed on an operator, it is the database's own
	// rule, said while the projection is being compiled and with the name of the
	// setting in it — rather than at connect time as an authentication failure
	// that names neither.
	//
	// Encrypted is never nil here: Register refuses a driver that offers an
	// AuthConnector without also being able to answer this question.
	if !d.Encrypted(dsn) {
		return nil, fmt.Errorf(
			"dataSource.auth obtains a short-lived credential, which is a bearer token, and this connection "+
				"string does not ask for transport encryption: %s",
			encryptionHint(d.Name))
	}

	provider, ok := LookupCredentialProvider(auth.Provider)
	if !ok {
		return nil, fmt.Errorf(
			"credential provider %q is not registered in this build; it knows %s",
			auth.Provider, joinOr(RegisteredCredentialProviders(), "no credential provider at all"))
	}

	creds, err := provider.Open(CredentialRequest{
		Driver:  d.Name,
		DSN:     dsn,
		Options: auth.Options,
	})
	if err != nil {
		return nil, fmt.Errorf("credential provider %q: %w", auth.Provider, err)
	}
	if creds == nil {
		return nil, fmt.Errorf("credential provider %q returned no credentials", auth.Provider)
	}

	return d.AuthConnector(dsn, creds)
}

// openConnection opens a *sql.DB outside the pool, the same way the pool itself
// was opened.
//
// One caller, the notification listener, which needs a session of its own
// because LISTEN occupies one. It exists so that the choice between a
// connection string and a connector is made in one place rather than twice: a
// listener opened from the DSN on a data source that mints its password would
// connect with no password at all.
func (p *Pool) openConnection(d Driver) (*sql.DB, error) {
	if p.connector != nil {
		return sql.OpenDB(p.connector), nil
	}
	return sql.Open(d.SQLDriver, p.dsn)
}

// driversWithAuthConnector lists the drivers a credential provider can be used
// with, so the error names them rather than sending somebody to the source.
func driversWithAuthConnector() []string {
	var names []string
	for _, name := range RegisteredDrivers() {
		if SupportsCredentialProviders(name) {
			names = append(names, name)
		}
	}
	return names
}

// joinOr renders a list for an error message, with something to say when it is
// empty. "this build knows " followed by nothing reads as a bug in the message
// rather than as an answer — and empty is the ordinary case for providers,
// since the binary this repository builds registers none.
func joinOr(names []string, empty string) string {
	if len(names) == 0 {
		return empty
	}
	return strings.Join(names, ", ")
}

// The connectors that let a driver take a password minted per connection.
//
// database/sql has exactly one seam for this, and it is not the connection
// string: sql.OpenDB takes a driver.Connector, and the pool calls its Connect
// whenever it decides it wants one more connection. Everything a short-lived
// credential needs follows from putting the minting there — the pool is opened
// once from a connection string that never contains a token, existing
// connections are untouched when the token changes, and only the connection
// being opened pays for a new one.
//
// A driver that has no such connector cannot be used with a credential
// provider, and says so by leaving Driver.AuthConnector nil. That is not a
// formality: the alternative is to write the token into the connection string,
// which either rebuilds the whole pool every fifteen minutes or leaves it
// authenticating with an expired token, and both of those look like the
// database having an outage rather than like a credential expiring.

// pgxAuthConnector builds a connector for the pgx driver — PostgreSQL and,
// through the same wire protocol, CockroachDB.
//
// pgx parses the connection string once, into a ConnConfig, and its stdlib
// connector hands each new connection a shallow copy of it through
// BeforeConnect. Setting Password there is the whole mechanism: the copy is per
// connection, so two connections opened an hour apart authenticate with two
// different tokens against one pool that was never rebuilt.
//
// Parsing here rather than at connect time is deliberate. A connection string
// pgx cannot read is a mistake in the Secret, and it should fail the projection
// when it is compiled, alongside every other thing that is wrong with it —
// rather than once per connection attempt, forever, from inside the pool.
func pgxAuthConnector(dsn string, creds Credentials) (driver.Connector, error) {
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing the connection string: %w", err)
	}

	return stdlib.GetConnector(*config, stdlib.OptionBeforeConnect(
		func(ctx context.Context, connConfig *pgx.ConnConfig) error {
			password, err := creds.Password(ctx)
			if err != nil {
				return fmt.Errorf("obtaining a database password: %w", err)
			}
			connConfig.Password = password
			return nil
		},
	)), nil
}

// mysqlAuthConnector builds a connector for the MySQL driver.
//
// The MySQL driver has no per-connection hook, so this is one of our own: the
// parsed configuration is kept, and each Connect copies it, sets the freshly
// minted password on the copy, and builds the driver's own connector from that.
// Building one per connection costs a struct and some validation, which is
// nothing next to the round trips of the handshake it precedes.
//
// AllowCleartextPasswords is forced on, and that deserves saying out loud. A
// minted password is not a password MySQL can challenge-response against — RDS
// IAM tokens are several hundred bytes of signed URL, and the server checks
// them with the mysql_clear_password plugin, which sends them as typed. The
// driver refuses that plugin unless asked, precisely because it puts a
// credential on the wire in the clear.
//
// What makes it defensible here is that Open refuses a credential provider on a
// connection the driver does not report as encrypted, so this code is only ever
// reached inside TLS. The alternative — leaving it to the connection string —
// is a projection that compiles, reports Ready, and then fails every connection
// with "this user requires mysql native password authentication", which names
// neither the setting nor the reason.
func mysqlAuthConnector(dsn string, creds Credentials) (driver.Connector, error) {
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing the connection string: %w", err)
	}
	config.AllowCleartextPasswords = true

	return &mysqlConnector{config: config, creds: creds}, nil
}

type mysqlConnector struct {
	config *mysql.Config
	creds  Credentials
}

// Connect mints a password and opens one connection with it.
func (c *mysqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	password, err := c.creds.Password(ctx)
	if err != nil {
		return nil, fmt.Errorf("obtaining a database password: %w", err)
	}

	config := c.config.Clone()
	config.Passwd = password

	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, err
	}
	return connector.Connect(ctx)
}

// Driver reports which driver these connections belong to. database/sql uses it
// for nothing but Conn.Driver, so it can be the plain driver.
func (c *mysqlConnector) Driver() driver.Driver { return mysql.MySQLDriver{} }
