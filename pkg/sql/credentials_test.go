package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// The fake driver these tests open pools against.
//
// It is the pattern staleplan_test.go established: a database/sql driver that
// implements just enough to be opened and queried, registered once, so that the
// pooling code can be exercised without a database. This one records what it
// was asked to authenticate with, which is the whole question here — a pool
// with a credential provider has to hand each new connection a password minted
// for it, and nothing else in the process can see whether it did.

// mintedPasswords is what each connection was opened with, in order.
var mintedPasswords struct {
	sync.Mutex
	seen []string
}

func recordMintedPassword(password string) {
	mintedPasswords.Lock()
	defer mintedPasswords.Unlock()
	mintedPasswords.seen = append(mintedPasswords.seen, password)
}

func mintedPasswordsSoFar() []string {
	mintedPasswords.Lock()
	defer mintedPasswords.Unlock()
	return append([]string(nil), mintedPasswords.seen...)
}

func resetMintedPasswords() {
	mintedPasswords.Lock()
	defer mintedPasswords.Unlock()
	mintedPasswords.seen = nil
}

type recordingDriver struct{}

// Open is the connection-string path, which a data source with no auth takes.
// The password is whatever the string carried, and the test asserts that this
// path is the one an ordinary projection still goes down.
func (recordingDriver) Open(dsn string) (driver.Conn, error) {
	recordMintedPassword(dsnParam(dsn, "password"))
	return &recordingConn{}, nil
}

// recordingConnector is the auth path: a password is minted per connection and
// recorded as the connection is opened, which is the shape every real
// AuthConnector has.
type recordingConnector struct{ creds Credentials }

func (c *recordingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	password, err := c.creds.Password(ctx)
	if err != nil {
		return nil, err
	}
	recordMintedPassword(password)
	return &recordingConn{}, nil
}

func (c *recordingConnector) Driver() driver.Driver { return recordingDriver{} }

func recordingAuthConnector(_ string, creds Credentials) (driver.Connector, error) {
	return &recordingConnector{creds: creds}, nil
}

type recordingConn struct{}

func (c *recordingConn) Prepare(string) (driver.Stmt, error) { return &recordingStmt{}, nil }
func (c *recordingConn) Close() error                        { return nil }
func (c *recordingConn) Begin() (driver.Tx, error)           { return nil, errors.New("no transactions") }

func (c *recordingConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &recordingRows{}, nil
}

type recordingStmt struct{}

func (s *recordingStmt) Close() error  { return nil }
func (s *recordingStmt) NumInput() int { return -1 }
func (s *recordingStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (s *recordingStmt) Query([]driver.Value) (driver.Rows, error) { return &recordingRows{}, nil }

type recordingRows struct{}

func (r *recordingRows) Columns() []string         { return []string{"id"} }
func (r *recordingRows) Close() error              { return nil }
func (r *recordingRows) Next([]driver.Value) error { return io.EOF }

// The registered names. Encrypted answers true unconditionally, since these
// tests are not about the transport, except where one of them says otherwise.
const (
	recordingDriverName          = "credentialsTest"
	recordingPlaintextDriverName = "credentialsTestPlaintext"
	// A driver with no AuthConnector, standing in for sqlite: it can be opened
	// from a connection string and nothing else.
	recordingNoAuthDriverName = "credentialsTestNoAuth"
)

func init() {
	sql.Register("credentials-test", recordingDriver{})

	for _, d := range []Driver{
		{
			Name:          recordingDriverName,
			SQLDriver:     "credentials-test",
			Placeholders:  PlaceholderQuestion,
			AuthConnector: recordingAuthConnector,
			Encrypted:     func(string) bool { return true },
			Verified:      func(string) bool { return true },
		},
		{
			Name:          recordingPlaintextDriverName,
			SQLDriver:     "credentials-test",
			Placeholders:  PlaceholderQuestion,
			AuthConnector: recordingAuthConnector,
			Encrypted:     func(string) bool { return false },
			Verified:      func(string) bool { return false },
		},
		{
			Name:         recordingNoAuthDriverName,
			SQLDriver:    "credentials-test",
			Placeholders: PlaceholderQuestion,
		},
	} {
		if err := Register(d); err != nil {
			panic(err)
		}
	}
}

// registerTestProvider registers a provider for the length of one test and
// returns the tokens it handed out, newest last.
func registerTestProvider(t *testing.T, name string, open func(CredentialRequest) (Credentials, error)) {
	t.Helper()

	if err := RegisterCredentialProvider(CredentialProvider{Name: name, Open: open}); err != nil {
		t.Fatalf("RegisterCredentialProvider() returned error: %v", err)
	}
	t.Cleanup(func() {
		credentialsMu.Lock()
		delete(credentials, name)
		credentialsMu.Unlock()
	})
}

// A token is a property of a connection, not of a pool, and this is the test
// that says so.
//
// Two connections are opened against one pool at two simulated times a token
// lifetime apart. Each has to authenticate with the token that was valid when
// it was opened — and the pool has to be the same pool throughout, with the
// connection it already had and the statement it had already prepared. Get
// either half wrong and the failure is the one this whole mechanism exists to
// avoid: a pool rebuilt every fifteen minutes, or a pool whose connections stop
// authenticating after the first quarter of an hour.
func TestEachConnectionAuthenticatesWithATokenMintedForItWhileThePoolIsUntouched(t *testing.T) {
	resetMintedPasswords()

	// A clock the test moves, so nothing here waits for real time. The token is
	// derived from it, exactly as a signed one is.
	var (
		clock  = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
		minted int
	)
	registerTestProvider(t, "fixedclock", func(req CredentialRequest) (Credentials, error) {
		if req.Driver != recordingDriverName {
			t.Errorf("the provider was asked to authenticate driver %q, want %q", req.Driver, recordingDriverName)
		}
		if req.DSN != "host=db user=orders" {
			t.Errorf("the provider was given DSN %q, want the one the pool was opened with", req.DSN)
		}
		if req.Options["region"] != "eu-central-1" {
			t.Errorf("the provider was given options %v, want the projection's", req.Options)
		}
		return CredentialsFunc(func(context.Context) (string, error) {
			minted++
			return fmt.Sprintf("token-at-%s", clock.Format(time.TimeOnly)), nil
		}), nil
	})

	pool, err := Open(PoolOptions{
		Driver:             recordingDriverName,
		DSN:                "host=db user=orders",
		PreparedStatements: true,
		Auth: &AuthOptions{
			Provider: "fixedclock",
			Options:  map[string]string{"region": "eu-central-1"},
		},
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	if pool.connector == nil {
		t.Fatal("a pool with auth was opened from its connection string, so no token can ever be minted")
	}

	// The first connection, and a statement prepared on it, so that there is
	// something to lose if the pool is rebuilt.
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	ctx := context.Background()
	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the first query returned error: %v", err)
	}
	if pool.PreparedCount() != 1 {
		t.Fatalf("the statement was not cached, so this test cannot tell whether the pool survives")
	}

	// Held, so that the pool has to open a second connection rather than
	// handing back the first. Reaching into pool.db is deliberate: the point is
	// two connections open at once, and no query this package exposes can be
	// made to overlap with another without a database that blocks.
	held, err := pool.db.Conn(ctx)
	if err != nil {
		t.Fatalf("taking a connection returned error: %v", err)
	}
	defer func() { _ = held.Close() }()

	// A token lifetime passes. Nothing about the data source changed.
	clock = clock.Add(20 * time.Minute)

	second, err := pool.db.Conn(ctx)
	if err != nil {
		t.Fatalf("taking a second connection returned error: %v", err)
	}
	defer func() { _ = second.Close() }()

	passwords := mintedPasswordsSoFar()
	if len(passwords) < 2 {
		t.Fatalf("the pool opened %d connections, want at least 2 so that two tokens can be compared", len(passwords))
	}
	first, last := passwords[0], passwords[len(passwords)-1]
	if first != "token-at-12:00:00" {
		t.Errorf("the first connection authenticated with %q, want the token valid when it was opened", first)
	}
	if last != "token-at-12:20:00" {
		t.Errorf("the connection opened twenty minutes later authenticated with %q, want a freshly minted token", last)
	}
	if minted < 2 {
		t.Errorf("the provider was asked for %d passwords; each new connection has to ask for its own", minted)
	}

	// And the pool itself is the pool it was: same *sql.DB, same statement
	// cache. A token that had gone into the connection string would have taken
	// both away.
	if pool.PreparedCount() != 1 {
		t.Errorf("the prepared statement cache holds %d entries, want the one it had before the token changed",
			pool.PreparedCount())
	}
}

// The ordinary case has to be exactly what it was: no connector, no provider
// consulted, and the connection string used as written.
func TestADataSourceWithoutAuthIsOpenedFromItsConnectionStringAsBefore(t *testing.T) {
	resetMintedPasswords()

	pool, err := Open(PoolOptions{Driver: recordingDriverName, DSN: "host=db user=orders password=static"})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	if pool.connector != nil {
		t.Fatal("a pool with no auth was built from a connector, which is not the path it used to take")
	}

	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() returned error: %v", err)
	}
	if got := mintedPasswordsSoFar(); len(got) == 0 || got[0] != "static" {
		t.Errorf("the connection authenticated with %v, want the password in the connection string", got)
	}
}

// Everything that can be wrong with an auth stanza is refused while the
// projection is being compiled — which is where Open is called from — rather
// than becoming a pool that reports Ready and then fails every request.
func TestAnAuthStanzaThatCannotWorkIsRefusedWhenThePoolIsOpened(t *testing.T) {
	registerTestProvider(t, "registered", func(CredentialRequest) (Credentials, error) {
		return CredentialsFunc(func(context.Context) (string, error) { return "token", nil }), nil
	})
	registerTestProvider(t, "refuses", func(CredentialRequest) (Credentials, error) {
		return nil, errors.New("no region and none to be discovered")
	})

	for _, tc := range []struct {
		name    string
		opts    PoolOptions
		wantAll []string
	}{
		{
			name: "a provider this build does not register",
			opts: PoolOptions{Driver: recordingDriverName, DSN: "x",
				Auth: &AuthOptions{Provider: "nonesuch"}},
			wantAll: []string{"nonesuch", "not registered", "refuses, registered"},
		},
		{
			name: "a driver that cannot be handed a password per connection",
			opts: PoolOptions{Driver: recordingNoAuthDriverName, DSN: "x",
				Auth: &AuthOptions{Provider: "registered"}},
			wantAll: []string{recordingNoAuthDriverName, "per connection", recordingDriverName},
		},
		{
			name: "a connection that is not encrypted",
			opts: PoolOptions{Driver: recordingPlaintextDriverName, DSN: "x",
				Auth: &AuthOptions{Provider: "registered"}},
			wantAll: []string{"bearer token", "transport encryption"},
		},
		{
			name: "a provider that refuses its own options",
			opts: PoolOptions{Driver: recordingDriverName, DSN: "x",
				Auth: &AuthOptions{Provider: "refuses"}},
			wantAll: []string{"refuses", "no region"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := Open(tc.opts)
			if err == nil {
				_ = pool.Close()
				t.Fatal("Open() accepted an auth stanza it cannot serve")
			}
			for _, want := range tc.wantAll {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// The registry is the point: a build links in the provider it needs and no
// other, exactly as it does for drivers.
func TestTheCredentialProviderRegistryOpensTheSet(t *testing.T) {
	const name = "testprovider"
	provider := CredentialProvider{
		Name: name,
		Open: func(CredentialRequest) (Credentials, error) {
			return CredentialsFunc(func(context.Context) (string, error) { return "", nil }), nil
		},
	}
	registerTestProvider(t, name, provider.Open)

	if _, ok := LookupCredentialProvider(name); !ok {
		t.Error("the registered provider was not found")
	}
	if !contains(RegisteredCredentialProviders(), name) {
		t.Errorf("RegisteredCredentialProviders() = %v, want it to list %s",
			RegisteredCredentialProviders(), name)
	}
	if err := RegisterCredentialProvider(provider); err == nil {
		t.Error("registering the same name twice was accepted")
	}
	if _, ok := LookupCredentialProvider("nonesuch"); ok {
		t.Error("an unregistered provider was found")
	}

	if err := RegisterCredentialProvider(CredentialProvider{Open: provider.Open}); err == nil {
		t.Error("a provider with no name was accepted")
	}
	if err := RegisterCredentialProvider(CredentialProvider{Name: "nameless"}); err == nil {
		t.Error("a provider with no way to obtain a password was accepted")
	}
}

// The stock build registers no provider at all, and the message somebody gets
// for asking one of it has to say so rather than trailing off.
func TestThisBuildRegistersNoCredentialProvider(t *testing.T) {
	if got := RegisteredCredentialProviders(); len(got) != 0 {
		t.Errorf("RegisteredCredentialProviders() = %v; the published build links no cloud SDK, so a "+
			"provider here is one that has to be justified and documented", got)
	}

	_, err := Open(PoolOptions{Driver: recordingDriverName, DSN: "x", Auth: &AuthOptions{Provider: "aws-rds-iam"}})
	if err == nil {
		t.Fatal("Open() accepted a provider nothing registered")
	}
	if !strings.Contains(err.Error(), "no credential provider at all") {
		t.Errorf("error %q does not say that this build has none", err)
	}
}

// A driver that mints passwords has to be able to say whether the connection
// carrying them is encrypted, because that is what the refusal above is decided
// on. One that cannot is refused at registration, where whoever is assembling
// the build can still fix it.
func TestRegisterRefusesADriverThatMintsPasswordsWithoutReportingEncryption(t *testing.T) {
	err := Register(Driver{
		Name:          "mintsWithoutTLSOpinion",
		SQLDriver:     "credentials-test",
		AuthConnector: recordingAuthConnector,
	})
	if err == nil {
		driversMu.Lock()
		delete(drivers, "mintsWithoutTLSOpinion")
		driversMu.Unlock()
		t.Fatal("a driver offering per-connection credentials and no opinion on encryption was accepted")
	}
	if !strings.Contains(err.Error(), "transport encryption") {
		t.Errorf("error %q does not say what is missing", err)
	}
}

// And whether the connection established which server it reached, which is the
// question a bearer token actually turns on. Encryption to an impersonator
// hands the credential over as surely as no encryption at all, and more
// quietly.
//
// Asked at registration rather than left to fail every auth attempt at runtime,
// so a driver added outside this repository says so where it is added instead
// of refusing every projection that configures dataSource.auth with a message
// about its connection string.
func TestRegisterRefusesADriverThatMintsPasswordsWithoutReportingVerification(t *testing.T) {
	err := Register(Driver{
		Name:          "mintsWithoutVerificationOpinion",
		SQLDriver:     "credentials-test",
		AuthConnector: recordingAuthConnector,
		Encrypted:     func(string) bool { return true },
	})
	if err == nil {
		driversMu.Lock()
		delete(drivers, "mintsWithoutVerificationOpinion")
		driversMu.Unlock()
		t.Fatal("a driver offering per-connection credentials and no opinion on verification was accepted")
	}
	if !strings.Contains(err.Error(), "server verification") {
		t.Errorf("error %q does not say what is missing", err)
	}
}

// A driver registered outside this package answers for itself, because only it
// knows what its own connection string can say. Before this, verification was a
// switch over the names this package ships, so such a driver could never carry
// a minted credential however its connection string was written -- while the
// documented way to add one is exactly this call.
func TestADriverAddedOutsideThisPackageCanAnswerForItself(t *testing.T) {
	const name = "thirdPartyVerifying"
	if err := Register(Driver{
		Name:          name,
		SQLDriver:     "credentials-test",
		Placeholders:  PlaceholderQuestion,
		AuthConnector: recordingAuthConnector,
		Encrypted:     func(dsn string) bool { return strings.Contains(dsn, "secure") },
		Verified:      func(dsn string) bool { return strings.Contains(dsn, "verified") },
	}); err != nil {
		t.Fatalf("Register() returned error: %v", err)
	}
	t.Cleanup(func() {
		driversMu.Lock()
		delete(drivers, name)
		driversMu.Unlock()
	})

	if !serverVerified(name, "host=db secure verified") {
		t.Error("a driver reporting a verified connection string was not believed")
	}
	if serverVerified(name, "host=db secure") {
		t.Error("a driver reporting an unverified connection string was treated as verified")
	}
}

// SupportsCredentialProviders is what the CRD's CEL rule is compared against,
// so it has to agree with what the built-in drivers actually offer.
func TestTheBuiltInDriversAgreeOnWhoCanTakeAMintedPassword(t *testing.T) {
	for driver, want := range map[string]bool{
		"postgres":  true,
		"cockroach": true,
		"mysql":     true,
		// A local file. There is no connection and no password.
		"sqlite": false,
	} {
		if got := SupportsCredentialProviders(driver); got != want {
			t.Errorf("SupportsCredentialProviders(%q) = %v, want %v", driver, got, want)
		}
	}
	if SupportsCredentialProviders("nonesuch") {
		t.Error("an unregistered driver reports that it can take a minted password")
	}
}
