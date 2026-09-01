package tokencommand_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrueg/kube-crisp/pkg/credentials/tokencommand"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// The fake driver these tests open pools against, as staleplan_test.go
// established the pattern in pkg/sql: a database/sql driver that implements
// just enough to be opened and queried, recording the password each connection
// was opened with. That recording is the only way to see what a provider
// actually handed to a connection.

var minted struct {
	sync.Mutex
	seen []string
}

func recordMinted(password string) {
	minted.Lock()
	defer minted.Unlock()
	minted.seen = append(minted.seen, password)
}

func mintedSoFar() []string {
	minted.Lock()
	defer minted.Unlock()
	return append([]string(nil), minted.seen...)
}

func resetMinted() {
	minted.Lock()
	defer minted.Unlock()
	minted.seen = nil
}

type recordingDriver struct{}

func (recordingDriver) Open(string) (driver.Conn, error) { return &recordingConn{}, nil }

type recordingConnector struct{ creds crispsql.Credentials }

func (c *recordingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	password, err := c.creds.Password(ctx)
	if err != nil {
		return nil, err
	}
	recordMinted(password)
	return &recordingConn{}, nil
}

func (c *recordingConnector) Driver() driver.Driver { return recordingDriver{} }

type recordingConn struct{}

func (c *recordingConn) Prepare(string) (driver.Stmt, error) { return &recordingStmt{}, nil }
func (c *recordingConn) Close() error                        { return nil }
func (c *recordingConn) Begin() (driver.Tx, error)           { return nil, errors.New("no transactions") }

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

const (
	encryptedDriver = "tokenCommandTest"
	plaintextDriver = "tokenCommandTestPlaintext"
)

func TestMain(m *testing.M) {
	sql.Register("token-command-test", recordingDriver{})

	authConnector := func(_ string, creds crispsql.Credentials) (driver.Connector, error) {
		return &recordingConnector{creds: creds}, nil
	}
	for _, d := range []crispsql.Driver{
		{
			Name:          encryptedDriver,
			SQLDriver:     "token-command-test",
			Placeholders:  crispsql.PlaceholderQuestion,
			AuthConnector: authConnector,
			Encrypted:     func(string) bool { return true },
		},
		{
			Name:          plaintextDriver,
			SQLDriver:     "token-command-test",
			Placeholders:  crispsql.PlaceholderQuestion,
			AuthConnector: authConnector,
			Encrypted:     func(string) bool { return false },
		},
	} {
		if err := crispsql.Register(d); err != nil {
			panic(err)
		}
	}
	if err := tokencommand.Register(); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

// enable installs a directory of commands for one test and puts back what was
// there. Every test that runs a command needs a shell, so this is also where
// the ones that cannot run are skipped.
func enable(t *testing.T, dir string) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("the commands these tests install are shell scripts")
	}
	before := tokencommand.Directory()
	t.Cleanup(func() {
		if err := tokencommand.Enable(before); err != nil {
			t.Fatalf("restoring the command directory returned error: %v", err)
		}
	})
	if err := tokencommand.Enable(dir); err != nil {
		t.Fatalf("Enable(%q) returned error: %v", dir, err)
	}
}

// script installs an executable in the command directory.
func script(t *testing.T, dir, name, body string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil { //nolint:gosec // G306: a command has to be executable to be run
		t.Fatalf("installing the command returned error: %v", err)
	}
	return path
}

// openPool opens a pool that takes a new connection for every query, so each
// one has to obtain a password of its own. A connection lifetime of one
// nanosecond is how that is arranged without a database that can be made to
// block: database/sql discards a connection it finds expired when it is handed
// back, while the pool above it is untouched.
func openPool(t *testing.T, driverName string, options map[string]string) *crispsql.Pool {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             driverName,
		DSN:                "host=db user=orders",
		PreparedStatements: true,
		ConnMaxLifetime:    time.Nanosecond,
		Auth: &crispsql.AuthOptions{
			Provider: tokencommand.ProviderName,
			Options:  options,
		},
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// The refusal this provider is built around: without --credential-command-dir
// it runs nothing, and says so while the projection is being compiled.
//
// The message matters as much as the refusal. "This build has never heard of
// that provider" would send somebody to rebuild a binary that is already the
// right one, so the provider is registered in every build and refuses by name,
// naming the flag an operator would have to set.
func TestNothingRunsUntilAnOperatorNamesADirectoryOfCommands(t *testing.T) {
	before := tokencommand.Directory()
	t.Cleanup(func() { _ = tokencommand.Enable(before) })
	if err := tokencommand.Enable(""); err != nil {
		t.Fatalf("Enable(\"\") returned error: %v", err)
	}

	if _, registered := crispsql.LookupCredentialProvider(tokencommand.ProviderName); !registered {
		t.Fatal("the provider is not registered while disabled, so a projection naming it would be told " +
			"this build has never heard of it")
	}

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver: encryptedDriver,
		DSN:    "host=db user=orders",
		Auth: &crispsql.AuthOptions{
			Provider: tokencommand.ProviderName,
			Options:  map[string]string{"command": "anything"},
		},
	})
	if err == nil {
		_ = pool.Close()
		t.Fatal("a projection ran a command on a server where no directory of commands was named")
	}
	for _, want := range []string{tokencommand.ProviderName, "not enabled", "--credential-command-dir"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// The command runs per connection, so a credential that changed is picked up
// without the pool being rebuilt — the same property token-file has and the
// reason both of them are providers rather than something that rewrites a
// connection string.
func TestTheCommandRunsAgainForEveryConnectionWhileThePoolStandsStill(t *testing.T) {
	resetMinted()

	dir := t.TempDir()
	enable(t, dir)
	state := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(state, []byte("first-token\n"), 0o600); err != nil {
		t.Fatalf("writing the token returned error: %v", err)
	}
	script(t, dir, "orders-db-token", "cat "+state+"\n")

	pool := openPool(t, encryptedDriver, map[string]string{"command": "orders-db-token"})
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

	// Whatever the command talks to hands out a different token now.
	if err := os.WriteFile(state, []byte("second-token\n"), 0o600); err != nil {
		t.Fatalf("rewriting the token returned error: %v", err)
	}

	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the second query returned error: %v", err)
	}

	passwords := mintedSoFar()
	if len(passwords) < 2 {
		t.Fatalf("the pool opened %d connections, want at least 2 so that two runs can be compared", len(passwords))
	}
	if first := passwords[0]; first != "first-token" {
		t.Errorf("the first connection authenticated with %q, want what the command printed then", first)
	}
	if last := passwords[len(passwords)-1]; last != "second-token" {
		t.Errorf("the last connection authenticated with %q, want what the command prints now", last)
	}
	if pool.PreparedCount() != 1 {
		t.Errorf("the prepared statement cache holds %d entries, want the one it had before the credential changed",
			pool.PreparedCount())
	}
}

// A command that hangs must not hang the pool. The context database/sql opens a
// connection with carries no deadline of its own — it lives as long as the pool
// — so without a timeout of this provider's own, one wedged process would hold
// a connection slot until the server restarted, and a pool whose connections
// are all waiting on one is indistinguishable from a database that stopped
// answering.
func TestACommandThatHangsFailsTheConnectionRatherThanHoldingIt(t *testing.T) {
	dir := t.TempDir()
	enable(t, dir)
	script(t, dir, "hangs", "sleep 60\n")

	pool := openPool(t, encryptedDriver, map[string]string{"command": "hangs", "timeout": "200ms"})

	// A query deadline far longer than the command's, so that what fails here
	// is the command running out of time rather than the request giving up on
	// a connection that never arrived. Both happen in production; only the
	// first one says anything about the cause.
	stmt, err := pool.Prepare("SELECT id FROM orders", 30*time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	started := time.Now()
	_, err = pool.Query(context.Background(), stmt, nil)
	if err == nil {
		t.Fatal("a connection was opened by a command that never finished")
	}
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Errorf("the query waited %s, so the timeout did not bound the command", elapsed)
	}
	if !strings.Contains(err.Error(), "did not finish within") {
		t.Errorf("error %q does not say that the command ran out of time", err)
	}
	if !strings.Contains(err.Error(), "hangs") {
		t.Errorf("error %q does not name the command", err)
	}
}

// A command that fails says why, in the projection's own words, because this
// message is what reaches the Ready condition of a projection that will not
// connect. Its stderr is quoted for that reason and only then: a command that
// succeeded has already said what it had to on stdout, and repeating its
// chatter once per connection would put the stream sitting beside a credential
// into the log at the rate connections are opened.
func TestACommandThatFailsSaysSoWithWhatItPrintedOnStderr(t *testing.T) {
	dir := t.TempDir()
	enable(t, dir)
	script(t, dir, "refuses", "echo 'no permission to mint a token' >&2\nexit 3\n")

	pool := openPool(t, encryptedDriver, map[string]string{"command": "refuses"})
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	_, err = pool.Query(context.Background(), stmt, nil)
	if err == nil {
		t.Fatal("a connection was opened by a command that exited non-zero")
	}
	for _, want := range []string{"refuses", "no permission to mint a token"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

// A command that prints nothing is refused rather than treated as an empty
// password, which would authenticate as whoever the database lets in without
// one.
func TestACommandThatPrintsNothingIsRefused(t *testing.T) {
	dir := t.TempDir()
	enable(t, dir)
	script(t, dir, "silent", "exit 0\n")

	pool := openPool(t, encryptedDriver, map[string]string{"command": "silent"})
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	_, err = pool.Query(context.Background(), stmt, nil)
	if err == nil {
		t.Fatal("a command that printed nothing was used as a password")
	}
	if !strings.Contains(err.Error(), "printed nothing") {
		t.Errorf("error %q does not say that the command printed nothing", err)
	}
}

// What the command prints is the credential, minus the newline every program
// that prints a line leaves behind.
func TestTheTrailingNewlineIsNotPartOfTheCredential(t *testing.T) {
	dir := t.TempDir()
	enable(t, dir)
	script(t, dir, "prints", "printf 'a token with spaces \\n'\n")

	pool := openPool(t, encryptedDriver, map[string]string{"command": "prints"})
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	resetMinted()
	if _, err := pool.Query(context.Background(), stmt, nil); err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	got := mintedSoFar()
	if len(got) == 0 {
		t.Fatal("no connection was opened")
	}
	// The newline goes; the trailing space does not, because unlike a line
	// ending it can be part of a credential somebody chose.
	if want := "a token with spaces "; got[0] != want {
		t.Errorf("the connection authenticated with %q, want %q", got[0], want)
	}
}

// Everything that can be decided about the options is decided while the
// projection is being compiled, which is where Open is called from and where
// somebody is looking — rather than becoming a pool that reports Ready and
// fails every request afterwards.
func TestAnOptionThatCannotWorkIsRefusedWhenTheProjectionIsCompiled(t *testing.T) {
	dir := t.TempDir()
	enable(t, dir)
	script(t, dir, "prints", "echo token\n")

	// Installed without an executable bit, which is what a ConfigMap mounted
	// with the default mode looks like.
	unreadable := filepath.Join(dir, "not-executable")
	if err := os.WriteFile(unreadable, []byte("#!/bin/sh\necho token\n"), 0o600); err != nil {
		t.Fatalf("installing the command returned error: %v", err)
	}

	// And a link out of the directory, which is executed rather than merely
	// read, so it is checked after resolution just as a credential file is.
	outside := script(t, t.TempDir(), "elsewhere", "echo token\n")
	if err := os.Symlink(outside, filepath.Join(dir, "linked-out")); err != nil {
		t.Fatalf("creating the link returned error: %v", err)
	}

	for _, tc := range []struct {
		name    string
		driver  string
		options map[string]string
		wantAll []string
	}{
		{
			name:    "no command at all",
			options: map[string]string{},
			wantAll: []string{"command"},
		},
		{
			name:    "a misspelt option",
			options: map[string]string{"command": "prints", "timout": "5s"},
			wantAll: []string{"timout", "unknown", "timeout"},
		},
		{
			// The escalation this provider is gated for: a path, rather than a
			// name, would let a projection run anything the server can.
			name:    "a path instead of a name",
			options: map[string]string{"command": "../../bin/sh"},
			wantAll: []string{"bare command name", dir},
		},
		{
			name:    "an absolute path",
			options: map[string]string{"command": "/bin/sh"},
			wantAll: []string{"bare command name"},
		},
		{
			name:    "a link out of the directory",
			options: map[string]string{"command": "linked-out"},
			wantAll: []string{"outside", dir},
		},
		{
			name:    "a command that is not there",
			options: map[string]string{"command": "never-installed"},
			wantAll: []string{"never-installed", dir},
		},
		{
			name:    "a command nothing may execute",
			options: map[string]string{"command": "not-executable"},
			wantAll: []string{"not executable", "defaultMode"},
		},
		{
			name:    "a timeout that is not a duration",
			options: map[string]string{"command": "prints", "timeout": "soon"},
			wantAll: []string{"soon", "duration"},
		},
		{
			name:    "a timeout longer than a connection attempt is worth",
			options: map[string]string{"command": "prints", "timeout": "10m"},
			wantAll: []string{"10m", "1m0s"},
		},
		{
			// The seam's refusal, restated for this provider: what the command
			// prints is a bearer credential sent as typed.
			name:    "a connection that is not encrypted",
			driver:  plaintextDriver,
			options: map[string]string{"command": "prints"},
			wantAll: []string{"bearer token", "transport encryption"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driverName := tc.driver
			if driverName == "" {
				driverName = encryptedDriver
			}
			pool, err := crispsql.Open(crispsql.PoolOptions{
				Driver: driverName,
				DSN:    "host=db user=orders",
				Auth: &crispsql.AuthOptions{
					Provider: tokencommand.ProviderName,
					Options:  tc.options,
				},
			})
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

// A directory that is not absolute is refused where the operator set it, rather
// than becoming a path resolved against a working directory nobody chose.
func TestTheCommandDirectoryHasToBeAbsolute(t *testing.T) {
	before := tokencommand.Directory()
	t.Cleanup(func() { _ = tokencommand.Enable(before) })

	if err := tokencommand.Enable("commands"); err == nil {
		t.Fatal("Enable() accepted a relative directory")
	}
	if got := tokencommand.Directory(); got != before {
		t.Errorf("a refused Enable() changed the command directory to %q", got)
	}
}
