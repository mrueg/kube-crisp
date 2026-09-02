package tokenfile_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrueg/kube-crisp/pkg/credentials/tokenfile"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// The fake driver these tests open pools against.
//
// It is the pattern staleplan_test.go established in pkg/sql: a database/sql
// driver that implements just enough to be opened and queried, so that the
// pooling code runs without a database. This one records the password each
// connection was opened with, which is the whole question here — a provider
// that reads a file per connection has to actually hand a different password to
// a connection opened after the file changed, and nothing else in the process
// can see whether it did.

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
	encryptedDriver = "tokenFileTest"
	plaintextDriver = "tokenFileTestPlaintext"
)

func TestMain(m *testing.M) {
	sql.Register("token-file-test", recordingDriver{})

	authConnector := func(_ string, creds crispsql.Credentials) (driver.Connector, error) {
		return &recordingConnector{creds: creds}, nil
	}
	for _, d := range []crispsql.Driver{
		{
			Name:          encryptedDriver,
			SQLDriver:     "token-file-test",
			Placeholders:  crispsql.PlaceholderQuestion,
			AuthConnector: authConnector,
			Encrypted:     func(string) bool { return true },
			Verified:      func(string) bool { return true },
		},
		{
			Name:          plaintextDriver,
			SQLDriver:     "token-file-test",
			Placeholders:  crispsql.PlaceholderQuestion,
			AuthConnector: authConnector,
			Encrypted:     func(string) bool { return false },
			Verified:      func(string) bool { return false },
		},
	} {
		if err := crispsql.Register(d); err != nil {
			panic(err)
		}
	}
	if err := tokenfile.Register(); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

// permit installs the directories for one test and puts back what was there.
func permit(t *testing.T, dirs ...string) {
	t.Helper()

	before := tokenfile.PermittedDirectories()
	t.Cleanup(func() {
		if err := tokenfile.Permit(before); err != nil {
			t.Fatalf("restoring the permitted directories returned error: %v", err)
		}
	})
	if err := tokenfile.Permit(dirs); err != nil {
		t.Fatalf("Permit(%v) returned error: %v", dirs, err)
	}
}

// write puts contents in a file the way a refresher does: into a new file that
// is then renamed over the old one, so a reader never sees a half-written
// credential.
func write(t *testing.T, path, contents string) {
	t.Helper()

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(contents), 0o600); err != nil {
		t.Fatalf("writing the credential file returned error: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("renaming the credential file returned error: %v", err)
	}
}

// openPool opens a pool that takes a new connection for every query, so that
// each one has to obtain a password of its own.
//
// A connection lifetime of one nanosecond is how that is arranged without a
// database that can be made to block: database/sql discards a connection it
// finds expired when it is handed back, and opens another when the next query
// asks for one. Everything above the connection — the *sql.DB, the pool, the
// prepared statement cache — is untouched by that, which is exactly the state
// this provider has to work in.
func openPool(t *testing.T, driverName, path string) *crispsql.Pool {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             driverName,
		DSN:                "host=db user=orders",
		PreparedStatements: true,
		ConnMaxLifetime:    time.Nanosecond,
		Auth: &crispsql.AuthOptions{
			Provider: tokenfile.ProviderName,
			Options:  map[string]string{"path": path},
		},
	})
	if err != nil {
		t.Fatalf("Open() returned error: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// The test this provider exists for: a file rewritten between two connections
// is read again by the second one, and the pool that opened both is the pool it
// was.
//
// Get the first half wrong and the provider is a slower way of putting a
// password in the connection string — the server keeps authenticating with a
// credential that expired, and every connection opened after that fails in a
// way that reads as the database going down. Get the second half wrong and the
// pool is rebuilt whenever the credential turns over, which is the cost the
// whole per-connection seam was built to avoid: live connections dropped,
// prepared statements thrown away, and the database asked to authenticate
// everything again.
func TestARewrittenFileIsReadAgainByTheNextConnectionWhileThePoolStandsStill(t *testing.T) {
	resetMinted()

	dir := t.TempDir()
	path := filepath.Join(dir, "orders-db")
	permit(t, dir)
	write(t, path, "first-token")

	pool := openPool(t, encryptedDriver, path)

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

	// The refresher writes the next credential. Nothing tells kube-crisp.
	write(t, path, "second-token")

	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the query after the file changed returned error: %v", err)
	}

	passwords := mintedSoFar()
	if len(passwords) < 2 {
		t.Fatalf("the pool opened %d connections, want at least 2 so that two reads can be compared", len(passwords))
	}
	if first := passwords[0]; first != "first-token" {
		t.Errorf("the first connection authenticated with %q, want what the file held when it was opened", first)
	}
	if last := passwords[len(passwords)-1]; last != "second-token" {
		t.Errorf("the connection opened after the file changed authenticated with %q, want the file's new contents", last)
	}
	if pool.PreparedCount() != 1 {
		t.Errorf("the prepared statement cache holds %d entries, want the one it had before the credential changed",
			pool.PreparedCount())
	}
}

// Kubernetes rotates a projected ServiceAccount token by writing a new
// timestamped directory and swapping a symlink, so the path in the projection
// never names the file that is actually read. Resolving it once and holding on
// to the result would pin the pool to a directory the kubelet has since
// deleted, and every connection after the first rotation would fail with a file
// that no longer exists.
func TestASymlinkSwappedUnderneathIsFollowedToTheNewFile(t *testing.T) {
	resetMinted()

	dir := t.TempDir()
	permit(t, dir)

	// The shape the kubelet writes: a data directory per version, a ..data link
	// pointing at the current one, and the visible name pointing through it.
	first := filepath.Join(dir, "..2026_01")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatalf("creating the versioned directory returned error: %v", err)
	}
	write(t, filepath.Join(first, "token"), "token-in-the-first-directory")
	if err := os.Symlink(first, filepath.Join(dir, "..data")); err != nil {
		t.Fatalf("creating the ..data link returned error: %v", err)
	}
	path := filepath.Join(dir, "token")
	if err := os.Symlink(filepath.Join(dir, "..data", "token"), path); err != nil {
		t.Fatalf("creating the token link returned error: %v", err)
	}

	pool := openPool(t, encryptedDriver, path)
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	ctx := context.Background()
	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the first query returned error: %v", err)
	}

	// The rotation: a new directory, the link swapped atomically over the old
	// one, the old directory removed.
	second := filepath.Join(dir, "..2026_02")
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatalf("creating the second versioned directory returned error: %v", err)
	}
	write(t, filepath.Join(second, "token"), "token-in-the-second-directory")
	link := filepath.Join(dir, "..data.tmp")
	if err := os.Symlink(second, link); err != nil {
		t.Fatalf("creating the replacement link returned error: %v", err)
	}
	if err := os.Rename(link, filepath.Join(dir, "..data")); err != nil {
		t.Fatalf("swapping the ..data link returned error: %v", err)
	}
	if err := os.RemoveAll(first); err != nil {
		t.Fatalf("removing the old versioned directory returned error: %v", err)
	}

	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the query after the rotation returned error: %v", err)
	}

	passwords := mintedSoFar()
	if len(passwords) < 2 {
		t.Fatalf("the pool opened %d connections, want at least 2", len(passwords))
	}
	if first := passwords[0]; first != "token-in-the-first-directory" {
		t.Errorf("the first connection authenticated with %q", first)
	}
	if last := passwords[len(passwords)-1]; last != "token-in-the-second-directory" {
		t.Errorf("the connection opened after the rotation authenticated with %q, want the rotated token", last)
	}
}

// Everything that can be decided about the options is decided while the
// projection is being compiled, which is where Open is called from and where
// somebody is looking. A projection that got any of this wrong must not compile
// clean, report Ready, and then fail every request.
func TestAnOptionThatCannotWorkIsRefusedWhenTheProjectionIsCompiled(t *testing.T) {
	dir := t.TempDir()
	permit(t, dir)
	path := filepath.Join(dir, "orders-db")
	write(t, path, "token")

	for _, tc := range []struct {
		name    string
		driver  string
		options map[string]string
		wantAll []string
	}{
		{
			name:    "no path at all",
			options: map[string]string{},
			wantAll: []string{"path"},
		},
		{
			// The failure this refusal exists for: a key nobody reads is a
			// default quietly taken, and the authentication error it produces
			// hours later says nothing about the typo.
			name:    "a misspelt option",
			options: map[string]string{"path": path, "pathh": "/elsewhere"},
			wantAll: []string{"pathh", "unknown", "path"},
		},
		{
			name:    "a relative path",
			options: map[string]string{"path": "token"},
			wantAll: []string{"absolute"},
		},
		{
			// The escalation: a projection is a cluster object, so an
			// unconstrained path would let whoever writes one read the
			// server's own identity and send it to a database as a password.
			name:    "a path outside every permitted directory",
			options: map[string]string{"path": "/var/run/secrets/kubernetes.io/serviceaccount/token"},
			wantAll: []string{"/var/run/secrets/kubernetes.io/serviceaccount/token", dir},
		},
		{
			// And the same by way of a traversal, which is the same path
			// written differently.
			name:    "a path that climbs out of a permitted directory",
			options: map[string]string{"path": filepath.Join(dir, "..", "..", "etc", "shadow")},
			wantAll: []string{dir},
		},
		{
			// The transport refusal belongs to the seam rather than to this
			// provider, but it has to apply here too: whatever is in the file
			// is a bearer credential, sent as typed.
			name:    "a connection that is not encrypted",
			driver:  plaintextDriver,
			options: map[string]string{"path": path},
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
					Provider: tokenfile.ProviderName,
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

// A file that is missing when the projection compiles is not an error, because
// the refresher writing it and this server start in no particular order and a
// projection failed for that race would stay failed. A file that is missing
// when a connection is opened is, and it has to name the path — the alternative
// is an empty password reaching the database, which authenticates as whoever
// the database lets in without one.
func TestWhatHappensWhenThereIsNoCredentialToRead(t *testing.T) {
	dir := t.TempDir()
	permit(t, dir)
	path := filepath.Join(dir, "not-written-yet")

	pool := openPool(t, encryptedDriver, path)
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	ctx := context.Background()

	_, err = pool.Query(ctx, stmt, nil)
	if err == nil {
		t.Fatal("a connection was opened while the credential file did not exist")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not name the file that is missing", err)
	}

	// It appears, and nothing had to be restarted for it to be used.
	write(t, path, "written-at-last")
	if _, err := pool.Query(ctx, stmt, nil); err != nil {
		t.Fatalf("the query after the file appeared returned error: %v", err)
	}

	// And an empty file is refused rather than sent as an empty password.
	write(t, path, "")
	_, err = pool.Query(ctx, stmt, nil)
	if err == nil {
		t.Fatal("an empty credential file was used as a password")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error %q does not say that the file is empty", err)
	}
}

// What the file holds is the credential, minus the line ending every tool that
// writes one leaves behind. A database refusing a token because of a byte that
// does not print is the least diagnosable failure in this path.
func TestTheTrailingNewlineIsNotPartOfTheCredential(t *testing.T) {
	dir := t.TempDir()
	permit(t, dir)
	path := filepath.Join(dir, "orders-db")
	pool := openPool(t, encryptedDriver, path)
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	for _, tc := range []struct{ contents, want string }{
		{"plain-token", "plain-token"},
		{"token-from-echo\n", "token-from-echo"},
		{"token-with-crlf\r\n", "token-with-crlf"},
		{"token-from-a-heredoc\n\n", "token-from-a-heredoc"},
		// Not trimmed: unlike a line ending, a space can be part of a
		// credential somebody chose, and silently removing it would be a
		// password this server changed.
		{"token with spaces \n", "token with spaces "},
	} {
		resetMinted()
		write(t, path, tc.contents)
		if _, err := pool.Query(context.Background(), stmt, nil); err != nil {
			t.Fatalf("querying with %q in the file returned error: %v", tc.contents, err)
		}
		got := mintedSoFar()
		if len(got) == 0 {
			t.Fatalf("no connection was opened for %q", tc.contents)
		}
		if last := got[len(got)-1]; last != tc.want {
			t.Errorf("the file holding %q authenticated with %q, want %q", tc.contents, last, tc.want)
		}
	}
}

// A path that turns out to name something enormous — a log, a database file, a
// core dump — is a mistake, and the ceiling is what keeps it from being an
// out-of-memory instead of a message. Nothing is truncated: a credential cut in
// half would be refused by the database with nothing pointing at the cause.
func TestAFileTooLargeToBeACredentialIsRefusedRatherThanTruncated(t *testing.T) {
	dir := t.TempDir()
	permit(t, dir)
	path := filepath.Join(dir, "not-a-credential")
	write(t, path, strings.Repeat("x", (64<<10)+1))

	pool := openPool(t, encryptedDriver, path)
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	_, err = pool.Query(context.Background(), stmt, nil)
	if err == nil {
		t.Fatal("a file too large to be a credential was read as one")
	}
	if !strings.Contains(err.Error(), "larger than") {
		t.Errorf("error %q does not say why the file was refused", err)
	}
}

// A directory permitted for credentials is not a promise about what is inside
// it: whoever writes the file could write a link instead. The check against the
// permitted directories is therefore made on the resolved path, every time,
// rather than once on the path as the projection wrote it.
func TestALinkOutOfAPermittedDirectoryIsNotFollowed(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "serviceaccount-token")
	write(t, outside, "the-servers-own-identity")
	permit(t, dir)

	path := filepath.Join(dir, "orders-db")
	if err := os.Symlink(outside, path); err != nil {
		t.Fatalf("creating the link returned error: %v", err)
	}

	pool := openPool(t, encryptedDriver, path)
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	resetMinted()
	_, err = pool.Query(context.Background(), stmt, nil)
	if err == nil {
		t.Fatal("a link out of a permitted directory was followed")
	}
	if !strings.Contains(err.Error(), "outside") {
		t.Errorf("error %q does not say that the link leaves the permitted directories", err)
	}
	for _, password := range mintedSoFar() {
		if password == "the-servers-own-identity" {
			t.Fatal("the contents of a file outside every permitted directory were sent as a password")
		}
	}
}

// Something other than a regular file is refused rather than read. A FIFO would
// block the connection opener until somebody wrote to it, and a character
// device would answer for as long as it was asked to; both look like the
// database being slow rather than like a path being wrong.
func TestSomethingThatIsNotARegularFileIsRefused(t *testing.T) {
	dir := t.TempDir()
	permit(t, dir)
	path := filepath.Join(dir, "a-directory")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("creating the directory returned error: %v", err)
	}

	pool := openPool(t, encryptedDriver, path)
	stmt, err := pool.Prepare("SELECT id FROM orders", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	_, err = pool.Query(context.Background(), stmt, nil)
	if err == nil {
		t.Fatal("a directory was read as a credential")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Errorf("error %q does not say what is wrong with the path", err)
	}
}

// Permitting nothing leaves the provider registered and refusing, which is a
// message somebody can act on. Reporting it as an unknown provider instead
// would send them to rebuild a binary that already has it.
func TestPermittingNoDirectoryRefusesByName(t *testing.T) {
	permit(t)

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver: encryptedDriver,
		DSN:    "host=db user=orders",
		Auth: &crispsql.AuthOptions{
			Provider: tokenfile.ProviderName,
			Options:  map[string]string{"path": "/var/run/kube-crisp/credentials/orders-db"},
		},
	})
	if err == nil {
		_ = pool.Close()
		t.Fatal("Open() accepted a projection while no directory was permitted")
	}
	if !strings.Contains(err.Error(), "--credential-token-file-dirs") {
		t.Errorf("error %q does not name the flag that permits a directory", err)
	}
}

// A directory that is not absolute is refused where the operator set it, rather
// than becoming a path resolved against a working directory nobody chose.
func TestAPermittedDirectoryHasToBeAbsolute(t *testing.T) {
	before := tokenfile.PermittedDirectories()
	t.Cleanup(func() { _ = tokenfile.Permit(before) })

	if err := tokenfile.Permit([]string{"credentials"}); err == nil {
		t.Fatal("Permit() accepted a relative directory")
	}
	if got := tokenfile.PermittedDirectories(); len(got) != len(before) {
		t.Errorf("a refused Permit() changed the permitted directories to %v", got)
	}
}

// The default is a directory that exists for this and nothing else, so that the
// answer to "which files may a projection name" is "the ones somebody mounted
// here on purpose" out of the box rather than after a flag.
func TestTheDefaultDirectoryIsOneNothingElseUses(t *testing.T) {
	if !filepath.IsAbs(tokenfile.DefaultDirectory) {
		t.Errorf("DefaultDirectory = %q, which is not an absolute path", tokenfile.DefaultDirectory)
	}
	if !strings.Contains(tokenfile.DefaultDirectory, "kube-crisp") {
		t.Errorf("DefaultDirectory = %q; a directory shared with anything else is a directory whose "+
			"contents somebody else decides", tokenfile.DefaultDirectory)
	}
}
