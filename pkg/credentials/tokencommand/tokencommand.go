// Package tokencommand authenticates a kube-crisp data source with the output
// of a command the operator installed.
//
// It is token-file's sibling for the case where nothing writes a file: a tool
// that prints a credential and exits — gcloud auth print-access-token, aws rds
// generate-db-auth-token, vault read -field=password — where the alternative is
// a sidecar looping around the same command, a shared volume, and a refresh
// interval to get wrong.
//
//	dataSource:
//	  driver: postgres
//	  secretRef: {name: orders-db, namespace: kube-crisp}
//	  auth:
//	    provider: token-command
//	    options:
//	      command: orders-db-token
//
// # Why this is off until an operator turns it on, and why the switch is a
// directory rather than a boolean
//
// Running a command is not a feature this can have casually. A
// CustomResourceProjection is a cluster object: whoever may create one would
// otherwise be able to run whatever they liked inside the API server pod, as the
// server, with its ServiceAccount token, its Secrets, and its position inside
// the cluster. That is not the bargain kubeconfig's exec credential plugin
// makes — there the file describing what to run is on the user's own machine and
// describes what that user already runs as themselves. Here the two are
// different principals, and the gap between them is the whole of the escalation.
//
// A boolean switch would not close it. It would only move the decision from
// "anybody who can write a projection" to "anybody who can write a projection,
// once", which is the same grant with a longer fuse. Nor is an allow-list of
// binaries enough, because the arguments are most of the danger: a permitted
// binary with attacker-chosen arguments is /bin/sh -c, or any of the ordinary
// tools that will read a file and print it.
//
// So the switch is a directory. --credential-command-dir names one the operator
// filled — a ConfigMap mounted executable, or something baked into the image —
// and a projection names one file in it by its bare name. No arguments, ever,
// from anywhere. What a projection contributes is a choice among things the
// operator wrote, which is the same shape as the provider name choosing among
// providers the build linked and secretRef choosing among Secrets the operator
// permitted. Unset, the provider stays registered and refuses every projection
// naming it, by name, while the projection is being compiled — which is where
// somebody is looking, and is what pkg/sql/session.go's sessionDialectFor
// documents at length: a capability claimed in one place and denied in another
// is denied on a path nobody watches.
//
// Point that flag at a directory somebody else fills and all of the above is
// undone: /usr/bin holds a great many programs that will print something
// sensitive when run with no arguments at all.
//
// # Prefer token-file
//
// Nothing here is better than a credential in a file, and most of what people
// reach for this to do is better done by writing that file. This exists for the
// tool that only prints, and it costs a fork and an exec on the connection path
// where token-file costs a read.
package tokencommand

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// ProviderName is what a projection's dataSource.auth.provider carries.
const ProviderName = "token-command"

// The options this provider understands. Anything else is refused rather than
// ignored, as it is everywhere else here: a misspelt key is a default quietly
// taken, and the authentication failure it produces later says nothing about
// the typo that caused it.
const (
	optionCommand = "command"
	optionTimeout = "timeout"
)

// How long a command may take, and the most a projection may ask for.
//
// Something has to bound it, because the context database/sql opens a
// connection with carries no deadline of its own: it lives as long as the pool
// does. A command that hangs would hold a connection slot until the process
// restarted, and a pool whose connections are all waiting on a hung process is
// indistinguishable from a database that stopped answering.
//
// The ceiling is there because the timeout is the one thing about this a
// projection may set, and a connection attempt that has been waiting a minute
// has already failed as far as any client of it is concerned.
const (
	defaultTimeout = 10 * time.Second
	maxTimeout     = time.Minute
)

// How long to keep waiting after the command was killed, and how much of what
// it printed to keep.
//
// A command whose grandchild inherited the pipes and outlived it would leave
// Wait blocking on a read that never ends, on the connection path, which is the
// failure the timeout above exists to prevent — arriving by another route.
// WaitDelay gives up on the pipes rather than on the process, and it is short
// because it is only ever reached after the command has already been killed:
// what it waits for is a write that is not coming, and every second of it is a
// second added to a connection attempt that has already failed.
//
// The output ceilings are the same judgement token-file's is: a credential is
// hundreds of bytes, and anything approaching these is a mistake that should be
// reported rather than an allocation nobody bounded.
const (
	waitDelay  = time.Second
	maxOutput  = 64 << 10
	maxStderr  = 4 << 10
	errorLimit = 512
)

// Register makes this provider available to projections. Call it from main
// before the server starts; see cmd/kube-crisp-apiserver in this module.
//
// Registering it does not enable it. The provider exists in every build, and
// refuses everything until an operator names a directory of commands with
// --credential-command-dir. Registering it unconditionally is what lets that
// refusal say "the operator has not enabled this" rather than "this build has
// never heard of it", which would send somebody to rebuild a binary that is
// already the right one.
func Register() error {
	return crispsql.RegisterCredentialProvider(crispsql.CredentialProvider{
		Name: ProviderName,
		Open: open,
	})
}

// directory holds the commands a projection may run, or nothing at all.
//
// Package state because the two halves of the decision arrive from different
// places at different times: whether the provider exists is settled by what main
// links in, before there is a command line to read, while whether it may do
// anything is an operator's flag, which does not exist until cobra has parsed
// one.
var directory struct {
	sync.RWMutex
	path string
}

// Enable installs the directory of commands a projection may name.
//
// The server calls this once, from --credential-command-dir, before it serves
// anything. An empty path leaves the provider registered and refusing, which is
// the state every build starts in.
func Enable(dir string) error {
	dir = strings.TrimSpace(dir)
	if dir != "" {
		if !filepath.IsAbs(dir) {
			return fmt.Errorf(
				"%q is not an absolute path; a directory of commands is resolved against nothing and has "+
					"to say where it is", dir)
		}
		dir = filepath.Clean(dir)
	}

	directory.Lock()
	defer directory.Unlock()
	directory.path = dir
	return nil
}

// Directory reports what Enable installed, for a message that can name it.
func Directory() string {
	directory.RLock()
	defer directory.RUnlock()
	return directory.path
}

// open settles everything that can be settled before a connection exists: the
// provider being enabled at all, the options, and the command being a file in
// the permitted directory that this process can actually run.
//
// All of it runs while the projection is being compiled, which is where somebody
// is looking. Unlike token-file, the command has to exist by then — it is
// installed with the server rather than written by something racing it, so an
// absent one is a mistake in the deployment and not a moment in its startup.
func open(req crispsql.CredentialRequest) (crispsql.Credentials, error) {
	dir := Directory()
	if dir == "" {
		return nil, fmt.Errorf(
			"the %s credential provider is registered but not enabled: this server was started without "+
				"--credential-command-dir, so there is no directory of commands a projection may run. "+
				"Running a command is the server's own privilege, and a projection is a cluster object, so "+
				"which commands exist is the operator's decision and not a projection's",
			ProviderName)
	}

	if err := checkOptions(req.Options); err != nil {
		return nil, err
	}

	name := strings.TrimSpace(req.Options[optionCommand])
	if name == "" {
		return nil, fmt.Errorf("%s needs the %q option: the name of a command in %s",
			ProviderName, optionCommand, dir)
	}
	if name != filepath.Base(name) || name == "." || name == ".." || strings.ContainsRune(name, os.PathSeparator) {
		return nil, fmt.Errorf(
			"%q is not a bare command name; a projection names one file in %s and cannot reach outside it",
			name, dir)
	}

	timeout, err := timeoutOf(req.Options)
	if err != nil {
		return nil, err
	}

	path, err := executable(dir, name)
	if err != nil {
		return nil, err
	}

	return &command{path: path, name: name, timeout: timeout}, nil
}

// checkOptions refuses a key this provider does not understand.
func checkOptions(options map[string]string) error {
	var unknown []string
	for key := range options {
		if key != optionCommand && key != optionTimeout {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("unknown auth option(s) %s; %s understands %s and %s",
		strings.Join(unknown, ", "), ProviderName, optionCommand, optionTimeout)
}

// timeoutOf reads the timeout option, or settles on the default.
func timeoutOf(options map[string]string) (time.Duration, error) {
	raw := strings.TrimSpace(options[optionTimeout])
	if raw == "" {
		return defaultTimeout, nil
	}

	timeout, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%q is not a duration: %w", raw, err)
	}
	switch {
	case timeout <= 0:
		return 0, fmt.Errorf("a %s of %s would leave the command no time to run", optionTimeout, raw)
	case timeout > maxTimeout:
		return 0, fmt.Errorf(
			"a %s of %s is longer than the %s a connection may spend obtaining a password; a connection "+
				"attempt waiting that long has already failed as far as anything asking for it is concerned",
			optionTimeout, raw, maxTimeout)
	}
	return timeout, nil
}

// executable resolves a command name inside the permitted directory and reports
// whether this process could run it.
//
// The resolved path is checked against the directory again, because a symlink
// in it would otherwise be a way out of it — and unlike a credential file, what
// is on the other end of that link is executed. The executable bit is checked
// here rather than discovered at connect time, because a ConfigMap mounted
// without a mode that permits execution is the single most likely way for this
// to be set up wrong, and "permission denied" once per connection attempt is a
// bad place to learn it.
func executable(dir, name string) (string, error) {
	path := filepath.Join(dir, name)

	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("%q is not a command in %s: %w", name, dir, err)
	}
	if !within(dir, resolved) {
		return "", fmt.Errorf(
			"%q resolves to %q, which is outside %s; a projection may name a command the operator "+
				"installed and nothing else", name, resolved, dir)
	}

	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("%q is not a command in %s: %w", name, dir, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%q in %s is not a regular file", name, dir)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf(
			"%q in %s is not executable (mode %v); a ConfigMap mounted as a command needs a defaultMode "+
				"that permits it", name, dir, info.Mode().Perm())
	}
	return resolved, nil
}

// within reports whether a path is inside a directory. The directory is resolved
// too, since /var/run is a symlink to /run on every current distribution and a
// path plainly inside one is lexically inside neither.
func within(dir, path string) bool {
	dirs := []string{dir}
	if resolved, err := filepath.EvalSymlinks(dir); err == nil && resolved != dir {
		dirs = append(dirs, resolved)
	}
	for _, root := range dirs {
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			continue
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// command runs one operator-installed executable and reads a credential off its
// standard output.
type command struct {
	path    string
	name    string
	timeout time.Duration

	// inflight is one run several connections may share.
	//
	// Not a cache, deliberately: a cache would hand out a credential minted
	// before whatever the command talks to had a chance to change its mind
	// about it, and there is no expiry here to bound that by — a command prints
	// a token and says nothing about how long it is good for. This only shares
	// a run that is happening anyway, which is what a pool refilling after an
	// idle period actually does: several connections opened in the same
	// instant, each of which would otherwise fork a process and ask a cloud for
	// a token that would be identical to the others.
	mu       sync.Mutex
	inflight *run
}

// run is one execution, and whatever came of it.
type run struct {
	done     chan struct{}
	password string
	err      error
}

// Password runs the command, or joins a run already under way.
func (c *command) Password(ctx context.Context) (string, error) {
	c.mu.Lock()
	if joined := c.inflight; joined != nil {
		c.mu.Unlock()
		select {
		case <-joined.done:
			return joined.password, joined.err
		case <-ctx.Done():
			// The run continues for whoever else is waiting on it; only this
			// caller gives up, because its connection is the one whose deadline
			// passed.
			return "", ctx.Err()
		}
	}

	current := &run{done: make(chan struct{})}
	c.inflight = current
	c.mu.Unlock()

	current.password, current.err = c.execute(ctx)

	c.mu.Lock()
	c.inflight = nil
	c.mu.Unlock()
	close(current.done)

	return current.password, current.err
}

// execute runs the command once and reads a credential off its stdout.
func (c *command) execute(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// No shell. The path is executed directly with no arguments and nothing to
	// quote, so there is no string anywhere that a shell could find a
	// metacharacter in — which is what makes "the operator wrote this command"
	// a statement about what runs rather than about what it starts with.
	//
	// #nosec G204 -- the path is a file in the directory the operator named
	// with --credential-command-dir, checked at compile time to be inside it
	// even after its symlinks are resolved. A projection contributes the bare
	// name of one file there and nothing else: no path, no arguments.
	cmd := exec.CommandContext(ctx, c.path)

	// The server's own environment, because a command that talks to a cloud
	// needs the same region, endpoint and credential configuration this process
	// was given, and it could read all of it anyway — it runs as this process.
	cmd.Env = os.Environ()

	// A command that leaves a grandchild holding the pipes would otherwise have
	// Wait blocking on a read that never ends, on the connection path.
	cmd.WaitDelay = waitDelay

	stdout := &capped{limit: maxOutput}
	stderr := &capped{limit: maxStderr}
	cmd.Stdout, cmd.Stderr = stdout, stderr

	err := cmd.Run()
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return "", fmt.Errorf("command %q did not finish within %s%s", c.name, c.timeout, diagnostics(stderr))
	case err != nil:
		return "", fmt.Errorf("command %q failed: %w%s", c.name, err, diagnostics(stderr))
	case stdout.overflowed:
		return "", fmt.Errorf("command %q printed more than %d bytes, so it did not print a credential",
			c.name, maxOutput)
	}

	// The trailing newline goes and nothing else does, for the reason it does
	// in token-file: a command that prints a credential prints a line, and a
	// database refusing a token because of a byte that does not print is the
	// least diagnosable failure in this path.
	password := strings.TrimRight(stdout.String(), "\r\n")
	if password == "" {
		return "", fmt.Errorf(
			"command %q printed nothing; connecting with no password at all would authenticate as whoever "+
				"the database lets in without one%s", c.name, diagnostics(stderr))
	}
	return password, nil
}

// diagnostics renders what the command said on stderr, for the message about it
// having failed.
//
// Only on failure, and this is the whole of what happens to stderr. A command
// that succeeded has already said what it had to say on stdout, and repeating
// its chatter into the log once per connection would put a stream that sits
// beside a credential into the log at the rate connections are opened. A command
// that failed has no credential to leak and every reason to be quoted: this
// message reaches the projection's Ready condition, which is where somebody
// looking at a broken projection is looking.
func diagnostics(stderr *capped) string {
	said := strings.TrimSpace(stderr.String())
	if said == "" {
		return ""
	}
	if len(said) > errorLimit {
		said = said[:errorLimit] + "..."
	}
	return fmt.Sprintf(" (stderr: %s)", said)
}

// capped collects output up to a limit and discards the rest, without ever
// telling the writer to stop — a command blocked on a full pipe would hang, and
// hanging is what everything here is arranged to avoid.
type capped struct {
	limit      int
	buf        bytes.Buffer
	overflowed bool
}

func (c *capped) Write(p []byte) (int, error) {
	if room := c.limit - c.buf.Len(); room > 0 {
		if len(p) <= room {
			c.buf.Write(p)
		} else {
			c.buf.Write(p[:room])
			c.overflowed = true
		}
	} else if len(p) > 0 {
		c.overflowed = true
	}
	return len(p), nil
}

func (c *capped) String() string { return c.buf.String() }
