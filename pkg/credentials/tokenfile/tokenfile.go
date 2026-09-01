// Package tokenfile authenticates a kube-crisp data source with whatever is in
// a file at the moment a connection is opened.
//
// It is the cloud-agnostic shape of the same idea AWS RDS IAM has: something
// else mints a short-lived credential and kube-crisp uses the current one. In
// Kubernetes that something else has already been written several times over — a
// projected ServiceAccount token, a Vault Agent sidecar, a cloud token
// refresher, a Secrets Store CSI driver — and every one of them delivers the
// credential the same way, as a file on a mounted volume that is rewritten
// before it expires. This provider is the other end of that: no SDK, no vendor,
// nothing to configure but where to look.
//
//	dataSource:
//	  driver: postgres
//	  secretRef: {name: orders-db, namespace: kube-crisp}
//	  auth:
//	    provider: token-file
//	    options:
//	      path: /var/run/kube-crisp/credentials/orders-db
//
// The file is read on every connection and never held on to. That is the whole
// reason the credential seam exists: a pool opened from a connection string
// with a token in it is a new pool every time the token turns over, and a pool
// that read the file once at startup would authenticate with a credential that
// stopped being valid an hour ago. Reading it per connection means whoever
// rewrites the file has to do nothing else — no restart, no rolled deployment,
// no pool rebuilt — and the connections already open are not disturbed either.
//
// # Which files a projection may name
//
// A path is not an innocent option. A CustomResourceProjection is a cluster
// object, so whoever can create one chooses the path, and an unconstrained path
// means they can have the server read any file its process can — its own
// ServiceAccount token, its serving key, a Secret mounted for something else —
// and send it to a database as a password. That is a privilege escalation from
// "may define a projection" to "may exfiltrate the server's identity", and it
// is not one the option is worth.
//
// So a path has to be absolute and has to lie inside a directory the operator
// permitted with --credential-token-file-dirs, which defaults to one directory
// that exists for this and nothing else. The check is made twice: lexically when
// the projection is compiled, and again on the resolved path every time the file
// is read, because a symlink planted inside a permitted directory would
// otherwise point wherever it liked. Resolving is also what makes a projected
// volume work at all — Kubernetes writes those as a symlink to a timestamped
// directory, and rotates them by swapping the link — so the resolution has to
// happen per read rather than once.
//
// The permitted directories are an operator's decision and the registration is a
// build's, which is why they arrive separately: cmd registers the provider, and
// the server installs the directories from its flag once it has parsed one.
package tokenfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"k8s.io/klog/v2"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// ProviderName is what a projection's dataSource.auth.provider carries.
const ProviderName = "token-file"

// DefaultDirectory is where a credential file is looked for unless the operator
// says otherwise.
//
// A path nothing else in a container image uses, so that the default answer to
// "which files may a projection name" is "the ones somebody mounted here on
// purpose" rather than "everything this process can read". An operator whose
// refresher writes somewhere else — /vault/secrets, or a projected token under
// /var/run/secrets — names that directory instead, and names it deliberately.
const DefaultDirectory = "/var/run/kube-crisp/credentials"

// maxSize is the most a credential file may hold.
//
// Not a security boundary — a path that got this far is already inside a
// directory the operator mounted for credentials — but a bound on what a mistake
// costs. Tokens are hundreds of bytes; a JWT with a large claim set is a few
// thousand. Anything approaching this is not a credential, and reading it into
// memory once per connection is the sort of thing that is discovered as an OOM
// rather than as an error message.
const maxSize = 64 << 10

// The options this provider understands. Anything else is refused rather than
// ignored: a misspelt key is a projection reading a file nobody wrote, and the
// failure — an authentication error, later, from the database — says nothing
// about the typo that caused it.
const optionPath = "path"

// Register makes this provider available to projections. Call it from main
// before the server starts; see cmd/kube-crisp-apiserver in this module.
//
// Registering it is unconditional. Unlike a cloud provider it links no SDK, and
// unlike a provider that runs commands it grants nothing that reading a mounted
// file does not already grant, so there is nothing here for an operator to
// enable — only the directories to say where, which Permit carries.
func Register() error {
	return crispsql.RegisterCredentialProvider(crispsql.CredentialProvider{
		Name: ProviderName,
		Open: open,
	})
}

// permitted holds the directories a credential file may live in.
//
// Package state because the two halves of the decision arrive from different
// places at different times: whether the provider exists at all is settled by
// what main links in, while where it may read is an operator's flag, which does
// not exist until cobra has parsed the command line. The registry above has the
// same shape for the same reason.
var permitted struct {
	sync.RWMutex
	dirs []string
}

func init() { permitted.dirs = []string{DefaultDirectory} }

// Permit replaces the directories a credential file may live in.
//
// The server calls this once, from the value of --credential-token-file-dirs,
// before it serves anything. An empty list permits nothing, which leaves the
// provider registered and refusing every projection that names it — a way of
// turning it off that says so, rather than one that reports the provider as
// unknown to a build that has it.
func Permit(dirs []string) error {
	cleaned := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		dir = strings.TrimSpace(dir)
		if dir == "" {
			continue
		}
		if !filepath.IsAbs(dir) {
			return fmt.Errorf(
				"%q is not an absolute path; a credential directory is resolved against nothing and has "+
					"to say where it is", dir)
		}
		cleaned = append(cleaned, filepath.Clean(dir))
	}

	permitted.Lock()
	defer permitted.Unlock()
	permitted.dirs = cleaned
	return nil
}

// PermittedDirectories reports what Permit last installed, sorted, for an error
// message that can name them.
func PermittedDirectories() []string {
	permitted.RLock()
	defer permitted.RUnlock()

	dirs := append([]string(nil), permitted.dirs...)
	sort.Strings(dirs)
	return dirs
}

// open settles everything that can be settled before a connection exists.
//
// It runs once, when the pool is opened, which is while the projection is being
// compiled and where somebody is looking. An unknown option, a relative path, a
// path outside every permitted directory: all of them fail the projection here,
// with the Ready condition to say so, rather than becoming a pool that reports
// itself healthy and fails every request afterwards.
//
// What is deliberately not checked here is whether the file exists. A refresher
// sidecar and this server start at the same time and in no particular order, so
// a projection compiled a moment before the first token is written would
// otherwise fail for a race nobody controls, and stay failed until something
// resynced it. The absence is logged instead, and a connection opened before the
// file appears fails with a message naming it.
func open(req crispsql.CredentialRequest) (crispsql.Credentials, error) {
	if err := checkOptions(req.Options); err != nil {
		return nil, err
	}

	path := strings.TrimSpace(req.Options[optionPath])
	if path == "" {
		return nil, fmt.Errorf("%s needs the %q option: the file holding the credential", ProviderName, optionPath)
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf(
			"%q is not an absolute path; this server has no working directory a projection can reason about",
			path)
	}
	path = filepath.Clean(path)

	roots := roots()
	if len(roots) == 0 {
		return nil, errors.New(
			"no directory is permitted to hold a credential file: --credential-token-file-dirs is empty, " +
				"so this server reads none")
	}
	if !within(roots, path) {
		return nil, fmt.Errorf(
			"%q is not inside a directory this server reads credentials from (%s); a projection may name "+
				"any path, so the operator says which of them hold credentials rather than every file this "+
				"process can read",
			path, strings.Join(roots, ", "))
	}

	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			klog.Warningf("credential file %q does not exist yet; connections will fail until whatever "+
				"writes it has, and none will need a restart when it does", path)
		} else {
			return nil, fmt.Errorf("reading %q: %w", path, err)
		}
	}

	return &tokenFile{path: path, roots: roots}, nil
}

// checkOptions refuses a key this provider does not understand.
func checkOptions(options map[string]string) error {
	var unknown []string
	for key := range options {
		if key != optionPath {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("unknown auth option(s) %s; %s understands %s and nothing else",
		strings.Join(unknown, ", "), ProviderName, optionPath)
}

// roots is the permitted directories as a path can be compared against them:
// each one as written, and each one with its symlinks resolved.
//
// Both, because /var/run is a symlink to /run on every current distribution, so
// a file that is plainly inside /var/run/kube-crisp/credentials resolves to a
// path that is lexically inside nothing of the sort. Resolving the roots too is
// what stops that from reading as an escape attempt. A root that does not exist
// resolves to nothing and is kept as written, since it may be mounted later.
func roots() []string {
	permitted.RLock()
	dirs := append([]string(nil), permitted.dirs...)
	permitted.RUnlock()

	all := make([]string, 0, len(dirs)*2)
	for _, dir := range dirs {
		all = append(all, dir)
		if resolved, err := filepath.EvalSymlinks(dir); err == nil && resolved != dir {
			all = append(all, resolved)
		}
	}
	sort.Strings(all)
	return all
}

// within reports whether a path is inside one of the directories.
func within(roots []string, path string) bool {
	for _, root := range roots {
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

// tokenFile reads one file, every time it is asked.
//
// There is deliberately no cache. The interface asks providers to cache because
// the model it was written for signs a token — a thing with a cost, a rate
// limit, and sometimes a bill — whereas this reads a page the kernel already has
// and does it once per new connection, which is rare by construction:
// maxIdleConns defaults to maxOpenConns precisely so that a pool is not
// constantly reopening connections. A cache would buy microseconds and pay for
// them in the one currency that matters here, which is staleness — a credential
// held for a few seconds after the writer replaced it is a connection that
// fails authentication for no visible reason. Statting the file first to decide
// whether to re-read it would cost about as much as reading it and would be
// wrong in the same way, since a rewritten file can share a timestamp with the
// one it replaced.
type tokenFile struct {
	path  string
	roots []string
}

// Password reads the file and returns what is in it now.
func (f *tokenFile) Password(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	// Resolved every time rather than once, because a projected ServiceAccount
	// token is a symlink whose target changes when the token is rotated —
	// caching the resolution would pin the connection to a directory
	// Kubernetes has already deleted. It doubles as the check that nothing
	// inside a permitted directory points out of it.
	resolved, err := filepath.EvalSymlinks(f.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf(
				"credential file %q does not exist; whatever writes it has not, or not yet", f.path)
		}
		return "", fmt.Errorf("resolving credential file %q: %w", f.path, err)
	}
	if !within(f.roots, resolved) {
		return "", fmt.Errorf(
			"credential file %q resolves to %q, which is outside every directory this server reads "+
				"credentials from (%s)",
			f.path, resolved, strings.Join(f.roots, ", "))
	}

	// #nosec G304 -- the path was checked against the operator's permitted
	// directories when the projection was compiled, and the resolved path is
	// checked against them again immediately above.
	file, err := os.Open(resolved)
	if err != nil {
		return "", fmt.Errorf("opening credential file %q: %w", f.path, err)
	}
	defer func() { _ = file.Close() }()

	// A regular file and nothing else. A FIFO would block until somebody wrote
	// to it, with the pool waiting; a character device would return as much as
	// it was asked for and never end. Neither is a credential, and both fail in
	// a way that looks like the database being slow rather than like a path
	// being wrong.
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("reading credential file %q: %w", f.path, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("credential file %q is not a regular file", f.path)
	}

	// One byte past the ceiling, so that a file exactly at it is read and one
	// over it is reported rather than silently truncated to a token that would
	// be refused with no explanation.
	raw, err := io.ReadAll(io.LimitReader(file, maxSize+1))
	if err != nil {
		return "", fmt.Errorf("reading credential file %q: %w", f.path, err)
	}
	if len(raw) > maxSize {
		return "", fmt.Errorf("credential file %q is larger than %d bytes, so it is not a credential",
			f.path, maxSize)
	}

	// The trailing newline goes, and nothing else does.
	//
	// Almost everything that writes a file writes a line: a shell redirect, a
	// Vault Agent template, kubectl create secret --from-file of something
	// somebody edited. A password ending in a newline is not a thing anybody
	// intends, and a database refusing one because of a byte that does not
	// print is the least diagnosable failure in this whole path. Spaces and
	// tabs are left alone, because unlike a line ending they can be part of a
	// credential somebody chose.
	password := strings.TrimRight(string(raw), "\r\n")
	if password == "" {
		return "", fmt.Errorf(
			"credential file %q is empty; connecting with no password at all would authenticate as "+
				"whoever the database lets in without one", f.path)
	}
	return password, nil
}
