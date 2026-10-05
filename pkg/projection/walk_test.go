package projection

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// manifest is one valid projection, named so the tests can tell them apart.
func manifest(name, plural string) string {
	return `apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: ` + name + `
spec:
  dataSource:
    driver: sqlite
    secretRef:
      name: ` + name + `
      namespace: kube-crisp
  resource:
    group: warehouse.example.com
    version: v1alpha1
    kind: Bin
    plural: ` + plural + `
    scope: Namespaced
    schema:
      type: object
  queries:
    list:
      sql: SELECT id, tenant FROM bins
  mapping:
    name: id
    namespace: tenant
`
}

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("creating %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

func loadedNames(t *testing.T, dir string) []string {
	t.Helper()
	loaded, err := LoadPath(dir)
	if err != nil {
		t.Fatalf("LoadPath(%s): %v", dir, err)
	}
	out := make([]string, 0, len(loaded))
	for i := range loaded {
		out = append(out, loaded[i].Name)
	}
	sort.Strings(out)
	return out
}

// A directory holding only subdirectories used to load nothing and say nothing,
// which is what happened to this repository's own examples/ when it grew
// folders.
func TestLoadPathReadsSubdirectories(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "top.yaml"), manifest("top", "tops"))
	write(t, filepath.Join(dir, "orders", "orders.yaml"), manifest("orders", "orders"))
	write(t, filepath.Join(dir, "pagila", "deep", "films.yml"), manifest("films", "films"))

	got := loadedNames(t, dir)
	want := []string{"films", "orders", "top"}
	if len(got) != len(want) {
		t.Fatalf("loaded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("loaded %v, want %v", got, want)
		}
	}
}

// A mounted ConfigMap must be read exactly once.
//
// The mount is not a flat directory: each key is a symlink beside a `..data`
// symlink pointing at a real timestamped directory that holds the files. Read
// recursively without care, every projection arrives twice — and two
// projections claiming one resource is a conflict, so a ConfigMap would fail
// every projection in it against its own twin.
func TestLoadPathReadsAConfigMapMountOnce(t *testing.T) {
	dir := t.TempDir()

	// The real files, where the kubelet puts them.
	data := filepath.Join(dir, "..2026_09_01_00_00_00.1234")
	write(t, filepath.Join(data, "orders.yaml"), manifest("orders", "orders"))
	write(t, filepath.Join(data, "films.yaml"), manifest("films", "films"))

	// ..data -> the timestamped directory, and one symlink per key beside it.
	if err := os.Symlink(data, filepath.Join(dir, "..data")); err != nil {
		t.Fatalf("linking ..data: %v", err)
	}
	for _, key := range []string{"orders.yaml", "films.yaml"} {
		if err := os.Symlink(filepath.Join("..data", key), filepath.Join(dir, key)); err != nil {
			t.Fatalf("linking %s: %v", key, err)
		}
	}

	got := loadedNames(t, dir)
	if len(got) != 2 || got[0] != "films" || got[1] != "orders" {
		t.Fatalf("loaded %v, want each projection exactly once", got)
	}
}

// Dotted directories are not where manifests live, and descending into them is
// how the ConfigMap above would double.
func TestLoadPathSkipsDottedDirectories(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "orders.yaml"), manifest("orders", "orders"))
	write(t, filepath.Join(dir, ".git", "stale.yaml"), manifest("stale", "stales"))
	write(t, filepath.Join(dir, ".cache", "deep", "stale2.yaml"), manifest("stale2", "stale2s"))

	got := loadedNames(t, dir)
	if len(got) != 1 || got[0] != "orders" {
		t.Fatalf("loaded %v, want just orders", got)
	}
}

// A dot on the directory the operator named is not a reason to read nothing:
// the rule is about what is found underneath, not about where the walk starts.
func TestLoadPathReadsADottedRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".projections")
	write(t, filepath.Join(dir, "orders.yaml"), manifest("orders", "orders"))

	if got := loadedNames(t, dir); len(got) != 1 || got[0] != "orders" {
		t.Fatalf("loaded %v, want orders", got)
	}
}

func symlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("linking %s -> %s: %v", link, target, err)
	}
}

// A --projection-dir that is itself a symlink -- the `current -> releases/42`
// shape a deploy swaps atomically -- used to load nothing, and say nothing:
// the walk looked at the link rather than through it, found a non-directory
// with no manifest extension, and was done.
func TestLoadPathFollowsASymlinkedRoot(t *testing.T) {
	base := t.TempDir()
	release := filepath.Join(base, "releases", "42")
	write(t, filepath.Join(release, "orders.yaml"), manifest("orders", "orders"))
	write(t, filepath.Join(release, "pagila", "films.yaml"), manifest("films", "films"))
	current := filepath.Join(base, "current")
	symlink(t, release, current)

	got := loadedNames(t, current)
	if len(got) != 2 || got[0] != "films" || got[1] != "orders" {
		t.Fatalf("loaded %v, want films and orders", got)
	}
}

// A ConfigMap whose items name nested paths is mounted with the subdirectory
// as a symlink, `sub -> ..data/sub`, and that subdirectory used to be skipped
// for being a link.
func TestLoadPathReadsAConfigMapMountsNestedItems(t *testing.T) {
	dir := t.TempDir()

	data := filepath.Join(dir, "..2026_09_01_00_00_00.1234")
	write(t, filepath.Join(data, "orders.yaml"), manifest("orders", "orders"))
	write(t, filepath.Join(data, "pagila", "films.yaml"), manifest("films", "films"))

	symlink(t, data, filepath.Join(dir, "..data"))
	symlink(t, filepath.Join("..data", "orders.yaml"), filepath.Join(dir, "orders.yaml"))
	symlink(t, filepath.Join("..data", "pagila"), filepath.Join(dir, "pagila"))

	got := loadedNames(t, dir)
	if len(got) != 2 || got[0] != "films" || got[1] != "orders" {
		t.Fatalf("loaded %v, want each projection exactly once", got)
	}
}

// Following links means a link can lead back to where the walk has been.
// Each directory is read once, by where it really is, so a loop ends and two
// links to one directory do not load its projections twice.
func TestLoadPathReadsEachDirectoryOnceWhateverLinksToIt(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "orders", "orders.yaml"), manifest("orders", "orders"))
	symlink(t, dir, filepath.Join(dir, "orders", "loop"))
	symlink(t, filepath.Join(dir, "orders"), filepath.Join(dir, "again"))

	got := loadedNames(t, dir)
	if len(got) != 1 || got[0] != "orders" {
		t.Fatalf("loaded %v, want orders once", got)
	}
}

// Dotted files are skipped as dotted directories are. An editor's lock file
// is the case that matters: emacs leaves `.#orders.yaml` as a symlink to
// nowhere while a buffer is modified, and reading it failed the whole
// directory for as long as anyone was editing it.
func TestLoadPathSkipsDottedFiles(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "orders.yaml"), manifest("orders", "orders"))
	write(t, filepath.Join(dir, ".hidden.yaml"), manifest("hidden", "hiddens"))
	symlink(t, "user@host.1234:1700000000", filepath.Join(dir, ".#orders.yaml"))

	got := loadedNames(t, dir)
	if len(got) != 1 || got[0] != "orders" {
		t.Fatalf("loaded %v, want just orders", got)
	}
}
