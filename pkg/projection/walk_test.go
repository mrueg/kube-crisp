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
	loaded, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir(%s): %v", dir, err)
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
func TestLoadDirReadsSubdirectories(t *testing.T) {
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
func TestLoadDirReadsAConfigMapMountOnce(t *testing.T) {
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
func TestLoadDirSkipsDottedDirectories(t *testing.T) {
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
func TestLoadDirReadsADottedRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".projections")
	write(t, filepath.Join(dir, "orders.yaml"), manifest("orders", "orders"))

	if got := loadedNames(t, dir); len(got) != 1 || got[0] != "orders" {
		t.Fatalf("loaded %v, want orders", got)
	}
}

// LoadPath backs the plugin and `validate`, and has to agree with the server
// about which files are in scope.
func TestLoadPathReadsSubdirectoriesToo(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "orders", "orders.yaml"), manifest("orders", "orders"))

	loaded, err := LoadPath(dir)
	if err != nil {
		t.Fatalf("LoadPath(%s): %v", dir, err)
	}
	if len(loaded) != 1 || loaded[0].Name != "orders" {
		t.Fatalf("LoadPath loaded %d projections, want orders", len(loaded))
	}
}
