package projection

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeStaticResource writes a file-backed projection whose resource stanza is
// the caller's, for the names the CRD would have refused.
func writeStaticResource(t *testing.T, dir, name, resource string) {
	t.Helper()

	manifest := fmt.Sprintf(`apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: %s
spec:
  dataSource:
    driver: sqlite
    secretRef: {name: bins-db, namespace: kube-crisp}
  resource:
%s
  queries:
    list:
      sql: SELECT id, tenant FROM bins WHERE tenant = :namespace
  mapping:
    name: id
    namespace: tenant
`, name, resource)

	if err := os.WriteFile(filepath.Join(dir, name+".yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
}

// One file's mistake must not be every file's outage.
//
// A projection loaded from --projection-dir never meets the CRD, so a name the
// CRD would have refused reaches the controller. Two such names used to take
// everything down with them. A plural such as bins/status is a subresource to
// the endpoint installer, which refused the surface whole; the sync failed,
// nothing was served, and the server never became ready. A version such as V2
// compiled and installed, and then produced an APIService the kube-apiserver
// rejects on every reconcile. And once the loader refused either, it refused
// the whole directory, which on a cold start left nothing to fall back to.
//
// Now each is refused where the projection is prepared, that projection alone
// is failed, and the good file beside it is served.
func TestAFileWithABadNameDoesNotStopTheOtherFiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resource string
	}{
		{
			name: "a plural naming a subresource",
			resource: `    group: warehouse.example.com
    version: v1alpha1
    kind: Crate
    plural: crates/status
    scope: Namespaced
    schema:
      type: object`,
		},
		{
			name: "a version name the kube-apiserver refuses",
			resource: `    group: warehouse.example.com
    version: v1alpha1
    kind: Crate
    plural: crates
    scope: Namespaced
    schema:
      type: object
    versions:
      - name: V2
        schema:
          type: object`,
		},
		{
			name: "an upper case group",
			resource: `    group: Warehouse.example.com
    version: v1alpha1
    kind: Crate
    plural: crates
    scope: Namespaced
    schema:
      type: object`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeStaticProjection(t, dir, "bins", "bins")
			writeStaticResource(t, dir, "broken", tc.resource)

			f := newFixture(t, nil)
			f.controller.staticDir = dir

			if err := f.controller.sync(context.Background()); err != nil {
				t.Fatalf("sync() returned error: %v", err)
			}

			if !f.controller.HasSynced() {
				t.Error("the controller never reported synced, so readiness would never close")
			}
			if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/bins]"; got != want {
				t.Errorf("served paths = %s, want the good file alone %s", got, want)
			}
			if degraded := f.controller.Degraded(); len(degraded) != 1 || degraded[0] != "broken" {
				t.Errorf("Degraded() = %v, want [broken]", degraded)
			}
		})
	}
}

// Fixing the file is enough: the next re-read serves it.
func TestAFileWithABadNameIsServedOnceFixed(t *testing.T) {
	dir := t.TempDir()
	writeStaticProjection(t, dir, "bins", "bins")
	writeStaticProjection(t, dir, "crates", "crates/status")

	f := newFixture(t, nil)
	f.controller.staticDir = dir

	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	if degraded := f.controller.Degraded(); len(degraded) != 1 {
		t.Fatalf("Degraded() = %v, want the broken file alone", degraded)
	}

	writeStaticProjection(t, dir, "crates", "crates")
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() after the fix returned error: %v", err)
	}

	if degraded := f.controller.Degraded(); len(degraded) != 0 {
		t.Errorf("Degraded() = %v, want nothing once the file is fixed", degraded)
	}
	if got := len(f.router.ServedPaths()); got != 2 {
		t.Errorf("served paths = %s, want both files", servedPaths(f.router))
	}
}
