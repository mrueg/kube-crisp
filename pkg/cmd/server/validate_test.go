package server

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validProjection = `apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: orders
spec:
  dataSource:
    driver: postgres
    secretRef: {name: orders-db, namespace: kube-crisp}
  resource:
    group: store.example.com
    version: v1alpha1
    kind: Order
    plural: orders
    scope: Namespaced
    schema: {type: object}
  queries:
    list: {sql: "SELECT id, tenant FROM orders WHERE tenant = :namespace"}
  mapping: {name: id, namespace: tenant}
`

// TestValidateAcceptsAWellFormedProjection, since a checker that rejects
// everything would pass a test that only ever feeds it broken input.
func TestValidateAcceptsAWellFormedProjection(t *testing.T) {
	path := write(t, "good.yaml", validProjection)

	var out, errOut bytes.Buffer
	if err := runValidate([]string{path}, &out, &errOut); err != nil {
		t.Fatalf("a valid projection was rejected: %v\n%s", err, errOut.String())
	}
	if !strings.Contains(out.String(), "orders.store.example.com/v1alpha1") {
		t.Errorf("the output does not say what was accepted:\n%s", out.String())
	}
}

// TestValidateRejectsWhatTheServerWouldReject: the whole point is finding out
// where the file is written rather than after it reaches a cluster.
func TestValidateRejectsWhatTheServerWouldReject(t *testing.T) {
	// A mapping with no name column: nothing to call the object.
	broken := strings.Replace(validProjection,
		"mapping: {name: id, namespace: tenant}",
		"mapping: {namespace: tenant}", 1)
	path := write(t, "broken.yaml", broken)

	var out, errOut bytes.Buffer
	err := runValidate([]string{path}, &out, &errOut)
	if err == nil {
		t.Fatal("an invalid projection was accepted")
	}
	if !strings.Contains(err.Error(), "1 of 1") {
		t.Errorf("summary = %q, want it to count one rejection of one projection", err.Error())
	}
	if !strings.Contains(errOut.String(), "broken.yaml") {
		t.Errorf("the failure does not name the file:\n%s", errOut.String())
	}
}

// TestValidateSeparatesUnreadableFromInvalid. Both fail, and they are different
// failures: one is a file that is not a projection, the other is a projection
// that is wrong. Counting them together produced "1 of 0 projection(s)
// rejected", which describes nothing.
func TestValidateSeparatesUnreadableFromInvalid(t *testing.T) {
	// Strict decoding refuses a field the API does not have.
	unreadable := strings.Replace(validProjection,
		"    driver: postgres", "    driver: postgres\n    notAField: true", 1)
	path := write(t, "unreadable.yaml", unreadable)

	var out, errOut bytes.Buffer
	err := runValidate([]string{path}, &out, &errOut)
	if err == nil {
		t.Fatal("a file that does not parse was accepted")
	}
	if !strings.Contains(err.Error(), "could not be read") {
		t.Errorf("summary = %q, want it to report an unreadable path rather than a rejected "+
			"projection", err.Error())
	}
}

// TestValidateReportsEveryProjectionRatherThanTheFirst, so one run names
// everything that has to be fixed.
func TestValidateReportsEveryProjectionRatherThanTheFirst(t *testing.T) {
	dir := t.TempDir()
	first := strings.Replace(validProjection, "name: orders", "name: first", 1)
	first = strings.Replace(first, "mapping: {name: id, namespace: tenant}", "mapping: {namespace: tenant}", 1)
	second := strings.Replace(validProjection, "name: orders", "name: second", 1)
	second = strings.Replace(second, "plural: orders", "plural: seconds", 1)
	second = strings.Replace(second, "mapping: {name: id, namespace: tenant}", "mapping: {namespace: tenant}", 1)

	if err := os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(first), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.yaml"), []byte(second), 0o600); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	err := runValidate([]string{dir}, &out, &errOut)
	if err == nil {
		t.Fatal("two invalid projections were accepted")
	}
	for _, name := range []string{"first", "second"} {
		if !strings.Contains(errOut.String(), name) {
			t.Errorf("%q is not reported, so a run only names some of what is broken:\n%s",
				name, errOut.String())
		}
	}
}

// TestValidateRefusesAPathHoldingNothing, since silence would read as approval
// of a directory that was, say, misspelt.
func TestValidateRefusesAPathHoldingNothing(t *testing.T) {
	var out, errOut bytes.Buffer
	if err := runValidate([]string{t.TempDir()}, &out, &errOut); err == nil {
		t.Error("an empty directory was reported as validated")
	}
}

func write(t *testing.T, name, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
	return path
}

// TestValidateReportsEveryFileInADirectory covers the case that made this
// command need running more than once.
//
// Reading a directory as a unit means the first file that will not parse ends
// the walk, so a directory with two problems reports one of them — and the
// second only appears after the first is fixed.
func TestValidateReportsEveryFileInADirectory(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write("a-broken.yaml", "not: [valid\n")
	write("b-invalid.yaml", strings.Replace(validProjection,
		"mapping: {name: id, namespace: tenant}", "mapping: {namespace: tenant}", 1))
	write("c-good.yaml", strings.Replace(validProjection, "name: orders", "name: fine", 1))

	var out, errOut bytes.Buffer
	if err := runValidate([]string{dir}, &out, &errOut); err == nil {
		t.Fatal("a directory holding a broken file and an invalid projection was accepted")
	}

	problems := errOut.String()
	for _, want := range []string{"a-broken.yaml", "b-invalid.yaml"} {
		if !strings.Contains(problems, want) {
			t.Errorf("%s is not reported, so this directory needs checking more than once:\n%s",
				want, problems)
		}
	}
	// And the good one is still reported as good, rather than being lost in
	// the walk that the broken file used to end.
	if !strings.Contains(out.String(), "c-good.yaml") {
		t.Errorf("the file that is fine was not reported:\n%s", out.String())
	}
}

// TestValidateIgnoresNonProjectionsInADirectory. A projection directory may
// hold Secrets and ConfigMaps as well — the loader is deliberately willing to
// skip them — so reporting them would make an ordinary layout fail.
func TestValidateIgnoresNonProjectionsInADirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cm.yaml"),
		[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "p.yaml"), []byte(validProjection), 0o600); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if err := runValidate([]string{dir}, &out, &errOut); err != nil {
		t.Errorf("a ConfigMap beside a projection failed the run: %v\n%s", err, errOut.String())
	}
}

// TestValidateStillRejectsANamedFileHoldingNothing, since a file given on the
// command line that holds no projection is a mistake — the wrong path, or a
// misspelt one — rather than an ordinary neighbour.
func TestValidateStillRejectsANamedFileHoldingNothing(t *testing.T) {
	path := write(t, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n")

	var out, errOut bytes.Buffer
	if err := runValidate([]string{path}, &out, &errOut); err == nil {
		t.Error("a named file holding no projection was reported as validated")
	}
}
