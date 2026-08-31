package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

const filmsProjection = `
apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: pagila-films
spec:
  dataSource:
    driver: postgres
    secretRef: {name: pagila-db, namespace: kube-crisp}
  resource:
    group: pagila.example.com
    version: v1alpha1
    kind: Film
    plural: films
    scope: Cluster
  queries:
    list:
      sql: SELECT film_id, title FROM film
  mapping:
    name: title
`

// writeProjection puts a manifest in a temporary directory, alongside a file
// that is not a projection, which the loader is meant to ignore.
func writeProjection(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "films.yaml"), []byte(filmsProjection), 0o600); err != nil {
		t.Fatal(err)
	}
	secret := "apiVersion: v1\nkind: Secret\nmetadata:\n  name: pagila-db\n"
	if err := os.WriteFile(filepath.Join(dir, "secret.yaml"), []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func run(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	cmd := NewCommandRBAC(&out, &errOut)
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

// TestRBACFromDirectory is the mode that needs no cluster: read the manifests,
// print the roles.
func TestRBACFromDirectory(t *testing.T) {
	stdout, _, err := run(t, "-f", writeProjection(t))
	if err != nil {
		t.Fatal(err)
	}

	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal([]byte(stdout), &role); err != nil {
		t.Fatalf("output is not a ClusterRole: %v\n%s", err, stdout)
	}
	if role.Name != "kube-crisp:pagila.example.com:view" {
		t.Fatalf("name = %s", role.Name)
	}
	if role.Kind != "ClusterRole" || role.APIVersion != "rbac.authorization.k8s.io/v1" {
		t.Fatalf("output is missing its type: %s/%s", role.APIVersion, role.Kind)
	}
	// A read-only projection: no edit role, so no document separator.
	if strings.Contains(stdout, "---") {
		t.Fatalf("expected a single document:\n%s", stdout)
	}
}

// TestJSONOutputIsAList because -o json has no document separator to stream
// with, which is also what kubectl -o json means elsewhere.
func TestJSONOutputIsAList(t *testing.T) {
	stdout, _, err := run(t, "-f", writeProjection(t), "-o", "json")
	if err != nil {
		t.Fatal(err)
	}

	var list rbacv1.ClusterRoleList
	if err := json.Unmarshal([]byte(stdout), &list); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, stdout)
	}
	if list.Kind != "ClusterRoleList" || len(list.Items) != 1 {
		t.Fatalf("got %s with %d item(s)", list.Kind, len(list.Items))
	}
}

// TestUnsupportedOutputFormat fails before anything is read, rather than
// printing YAML for a format nobody asked for.
func TestUnsupportedOutputFormat(t *testing.T) {
	if _, _, err := run(t, "-f", writeProjection(t), "-o", "table"); err == nil {
		t.Fatal("expected an error")
	}
}

// TestFilenamesAndNamesAreExclusive: -f reads manifests and names read the
// cluster, so a command carrying both has two answers.
func TestFilenamesAndNamesAreExclusive(t *testing.T) {
	_, _, err := run(t, "-f", writeProjection(t), "pagila-films")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "cannot combine") {
		t.Fatalf("unhelpful error: %v", err)
	}
}

// TestPathWithNoProjectionIsAnError, because at this end it means a mistyped
// argument. An empty cluster is the case that is not an error.
func TestPathWithNoProjectionIsAnError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "other.yaml"), []byte("apiVersion: v1\nkind: Secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := run(t, "-f", dir); err == nil {
		t.Fatal("expected an error")
	}
}

// TestAggregateFlag reaches the generator, so the plugin's default is the
// generator's default.
func TestAggregateFlag(t *testing.T) {
	dir := writeProjection(t)

	plain, _, err := run(t, "-f", dir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, "aggregate-to-view") {
		t.Fatalf("aggregation labels without --aggregate:\n%s", plain)
	}

	aggregated, _, err := run(t, "-f", dir, "--aggregate")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(aggregated, "aggregate-to-view") {
		t.Fatalf("--aggregate did not label the role:\n%s", aggregated)
	}
}
