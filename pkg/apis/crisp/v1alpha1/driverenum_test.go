package v1alpha1_test

import (
	"os"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// crdPath is the served CRD, relative to this package. A constant so that
// nothing can be read here but the file this test is about.
const crdPath = "../../../../manifests/10-crd-customresourceprojection.yaml"

// The CRD's driver enum is what a projection is actually validated against, and
// it is written by hand on the field. A driver registered in pkg/sql but left
// out of it is rejected by the API server before it reaches any of the code
// that supports it — which is how the cockroach driver shipped unusable.
func TestTheDriverEnumListsEveryRegisteredDriver(t *testing.T) {
	raw, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("reading the CRD: %v", err)
	}

	var crd map[string]any
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("parsing the CRD: %v", err)
	}

	enum := driverEnum(t, crd)
	sort.Strings(enum)

	registered := crispsql.RegisteredDrivers()
	sort.Strings(registered)

	if strings.Join(enum, ",") != strings.Join(registered, ",") {
		t.Errorf("the CRD accepts %v; pkg/sql registers %v", enum, registered)
	}
}

// driverEnum digs spec.dataSource.driver's enum out of the served version.
func driverEnum(t *testing.T, crd map[string]any) []string {
	t.Helper()

	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	for _, version := range versions {
		node, ok := version.(map[string]any)
		if !ok {
			continue
		}
		properties := dig(node, "schema", "openAPIV3Schema", "properties", "spec", "properties", "dataSource", "properties", "driver")
		if properties == nil {
			continue
		}
		values, _ := properties["enum"].([]any)
		out := make([]string, 0, len(values))
		for _, value := range values {
			name, _ := value.(string)
			out = append(out, name)
		}
		if len(out) > 0 {
			return out
		}
	}
	t.Fatal("the CRD has no enum on spec.dataSource.driver")
	return nil
}

func dig(node map[string]any, path ...string) map[string]any {
	for _, step := range path {
		next, ok := node[step].(map[string]any)
		if !ok {
			return nil
		}
		node = next
	}
	return node
}
