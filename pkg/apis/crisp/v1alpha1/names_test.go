package v1alpha1_test

import (
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// The patterns a projected resource's names are held to are written twice: as
// kubebuilder markers, which become the CRD and refuse a projection applied to
// a cluster, and as constants, which projection.Validate reads for a projection
// loaded from a file — where the CRD is never consulted. A marker is a comment,
// so nothing but this test makes the two say the same thing.
func TestTheNamePatternsMatchTheCRD(t *testing.T) {
	crd := readCRD(t)
	resource := servedProperty(t, crd, "resource")

	for _, field := range []struct {
		name      string
		path      []string
		pattern   string
		maxLength int
	}{
		{"group", []string{"properties", "group"},
			crispv1alpha1.GroupNamePattern, crispv1alpha1.GroupNameMaxLength},
		{"version", []string{"properties", "version"},
			crispv1alpha1.VersionNamePattern, 0},
		{"versions[].name", []string{"properties", "versions", "items", "properties", "name"},
			crispv1alpha1.VersionNamePattern, 0},
		{"kind", []string{"properties", "kind"},
			crispv1alpha1.KindNamePattern, crispv1alpha1.KindNameMaxLength},
		{"listKind", []string{"properties", "listKind"},
			crispv1alpha1.KindNamePattern, crispv1alpha1.KindNameMaxLength},
		{"plural", []string{"properties", "plural"},
			crispv1alpha1.ResourceNamePattern, crispv1alpha1.ResourceNameMaxLength},
		{"singular", []string{"properties", "singular"},
			crispv1alpha1.ResourceNamePattern, crispv1alpha1.ResourceNameMaxLength},
		{"shortNames[]", []string{"properties", "shortNames", "items"},
			crispv1alpha1.ResourceNamePattern, crispv1alpha1.ResourceNameMaxLength},
	} {
		t.Run(field.name, func(t *testing.T) {
			node := dig(resource, field.path...)
			if node == nil {
				t.Fatalf("the CRD has no spec.resource.%s", field.name)
			}
			if got, _ := node["pattern"].(string); got != field.pattern {
				t.Errorf("the CRD's pattern is %q; the constant is %q", got, field.pattern)
			}
			if field.maxLength == 0 {
				return
			}
			if got, _ := node["maxLength"].(float64); int(got) != field.maxLength {
				t.Errorf("the CRD's maxLength is %v; the constant is %d", got, field.maxLength)
			}
		})
	}
}

// servedProperty digs one of spec's properties out of the served version.
func servedProperty(t *testing.T, crd map[string]any, property string) map[string]any {
	t.Helper()

	versions, _ := crd["spec"].(map[string]any)["versions"].([]any)
	for _, version := range versions {
		node, ok := version.(map[string]any)
		if !ok {
			continue
		}
		if found := dig(node, "schema", "openAPIV3Schema", "properties", "spec", "properties", property); found != nil {
			return found
		}
	}
	t.Fatalf("the CRD has no spec.%s", property)
	return nil
}
