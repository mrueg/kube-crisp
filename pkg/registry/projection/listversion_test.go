package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestHighestVersion covers what a watch-disabled projection reports as its
// list resourceVersion: the newest version among the rows it returned.
func TestHighestVersion(t *testing.T) {
	rows := func(versions ...string) []unstructured.Unstructured {
		items := make([]unstructured.Unstructured, 0, len(versions))
		for _, v := range versions {
			item := unstructured.Unstructured{Object: map[string]any{}}
			if v != "" {
				item.SetResourceVersion(v)
			}
			items = append(items, item)
		}
		return items
	}

	for _, tc := range []struct {
		name  string
		items []unstructured.Unstructured
		want  string
	}{
		{"no rows", nil, ""},
		{"one row", rows("17"), "17"},
		// Numerically, not lexically: "9" is newer than "10", and a string
		// comparison would say the opposite.
		{"numeric ordering", rows("10", "9", "8"), "10"},
		{"unordered rows", rows("3", "100", "7"), "100"},
		// A projection that maps no resourceVersion has nothing to report, and
		// reporting something invented would be worse than reporting nothing.
		{"unversioned rows", rows("", "", ""), ""},
		// Timestamps and other non-numeric versions still order, lexically,
		// which is how the watch cache compares them too.
		{"string versions", rows("2024-01-02T00:00:00Z", "2024-03-01T00:00:00Z"), "2024-03-01T00:00:00Z"},
		{"mixed", rows("", "42", ""), "42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := highestVersion(tc.items); got != tc.want {
				t.Fatalf("highestVersion = %q, want %q", got, tc.want)
			}
		})
	}
}
