package projection

import (
	"strings"
	"testing"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestRowRefusesANamespaceThatIsNotOne covers the namespace column's value. It
// was only checked for NULL, so a row whose tenant was "", "Acme Corp" or
// "ACME" became an object with that as metadata.namespace: listed and watched
// across all namespaces, and then never fetched, updated or deleted, because no
// request path can name it. The name has always been held to the rule its
// request path needs; the namespace is now held to the one its path needs.
func TestRowRefusesANamespaceThatIsNotOne(t *testing.T) {
	m, err := NewMapper(testResource(), testMapping())
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	for _, ns := range []string{"", "Acme Corp", "ACME", "acme.corp", strings.Repeat("a", 64), "-acme"} {
		row := crispsql.Row{
			"id": "order-1", "tenant": ns, "updated_at": "1", "created_at": nil,
			"status": "open", "customer": "ada", "total_cents": int64(1),
			"line_items": nil,
		}

		if obj, err := m.Row(row); err == nil {
			t.Errorf("Row() mapped namespace %q onto an object: %q", ns, obj.GetNamespace())
		} else if !strings.Contains(err.Error(), "tenant") {
			t.Errorf("Row() error for namespace %q does not name the column: %v", ns, err)
		}
		if got, err := m.NamespaceFrom(row); err == nil {
			t.Errorf("NamespaceFrom() accepted namespace %q as %q", ns, got)
		}
	}

	// And an ordinary one is still an ordinary one.
	row := crispsql.Row{
		"id": "order-1", "tenant": "acme", "updated_at": "1", "created_at": nil,
		"status": "open", "customer": "ada", "total_cents": int64(1), "line_items": nil,
	}
	if obj, err := m.Row(row); err != nil || obj.GetNamespace() != "acme" {
		t.Errorf("Row() = %v, %v; want an object in acme", obj, err)
	}
	if got, err := m.NamespaceFrom(row); err != nil || got != "acme" {
		t.Errorf("NamespaceFrom() = %q, %v; want acme", got, err)
	}
}
