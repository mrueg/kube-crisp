package projection

import (
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestAnInvalidNameNamesTheColumnsItCameFrom. A row whose name is not a valid
// object name is the failure an operator has to go and find in the table, so
// the message has to say where to look. mapping.name is empty on a projection
// using nameColumns, and interpolating it blamed column "" for a name built
// from three others — pointing at nothing, in the case where knowing which
// column to look at matters most.
func TestAnInvalidNameNamesTheColumnsItCameFrom(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Shipment",
		Plural: "shipments", Scope: crispv1alpha1.ClusterScoped,
	}

	composite, err := NewMapper(res, crispv1alpha1.Mapping{
		NameColumns: []string{"region", "tier", "order_no"},
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	// Upper case is not a DNS-1123 subdomain, so the name is built and then
	// refused — which is the branch that reports the column.
	_, err = composite.Row(crispsql.Row{"region": "EU", "tier": "gold", "order_no": "1042"})
	if err == nil {
		t.Fatal("Row() accepted a name that is not a valid object name")
	}
	if strings.Contains(err.Error(), `column ""`) {
		t.Errorf("the error blames an empty column name: %v", err)
	}
	for _, column := range []string{"region", "tier", "order_no"} {
		if !strings.Contains(err.Error(), column) {
			t.Errorf("the error does not name column %q: %v", column, err)
		}
	}

	// The single-column case says the same thing about the one column there is.
	single, err := NewMapper(res, crispv1alpha1.Mapping{Name: "id"})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}
	_, err = single.Row(crispsql.Row{"id": "EU"})
	if err == nil {
		t.Fatal("Row() accepted a name that is not a valid object name")
	}
	if !strings.Contains(err.Error(), `column "id"`) {
		t.Errorf("the error does not name the column it came from: %v", err)
	}
}
