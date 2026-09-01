package projection

import (
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// threePartMapper names rows from three columns, which is where the gap was:
// with two, an empty part always lands at one end of the name and the
// DNS-1123 rules refuse it there. A third column gives it somewhere to hide.
func threePartMapper(t *testing.T) *Mapper {
	t.Helper()

	mapper, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Shipment",
		Plural: "shipments", Scope: crispv1alpha1.ClusterScoped,
	}, crispv1alpha1.Mapping{NameColumns: []string{"region", "tier", "order_no"}})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}
	return mapper
}

// TestBuildNameRefusesAnEmptyPart is the disagreement this closes. buildName
// joined an empty part without comment while SplitName refuses to take such a
// name back apart, so a row with an empty interior identity column was listed
// as "eu--1042" and then could not be fetched, updated or deleted: every one
// of those answered `name "eu--1042" has an empty part for column "tier"`, for
// an object the same server had just handed out.
func TestBuildNameRefusesAnEmptyPart(t *testing.T) {
	mapper := threePartMapper(t)

	_, err := mapper.Row(crispsql.Row{"region": "eu", "tier": "", "order_no": "1042"})
	if err == nil {
		t.Fatal("a row with an empty identity column produced a name anyway")
	}
	if !strings.Contains(err.Error(), `"tier"`) {
		t.Errorf("the error does not name the column that is empty: %v", err)
	}
}

// TestBuildNameRefusesAnEmptyPartWhereverItIs. An empty part at either end was
// already refused, but only as a side effect of the name it produced failing
// the DNS-1123 rules. One rule about identity beats a rule that depends on
// which column happened to be empty, and the message now names the column
// instead of describing a malformed name.
func TestBuildNameRefusesAnEmptyPartWhereverItIs(t *testing.T) {
	mapper := threePartMapper(t)

	for _, row := range []crispsql.Row{
		{"region": "", "tier": "gold", "order_no": "1042"},
		{"region": "eu", "tier": "gold", "order_no": ""},
	} {
		if _, err := mapper.Row(row); err == nil {
			t.Errorf("row %v produced a name with an empty part", row)
		}
	}
}

// TestACompositeNameStillRoundTrips, so none of the above is satisfied by
// refusing composite names in general: what buildName produces is exactly what
// SplitName takes apart again.
func TestACompositeNameStillRoundTrips(t *testing.T) {
	mapper := threePartMapper(t)

	name, err := mapper.NameFrom(crispsql.Row{"region": "eu", "tier": "gold", "order_no": "1042"})
	if err != nil {
		t.Fatalf("NameFrom() returned error: %v", err)
	}
	if want := "eu-gold-1042"; name != want {
		t.Fatalf("NameFrom() = %q, want %q", name, want)
	}

	args, err := mapper.SplitName(name)
	if err != nil {
		t.Fatalf("the name %q that was just built cannot be split back: %v", name, err)
	}
	for column, want := range map[string]any{"region": "eu", "tier": "gold", "order_no": "1042"} {
		if args[column] != want {
			t.Errorf("SplitName()[%q] = %#v, want %#v", column, args[column], want)
		}
	}
}
