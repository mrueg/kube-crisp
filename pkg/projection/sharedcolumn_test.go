package projection

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// sharedMapper maps one column both as a label and as a field, which is how the
// e2e orders projection is written and a reasonable thing to want: select on it
// as a label, show it as a field.
func sharedMapper(t *testing.T) *Mapper {
	t.Helper()

	m, err := NewMapper(
		crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
			Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
		},
		crispv1alpha1.Mapping{
			Name:      "id",
			Namespace: "tenant",
			Labels:    map[string]string{"store.example.com/status": "status"},
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "status", Path: "status.phase"},
				{Column: "customer", Path: "spec.customer"},
			},
		},
	)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}
	return m
}

func order(labels map[string]string, phase string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": "order-1", "namespace": "acme"},
		"spec":       map[string]any{"customer": "ada"},
	}}
	if labels != nil {
		obj.SetLabels(labels)
	}
	if phase != "" {
		_ = unstructured.SetNestedField(obj.Object, phase, "status", "phase")
	}
	return obj
}

// TestParamsBindsTheFieldWhenAColumnIsMappedTwice pins the behaviour the
// warning describes. The field mapping runs last and wins.
func TestParamsBindsTheFieldWhenAColumnIsMappedTwice(t *testing.T) {
	m := sharedMapper(t)

	args, err := m.Params(order(map[string]string{"store.example.com/status": "cancelled"}, "shipped"))
	if err != nil {
		t.Fatalf("Params() returned error: %v", err)
	}
	if got := args["status"]; got != "shipped" {
		t.Errorf("status bound as %v, want the field's value \"shipped\"", got)
	}
}

// TestDroppedOnWriteNamesWhatWasIgnored is the fix: the write is answered 200
// either way, so the half that was ignored has to be said out loud. Before
// this, kubectl reported "labeled" for a row that had not moved.
func TestDroppedOnWriteNamesWhatWasIgnored(t *testing.T) {
	m := sharedMapper(t)

	dropped := m.DroppedOnWrite(order(map[string]string{"store.example.com/status": "cancelled"}, "shipped"))
	if len(dropped) != 1 {
		t.Fatalf("DroppedOnWrite() reported %d things, want 1: %v", len(dropped), dropped)
	}
	for _, want := range []string{
		"store.example.com/status", // which label
		`column "status"`,          // which column it collides on
		"status.phase",             // which field took it
		"shipped",                  // and what the write actually set
	} {
		if !strings.Contains(dropped[0], want) {
			t.Errorf("the warning does not mention %q: %s", want, dropped[0])
		}
	}
}

// TestDroppedOnWriteIsQuietWhenTheyAgree keeps the warning meaningful. Reads
// fill both sides from the same column, so agreeing is the normal case and
// warning about it would make the message worthless.
func TestDroppedOnWriteIsQuietWhenTheyAgree(t *testing.T) {
	m := sharedMapper(t)

	for _, tc := range []struct {
		name string
		obj  *unstructured.Unstructured
	}{
		{"both the same", order(map[string]string{"store.example.com/status": "shipped"}, "shipped")},
		{"neither present", order(nil, "")},
		{"a label the projection does not map", order(map[string]string{"team": "payments"}, "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if dropped := m.DroppedOnWrite(tc.obj); len(dropped) != 0 {
				t.Errorf("DroppedOnWrite() complained about nothing: %v", dropped)
			}
		})
	}
}

// TestNoSharedColumnsWhenTheMappingsAreDistinct: a projection whose labels and
// fields read different columns has nothing to warn about, and writing the
// label persists exactly as writing a field does.
func TestNoSharedColumnsWhenTheMappingsAreDistinct(t *testing.T) {
	m, err := NewMapper(
		crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
			Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
		},
		crispv1alpha1.Mapping{
			Name:      "id",
			Namespace: "tenant",
			Labels:    map[string]string{"store.example.com/status": "status"},
			Fields:    []crispv1alpha1.FieldMapping{{Column: "customer", Path: "spec.customer"}},
		},
	)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	if shared := m.SharedColumns(); len(shared) != 0 {
		t.Errorf("SharedColumns() = %v, want none", shared)
	}

	args, err := m.Params(order(map[string]string{"store.example.com/status": "cancelled"}, ""))
	if err != nil {
		t.Fatalf("Params() returned error: %v", err)
	}
	if got := args["status"]; got != "cancelled" {
		t.Errorf("status bound as %v, want the label's value \"cancelled\"", got)
	}
	if dropped := m.DroppedOnWrite(order(map[string]string{"store.example.com/status": "cancelled"}, "")); len(dropped) != 0 {
		t.Errorf("DroppedOnWrite() complained where nothing is shared: %v", dropped)
	}
}
