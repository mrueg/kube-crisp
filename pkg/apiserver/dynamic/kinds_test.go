package dynamic

import (
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
)

// TestTwoResourcesOfOneKindKeepBothSelectors covers the case only the plural
// has to be unique for.
//
// The scheme keeps one field-label conversion per GroupVersionKind, so
// registering each resource in turn left the last one's selectable fields as
// the kind's — and the other resource's declared selectors started being
// rejected as unknown before the request ever reached storage. Nothing said so.
func TestTwoResourcesOfOneKindKeepBothSelectors(t *testing.T) {
	orders := testResource()
	orders.SelectableFields = []crispv1alpha1.SelectableField{
		{JSONPath: ".spec.customer", Column: "customer"},
	}

	// Same kind, different plural — a second view of the same rows.
	archived := testResource()
	archived.Plural = "archivedorders"
	archived.SelectableFields = []crispv1alpha1.SelectableField{
		{JSONPath: ".spec.archivedBy", Column: "archived_by"},
	}

	apiScheme, _ := crispscheme.New()
	registerKinds(apiScheme, []Resource{orders, archived})

	for _, field := range []string{
		"metadata.name",
		"metadata.namespace",
		"spec.customer",
		"spec.archivedBy",
	} {
		label, value, err := apiScheme.ConvertFieldLabel(testGVK, field, "x")
		if err != nil {
			t.Errorf("field selector %q was rejected: %v", field, err)
			continue
		}
		if label != field || value != "x" {
			t.Errorf("field selector %q converted to %q=%q", field, label, value)
		}
	}

	// A field neither of them declares is still refused, and the error names
	// both resources rather than whichever registered last.
	_, _, err := apiScheme.ConvertFieldLabel(testGVK, "spec.nonesuch", "x")
	if err == nil {
		t.Fatal("an undeclared field selector was accepted")
	}
	for _, plural := range []string{"orders", "archivedorders"} {
		if !strings.Contains(err.Error(), plural) {
			t.Errorf("error %q does not name %s", err, plural)
		}
	}
}

// TestRegisterKindsHonoursAnExplicitListKind keeps the list type registered
// under the name the projection gave it, since that is what responses are
// stamped with.
func TestRegisterKindsHonoursAnExplicitListKind(t *testing.T) {
	res := testResource()
	res.ListKind = "OrderCollection"

	apiScheme, _ := crispscheme.New()
	registerKinds(apiScheme, []Resource{res})

	if !apiScheme.Recognizes(testGVK.GroupVersion().WithKind("OrderCollection")) {
		t.Error("the declared list kind was not registered")
	}
	if !apiScheme.Recognizes(testGVK) {
		t.Error("the projected kind was not registered")
	}
}
