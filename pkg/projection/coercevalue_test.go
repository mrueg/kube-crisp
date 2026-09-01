package projection

import (
	"database/sql/driver"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// TestADeclaredJSONValueBindsLikeAFieldOne is the bug. Both parameter sources
// end up bound to the same statement, so they have to produce something a
// driver accepts — and the Field source already did. The Value source went
// through the row conversion instead, which runs the other way: it exists to
// turn a column into part of an object, so it decoded the literal into a Go
// map that database/sql cannot bind at all.
func TestADeclaredJSONValueBindsLikeAFieldOne(t *testing.T) {
	const literal = `{"status":"shipped"}`

	fromValue, err := CoerceValue(literal, crispv1alpha1.FieldTypeJSON)
	if err != nil {
		t.Fatalf("CoerceValue() returned error: %v", err)
	}
	if !driver.IsValue(fromValue) {
		t.Fatalf("CoerceValue() produced %T, which database/sql cannot bind", fromValue)
	}
	if fromValue != literal {
		t.Errorf("CoerceValue() = %#v, want the literal %q", fromValue, literal)
	}

	obj := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"filter": map[string]any{"status": "shipped"}},
	}}
	fromField, err := FieldValue(obj, "spec.filter", crispv1alpha1.FieldTypeJSON)
	if err != nil {
		t.Fatalf("FieldValue() returned error: %v", err)
	}
	if fromValue != fromField {
		t.Errorf("the two sources bind differently: value %#v, field %#v", fromValue, fromField)
	}
}

// TestADeclaredJSONValueHasToBeJSON keeps the check that makes the message the
// projection author's rather than the database's. A malformed literal sent on
// to the driver comes back in the database's own words, about a value the
// client never supplied and cannot do anything about.
func TestADeclaredJSONValueHasToBeJSON(t *testing.T) {
	for _, literal := range []string{"", "{oops", "{\"a\":}"} {
		if got, err := CoerceValue(literal, crispv1alpha1.FieldTypeJSON); err == nil {
			t.Errorf("CoerceValue(%q) = %#v, want an error", literal, got)
		}
	}
}

// TestADeclaredJSONArrayIsAValueToo, because a literal is not always an object
// and a scalar is legal JSON as well.
func TestADeclaredJSONArrayIsAValueToo(t *testing.T) {
	for _, literal := range []string{`[1,2,3]`, `"text"`, `null`, `12`} {
		got, err := CoerceValue(literal, crispv1alpha1.FieldTypeJSON)
		if err != nil {
			t.Fatalf("CoerceValue(%q) returned error: %v", literal, err)
		}
		if got != literal {
			t.Errorf("CoerceValue(%q) = %#v, want the literal", literal, got)
		}
	}
}
