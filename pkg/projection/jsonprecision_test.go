package projection

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestJSONNumbersAreReadAtTheirDeclaredWidth is the shape resultFormat:
// JSONArray and a type: json column now produce: the aggregate is decoded
// keeping every number's digits, so a value arrives as a json.Number rather
// than a float64 that has already rounded it. Each field type parses the
// digits at the width it promises — the full int64 range for integer, the
// exact text for string — and the float64 cases that used to be the only ones
// still hold for values the request side hands over.
func TestJSONNumbersAreReadAtTheirDeclaredWidth(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  json.Number
		kind crispv1alpha1.FieldType
		want any
	}{
		{"an integer past fifty-three bits", "9007199254740993", crispv1alpha1.FieldTypeInteger, int64(9007199254740993)},
		{"the top of int64", "9223372036854775807", crispv1alpha1.FieldTypeInteger, int64(9223372036854775807)},
		{"the bottom of int64", "-9223372036854775808", crispv1alpha1.FieldTypeInteger, int64(-9223372036854775808)},
		{"an integral fraction, as json.Unmarshal read every integer", "3.0", crispv1alpha1.FieldTypeInteger, int64(3)},
		{"an exponent", "1e3", crispv1alpha1.FieldTypeInteger, int64(1000)},

		{"the digits, exactly", "9007199254740993", crispv1alpha1.FieldTypeString, "9007199254740993"},
		{"digits past int64", "18446744073709551615", crispv1alpha1.FieldTypeString, "18446744073709551615"},
		{"a fraction as written", "0.10", crispv1alpha1.FieldTypeString, "0.10"},

		{"a fraction", "1.5", crispv1alpha1.FieldTypeNumber, 1.5},
		{"an integer as a number", "42", crispv1alpha1.FieldTypeNumber, 42.0},

		{"a flag from an aggregate", "1", crispv1alpha1.FieldTypeBoolean, true},
		{"a cleared flag", "0", crispv1alpha1.FieldTypeBoolean, false},

		{"a bare number in a json field", "9007199254740993", crispv1alpha1.FieldTypeJSON, int64(9007199254740993)},
		{"a bare fraction in a json field", "2.5", crispv1alpha1.FieldTypeJSON, 2.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := coerce(tc.raw, tc.kind)
			if err != nil {
				t.Fatalf("coerce(%s, %s) returned error: %v", tc.raw, tc.kind, err)
			}
			if got != tc.want {
				t.Errorf("coerce(%s, %s) = %#v (%T), want %#v", tc.raw, tc.kind, got, got, tc.want)
			}
		})
	}
}

// TestAJSONIntegerPastInt64SaysWhatToDo pins the refusal. A whole number past
// what an int64 holds cannot become a JSON number in an integer field, exactly
// as a BIGINT UNSIGNED cannot, and the error has to name the same way out —
// which toString then honours by carrying the digits.
func TestAJSONIntegerPastInt64SaysWhatToDo(t *testing.T) {
	for _, raw := range []json.Number{"9223372036854775808", "-9223372036854775809", "18446744073709551615"} {
		_, err := coerce(raw, crispv1alpha1.FieldTypeInteger)
		if err == nil {
			t.Fatalf("%s was accepted as an integer field", raw)
		}
		if !strings.Contains(err.Error(), "type: string") {
			t.Errorf("the error for %s does not say how to carry the value: %v", raw, err)
		}
	}

	// A fraction is not an integer at all, and the error must not send the
	// author to type: string for it.
	if _, err := coerce(json.Number("2.5"), crispv1alpha1.FieldTypeInteger); err == nil {
		t.Error("a fraction was accepted as an integer field")
	}
}

// TestAJSONAggregateRowKeepsALargeInteger takes a row the way scanJSONArray
// hands it over — decoded keeping every digit — and maps it, which is where
// the loss was met: an object named after a row that is not in the table, and
// a spec whose integer had been rounded before any field type saw it.
//
// The same column is read as the name, as an integer, and as a string, and an
// untyped json column carries the value nested inside it. Every reading has
// to agree on the digits, and none may leave a json.Number in the object: the
// unstructured converter takes only int64 and float64 for numbers, and
// DeepCopyJSONValue — which the apiserver runs on everything it serves —
// panics on anything else.
func TestAJSONAggregateRowKeepsALargeInteger(t *testing.T) {
	const aggregate = `[{"id":9007199254740993,"qty":9007199254740993,"ref":9007199254740993,` +
		`"attrs":{"parent":9007199254740993,"weights":[9223372036854775807,0.25],"label":"x"},"paid":1}]`
	var decoded []map[string]any
	if err := crispsql.DecodeJSON([]byte(aggregate), &decoded); err != nil {
		t.Fatalf("decoding the aggregate: %v", err)
	}

	mapper, err := NewMapper(
		crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
			Scope: crispv1alpha1.ClusterScoped,
		},
		crispv1alpha1.Mapping{
			Name: "id",
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "qty", Path: "spec.qty", Type: crispv1alpha1.FieldTypeInteger},
				{Column: "ref", Path: "spec.ref", Type: crispv1alpha1.FieldTypeString},
				{Column: "attrs", Path: "spec.attrs", Type: crispv1alpha1.FieldTypeJSON},
				{Column: "paid", Path: "spec.paid", Type: crispv1alpha1.FieldTypeBoolean},
			},
		},
	)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj, err := mapper.Row(crispsql.Row(decoded[0]))
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if got, want := obj.GetName(), "9007199254740993"; got != want {
		t.Errorf("name = %q, want %q: the identity was rounded on the way in", got, want)
	}
	assertNested(t, obj, int64(9007199254740993), "spec", "qty")
	assertNested(t, obj, "9007199254740993", "spec", "ref")
	assertNested(t, obj, int64(9007199254740993), "spec", "attrs", "parent")
	assertNested(t, obj, "x", "spec", "attrs", "label")
	assertNested(t, obj, true, "spec", "paid")
	weights, _, _ := unstructured.NestedSlice(obj.Object, "spec", "attrs", "weights")
	if len(weights) != 2 || weights[0] != int64(9223372036854775807) || weights[1] != 0.25 {
		t.Errorf("spec.attrs.weights = %#v, want the int64 and the fraction", weights)
	}

	// The apiserver deep-copies every object it serves, and this is what a
	// json.Number left anywhere in the object would do to it.
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("DeepCopyJSONValue panicked: %v", recovered)
		}
	}()
	runtime.DeepCopyJSONValue(obj.Object)

	// And the write direction binds back exactly what was read, so an update
	// to the object reaches the row that produced it.
	args, err := mapper.Params(obj)
	if err != nil {
		t.Fatalf("Params() returned error: %v", err)
	}
	if got := args["id"]; got != "9007199254740993" {
		t.Errorf("id binds %#v, want the exact digits", got)
	}
	if got := args["qty"]; got != int64(9007199254740993) {
		t.Errorf("qty binds %#v, want int64(9007199254740993)", got)
	}
	if got := args["ref"]; got != "9007199254740993" {
		t.Errorf("ref binds %#v, want the exact digits", got)
	}
	attrs, _ := args["attrs"].(string)
	for _, want := range []string{`"parent":9007199254740993`, `9223372036854775807`, `0.25`} {
		if !strings.Contains(attrs, want) {
			t.Errorf("attrs binds %s, which does not carry %s", attrs, want)
		}
	}
}

// TestAJSONColumnKeepsALargeInteger is the same loss on a column scanned
// directly: a jsonb or TEXT column holding a document is parsed by the json
// field type, and it too went through json.Unmarshal.
func TestAJSONColumnKeepsALargeInteger(t *testing.T) {
	for _, raw := range []any{
		`{"parent":9007199254740993,"weights":[9223372036854775807,0.25]}`,
		[]byte(`{"parent":9007199254740993,"weights":[9223372036854775807,0.25]}`),
	} {
		got, err := coerce(raw, crispv1alpha1.FieldTypeJSON)
		if err != nil {
			t.Fatalf("coerce(%T, json) returned error: %v", raw, err)
		}
		doc, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("coerce(%T, json) = %T, want a map", raw, got)
		}
		if doc["parent"] != int64(9007199254740993) {
			t.Errorf("parent = %#v, want int64(9007199254740993)", doc["parent"])
		}
		weights, _ := doc["weights"].([]any)
		if len(weights) != 2 || weights[0] != int64(9223372036854775807) || weights[1] != 0.25 {
			t.Errorf("weights = %#v, want the int64 and the fraction", weights)
		}
		runtime.DeepCopyJSONValue(got)
	}

	// Past int64 there is no exact JSON number, and the value is the float64
	// it always was rather than a json.Number the object cannot hold.
	got, err := coerce(`{"n":18446744073709551615}`, crispv1alpha1.FieldTypeJSON)
	if err != nil {
		t.Fatalf("coerce(json) returned error: %v", err)
	}
	if n, ok := got.(map[string]any)["n"].(float64); !ok || n != 18446744073709551615.0 {
		t.Errorf("n = %#v, want a float64 past int64", got.(map[string]any)["n"])
	}

	// A magnitude no float64 holds was refused by json.Unmarshal, and still is.
	if _, err := coerce(`{"n":1e999}`, crispv1alpha1.FieldTypeJSON); err == nil {
		t.Error("coerce(json) accepted a number no float64 holds")
	}
}

func assertNested(t *testing.T, obj *unstructured.Unstructured, want any, path ...string) {
	t.Helper()
	got, found, err := unstructured.NestedFieldNoCopy(obj.Object, path...)
	if err != nil || !found {
		t.Errorf("%s missing: %v", strings.Join(path, "."), err)
		return
	}
	if got != want {
		t.Errorf("%s = %#v (%T), want %#v", strings.Join(path, "."), got, got, want)
	}
}
