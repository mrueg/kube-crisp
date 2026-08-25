package projection

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// TestParseTimestampAcceptsWhatDriversProduce. Each driver renders a date its
// own way, and a column that cannot be read is a row that cannot be served.
func TestParseTimestampAcceptsWhatDriversProduce(t *testing.T) {
	for _, in := range []string{
		"2026-08-22T18:21:25Z",
		"2026-08-22T18:21:25.123456789Z",
		"2026-08-22 18:21:25",
		"2026-08-22 18:21:25.123456",
		"2026-08-22",
		"2026-08-22 18:21:25.123456+02",
		"2026-08-22 18:21:25.123456 +0000 UTC",
		"2026-08-22T18:21:25.123456",
	} {
		if _, err := parseTimestamp(in); err != nil {
			t.Errorf("parseTimestamp(%q) returned error: %v", in, err)
		}
	}

	if _, err := parseTimestamp("last tuesday"); err == nil {
		t.Error("parseTimestamp() accepted something that is not a timestamp")
	}
}

// TestTimestampsKeepSubSecondPrecision: rounding a microsecond column to the
// second loses data the row actually holds, and does it again on the way back.
func TestTimestampsKeepSubSecondPrecision(t *testing.T) {
	got, err := coerce("2026-08-22T18:21:25.123456Z", crispv1alpha1.FieldTypeTimestamp)
	if err != nil {
		t.Fatalf("coerce() returned error: %v", err)
	}
	if !strings.Contains(got.(string), ".123456") {
		t.Errorf("coerce() = %v, which dropped the sub-second part", got)
	}
}

// TestCoerceConvertsWhatDriversHandBack. Drivers are inconsistent about the Go
// type a column arrives as, and the mapping's declared type is what decides
// what the object should carry.
func TestCoerceConvertsWhatDriversHandBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		as   crispv1alpha1.FieldType
		want any
	}{
		{"bytes to string", []byte("ada"), crispv1alpha1.FieldTypeString, "ada"},
		{"int to string", int64(7), crispv1alpha1.FieldTypeString, "7"},
		{"float to string", 1.5, crispv1alpha1.FieldTypeString, "1.5"},
		{"bool to string", true, crispv1alpha1.FieldTypeString, "true"},
		{"int32 to integer", int32(7), crispv1alpha1.FieldTypeInteger, int64(7)},
		{"int to integer", 7, crispv1alpha1.FieldTypeInteger, int64(7)},
		{"integral float to integer", float64(7), crispv1alpha1.FieldTypeInteger, int64(7)},
		{"text to integer", " 7 ", crispv1alpha1.FieldTypeInteger, int64(7)},
		{"int to number", int64(7), crispv1alpha1.FieldTypeNumber, float64(7)},
		{"float32 to number", float32(1.5), crispv1alpha1.FieldTypeNumber, float64(1.5)},
		{"text to number", "1.5", crispv1alpha1.FieldTypeNumber, float64(1.5)},
		{"int to boolean", int64(1), crispv1alpha1.FieldTypeBoolean, true},
		{"zero to boolean", int64(0), crispv1alpha1.FieldTypeBoolean, false},
		{"text to boolean", "true", crispv1alpha1.FieldTypeBoolean, true},
		{"empty type defaults to string", int64(7), "", "7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := coerce(tc.in, tc.as)
			if err != nil {
				t.Fatalf("coerce() returned error: %v", err)
			}
			if got != tc.want {
				t.Errorf("coerce(%#v, %q) = %#v, want %#v", tc.in, tc.as, got, tc.want)
			}
		})
	}
}

// TestCoerceRefusesWhatItCannotConvert rather than storing something arbitrary.
func TestCoerceRefusesWhatItCannotConvert(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   any
		as   crispv1alpha1.FieldType
	}{
		{"fractional float as integer", 1.5, crispv1alpha1.FieldTypeInteger},
		{"words as integer", "seven", crispv1alpha1.FieldTypeInteger},
		{"words as number", "seven", crispv1alpha1.FieldTypeNumber},
		{"words as boolean", "perhaps", crispv1alpha1.FieldTypeBoolean},
		{"struct as integer", struct{}{}, crispv1alpha1.FieldTypeInteger},
		{"struct as number", struct{}{}, crispv1alpha1.FieldTypeNumber},
		{"struct as boolean", struct{}{}, crispv1alpha1.FieldTypeBoolean},
		{"struct as timestamp", struct{}{}, crispv1alpha1.FieldTypeTimestamp},
		{"struct as string", struct{}{}, crispv1alpha1.FieldTypeString},
		{"broken json", "{not json", crispv1alpha1.FieldTypeJSON},
		{"unknown type", "x", crispv1alpha1.FieldType("bogus")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := coerce(tc.in, tc.as); err == nil {
				t.Errorf("coerce(%#v, %q) was accepted", tc.in, tc.as)
			}
		})
	}
}

// TestCoerceJSONNormalisesNumbers: JSON decoding yields float64 for every
// number, and an unstructured object requires int64 for the integral ones.
func TestCoerceJSONNormalisesNumbers(t *testing.T) {
	got, err := coerce(`{"qty":3,"price":1.5,"tags":["a"],"nested":{"n":4}}`, crispv1alpha1.FieldTypeJSON)
	if err != nil {
		t.Fatalf("coerce() returned error: %v", err)
	}

	decoded := got.(map[string]any)
	if decoded["qty"] != int64(3) {
		t.Errorf("qty = %#v, want int64(3)", decoded["qty"])
	}
	if decoded["price"] != 1.5 {
		t.Errorf("price = %#v, want 1.5", decoded["price"])
	}
	if nested := decoded["nested"].(map[string]any); nested["n"] != int64(4) {
		t.Errorf("nested.n = %#v, want int64(4)", nested["n"])
	}

	// A value the json_agg path already decoded takes the same route.
	preDecoded, err := coerce(map[string]any{"qty": float64(3)}, crispv1alpha1.FieldTypeJSON)
	if err != nil {
		t.Fatalf("coerce() returned error: %v", err)
	}
	if preDecoded.(map[string]any)["qty"] != int64(3) {
		t.Errorf("a pre-decoded value was not normalised: %#v", preDecoded)
	}
}

// TestDecodeStringsReadsFinalizers, which live in a column as a JSON array.
func TestDecodeStringsReadsFinalizers(t *testing.T) {
	got, err := decodeStrings(crispsql.Row{"f": `["a","b"]`}, "f", "finalizers")
	if err != nil {
		t.Fatalf("decodeStrings() returned error: %v", err)
	}
	if len(got) != 2 || got[0] != "a" {
		t.Errorf("decodeStrings() = %v", got)
	}

	// NULL and empty are no list at all, since most rows have none.
	for _, raw := range []any{nil, "", "   "} {
		if got, err := decodeStrings(crispsql.Row{"f": raw}, "f", "finalizers"); err != nil || got != nil {
			t.Errorf("decodeStrings(%#v) = %v, %v; want nil, nil", raw, got, err)
		}
	}

	// The json_agg path hands back a decoded slice.
	if got, err := decodeStrings(crispsql.Row{"f": []any{"a"}}, "f", "finalizers"); err != nil || len(got) != 1 {
		t.Errorf("decodeStrings() on a decoded value = %v, %v", got, err)
	}

	for _, tc := range []struct {
		name string
		row  crispsql.Row
	}{
		{"column missing", crispsql.Row{}},
		{"not an array", crispsql.Row{"f": `{"a":1}`}},
		{"wrong element type", crispsql.Row{"f": []any{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeStrings(tc.row, "f", "finalizers"); err == nil {
				t.Error("decodeStrings() accepted it")
			}
		})
	}
}

// TestDecodeOwnerReferencesRefusesWhatTheCollectorCannotResolve. A reference the
// garbage collector cannot act on is how objects get deleted by surprise, so it
// is refused on read rather than served.
func TestDecodeOwnerReferencesRefusesWhatTheCollectorCannotResolve(t *testing.T) {
	valid := `[{"apiVersion":"v1","kind":"ConfigMap","name":"c","uid":"u"}]`
	got, err := decodeOwnerReferences(crispsql.Row{"o": valid}, "o")
	if err != nil {
		t.Fatalf("decodeOwnerReferences() returned error: %v", err)
	}
	if len(got) != 1 || got[0].Name != "c" {
		t.Errorf("decodeOwnerReferences() = %v", got)
	}

	// Bytes, and a value a jsonb column already decoded, take the same route.
	if _, err := decodeOwnerReferences(crispsql.Row{"o": []byte(valid)}, "o"); err != nil {
		t.Errorf("decodeOwnerReferences() on bytes: %v", err)
	}
	decoded := []any{map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "c", "uid": "u"}}
	if _, err := decodeOwnerReferences(crispsql.Row{"o": decoded}, "o"); err != nil {
		t.Errorf("decodeOwnerReferences() on a decoded value: %v", err)
	}

	for _, tc := range []struct {
		name string
		raw  any
	}{
		{"no apiVersion", `[{"kind":"ConfigMap","name":"c","uid":"u"}]`},
		{"no kind", `[{"apiVersion":"v1","name":"c","uid":"u"}]`},
		{"no name", `[{"apiVersion":"v1","kind":"ConfigMap","uid":"u"}]`},
		{"no uid", `[{"apiVersion":"v1","kind":"ConfigMap","name":"c"}]`},
		{"not json", `nonsense`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeOwnerReferences(crispsql.Row{"o": tc.raw}, "o"); err == nil {
				t.Error("decodeOwnerReferences() accepted it")
			}
		})
	}

	// NULL, empty and the literal null are simply no owners.
	for _, raw := range []any{nil, "", "null"} {
		if got, err := decodeOwnerReferences(crispsql.Row{"o": raw}, "o"); err != nil || got != nil {
			t.Errorf("decodeOwnerReferences(%#v) = %v, %v", raw, got, err)
		}
	}
	if _, err := decodeOwnerReferences(crispsql.Row{}, "o"); err == nil {
		t.Error("decodeOwnerReferences() accepted a column the query did not return")
	}
}

// TestDecodeManagedFieldsRoundTrip: the apiserver writes these and reads them
// back, and losing them silently is what makes an apply overwrite something it
// should have conflicted with.
func TestDecodeManagedFieldsRoundTrip(t *testing.T) {
	raw := `[{"manager":"ctl","operation":"Apply","apiVersion":"v1","fieldsType":"FieldsV1"}]`

	got, err := decodeManagedFields(crispsql.Row{"m": raw}, "m")
	if err != nil {
		t.Fatalf("decodeManagedFields() returned error: %v", err)
	}
	if len(got) != 1 || got[0].Manager != "ctl" {
		t.Errorf("decodeManagedFields() = %v", got)
	}

	for _, raw := range []any{nil, "", "null"} {
		if got, err := decodeManagedFields(crispsql.Row{"m": raw}, "m"); err != nil || got != nil {
			t.Errorf("decodeManagedFields(%#v) = %v, %v", raw, got, err)
		}
	}
	if _, err := decodeManagedFields(crispsql.Row{"m": "not json"}, "m"); err == nil {
		t.Error("decodeManagedFields() accepted malformed JSON")
	}
	if _, err := decodeManagedFields(crispsql.Row{}, "m"); err == nil {
		t.Error("decodeManagedFields() accepted a column the query did not return")
	}
}

// TestSplitNameReversesACompositeIdentity. The name is the only handle the API
// has on a row, so it has to split back into the columns it was built from.
func TestSplitNameReversesACompositeIdentity(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Shipment",
		Plural: "shipments", Scope: crispv1alpha1.NamespaceScoped,
	}
	mapper, err := NewMapper(res, crispv1alpha1.Mapping{
		NameColumns: []string{"region", "order_no"},
		Namespace:   "tenant",
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	got, err := mapper.SplitName("eu-1042")
	if err != nil {
		t.Fatalf("SplitName() returned error: %v", err)
	}
	if got["region"] != "eu" || got["order_no"] != "1042" {
		t.Errorf("SplitName() = %v", got)
	}

	for _, name := range []string{"eu", "eu-1042-extra", "-1042", "eu-"} {
		if _, err := mapper.SplitName(name); err == nil {
			t.Errorf("SplitName(%q) was accepted; it names no row", name)
		}
	}

	// NameFrom and NamespaceFrom read the identity without mapping the object,
	// which is all a tombstone row can offer.
	name, err := mapper.NameFrom(crispsql.Row{"region": "eu", "order_no": "1042"})
	if err != nil {
		t.Fatalf("NameFrom() returned error: %v", err)
	}
	if name != "eu-1042" {
		t.Errorf("NameFrom() = %q, want eu-1042", name)
	}
	namespace, err := mapper.NamespaceFrom(crispsql.Row{"tenant": "acme"})
	if err != nil {
		t.Fatalf("NamespaceFrom() returned error: %v", err)
	}
	if namespace != "acme" {
		t.Errorf("NamespaceFrom() = %q, want acme", namespace)
	}

	if _, err := mapper.NameFrom(crispsql.Row{"region": "eu!", "order_no": "1042"}); err == nil {
		t.Error("NameFrom() produced a name that is not a valid object name")
	}
}

// TestNamespaceFromIsEmptyForClusterScoped, which has no namespace column.
func TestNamespaceFromIsEmptyForClusterScoped(t *testing.T) {
	mapper, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Region",
		Plural: "regions", Scope: crispv1alpha1.ClusterScoped,
	}, crispv1alpha1.Mapping{Name: "id"})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	got, err := mapper.NamespaceFrom(crispsql.Row{})
	if err != nil || got != "" {
		t.Errorf("NamespaceFrom() = %q, %v; want empty and no error", got, err)
	}
}

// TestNewMapperRejectsMappingsThatCannotWork, at load time rather than on the
// first request.
func TestNewMapperRejectsMappingsThatCannotWork(t *testing.T) {
	namespaced := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}
	clusterScoped := namespaced
	clusterScoped.Scope = crispv1alpha1.ClusterScoped

	for _, tc := range []struct {
		name    string
		res     crispv1alpha1.ProjectedResource
		mapping crispv1alpha1.Mapping
	}{
		{"no name at all", namespaced, crispv1alpha1.Mapping{Namespace: "tenant"}},
		{"both name forms", namespaced, crispv1alpha1.Mapping{
			Name: "id", NameColumns: []string{"a", "b"}, Namespace: "tenant"}},
		{"an empty name column", namespaced, crispv1alpha1.Mapping{
			NameColumns: []string{"a", ""}, Namespace: "tenant"}},
		{"a separator that cannot appear in a name", namespaced, crispv1alpha1.Mapping{
			NameColumns: []string{"a", "b"}, NameSeparator: "/", Namespace: "tenant"}},
		{"namespaced with no namespace", namespaced, crispv1alpha1.Mapping{Name: "id"}},
		{"cluster-scoped with one", clusterScoped, crispv1alpha1.Mapping{Name: "id", Namespace: "tenant"}},
		{"a field with no column", namespaced, crispv1alpha1.Mapping{
			Name: "id", Namespace: "tenant",
			Fields: []crispv1alpha1.FieldMapping{{Path: "spec.x"}}}},
		{"a field targeting metadata", namespaced, crispv1alpha1.Mapping{
			Name: "id", Namespace: "tenant",
			Fields: []crispv1alpha1.FieldMapping{{Column: "c", Path: "metadata.name"}}}},
		{"a field targeting kind", namespaced, crispv1alpha1.Mapping{
			Name: "id", Namespace: "tenant",
			Fields: []crispv1alpha1.FieldMapping{{Column: "c", Path: "kind"}}}},
		{"a field with an empty path", namespaced, crispv1alpha1.Mapping{
			Name: "id", Namespace: "tenant",
			Fields: []crispv1alpha1.FieldMapping{{Column: "c", Path: ""}}}},
		{"a field with an empty segment", namespaced, crispv1alpha1.Mapping{
			Name: "id", Namespace: "tenant",
			Fields: []crispv1alpha1.FieldMapping{{Column: "c", Path: "spec..x"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewMapper(tc.res, tc.mapping); err == nil {
				t.Error("NewMapper() accepted a mapping that cannot work")
			}
		})
	}
}

// TestFieldValueAndBindValue cover the write direction: reading a value out of
// a submitted object and rendering it as something a driver can bind.
func TestFieldValueAndBindValue(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	if err := unstructured.SetNestedMap(obj.Object,
		map[string]any{"cents": int64(4999)}, "spec", "amount"); err != nil {
		t.Fatalf("building the object: %v", err)
	}

	got, err := FieldValue(obj, "spec.amount.cents", crispv1alpha1.FieldTypeInteger)
	if err != nil {
		t.Fatalf("FieldValue() returned error: %v", err)
	}
	if got != int64(4999) {
		t.Errorf("FieldValue() = %#v, want int64(4999)", got)
	}

	// A path the object does not carry binds NULL rather than a zero value.
	if got, err := FieldValue(obj, "spec.missing", crispv1alpha1.FieldTypeString); err != nil || got != nil {
		t.Errorf("FieldValue() on a missing path = %#v, %v; want nil, nil", got, err)
	}

	// A structured value is re-encoded as JSON so it can reach a json column.
	structured, err := FieldValue(obj, "spec.amount", crispv1alpha1.FieldTypeJSON)
	if err != nil {
		t.Fatalf("FieldValue() returned error: %v", err)
	}
	if !strings.Contains(structured.(string), "4999") {
		t.Errorf("FieldValue() = %v, want encoded JSON", structured)
	}

	// The same happens for a map with no declared type, since a driver cannot
	// bind a Go map.
	untyped, err := bindValue(map[string]any{"a": 1}, "")
	if err != nil {
		t.Fatalf("bindValue() returned error: %v", err)
	}
	if !strings.Contains(untyped.(string), `"a"`) {
		t.Errorf("bindValue() = %v, want encoded JSON", untyped)
	}

	if _, err := bindValue("not a number", crispv1alpha1.FieldTypeInteger); err == nil {
		t.Error("bindValue() accepted a value the column cannot take")
	}

	// A path that runs through a scalar is a mapping mistake, not a miss.
	if _, err := FieldValue(obj, "spec.amount.cents.deeper", crispv1alpha1.FieldTypeString); err == nil {
		t.Error("FieldValue() walked through a scalar")
	}
}

// TestCoerceValueRendersDeclaredLiterals, which is what a query parameter with
// a constant value binds.
func TestCoerceValueRendersDeclaredLiterals(t *testing.T) {
	if got, err := CoerceValue("acme", ""); err != nil || got != "acme" {
		t.Errorf("CoerceValue() = %v, %v", got, err)
	}
	if got, err := CoerceValue("7", crispv1alpha1.FieldTypeInteger); err != nil || got != int64(7) {
		t.Errorf("CoerceValue() = %#v, %v", got, err)
	}
	if _, err := CoerceValue("seven", crispv1alpha1.FieldTypeInteger); err == nil {
		t.Error("CoerceValue() accepted a literal the type cannot take")
	}
}

// TestOwnerReferenceColumnIsReported, since the write path has to know whether
// there is one before it can encode into it.
func TestOwnerReferenceColumnIsReported(t *testing.T) {
	mapper, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.ClusterScoped,
	}, crispv1alpha1.Mapping{Name: "id", OwnerReferences: "owners"})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}
	if got := mapper.OwnerReferenceColumn(); got != "owners" {
		t.Errorf("OwnerReferenceColumn() = %q, want owners", got)
	}
}

// TestRowRejectsRowsItCannotIdentify. A row that produces no usable name, or a
// NULL where the mapping needs a value, cannot become an object.
func TestRowRejectsRowsItCannotIdentify(t *testing.T) {
	mapper, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}, crispv1alpha1.Mapping{
		Name:      "id",
		Namespace: "tenant",
		Fields:    []crispv1alpha1.FieldMapping{{Column: "qty", Path: "spec.qty", Type: crispv1alpha1.FieldTypeInteger}},
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	for _, tc := range []struct {
		name string
		row  crispsql.Row
	}{
		{"no id column", crispsql.Row{"tenant": "acme", "qty": int64(1)}},
		{"null id", crispsql.Row{"id": nil, "tenant": "acme", "qty": int64(1)}},
		{"invalid name", crispsql.Row{"id": "Not A Name", "tenant": "acme", "qty": int64(1)}},
		{"no namespace column", crispsql.Row{"id": "a", "qty": int64(1)}},
		{"missing mapped column", crispsql.Row{"id": "a", "tenant": "acme"}},
		{"uncoercible value", crispsql.Row{"id": "a", "tenant": "acme", "qty": "many"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := mapper.Row(tc.row); err == nil {
				t.Error("Row() accepted a row it cannot identify")
			}
		})
	}

	// omitEmpty is how a NULL becomes an absent field rather than an error.
	lenient, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}, crispv1alpha1.Mapping{
		Name: "id", Namespace: "tenant",
		Fields: []crispv1alpha1.FieldMapping{{Column: "qty", Path: "spec.qty", OmitEmpty: true}},
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}
	obj, err := lenient.Row(crispsql.Row{"id": "a", "tenant": "acme", "qty": nil})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if _, found, _ := unstructured.NestedString(obj.Object, "spec", "qty"); found {
		t.Error("an omitEmpty field was set from a NULL column")
	}
}

// TestDerivedUIDIsStableAndScoped. Controllers use the UID for owner references
// and to tell "same name, different object" apart, so every replica and every
// restart has to derive the same one.
func TestDerivedUIDIsStableAndScoped(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}
	mapper, err := NewMapper(res, crispv1alpha1.Mapping{Name: "id", Namespace: "tenant"})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	row := crispsql.Row{"id": "order-1", "tenant": "acme"}
	first, err := mapper.Row(row)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	second, err := mapper.Row(row)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if first.GetUID() == "" {
		t.Fatal("no UID was derived; owner references would be malformed")
	}
	if first.GetUID() != second.GetUID() {
		t.Error("the derived UID is not stable between reads")
	}

	// A different namespace is a different object.
	other, err := mapper.Row(crispsql.Row{"id": "order-1", "tenant": "globex"})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if other.GetUID() == first.GetUID() {
		t.Error("two rows in different namespaces derived the same UID")
	}

	// The version is deliberately absent from it: one row served at two
	// versions is one object, so an owner reference keeps resolving.
	v2 := res
	v2.Version = "v1beta1"
	versioned, err := NewMapper(v2, crispv1alpha1.Mapping{Name: "id", Namespace: "tenant"})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}
	sameRow, err := versioned.Row(row)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if sameRow.GetUID() != first.GetUID() {
		t.Error("the same row derived different UIDs at two versions")
	}
}

// TestMappedMetadataReachesTheObject covers the lifecycle columns: generation,
// deletionTimestamp, creationTimestamp, uid and resourceVersion.
func TestMappedMetadataReachesTheObject(t *testing.T) {
	mapper, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}, crispv1alpha1.Mapping{
		Name:              "id",
		Namespace:         "tenant",
		UID:               "uid",
		ResourceVersion:   "rv",
		Generation:        "gen",
		CreationTimestamp: "created",
		DeletionTimestamp: "deleted",
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj, err := mapper.Row(crispsql.Row{
		"id": "a", "tenant": "acme", "uid": "u-1", "rv": "7", "gen": int64(3),
		"created": "2026-08-22T18:21:25Z", "deleted": "2026-08-23T18:21:25Z",
	})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}

	if got := string(obj.GetUID()); got != "u-1" {
		t.Errorf("uid = %q, want u-1; a mapped column beats a derived one", got)
	}
	if got := obj.GetResourceVersion(); got != "7" {
		t.Errorf("resourceVersion = %q, want 7", got)
	}
	if got := obj.GetGeneration(); got != 3 {
		t.Errorf("generation = %d, want 3", got)
	}
	if created := obj.GetCreationTimestamp(); created.IsZero() {
		t.Error("creationTimestamp was not set")
	}
	if obj.GetDeletionTimestamp() == nil {
		t.Error("deletionTimestamp was not set; the object would not read as terminating")
	}

	// NULL is the ordinary case for a deletion timestamp, and must not be
	// stamped: an object carrying one is terminating.
	live, err := mapper.Row(crispsql.Row{
		"id": "a", "tenant": "acme", "uid": "u-1", "rv": "7", "gen": int64(3),
		"created": "2026-08-22T18:21:25Z", "deleted": nil,
	})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if live.GetDeletionTimestamp() != nil {
		t.Error("a NULL deletion column made the object read as terminating")
	}

	// A generation that is not a number is a mapping mistake.
	if _, err := mapper.Row(crispsql.Row{
		"id": "a", "tenant": "acme", "uid": "u", "rv": "7", "gen": "three",
		"created": "2026-08-22T18:21:25Z", "deleted": nil,
	}); err == nil {
		t.Error("Row() accepted a non-numeric generation")
	}
}

// TestParamsCarriesMetadataBack is the write direction of the same mapping.
func TestParamsCarriesMetadataBack(t *testing.T) {
	mapper, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}, crispv1alpha1.Mapping{
		Name: "id", Namespace: "tenant", UID: "uid", ResourceVersion: "rv",
		Finalizers: "fin", OwnerReferences: "owners", ManagedFields: "managed",
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetName("order-1")
	obj.SetNamespace("acme")
	obj.SetUID("u-1")
	obj.SetResourceVersion("7")
	obj.SetFinalizers([]string{"crisp.io/cleanup"})
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "v1", Kind: "ConfigMap", Name: "c", UID: "cu",
	}})

	args, err := mapper.Params(obj)
	if err != nil {
		t.Fatalf("Params() returned error: %v", err)
	}

	for name, want := range map[string]any{
		"id": "order-1", "tenant": "acme", "uid": "u-1", "rv": "7",
		"name": "order-1", "namespace": "acme",
	} {
		if args[name] != want {
			t.Errorf("%s = %#v, want %#v", name, args[name], want)
		}
	}
	if !strings.Contains(args["fin"].(string), "crisp.io/cleanup") {
		t.Errorf("finalizers = %v", args["fin"])
	}
	if !strings.Contains(args["owners"].(string), "ConfigMap") {
		t.Errorf("owners = %v", args["owners"])
	}
	// No managed fields on the object means NULL rather than "[]".
	if args["managed"] != nil {
		t.Errorf("managedFields = %#v with none set, want nil", args["managed"])
	}

	// A name that does not split into the identity columns is refused.
	composite, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Shipment",
		Plural: "shipments", Scope: crispv1alpha1.ClusterScoped,
	}, crispv1alpha1.Mapping{NameColumns: []string{"a", "b"}})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}
	bad := &unstructured.Unstructured{Object: map[string]any{}}
	bad.SetName("only-one-more-part-too-many")
	if _, err := composite.Params(bad); err == nil {
		t.Error("Params() accepted a name that does not split into the identity columns")
	}
}

// TestBuildNameRefusesAnAmbiguousComposite: two rows must never produce one
// name, so a value carrying the separator is refused rather than escaped.
func TestBuildNameRefusesAnAmbiguousComposite(t *testing.T) {
	mapper, err := NewMapper(crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Shipment",
		Plural: "shipments", Scope: crispv1alpha1.ClusterScoped,
	}, crispv1alpha1.Mapping{NameColumns: []string{"region", "order_no"}})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	if _, err := mapper.Row(crispsql.Row{"region": "eu-west", "order_no": "1042"}); err == nil {
		t.Error("a value carrying the separator produced a name anyway")
	}
	if _, err := mapper.Row(crispsql.Row{"region": nil, "order_no": "1042"}); err == nil {
		t.Error("a NULL identity column produced a name")
	}
}

// TestTimeValuesRenderConsistently: a driver that hands back a time.Time and one
// that hands back text have to produce the same string, or a resourceVersion
// read one way will not match one written the other.
func TestTimeValuesRenderConsistently(t *testing.T) {
	moment := time.Date(2026, 8, 22, 18, 21, 25, 123456000, time.UTC)

	fromTime, err := toString(moment)
	if err != nil {
		t.Fatalf("toString() returned error: %v", err)
	}
	if !strings.Contains(fromTime, ".123456") {
		t.Errorf("toString(time.Time) = %q, which dropped the sub-second part", fromTime)
	}

	fromText, err := coerce(fromTime, crispv1alpha1.FieldTypeTimestamp)
	if err != nil {
		t.Fatalf("coerce() returned error: %v", err)
	}
	if fromText != fromTime {
		t.Errorf("a timestamp rendered %q from a time and %q from text", fromTime, fromText)
	}
}

// TestDecodersAcceptBytes is a regression test.
//
// pgx returns a jsonb column as []byte, and the decoders that read a column
// directly — rather than through coerce, which normalises first — went to
// toString, which did not know about bytes. Mapping labels or finalizers out of
// a jsonb column failed with "cannot convert []uint8 to string", and only
// against a real driver: a test feeding them a string never saw it.
func TestDecodersAcceptBytes(t *testing.T) {
	if got, err := toString([]byte("ada")); err != nil || got != "ada" {
		t.Errorf("toString([]byte) = %q, %v; want ada", got, err)
	}

	labels, err := decodeStringMap(crispsql.Row{"l": []byte(`{"team":"payments"}`)}, "l", "labelsFrom")
	if err != nil {
		t.Fatalf("decodeStringMap() on bytes returned error: %v", err)
	}
	if labels["team"] != "payments" {
		t.Errorf("decodeStringMap() = %v", labels)
	}

	finalizers, err := decodeStrings(crispsql.Row{"f": []byte(`["crisp.io/cleanup"]`)}, "f", "finalizers")
	if err != nil {
		t.Fatalf("decodeStrings() on bytes returned error: %v", err)
	}
	if len(finalizers) != 1 || finalizers[0] != "crisp.io/cleanup" {
		t.Errorf("decodeStrings() = %v", finalizers)
	}

	// And the identity columns, for a driver that hands back bytes for text.
	if got, err := optionalString(crispsql.Row{"c": []byte("v")}, "c"); err != nil || got != "v" {
		t.Errorf("optionalString([]byte) = %q, %v", got, err)
	}
}
