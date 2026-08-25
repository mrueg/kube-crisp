package projection

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

func testResource() crispv1alpha1.ProjectedResource {
	return crispv1alpha1.ProjectedResource{
		Group:   "store.example.com",
		Version: "v1alpha1",
		Kind:    "Order",
		Plural:  "orders",
		Scope:   crispv1alpha1.NamespaceScoped,
	}
}

func testMapping() crispv1alpha1.Mapping {
	return crispv1alpha1.Mapping{
		Name:              "id",
		Namespace:         "tenant",
		ResourceVersion:   "updated_at",
		CreationTimestamp: "created_at",
		Labels:            map[string]string{"store.example.com/status": "status"},
		Fields: []crispv1alpha1.FieldMapping{
			{Column: "customer", Path: "spec.customer"},
			{Column: "total_cents", Path: "spec.totalCents", Type: crispv1alpha1.FieldTypeInteger},
			{Column: "line_items", Path: "spec.lineItems", Type: crispv1alpha1.FieldTypeJSON},
			{Column: "status", Path: "status.phase"},
		},
	}
}

func TestMapperRow(t *testing.T) {
	m, err := NewMapper(testResource(), testMapping())
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	created := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	obj, err := m.Row(crispsql.Row{
		"id":          "order-1001",
		"tenant":      "acme",
		"customer":    "ada",
		"status":      "shipped",
		"total_cents": int64(4999),
		"line_items":  []byte(`[{"sku":"widget","qty":2}]`),
		"created_at":  created,
		"updated_at":  "42",
	})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}

	if got, want := obj.GetName(), "order-1001"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	if got, want := obj.GetNamespace(), "acme"; got != want {
		t.Errorf("namespace = %q, want %q", got, want)
	}
	if got, want := obj.GetAPIVersion(), "store.example.com/v1alpha1"; got != want {
		t.Errorf("apiVersion = %q, want %q", got, want)
	}
	if got, want := obj.GetResourceVersion(), "42"; got != want {
		t.Errorf("resourceVersion = %q, want %q", got, want)
	}
	if got, want := obj.GetLabels()["store.example.com/status"], "shipped"; got != want {
		t.Errorf("label = %q, want %q", got, want)
	}

	spec, ok := obj.Object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec is %T, want map", obj.Object["spec"])
	}
	// Integers must survive as int64: unstructured rejects other numeric types.
	if got, ok := spec["totalCents"].(int64); !ok || got != 4999 {
		t.Errorf("spec.totalCents = %#v, want int64(4999)", spec["totalCents"])
	}

	items, ok := spec["lineItems"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("spec.lineItems = %#v, want a one-element slice", spec["lineItems"])
	}
	item := items[0].(map[string]any)
	if got, ok := item["qty"].(int64); !ok || got != 2 {
		t.Errorf("lineItems[0].qty = %#v, want int64(2); JSON numbers must be normalised to int64", item["qty"])
	}
}

func TestMapperRejectsInvalidName(t *testing.T) {
	m, err := NewMapper(testResource(), testMapping())
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	_, err = m.Row(crispsql.Row{
		"id":     "Order 1001", // spaces and uppercase are not valid object names
		"tenant": "acme", "customer": "ada", "status": "shipped",
		"total_cents": int64(1), "line_items": []byte("[]"),
		"created_at": time.Now(), "updated_at": "1",
	})
	if err == nil {
		t.Fatal("expected an error for a name that is not a DNS subdomain")
	}
}

func TestNewMapperRequiresNamespaceColumn(t *testing.T) {
	mapping := testMapping()
	mapping.Namespace = ""

	if _, err := NewMapper(testResource(), mapping); err == nil {
		t.Fatal("expected an error when a namespaced projection has no namespace column")
	}
}

func TestNewMapperRejectsMetadataPaths(t *testing.T) {
	mapping := testMapping()
	mapping.Fields = append(mapping.Fields, crispv1alpha1.FieldMapping{
		Column: "id", Path: "metadata.name",
	})

	if _, err := NewMapper(testResource(), mapping); err == nil {
		t.Fatal("expected an error for a field mapping targeting metadata")
	}
}

func TestCoerceValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		kind  crispv1alpha1.FieldType
		want  any
	}{
		{"defaults to string", "1000", "", "1000"},
		{"integer", "1000", crispv1alpha1.FieldTypeInteger, int64(1000)},
		{"number", "1.5", crispv1alpha1.FieldTypeNumber, 1.5},
		{"boolean", "true", crispv1alpha1.FieldTypeBoolean, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CoerceValue(tc.value, tc.kind)
			if err != nil {
				t.Fatalf("CoerceValue() returned error: %v", err)
			}
			if got != tc.want {
				t.Errorf("CoerceValue(%q, %q) = %#v, want %#v", tc.value, tc.kind, got, tc.want)
			}
		})
	}

	if _, err := CoerceValue("not-a-number", crispv1alpha1.FieldTypeInteger); err == nil {
		t.Error("a literal that does not parse was accepted")
	}
}

// TestRowAlwaysHasAUID covers what controllers depend on: an object with an
// empty UID produces malformed owner references, so a projection that maps no
// UID column still has to produce one.
func TestRowAlwaysHasAUID(t *testing.T) {
	mapping := testMapping()
	mapping.UID = ""

	m, err := NewMapper(testResource(), mapping)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	row := crispsql.Row{
		"id": "order-1001", "tenant": "acme", "customer": "ada", "status": "shipped",
		"total_cents": int64(1), "line_items": []byte("[]"),
		"created_at": time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC), "updated_at": "1",
	}

	obj, err := m.Row(row)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if obj.GetUID() == "" {
		t.Fatal("the object has no UID")
	}

	// Deterministic: every replica and every restart must agree, or owner
	// references stop resolving.
	again, err := m.Row(row)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if again.GetUID() != obj.GetUID() {
		t.Errorf("UID changed between reads: %q then %q", obj.GetUID(), again.GetUID())
	}

	// Distinct objects must not share one.
	other := crispsql.Row{}
	for k, v := range row {
		other[k] = v
	}
	other["id"] = "order-1002"

	different, err := m.Row(other)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if different.GetUID() == obj.GetUID() {
		t.Error("two objects were given the same UID")
	}
}

// TestDerivedUIDTracksRecreation checks the refinement: with a creation
// timestamp mapped, a row recreated under the same name is a different object.
func TestDerivedUIDTracksRecreation(t *testing.T) {
	mapping := testMapping()
	mapping.UID = ""

	m, err := NewMapper(testResource(), mapping)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	base := crispsql.Row{
		"id": "order-1001", "tenant": "acme", "customer": "ada", "status": "shipped",
		"total_cents": int64(1), "line_items": []byte("[]"), "updated_at": "1",
	}

	base["created_at"] = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	first, err := m.Row(base)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}

	base["created_at"] = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	recreated, err := m.Row(base)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}

	if first.GetUID() == recreated.GetUID() {
		t.Error("a row recreated under the same name kept its UID")
	}
}

// TestMappedUIDWins keeps the explicit mapping authoritative.
func TestMappedUIDWins(t *testing.T) {
	mapping := testMapping()
	mapping.UID = "id"

	m, err := NewMapper(testResource(), mapping)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj, err := m.Row(crispsql.Row{
		"id": "order-1001", "tenant": "acme", "customer": "ada", "status": "shipped",
		"total_cents": int64(1), "line_items": []byte("[]"),
		"created_at": time.Now(), "updated_at": "1",
	})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	if got, want := string(obj.GetUID()), "order-1001"; got != want {
		t.Errorf("UID = %q, want the mapped %q", got, want)
	}
}

// TestLabelsFromJSONColumn: mapping.labels names one key per column, which is
// right for a fixed set and useless for a table whose labels vary per row.
// labelsFrom reads the whole map out of one column.
func TestLabelsFromJSONColumn(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}
	mapping := crispv1alpha1.Mapping{
		Name:            "id",
		Namespace:       "tenant",
		LabelsFrom:      "labels",
		AnnotationsFrom: "annotations",
		// A key with a column of its own wins over the same key in the JSON,
		// so it can be promoted without moving the rest.
		Labels: map[string]string{"store.example.com/status": "status"},
	}

	mapper, err := NewMapper(res, mapping)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj, err := mapper.Row(crispsql.Row{
		"id":          "order-1",
		"tenant":      "acme",
		"status":      "shipped",
		"labels":      `{"team":"payments","store.example.com/status":"stale"}`,
		"annotations": `{"note":"hand written"}`,
	})
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}

	labels := obj.GetLabels()
	if got, want := labels["team"], "payments"; got != want {
		t.Errorf("labels[team] = %q, want %q", got, want)
	}
	if got, want := labels["store.example.com/status"], "shipped"; got != want {
		t.Errorf("labels[status] = %q, want %q; the dedicated column has to win", got, want)
	}
	if got, want := obj.GetAnnotations()["note"], "hand written"; got != want {
		t.Errorf("annotations[note] = %q, want %q", got, want)
	}
}

// TestLabelsFromRoundTripsOnWrite: what comes back out has to be what a write
// puts in, minus whatever has a column of its own — storing a key twice invites
// the two copies to disagree.
func TestLabelsFromRoundTripsOnWrite(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}
	mapper, err := NewMapper(res, crispv1alpha1.Mapping{
		Name:       "id",
		Namespace:  "tenant",
		LabelsFrom: "labels",
		Labels:     map[string]string{"store.example.com/status": "status"},
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetName("order-1")
	obj.SetNamespace("acme")
	obj.SetLabels(map[string]string{
		"team":                     "payments",
		"store.example.com/status": "shipped",
	})

	args, err := mapper.Params(obj)
	if err != nil {
		t.Fatalf("Params() returned error: %v", err)
	}

	if got, want := args["status"], "shipped"; got != want {
		t.Errorf("status column = %v, want %v", got, want)
	}
	if got, want := args["labels"], `{"team":"payments"}`; got != want {
		t.Errorf("labels column = %v, want %v; a key with its own column must not be stored twice", got, want)
	}

	// An object with nothing left over binds NULL, not "{}".
	obj.SetLabels(map[string]string{"store.example.com/status": "shipped"})
	args, err = mapper.Params(obj)
	if err != nil {
		t.Fatalf("Params() returned error: %v", err)
	}
	if args["labels"] != nil {
		t.Errorf("labels column = %v with nothing to store, want nil", args["labels"])
	}
}

// TestLabelsFromRejectsInvalidKeys: a column is not a validated field, so what
// comes out of it has to be checked before it becomes a label.
func TestLabelsFromRejectsInvalidKeys(t *testing.T) {
	res := crispv1alpha1.ProjectedResource{
		Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
		Plural: "orders", Scope: crispv1alpha1.NamespaceScoped,
	}
	mapper, err := NewMapper(res, crispv1alpha1.Mapping{
		Name: "id", Namespace: "tenant", LabelsFrom: "labels",
	})
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	if _, err := mapper.Row(crispsql.Row{
		"id": "order-1", "tenant": "acme", "labels": `{"not a valid key":"x"}`,
	}); err == nil {
		t.Error("an invalid label key was accepted")
	}
	if _, err := mapper.Row(crispsql.Row{
		"id": "order-1", "tenant": "acme", "labels": `{"team":"a value with spaces"}`,
	}); err == nil {
		t.Error("an invalid label value was accepted")
	}
}

// TestMappedObjectDoesNotAliasItsRow is the rule setNestedFieldNoCopy depends
// on.
//
// A row can be shared: identical concurrent reads are collapsed onto one query,
// and every waiter maps the same rows into objects of its own. An object holding
// a reference into a row it was built from would let one client's response
// mutate another's — so nothing built here may point back into the row, which is
// what lets the copy be skipped on the way in.
func TestMappedObjectDoesNotAliasItsRow(t *testing.T) {
	mapper, err := NewMapper(
		crispv1alpha1.ProjectedResource{
			Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
			Scope: crispv1alpha1.ClusterScoped,
		},
		crispv1alpha1.Mapping{
			Name: "id",
			Fields: []crispv1alpha1.FieldMapping{
				{Column: "line_items", Path: "spec.lineItems", Type: crispv1alpha1.FieldTypeJSON},
				{Column: "customer", Path: "spec.customer"},
			},
		},
	)
	if err != nil {
		t.Fatalf("NewMapper() returned error: %v", err)
	}

	// A row as the json_agg path produces it: already-decoded structures, which
	// are the values that could be shared if anything held on to them.
	decoded := []any{map[string]any{"sku": "widget", "qty": int64(2)}}
	row := crispsql.Row{
		"id":         "order-1001",
		"customer":   "ada",
		"line_items": decoded,
	}

	first, err := mapper.Row(row)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}
	second, err := mapper.Row(row)
	if err != nil {
		t.Fatalf("Row() returned error: %v", err)
	}

	// Mutating one object must not reach the row, and so must not reach the
	// other object built from it.
	nested, found, err := unstructured.NestedFieldNoCopy(first.Object, "spec", "lineItems")
	if err != nil || !found {
		t.Fatalf("reading spec.lineItems: found=%v err=%v", found, err)
	}
	list, ok := nested.([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("spec.lineItems is %T", nested)
	}
	entry, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("spec.lineItems[0] is %T", list[0])
	}
	entry["sku"] = "rewritten"

	if sku := decoded[0].(map[string]any)["sku"]; sku != "widget" {
		t.Errorf("the row now holds sku %q: the object was pointing into it", sku)
	}
	secondNested, _, _ := unstructured.NestedFieldNoCopy(second.Object, "spec", "lineItems")
	if sku := secondNested.([]any)[0].(map[string]any)["sku"]; sku != "widget" {
		t.Errorf("a second object built from the same row holds sku %q", sku)
	}
}
