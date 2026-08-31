package projection

import (
	"reflect"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// notAColumn are the Mapping fields that do not name one, and why.
//
// Spelled out so that adding a field to the API forces a decision: it either
// names a column, and MappingColumns has to report it, or it belongs here with
// a reason. What must not happen is a field being added and neither caller
// noticing.
var notAColumn = map[string]string{
	"NameSeparator":   "joins the name columns together; it is punctuation, not a column",
	"OnUnmappableRow": "a policy for rows that cannot be mapped, not a column to map",
}

// TestMappingColumnsCoversEveryColumnBearingField is the guard on the seam that
// is left after the two enumerations became one.
//
// MappingColumns is now the only walk over a mapping's columns, so its two
// callers cannot disagree with each other. They can both still disagree with
// the API type: someone adds a field to Mapping, maps a column with it, and
// neither RequiredSchema nor the round-trip check knows it exists.
//
// The second of those is the one that hurts. The round-trip check compares the
// columns two versions cover, so a column it cannot see is a column it stops
// comparing — the check passes, the versions are declared equivalent, and a
// write through the version that does not map it drops a value the other one
// displays. It fails open and says nothing.
//
// So the mapping is filled by reflection rather than by hand: every field gets
// a sentinel, and every sentinel has to come back.
func TestMappingColumnsCoversEveryColumnBearingField(t *testing.T) {
	mapping := &crispv1alpha1.Mapping{}
	value := reflect.ValueOf(mapping).Elem()
	mappingType := value.Type()

	var (
		stringSlice  = reflect.TypeOf([]string{})
		stringMap    = reflect.TypeOf(map[string]string{})
		fieldMapping = reflect.TypeOf([]crispv1alpha1.FieldMapping{})
	)

	// sentinel -> the field that should have produced it.
	want := map[string]string{}

	for i := 0; i < mappingType.NumField(); i++ {
		field := mappingType.Field(i)
		if _, skip := notAColumn[field.Name]; skip {
			continue
		}

		sentinel := "column_from_" + field.Name
		switch {
		case field.Type == stringSlice:
			value.Field(i).Set(reflect.ValueOf([]string{sentinel}))
		case field.Type == stringMap:
			value.Field(i).Set(reflect.ValueOf(map[string]string{"key": sentinel}))
		case field.Type == fieldMapping:
			value.Field(i).Set(reflect.ValueOf([]crispv1alpha1.FieldMapping{
				{Column: sentinel, Path: "spec.example"},
			}))
		case field.Type.Kind() == reflect.String:
			value.Field(i).SetString(sentinel)
		default:
			t.Fatalf("Mapping.%s is a %s, which this test cannot fill.\n"+
				"Teach it how, or add the field to notAColumn with the reason it names no column.",
				field.Name, field.Type)
		}
		want[sentinel] = field.Name
	}

	got := MappingColumnNames(mapping)
	for sentinel, field := range want {
		if _, found := got[sentinel]; !found {
			t.Errorf("Mapping.%s names a column and MappingColumns does not report it.\n"+
				"RequiredSchema would omit it, and the round-trip check would stop comparing it — "+
				"which makes that check pass for versions that do not in fact cover the same columns.",
				field)
		}
	}
}

// TestMappingColumnsReportsIdentityFirst is a contract, not a detail.
//
// requiredColumns keeps the first description of a column read twice, so a
// column that is both the object's name and one of its fields is reported as
// identity — the half that cannot be dropped. That is only true while identity
// comes first out of here.
func TestMappingColumnsReportsIdentityFirst(t *testing.T) {
	columns := MappingColumns(&crispv1alpha1.Mapping{
		Name: "id",
		Fields: []crispv1alpha1.FieldMapping{
			{Column: "id", Path: "spec.id"},
		},
	})

	if len(columns) != 2 {
		t.Fatalf("got %d column(s), want the identity and the field: %+v", len(columns), columns)
	}
	if columns[0].UsedFor != usedForIdentity {
		t.Fatalf("the first report of id is %q, want identity", columns[0].UsedFor)
	}

	// And the caller that depends on it.
	required := requiredColumns(crispv1alpha1.Mapping{
		Name:   "id",
		Fields: []crispv1alpha1.FieldMapping{{Column: "id", Path: "spec.id"}},
	})
	if len(required) != 1 || required[0].UsedFor != usedForIdentity {
		t.Fatalf("requiredColumns reported %+v, want id once, as identity", required)
	}
}

// TestMappingColumnsCarriesFieldTypes, since a field's declared type is what
// tells whoever writes the DDL that a column holds a number rather than text.
func TestMappingColumnsCarriesFieldTypes(t *testing.T) {
	columns := MappingColumns(&crispv1alpha1.Mapping{
		Fields: []crispv1alpha1.FieldMapping{
			{Column: "total_cents", Path: "spec.total", Type: crispv1alpha1.FieldTypeInteger},
			{Column: "customer", Path: "spec.customer"},
		},
	})

	byColumn := map[string]MappedColumn{}
	for _, column := range columns {
		byColumn[column.Column] = column
	}

	if got := byColumn["total_cents"].Type; got != crispv1alpha1.FieldTypeInteger {
		t.Errorf("total_cents type = %q, want integer", got)
	}
	// An unset type is a string, which is what the mapper coerces to.
	if got := byColumn["customer"].Type; got != crispv1alpha1.FieldTypeString {
		t.Errorf("customer type = %q, want string for an unset type", got)
	}
}

// TestMappingColumnsOfNil, because a version without a mapping of its own uses
// the projection's, and the round-trip check reaches here with nil for it.
func TestMappingColumnsOfNil(t *testing.T) {
	if columns := MappingColumns(nil); columns != nil {
		t.Fatalf("MappingColumns(nil) = %+v, want nil", columns)
	}
	if names := MappingColumnNames(nil); len(names) != 0 {
		t.Fatalf("MappingColumnNames(nil) = %+v, want empty", names)
	}
}
