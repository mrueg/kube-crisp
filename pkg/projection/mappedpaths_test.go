package projection

import (
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// mappedProjection maps a notes column to spec.notes under the given schema.
func mappedProjection(schema *apiextensionsv1.JSONSchemaProps) *crispv1alpha1.CustomResourceProjection {
	p := incrementalProjection()
	p.Spec.Watch = nil
	p.Spec.Resource.Schema = schema
	p.Spec.Mapping.Fields = []crispv1alpha1.FieldMapping{{Column: "notes", Path: "spec.notes"}}
	return p
}

func objectOf(properties map[string]apiextensionsv1.JSONSchemaProps) *apiextensionsv1.JSONSchemaProps {
	return &apiextensionsv1.JSONSchemaProps{Type: "object", Properties: properties}
}

func preserving() *apiextensionsv1.JSONSchemaProps {
	yes := true
	return &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &yes}
}

// describingNotes is the schema the mapping above is right for.
func describingNotes() *apiextensionsv1.JSONSchemaProps {
	return objectOf(map[string]apiextensionsv1.JSONSchemaProps{
		"spec": *objectOf(map[string]apiextensionsv1.JSONSchemaProps{"notes": {Type: "string"}}),
	})
}

// TestValidateRefusesAMappedPathTheSchemaDoesNotDescribe is the data-loss
// case: a read shows the field, a write prunes it and NULLs the column.
func TestValidateRefusesAMappedPathTheSchemaDoesNotDescribe(t *testing.T) {
	err := Validate(mappedProjection(objectOf(map[string]apiextensionsv1.JSONSchemaProps{
		"spec": *objectOf(map[string]apiextensionsv1.JSONSchemaProps{"customer": {Type: "string"}}),
	})))
	if err == nil {
		t.Fatal("Validate() accepted a mapped path the schema does not describe")
	}
	for _, want := range []string{"version v1alpha1", "spec.notes", `"notes"`, "NULL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

// TestValidateRefusesAMappedPathUnderABareObjectSchema: `type: object` with no
// properties describes nothing, and pruning against it drops every field. This
// is the shape a first draft of a projection tends to have.
func TestValidateRefusesAMappedPathUnderABareObjectSchema(t *testing.T) {
	err := Validate(mappedProjection(&apiextensionsv1.JSONSchemaProps{Type: "object"}))
	if err == nil {
		t.Fatal("Validate() accepted a mapped path under a schema that describes nothing")
	}
	if !strings.Contains(err.Error(), "does not describe spec") {
		t.Errorf("error %q does not name the segment pruning drops", err)
	}
}

func TestValidateAcceptsAMappedPathTheSchemaDescribes(t *testing.T) {
	if err := Validate(mappedProjection(describingNotes())); err != nil {
		t.Fatalf("Validate() refused a mapped path the schema describes: %v", err)
	}
}

// TestValidateAcceptsAMappedPathUnderPreservedUnknownFields, at the root and
// on the object holding the field: pruning stops at the marker, so the path
// need not be spelled out.
func TestValidateAcceptsAMappedPathUnderPreservedUnknownFields(t *testing.T) {
	for name, schema := range map[string]*apiextensionsv1.JSONSchemaProps{
		"at the root":   preserving(),
		"on the object": objectOf(map[string]apiextensionsv1.JSONSchemaProps{"spec": *preserving()}),
	} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(mappedProjection(schema)); err != nil {
				t.Fatalf("Validate() refused a path under x-kubernetes-preserve-unknown-fields: %v", err)
			}
		})
	}
}

// TestValidateAcceptsAMappedPathUnderAdditionalProperties: a map schema keeps
// any key, so a column may be mapped to one of them.
func TestValidateAcceptsAMappedPathUnderAdditionalProperties(t *testing.T) {
	p := mappedProjection(objectOf(map[string]apiextensionsv1.JSONSchemaProps{
		"spec": *objectOf(map[string]apiextensionsv1.JSONSchemaProps{
			"tags": {
				Type: "object",
				AdditionalProperties: &apiextensionsv1.JSONSchemaPropsOrBool{
					Schema: &apiextensionsv1.JSONSchemaProps{Type: "string"},
				},
			},
		}),
	}))
	p.Spec.Mapping.Fields = []crispv1alpha1.FieldMapping{{Column: "notes", Path: "spec.tags.notes"}}

	if err := Validate(p); err != nil {
		t.Fatalf("Validate() refused a path under additionalProperties: %v", err)
	}
}

// TestValidateRefusesAMappedPathThroughAList: a dotted path names map keys,
// so a segment inside an array can never be reached by one.
func TestValidateRefusesAMappedPathThroughAList(t *testing.T) {
	p := mappedProjection(objectOf(map[string]apiextensionsv1.JSONSchemaProps{
		"spec": *objectOf(map[string]apiextensionsv1.JSONSchemaProps{
			"items": {
				Type: "array",
				Items: &apiextensionsv1.JSONSchemaPropsOrArray{
					Schema: objectOf(map[string]apiextensionsv1.JSONSchemaProps{"notes": {Type: "string"}}),
				},
			},
		}),
	}))
	p.Spec.Mapping.Fields = []crispv1alpha1.FieldMapping{{Column: "notes", Path: "spec.items.notes"}}

	err := Validate(p)
	if err == nil {
		t.Fatal("Validate() accepted a path through a list")
	}
	if !strings.Contains(err.Error(), "spec.items") || !strings.Contains(err.Error(), "list") {
		t.Errorf("error %q does not say the path runs through a list", err)
	}
}

// TestValidateRefusesTheVersionThatLacksThePath: the primary version describes
// the field and a secondary one does not, and the error names the one that is
// wrong rather than the projection as a whole.
func TestValidateRefusesTheVersionThatLacksThePath(t *testing.T) {
	p := mappedProjection(describingNotes())
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{
		Name:   "v1beta1",
		Schema: objectOf(map[string]apiextensionsv1.JSONSchemaProps{"spec": *objectOf(nil)}),
	}}

	err := Validate(p)
	if err == nil {
		t.Fatal("Validate() accepted a secondary version that does not describe the mapped path")
	}
	if !strings.Contains(err.Error(), "version v1beta1") {
		t.Errorf("error %q does not name the version that lacks the path", err)
	}
	if strings.Contains(err.Error(), "version v1alpha1") {
		t.Errorf("error %q blames the version that describes the path", err)
	}

	// A version nobody can write through is not held to it.
	served := false
	p.Spec.Resource.Versions[0].Served = &served
	if err := Validate(p); err != nil {
		t.Fatalf("Validate() checked a version that is not served: %v", err)
	}
}

// TestValidateChecksAVersionsOwnMapping: a secondary version with a mapping of
// its own is checked against its own schema, not the primary's.
func TestValidateChecksAVersionsOwnMapping(t *testing.T) {
	p := mappedProjection(describingNotes())
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{
		Name:   "v1beta1",
		Schema: describingNotes(),
		Mapping: &crispv1alpha1.Mapping{
			Name:      "id",
			Namespace: "tenant",
			Fields:    []crispv1alpha1.FieldMapping{{Column: "notes", Path: "spec.remarks"}},
		},
	}}

	err := Validate(p)
	if err == nil {
		t.Fatal("Validate() accepted a version whose own mapping names a path its schema lacks")
	}
	if !strings.Contains(err.Error(), "version v1beta1") || !strings.Contains(err.Error(), "spec.remarks") {
		t.Errorf("error %q does not name the version and the path", err)
	}
}

// TestMappedPathsUnderABorrowedSchema: Validate has no schema to check a
// schemaFrom version against and lets it through; the compile hands the
// resolved one back in, and the same refusal follows.
func TestMappedPathsUnderABorrowedSchema(t *testing.T) {
	p := mappedProjection(nil)
	p.Spec.Resource.SchemaFrom = &crispv1alpha1.CRDReference{Name: "orders.acme.example.com"}

	if err := Validate(p); err != nil {
		t.Fatalf("Validate() refused a borrowed schema it cannot see: %v", err)
	}

	lacking := map[string]*apiextensionsv1.JSONSchemaProps{"v1alpha1": objectOf(map[string]apiextensionsv1.JSONSchemaProps{"spec": *objectOf(nil)})}
	err := CheckMappedPaths(p, lacking)
	if err == nil {
		t.Fatal("CheckMappedPaths() accepted a borrowed schema that does not describe the mapped path")
	}
	if !strings.Contains(err.Error(), "spec.notes") {
		t.Errorf("error %q does not name the path", err)
	}

	describing := map[string]*apiextensionsv1.JSONSchemaProps{"v1alpha1": describingNotes()}
	if err := CheckMappedPaths(p, describing); err != nil {
		t.Fatalf("CheckMappedPaths() refused a borrowed schema that describes the path: %v", err)
	}
}

// TestValidateRefusesAJSONColumnAtALeafThatKeepsNothing: an object with no
// properties is pruned to {}, which erases a json column as surely as dropping
// the field would. A list of such objects is the same. Marking the leaf, or
// the items, x-kubernetes-preserve-unknown-fields is what makes it safe.
func TestValidateRefusesAJSONColumnAtALeafThatKeepsNothing(t *testing.T) {
	spec := func(notes apiextensionsv1.JSONSchemaProps) *apiextensionsv1.JSONSchemaProps {
		return objectOf(map[string]apiextensionsv1.JSONSchemaProps{
			"spec": *objectOf(map[string]apiextensionsv1.JSONSchemaProps{"notes": notes}),
		})
	}
	listOf := func(items *apiextensionsv1.JSONSchemaProps) apiextensionsv1.JSONSchemaProps {
		return apiextensionsv1.JSONSchemaProps{Type: "array", Items: &apiextensionsv1.JSONSchemaPropsOrArray{Schema: items}}
	}

	for _, tc := range []struct {
		name    string
		schema  *apiextensionsv1.JSONSchemaProps
		refused bool
	}{
		{"a bare object", spec(*objectOf(nil)), true},
		{"a list of bare objects", spec(listOf(objectOf(nil))), true},
		{"a preserved object", spec(*preserving()), false},
		{"a list of preserved objects", spec(listOf(preserving())), false},
		{"an object with properties", spec(*objectOf(map[string]apiextensionsv1.JSONSchemaProps{"text": {Type: "string"}})), false},
		{"a list of strings", spec(listOf(&apiextensionsv1.JSONSchemaProps{Type: "string"})), false},
		{"a string", spec(apiextensionsv1.JSONSchemaProps{Type: "string"}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := mappedProjection(tc.schema)
			p.Spec.Mapping.Fields[0].Type = crispv1alpha1.FieldTypeJSON

			err := Validate(p)
			switch {
			case tc.refused && err == nil:
				t.Fatal("Validate() accepted a json column the schema would empty on write")
			case tc.refused && !strings.Contains(err.Error(), "empty"):
				t.Errorf("error %q does not say the value would be emptied", err)
			case !tc.refused && err != nil:
				t.Fatalf("Validate() refused a json column the schema keeps: %v", err)
			}
		})
	}
}

// TestAMetadataPathIsExemptFromPruning documents the exemption the walk
// carries, although the mapper refuses to put a column under metadata at all:
// pruning leaves the root's metadata alone whatever the schema says.
func TestAMetadataPathIsExemptFromPruning(t *testing.T) {
	structural, err := toStructural(&apiextensionsv1.JSONSchemaProps{Type: "object"})
	if err != nil {
		t.Fatalf("toStructural(): %v", err)
	}
	if end := walkMappedPath(structural, []string{"metadata", "labels", "team"}); !end.kept {
		t.Errorf("a metadata path was reported as pruned at %s", end.at)
	}
}
