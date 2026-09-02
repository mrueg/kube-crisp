package projection

import (
	"strings"
	"testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// Every query is bound from one flat map: the server seeds it with the caller's
// identity and the page that was asked for, and the projection's own columns are
// written into the same map afterwards. A column named after one of those
// parameters takes its place.
//
// The identity binds are the ones that matter. A row-level security clause is
// written as "WHERE id = :name AND owner = :user", so a mapped column named
// "user" leaves the clause comparing the row's owner against a string the caller
// put in the object they submitted. It still reads as a guard.
func TestAMappedColumnCannotTakeTheNameOfAParameterTheServerBinds(t *testing.T) {
	for _, column := range []string{
		"user", "userUID", "userGroups", "userExtra",
		"limit", "offset", "after", "since", "labelSelector", "name_not",
		"resourceVersion",
		"label_tier",
	} {
		t.Run(column, func(t *testing.T) {
			mapping := testMapping()
			mapping.Fields = append(mapping.Fields, crispv1alpha1.FieldMapping{
				Path:   "spec.owner",
				Column: column,
				Type:   crispv1alpha1.FieldTypeString,
			})

			_, err := NewMapper(testResource(), mapping)
			if err == nil {
				t.Fatalf("a mapping was accepted whose column displaces the %q parameter", column)
			}
			if !strings.Contains(err.Error(), column) {
				t.Errorf("the refusal does not name the column: %v", err)
			}
		})
	}
}

// A column reached any other way is the same collision, so the check runs over
// the whole mapping rather than over the fields it was easiest to reach.
func TestTheCheckCoversColumnsThatAreNotFields(t *testing.T) {
	t.Run("a label column", func(t *testing.T) {
		mapping := testMapping()
		mapping.Labels = map[string]string{"tier": "user"}
		if _, err := NewMapper(testResource(), mapping); err == nil {
			t.Fatal("a label mapped to a column named user was accepted")
		}
	})

	t.Run("a metadata column", func(t *testing.T) {
		mapping := testMapping()
		mapping.Finalizers = "since"
		if _, err := NewMapper(testResource(), mapping); err == nil {
			t.Fatal("a finalizers column named since was accepted")
		}
	})
}

// name and namespace are deliberately allowed, because calling the identity
// columns that is the obvious thing to do and the mapper binds both from an
// object the server already forced to agree with the request path. Refusing
// them would break ordinary projections to no purpose.
func TestIdentityColumnsMayStillBeCalledNameAndNamespace(t *testing.T) {
	mapping := testMapping()
	mapping.Name = "name"
	mapping.Namespace = "namespace"

	if _, err := NewMapper(testResource(), mapping); err != nil {
		t.Fatalf("NewMapper() refused the ordinary identity column names: %v", err)
	}
}

// A selectable column is bound on every list whether or not the client selected
// on it — NULL when they did not — so a query can carry
// "(:customer IS NULL OR customer = :customer)" unconditionally.
//
// That makes the reserved set wider here. A selectable column named "namespace"
// binds NULL over the request's own namespace on every list, and the list clause
// the reference documents, "(:namespace IS NULL OR tenant = :namespace)", stops
// being scoped to a tenant at all. Unlike a mapping there is no second value
// that happens to agree: binding NULL is the point.
func TestASelectableColumnCannotTakeTheNameOfAParameterTheServerBinds(t *testing.T) {
	for _, column := range []string{"namespace", "name", "user", "limit", "label_tier"} {
		t.Run(column, func(t *testing.T) {
			err := CheckSelectableColumns([]crispv1alpha1.SelectableField{
				{JSONPath: ".spec.customer", Column: column},
			})
			if err == nil {
				t.Fatalf("a selectable column named %q was accepted", column)
			}
			if !strings.Contains(err.Error(), column) {
				t.Errorf("the refusal does not name the column: %v", err)
			}
		})
	}
}

// And an ordinary projection still compiles, or the check has eaten the feature.
func TestOrdinaryColumnsAreUntouched(t *testing.T) {
	mapping := testMapping()
	mapping.Fields = append(mapping.Fields, crispv1alpha1.FieldMapping{
		Path:   "spec.owner",
		Column: "owner_user",
		Type:   crispv1alpha1.FieldTypeString,
	})
	if _, err := NewMapper(testResource(), mapping); err != nil {
		t.Fatalf("NewMapper() refused an ordinary mapping: %v", err)
	}

	if err := CheckSelectableColumns([]crispv1alpha1.SelectableField{
		{JSONPath: ".spec.customer", Column: "customer"},
		// A field with no column is filtered after mapping and binds nothing.
		{JSONPath: ".spec.region"},
	}); err != nil {
		t.Errorf("CheckSelectableColumns() refused ordinary columns: %v", err)
	}
}
