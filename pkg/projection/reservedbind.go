package projection

import (
	"fmt"
	"sort"
	"strings"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// The bind names the server fills in itself.
//
// Every query a projection declares is bound from one flat map. The server
// seeds it with what only the server can know — who the caller is, which page
// was asked for, which version the write is conditional on — and then writes
// the projection's own columns into the same map. Nothing separates the two, so
// a column named after one of these takes its place, and the value the server
// put there is gone by the time the statement runs.
//
// For most of them that is a wrong answer. For the caller identity it is worse
// than that: :user, :userUID, :userGroups and :userExtra are what a row-level
// security clause is written against, and the documented shape of one is
//
//	WHERE id = :name AND owner = :user
//
// A mapped column named "user" is filled from the object the client submitted,
// so the clause ends up comparing the row's owner against a string the caller
// chose. The guard still reads as a guard and no longer is one. Whoever may
// write a projection is not necessarily whoever may read another tenant's rows,
// and this keeps those two apart.
//
// name and namespace are deliberately absent. A mapping names the columns that
// carry an object's identity, and calling them "name" and "namespace" is the
// obvious thing to do; the mapper binds both from the same object the server
// already forced to agree with the request path, so a collision there resolves
// to the value the server would have bound anyway.
var serverOwnedBinds = map[string]bool{
	// Who is asking. These are the ones that matter.
	"user":       true,
	"userUID":    true,
	"userGroups": true,
	"userExtra":  true,

	// Which rows were asked for.
	"limit":         true,
	"offset":        true,
	"after":         true,
	"since":         true,
	"labelSelector": true,
	"name_not":      true,

	// What the write is conditional on. A field column named this would let
	// the submitted object choose the version its own update is checked
	// against, which is the check answering to the thing it is checking.
	"resourceVersion": true,
}

// labelBindPrefix is where a label mapped to a column is bound, as label_<column>
// along with its _not and _in variants. A column starting with it collides with
// whichever label shares the rest of the name.
const labelBindPrefix = "label_"

// CheckMappedColumns refuses a mapping whose columns would displace a bind the
// server fills in. NewMapper calls it, so every path that builds a mapper is
// covered: validation, the per-version mappings, and compilation.
func CheckMappedColumns(mapping *crispv1alpha1.Mapping) error {
	for _, column := range MappingColumns(mapping) {
		if err := checkColumn("mapping", column.Column, serverOwnedBinds); err != nil {
			return err
		}
	}
	return nil
}

// CheckSelectableColumns refuses a selectable field whose column would displace
// a bind the server fills in.
//
// The reserved set is wider here than for a mapping, and name and namespace are
// in it. A declared selectable column is bound on every list whether or not the
// client selected on it — NULL when they did not — so that a query can carry
// "(:customer IS NULL OR customer = :customer)" unconditionally. A selectable
// column named "namespace" therefore binds NULL over the request's own
// namespace on every list, and a list clause written the way the reference
// documents it, "(:namespace IS NULL OR tenant = :namespace)", stops being
// scoped to a tenant at all. Unlike a mapping there is no second value that
// happens to agree: the point of the binding is that it is NULL.
func CheckSelectableColumns(fields []crispv1alpha1.SelectableField) error {
	for _, field := range fields {
		if field.Column == "" {
			continue
		}
		if err := checkColumn("resource.selectableFields", field.Column, selectableReserved); err != nil {
			return err
		}
	}
	return nil
}

// selectableReserved is serverOwnedBinds plus the identity binds, for the
// reason CheckSelectableColumns gives.
var selectableReserved = func() map[string]bool {
	reserved := map[string]bool{"name": true, "namespace": true}
	for name := range serverOwnedBinds {
		reserved[name] = true
	}
	return reserved
}()

func checkColumn(stanza, column string, reserved map[string]bool) error {
	if reserved[column] {
		return fmt.Errorf(
			"%s: column %q is the name of a parameter the server binds itself (%s); "+
				"a query would read the projection's column where it asked for the server's value, "+
				"so the column has to be named something else or given an alias in the query",
			stanza, column, strings.Join(sortedNames(reserved), ", "))
	}
	if strings.HasPrefix(column, labelBindPrefix) {
		return fmt.Errorf(
			"%s: column %q starts with %q, which is where a label mapped to a column is bound; "+
				"a query would read this column where it asked for the label",
			stanza, column, labelBindPrefix)
	}
	return nil
}

func sortedNames(reserved map[string]bool) []string {
	names := make([]string, 0, len(reserved))
	for name := range reserved {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
