package projection

import (
	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// What a column provides, reported alongside it so a reader can tell a column
// the projection cannot work without from one that fills in a field.
const (
	usedForIdentity = "identity"
	usedForMetadata = "metadata"
	usedForLabel    = "label"
	usedForField    = "field"
)

// MappedColumn is one column a mapping reads, and what it is read for.
type MappedColumn struct {
	// Column is the result column's name.
	Column string

	// UsedFor is "identity", "metadata", "label" or "field".
	UsedFor string

	// Type is what the value is coerced to. Everything except a field mapping
	// is read as a string.
	Type crispv1alpha1.FieldType
}

// MappingColumns walks a mapping and reports every column it reads, in the
// order the mapping is applied: identity first, then metadata, then labels and
// annotations, then fields.
//
// One enumeration, because there were two and they had no reason to agree
// beyond somebody remembering. RequiredSchema walks a mapping to say what the
// database must provide, and the round-trip check walks it to compare the
// versions of a kind against each other — the same question, asked by two
// callers, answered by two functions with the same list of struct fields
// spelled out twice.
//
// The cost of drift is not symmetrical. A column missing from what
// RequiredSchema reports is an incomplete checklist. A column missing from what
// the round-trip check compares is a check that passes when it should not: two
// versions are declared to cover the same columns while one of them silently
// does not, and a write through it drops a value the other displays. That check
// fails open, which is the reason to have one list rather than two.
//
// TestMappingColumnsCoversEveryColumnBearingField guards the remaining seam —
// between this function and the Mapping struct — so a field added to the API
// cannot be quietly left out of both callers at once.
func MappingColumns(mapping *crispv1alpha1.Mapping) []MappedColumn {
	if mapping == nil {
		return nil
	}

	var out []MappedColumn
	add := func(column, usedFor string, fieldType crispv1alpha1.FieldType) {
		if column == "" {
			return
		}
		out = append(out, MappedColumn{Column: column, UsedFor: usedFor, Type: fieldType})
	}

	// Identity first: without these a row cannot become an object at all, and
	// callers that keep only the first description of a column read twice
	// therefore keep the half that cannot be dropped.
	add(mapping.Name, usedForIdentity, crispv1alpha1.FieldTypeString)
	for _, column := range mapping.NameColumns {
		add(column, usedForIdentity, crispv1alpha1.FieldTypeString)
	}
	add(mapping.Namespace, usedForIdentity, crispv1alpha1.FieldTypeString)
	add(mapping.UID, usedForIdentity, crispv1alpha1.FieldTypeString)

	for _, column := range []string{
		mapping.ResourceVersion, mapping.CreationTimestamp, mapping.DeletionTimestamp,
		mapping.Generation, mapping.Finalizers, mapping.OwnerReferences,
		mapping.ManagedFields, mapping.LabelsFrom, mapping.AnnotationsFrom,
	} {
		add(column, usedForMetadata, crispv1alpha1.FieldTypeString)
	}

	for _, column := range mapping.Labels {
		add(column, usedForLabel, crispv1alpha1.FieldTypeString)
	}
	for _, column := range mapping.Annotations {
		add(column, usedForLabel, crispv1alpha1.FieldTypeString)
	}

	for _, field := range mapping.Fields {
		fieldType := field.Type
		if fieldType == "" {
			fieldType = crispv1alpha1.FieldTypeString
		}
		add(field.Column, usedForField, fieldType)
	}

	return out
}

// MappingColumnNames is MappingColumns reduced to the set of names, for callers
// comparing one mapping against another rather than describing one.
//
// Labels and Annotations are maps, so the order two mappings are walked in is
// not stable between them; a set is what the comparison wanted anyway.
func MappingColumnNames(mapping *crispv1alpha1.Mapping) map[string]struct{} {
	names := map[string]struct{}{}
	for _, column := range MappingColumns(mapping) {
		names[column.Column] = struct{}{}
	}
	return names
}
