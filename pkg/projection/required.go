package projection

import (
	"sort"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// What a column provides, reported alongside it so a reader can tell a column
// the projection cannot work without from one that fills in a field.
const (
	usedForIdentity = "identity"
	usedForMetadata = "metadata"
	usedForLabel    = "label"
	usedForField    = "field"
)

// RequiredSchema gathers what a projection reads from its database: the tables
// its statements name, and the columns its mapping takes out of their results.
//
// Derived from the projection alone. It says what the projection asks for, not
// what the database has — whether the two agree is what compiling the queries
// answers, and a projection whose table is missing reports CompilationFailed
// with the database's own message.
//
// The point is that neither half is written down anywhere today. The columns
// are spread across a dozen mapping fields, and the tables are only in the SQL,
// so working out what a projection needs means reading the whole spec. This is
// that reading, done once, in a form that can be handed to whatever manages the
// schema.
func RequiredSchema(spec crispv1alpha1.CustomResourceProjectionSpec) *crispv1alpha1.RequiredSchema {
	required := &crispv1alpha1.RequiredSchema{
		Tables:  requiredTables(spec),
		Columns: requiredColumns(spec.Mapping),
	}
	if len(required.Tables) == 0 && len(required.Columns) == 0 {
		return nil
	}
	return required
}

// requiredTables names every table the projection's statements refer to,
// across every query it declares.
func requiredTables(spec crispv1alpha1.CustomResourceProjectionSpec) []string {
	queries := []*crispv1alpha1.Query{
		&spec.Queries.List,
		spec.Queries.Get, spec.Queries.Create, spec.Queries.Update,
		spec.Queries.Delete, spec.Queries.MarkDeleted,
		spec.Queries.DeleteCollection, spec.Queries.UpdateStatus,
		spec.Queries.Count,
	}
	if spec.Watch != nil {
		queries = append(queries, spec.Watch.Query, spec.Watch.DeletedQuery)
	}

	seen := map[string]bool{}
	var tables []string
	for _, query := range queries {
		if query == nil {
			continue
		}
		// A query is one statement or several; a transactional write spans
		// tables the single-statement form never would, which is exactly the
		// case worth reporting.
		statements := query.Statements
		if query.SQL != "" {
			statements = append([]string{query.SQL}, statements...)
		}
		for _, statement := range statements {
			for _, table := range crispsql.Tables(statement, spec.DataSource.Driver) {
				if seen[table] {
					continue
				}
				seen[table] = true
				tables = append(tables, table)
			}
		}
	}

	sort.Strings(tables)
	return tables
}

// requiredColumns lists the result columns the mapping reads.
func requiredColumns(mapping crispv1alpha1.Mapping) []crispv1alpha1.RequiredColumn {
	columns := map[string]crispv1alpha1.RequiredColumn{}
	add := func(name, usedFor string, fieldType crispv1alpha1.FieldType) {
		if name == "" {
			return
		}
		// A column read twice keeps the first description of it. Identity comes
		// first below, so a column that is both the name and a field is
		// reported as identity — which is the half that cannot be dropped.
		if _, taken := columns[name]; taken {
			return
		}
		columns[name] = crispv1alpha1.RequiredColumn{
			Name: name, Type: fieldType, UsedFor: usedFor,
		}
	}

	// Identity first: without these a row cannot become an object at all.
	add(mapping.Name, usedForIdentity, crispv1alpha1.FieldTypeString)
	for _, column := range mapping.NameColumns {
		add(column, usedForIdentity, crispv1alpha1.FieldTypeString)
	}
	add(mapping.Namespace, usedForIdentity, crispv1alpha1.FieldTypeString)
	add(mapping.UID, usedForIdentity, crispv1alpha1.FieldTypeString)

	for _, meta := range []string{
		mapping.ResourceVersion, mapping.CreationTimestamp, mapping.DeletionTimestamp,
		mapping.Generation, mapping.Finalizers, mapping.OwnerReferences,
		mapping.ManagedFields, mapping.LabelsFrom, mapping.AnnotationsFrom,
	} {
		add(meta, usedForMetadata, crispv1alpha1.FieldTypeString)
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

	out := make([]crispv1alpha1.RequiredColumn, 0, len(columns))
	for _, column := range columns {
		out = append(out, column)
	}
	// By name, because a map has no order and a status that reshuffles on every
	// write is a status that never compares equal to itself.
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
