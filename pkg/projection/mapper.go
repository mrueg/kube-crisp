// Package projection turns SQL result rows into Kubernetes API objects
// according to a CustomResourceProjection's mapping rules.
package projection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// Mapper converts result rows into objects of a single projected kind.
type Mapper struct {
	gvk        schema.GroupVersionKind
	mapping    crispv1alpha1.Mapping
	namespaced bool

	// separator joins the parts of a composite name and splits them back.
	separator string

	// fields is mapping.Fields with each destination already split into the
	// path the unstructured helpers take. Splitting it per field per row made
	// a ten thousand row list allocate forty thousand slices to say something
	// the projection stated once.
	fields []mappedField

	// shared are the columns mapped both as a label or annotation and as a
	// field, where only one of them can win a write.
	shared []sharedColumn
}

// mappedField is one column-to-field rule with its destination pre-split.
type mappedField struct {
	column    string
	path      []string
	fieldType crispv1alpha1.FieldType
	omitEmpty bool
}

// NewMapper validates a projection's mapping and returns a Mapper for it.
func NewMapper(res crispv1alpha1.ProjectedResource, mapping crispv1alpha1.Mapping) (*Mapper, error) {
	switch {
	case mapping.Name == "" && len(mapping.NameColumns) == 0:
		return nil, fmt.Errorf("mapping.name or mapping.nameColumns is required")
	case mapping.Name != "" && len(mapping.NameColumns) > 0:
		return nil, fmt.Errorf("set either mapping.name or mapping.nameColumns, not both")
	}

	separator := mapping.NameSeparator
	if len(mapping.NameColumns) > 0 {
		if separator == "" {
			separator = DefaultNameSeparator
		}
		if errs := validation.IsDNS1123Subdomain("a" + separator + "b"); len(errs) > 0 {
			return nil, fmt.Errorf("mapping.nameSeparator %q cannot appear in an object name: %s",
				separator, strings.Join(errs, "; "))
		}
		for _, column := range mapping.NameColumns {
			if column == "" {
				return nil, fmt.Errorf("mapping.nameColumns: a column name is empty")
			}
		}
	}

	namespaced := res.Scope == crispv1alpha1.NamespaceScoped
	if namespaced && mapping.Namespace == "" {
		return nil, fmt.Errorf("mapping.namespace is required for namespaced projections")
	}
	if !namespaced && mapping.Namespace != "" {
		return nil, fmt.Errorf("mapping.namespace must be empty for cluster-scoped projections")
	}

	fields := make([]mappedField, 0, len(mapping.Fields))
	for _, f := range mapping.Fields {
		if f.Column == "" {
			return nil, fmt.Errorf("mapping.fields: column is required")
		}
		if err := validatePath(f.Path); err != nil {
			return nil, fmt.Errorf("mapping.fields[%s]: %w", f.Column, err)
		}
		fields = append(fields, mappedField{
			column:    f.Column,
			path:      strings.Split(f.Path, "."),
			fieldType: f.Type,
			omitEmpty: f.OmitEmpty,
		})
	}

	// The keys of these come from the projection rather than from a row, so
	// they are the same for every object this mapper will ever build. Checking
	// them here is both an earlier error and one regular expression instead of
	// one per key per row.
	for key := range mapping.Labels {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return nil, fmt.Errorf("mapping.labels: %q is not a valid label key: %s",
				key, strings.Join(errs, "; "))
		}
	}
	for key := range mapping.Annotations {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return nil, fmt.Errorf("mapping.annotations: %q is not a valid annotation key: %s",
				key, strings.Join(errs, "; "))
		}
	}

	return &Mapper{
		gvk:        schema.GroupVersionKind{Group: res.Group, Version: res.Version, Kind: res.Kind},
		mapping:    mapping,
		namespaced: namespaced,
		separator:  separator,
		fields:     fields,
		shared:     sharedColumns(mapping, fields),
	}, nil
}

// sharedColumn is a column the projection reads twice: once through a label or
// annotation key, and once through a field path.
//
// Reading it twice is a reasonable thing to want — select on it as a label,
// show it as a field — and on the way out both are filled from the same column,
// so they always agree. Writing is where they can disagree, and only one of
// them can win.
type sharedColumn struct {
	column string

	// kind is "label" or "annotation", and key is the one it is read under.
	kind string
	key  string

	// path is where the field mapping reads the same column from.
	path []string
}

// sharedColumns finds the columns a projection maps both ways.
func sharedColumns(mapping crispv1alpha1.Mapping, fields []mappedField) []sharedColumn {
	byColumn := make(map[string][]string, len(fields))
	for i := range fields {
		byColumn[fields[i].column] = fields[i].path
	}

	var shared []sharedColumn
	for key, column := range mapping.Labels {
		if path, ok := byColumn[column]; ok {
			shared = append(shared, sharedColumn{column: column, kind: "label", key: key, path: path})
		}
	}
	for key, column := range mapping.Annotations {
		if path, ok := byColumn[column]; ok {
			shared = append(shared, sharedColumn{column: column, kind: "annotation", key: key, path: path})
		}
	}

	// A map has no order and this ends up in a warning message and a log line.
	sort.Slice(shared, func(i, j int) bool {
		if shared[i].column != shared[j].column {
			return shared[i].column < shared[j].column
		}
		return shared[i].key < shared[j].key
	})
	return shared
}

// SharedColumns describes the columns this projection maps both as a
// label or annotation and as a field, for reporting when a projection loads.
func (m *Mapper) SharedColumns() []string {
	out := make([]string, 0, len(m.shared))
	for _, sc := range m.shared {
		out = append(out, fmt.Sprintf("column %q is both %s %q and field %s",
			sc.column, sc.kind, sc.key, strings.Join(sc.path, ".")))
	}
	return out
}

// DroppedOnWrite reports the labels and annotations a write would discard.
//
// A column mapped both ways can only be written from one of them, and the field
// is the one that wins: Params binds the label first and the field mapping
// overwrites it. That is a defensible choice — the field names an exact path
// while the label is a view of it — but it is not one anybody can see. Changing
// the label alone was answered 200, and kubectl said "labeled", and the row did
// not move.
//
// So the write still goes through as it always did, and the client is told what
// of it was ignored.
func (m *Mapper) DroppedOnWrite(obj *unstructured.Unstructured) []string {
	if len(m.shared) == 0 {
		return nil
	}

	labels := obj.GetLabels()
	annotations := obj.GetAnnotations()

	var dropped []string
	for _, sc := range m.shared {
		asMetadata, present := labels[sc.key]
		if sc.kind == "annotation" {
			asMetadata, present = annotations[sc.key]
		}

		value, found, err := unstructured.NestedFieldNoCopy(obj.Object, sc.path...)
		if err != nil {
			continue
		}
		asField := ""
		if found && value != nil {
			asField = fmt.Sprint(value)
		}

		// Absent on both sides is agreement, not a conflict.
		if !present && !found {
			continue
		}
		if asMetadata == asField {
			continue
		}
		dropped = append(dropped, fmt.Sprintf(
			"%s %q was not written: it shares column %q with field %s, which the write set to %q",
			sc.kind, sc.key, sc.column, strings.Join(sc.path, "."), asField))
	}
	return dropped
}

// DefaultNameSeparator joins the parts of a composite name.
const DefaultNameSeparator = "-"

// NameColumns reports the columns the object's name is built from, which is one
// column for the ordinary case.
func (m *Mapper) NameColumns() []string {
	if len(m.mapping.NameColumns) > 0 {
		return m.mapping.NameColumns
	}
	return []string{m.mapping.Name}
}

// OwnerReferenceColumn reports where ownerReferences are stored, if anywhere.
func (m *Mapper) OwnerReferenceColumn() string { return m.mapping.OwnerReferences }

// SplitName turns an object name back into the column values it was built
// from, so a query can ask for the row it names.
//
// A name that does not split into the right number of parts cannot refer to a
// row, and saying so beats binding nonsense and reporting whatever the database
// makes of it.
func (m *Mapper) SplitName(name string) (map[string]any, error) {
	columns := m.NameColumns()
	if len(columns) == 1 {
		return map[string]any{columns[0]: name}, nil
	}

	parts := strings.Split(name, m.separator)
	if len(parts) != len(columns) {
		return nil, fmt.Errorf("name %q does not split into %d parts on %q",
			name, len(columns), m.separator)
	}

	args := make(map[string]any, len(columns))
	for i, column := range columns {
		if parts[i] == "" {
			return nil, fmt.Errorf("name %q has an empty part for column %q", name, column)
		}
		args[column] = parts[i]
	}
	return args, nil
}

// NameFrom builds an object's name out of a row, for a caller that wants the
// identity without mapping the whole object — a tombstone row has nothing else
// in it to map.
func (m *Mapper) NameFrom(row crispsql.Row) (string, error) {
	name, err := m.buildName(row)
	if err != nil {
		return "", err
	}
	if !isDNS1123Subdomain(name) {
		return "", fmt.Errorf("%q is not a valid object name: %s",
			name, strings.Join(validation.IsDNS1123Subdomain(name), "; "))
	}
	return name, nil
}

// NamespaceFrom reads the namespace column out of a row.
func (m *Mapper) NamespaceFrom(row crispsql.Row) (string, error) {
	if !m.namespaced {
		return "", nil
	}
	return requiredString(row, m.mapping.Namespace, "namespace")
}

// buildName assembles an object's name out of a row.
func (m *Mapper) buildName(row crispsql.Row) (string, error) {
	columns := m.NameColumns()
	if len(columns) == 1 {
		return requiredString(row, columns[0], "name")
	}

	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		value, err := requiredString(row, column, "name")
		if err != nil {
			return "", err
		}
		// Two rows must never produce one name. A part carrying the separator
		// would do exactly that, so it is refused rather than escaped.
		if strings.Contains(value, m.separator) {
			return "", fmt.Errorf("column %q contains the name separator %q, so the name it would build is ambiguous",
				column, m.separator)
		}
		parts = append(parts, value)
	}
	return strings.Join(parts, m.separator), nil
}

// dns1123SubdomainMaxLength is what a DNS-1123 subdomain, and so an object
// name, may not exceed.
const dns1123SubdomainMaxLength = 253

// isDNS1123Subdomain reports whether a name is one Kubernetes will accept.
//
// The same question k8s.io/apimachinery/pkg/util/validation answers, asked
// without a regular expression. It runs once per row, so on a ten thousand row
// list it is ten thousand regex matches — around a tenth of the time a list
// spends, for a question that is a character scan. The same reasoning as
// ValidateSessionVariableName, at a much higher frequency.
//
// A name that fails still gets its message from apimachinery, so the two never
// disagree about why: only the common answer is computed here.
func isDNS1123Subdomain(name string) bool {
	if len(name) == 0 || len(name) > dns1123SubdomainMaxLength {
		return false
	}

	// A label is a run of lower-case alphanumerics and hyphens that neither
	// begins nor ends with a hyphen; a subdomain is labels joined by dots.
	labelStart := true
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case c == '.':
			// Neither an empty label nor one ending in a hyphen.
			if labelStart || name[i-1] == '-' {
				return false
			}
			labelStart = true
		case (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'):
			labelStart = false
		case c == '-':
			// A label may not begin with a hyphen.
			if labelStart {
				return false
			}
		default:
			return false
		}
	}

	// The final label is subject to the same two rules.
	return !labelStart && name[len(name)-1] != '-'
}

// validatePath rejects destinations that would collide with identity fields
// that the mapping sets explicitly.
func validatePath(path string) error {
	if path == "" {
		return fmt.Errorf("path is required")
	}
	parts := strings.Split(path, ".")
	for _, p := range parts {
		if p == "" {
			return fmt.Errorf("path %q has an empty segment", path)
		}
	}
	switch parts[0] {
	case "metadata":
		return fmt.Errorf("path %q may not target metadata; use the dedicated mapping fields", path)
	case "apiVersion", "kind":
		return fmt.Errorf("path %q may not target %s", path, parts[0])
	}
	return nil
}

// Row converts one result row into a projected object.
func (m *Mapper) Row(row crispsql.Row) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(m.gvk)

	name, err := m.buildName(row)
	if err != nil {
		return nil, err
	}
	if !isDNS1123Subdomain(name) {
		return nil, fmt.Errorf("column %q produced %q, which is not a valid object name: %s",
			m.mapping.Name, name, strings.Join(validation.IsDNS1123Subdomain(name), "; "))
	}
	obj.SetName(name)

	if m.namespaced {
		ns, err := requiredString(row, m.mapping.Namespace, "namespace")
		if err != nil {
			return nil, err
		}
		obj.SetNamespace(ns)
	}

	// Every object gets a UID. Controllers use it for owner references and to
	// tell "same name, different object" apart, so an empty one produces
	// malformed ownerReferences and caches that key by UID misbehave.
	if col := m.mapping.UID; col != "" {
		uid, err := optionalString(row, col)
		if err != nil {
			return nil, err
		}
		if uid != "" {
			obj.SetUID(types.UID(uid))
		}
	}

	if col := m.mapping.ResourceVersion; col != "" {
		rv, err := optionalString(row, col)
		if err != nil {
			return nil, err
		}
		if rv != "" {
			obj.SetResourceVersion(rv)
		}
	}

	if col := m.mapping.CreationTimestamp; col != "" {
		raw, ok := row[col]
		if !ok {
			return nil, fmt.Errorf("mapping.creationTimestamp references column %q, which the query did not return", col)
		}
		if raw != nil {
			ts, err := coerce(raw, crispv1alpha1.FieldTypeTimestamp)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col, err)
			}
			if err := unstructured.SetNestedField(obj.Object, ts, "metadata", "creationTimestamp"); err != nil {
				return nil, err
			}
		}
	}

	if col := m.mapping.DeletionTimestamp; col != "" {
		raw, ok := row[col]
		if !ok {
			return nil, fmt.Errorf("mapping.deletionTimestamp references column %q, which the query did not return", col)
		}
		// NULL is the ordinary case: the object is not being deleted. Only a
		// value means terminating, so an absent one must not be stamped.
		if raw != nil {
			ts, err := coerce(raw, crispv1alpha1.FieldTypeTimestamp)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col, err)
			}
			if err := unstructured.SetNestedField(obj.Object, ts, "metadata", "deletionTimestamp"); err != nil {
				return nil, err
			}
		}
	}

	if col := m.mapping.Generation; col != "" {
		raw, ok := row[col]
		if !ok {
			return nil, fmt.Errorf("mapping.generation references column %q, which the query did not return", col)
		}
		if raw != nil {
			value, err := coerce(raw, crispv1alpha1.FieldTypeInteger)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", col, err)
			}
			generation, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("column %q produced %T, which is not a generation", col, value)
			}
			obj.SetGeneration(generation)
		}
	}

	if col := m.mapping.Finalizers; col != "" {
		finalizers, err := decodeStrings(row, col, "finalizers")
		if err != nil {
			return nil, err
		}
		if len(finalizers) > 0 {
			obj.SetFinalizers(finalizers)
		}
	}

	if col := m.mapping.OwnerReferences; col != "" {
		owners, err := decodeOwnerReferences(row, col)
		if err != nil {
			return nil, err
		}
		if len(owners) > 0 {
			obj.SetOwnerReferences(owners)
		}
	}

	if col := m.mapping.ManagedFields; col != "" {
		managed, err := decodeManagedFields(row, col)
		if err != nil {
			return nil, err
		}
		if len(managed) > 0 {
			obj.SetManagedFields(managed)
		}
	}

	if m.mapping.LabelsFrom != "" || len(m.mapping.Labels) > 0 {
		// The JSON column first, then the per-key columns on top: a key named
		// in both is read from its own column, so it can be promoted out of the
		// JSON without moving the rest.
		labels, err := decodeStringMap(row, m.mapping.LabelsFrom, "labelsFrom")
		if err != nil {
			return nil, err
		}
		for key, col := range m.mapping.Labels {
			v, err := optionalString(row, col)
			if err != nil {
				return nil, err
			}
			if v == "" {
				continue
			}
			labels[key] = v
		}
		for key, value := range labels {
			// A key the mapping names was checked when the mapper was built and
			// is the same for every row. Only what a JSON column produced can
			// differ, so only that is worth a regular expression per object.
			if _, declared := m.mapping.Labels[key]; !declared {
				if errs := validation.IsQualifiedName(key); len(errs) > 0 {
					return nil, fmt.Errorf("%q is not a valid label key: %s", key, strings.Join(errs, "; "))
				}
			}
			// Values come out of a column either way, so they are always new.
			if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
				return nil, fmt.Errorf("label %q has an invalid value: %s", key, strings.Join(errs, "; "))
			}
		}
		if len(labels) > 0 {
			obj.SetLabels(labels)
		}
	}

	if m.mapping.AnnotationsFrom != "" || len(m.mapping.Annotations) > 0 {
		annotations, err := decodeStringMap(row, m.mapping.AnnotationsFrom, "annotationsFrom")
		if err != nil {
			return nil, err
		}
		for key, col := range m.mapping.Annotations {
			v, err := optionalString(row, col)
			if err != nil {
				return nil, err
			}
			if v != "" {
				annotations[key] = v
			}
		}
		// Annotation values are arbitrary; only the key is constrained — and a
		// key the mapping names was already checked when the mapper was built.
		for key := range annotations {
			if _, declared := m.mapping.Annotations[key]; declared {
				continue
			}
			if errs := validation.IsQualifiedName(key); len(errs) > 0 {
				return nil, fmt.Errorf("%q is not a valid annotation key: %s", key, strings.Join(errs, "; "))
			}
		}
		if len(annotations) > 0 {
			obj.SetAnnotations(annotations)
		}
	}

	if obj.GetUID() == "" {
		obj.SetUID(m.derivedUID(obj))
	}

	for i := range m.fields {
		f := &m.fields[i]

		raw, ok := row[f.column]
		if !ok {
			return nil, fmt.Errorf("mapping references column %q, which the query did not return", f.column)
		}
		if raw == nil && f.omitEmpty {
			continue
		}

		var value any
		if raw != nil {
			value, err = coerce(raw, f.fieldType)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", f.column, err)
			}
		}

		// NoCopy: coerce hands back something built for this object — a scalar,
		// or a structure normalizeJSON has already copied out of the row. The
		// row itself can be shared with other readers answering the same query,
		// so the rule that matters is that nothing here points into it, and
		// nothing does.
		if err := setNestedFieldNoCopy(obj.Object, value, f.path...); err != nil {
			return nil, fmt.Errorf("setting %s from column %q: %w",
				strings.Join(f.path, "."), f.column, err)
		}
	}

	return obj, nil
}

// setNestedFieldNoCopy places a value at a dotted path, creating the maps along
// the way.
//
// It is unstructured.SetNestedField without the deep copy of the value, which
// apimachinery does not export. The copy is there because a caller may hand in
// something it still holds a reference to; nothing here does. Every value set
// through this was built for the object being built — a scalar out of coerce,
// or a structure normalizeJSON already copied out of the row — and the row a
// value came from can be shared with other readers answering the same query,
// which is exactly why nothing must point back into it.
func setNestedFieldNoCopy(obj map[string]any, value any, fields ...string) error {
	m := obj
	for i, field := range fields[:len(fields)-1] {
		if existing, ok := m[field]; ok {
			nested, ok := existing.(map[string]any)
			if !ok {
				return fmt.Errorf("cannot set %s: %s is not an object",
					strings.Join(fields, "."), strings.Join(fields[:i+1], "."))
			}
			m = nested
			continue
		}
		nested := map[string]any{}
		m[field] = nested
		m = nested
	}
	m[fields[len(fields)-1]] = value
	return nil
}

// uidNamespace scopes derived UIDs to kube-crisp, so they cannot collide with
// UUIDs generated elsewhere. It is itself a v5 UUID of the project's API group.
var uidNamespace = uuid.NewSHA1(uuid.NameSpaceDNS, []byte("crisp.kubecrisp.io"))

// derivedUID produces a stable UID for a projection that does not map one.
//
// It is deterministic, which is the property that matters: every replica and
// every restart derives the same UID for the same row, so owner references keep
// resolving. When the projection maps a creation timestamp it is included, so
// that a row deleted and recreated under the same name is recognisably a
// different object; without one, the name is all there is to go on.
func (m *Mapper) derivedUID(obj *unstructured.Unstructured) types.UID {
	// The version is deliberately absent: the same row served at v1alpha1 and
	// v1beta1 is one object, and an owner reference written against one version
	// has to keep resolving when read through the other.
	identity := strings.Join([]string{
		m.gvk.Group, m.gvk.Kind,
		obj.GetNamespace(), obj.GetName(),
		obj.GetCreationTimestamp().UTC().Format(time.RFC3339Nano),
	}, "/")

	return types.UID(uuid.NewSHA1(uidNamespace, []byte(identity)).String())
}

// coerce converts a driver value into the JSON type the mapping asks for.
// Only types accepted by unstructured objects are produced: string, int64,
// float64, bool, and nested maps or slices decoded from JSON.
func coerce(raw any, t crispv1alpha1.FieldType) (any, error) {
	if t == "" {
		t = crispv1alpha1.FieldTypeString
	}

	// Drivers hand back []byte for text and numeric columns often enough that
	// normalising first keeps every case below simple.
	if b, ok := raw.([]byte); ok {
		raw = string(b)
	}

	switch t {
	case crispv1alpha1.FieldTypeString:
		return toString(raw)

	case crispv1alpha1.FieldTypeInteger:
		switch v := raw.(type) {
		case int64:
			return v, nil
		case int32:
			return int64(v), nil
		case int:
			return int64(v), nil
		case float64:
			if v != float64(int64(v)) {
				return nil, fmt.Errorf("value %v is not an integer", v)
			}
			return int64(v), nil
		case uint64:
			// MySQL BIGINT UNSIGNED reaches past what an int64 holds, and a
			// JSON number cannot represent it either. Saying so beats dropping
			// the row, which is what happened before: with the default
			// onUnmappableRow the object simply stopped existing.
			if v > math.MaxInt64 {
				return nil, fmt.Errorf(
					"value %d does not fit an integer field; map this column as type: string", v)
			}
			return int64(v), nil
		case uint32:
			return int64(v), nil
		case uint:
			// A uint is never wider than a uint64, so the case above already
			// says everything true about it — including the range check, which
			// is worth having in exactly one place.
			return coerce(uint64(v), t)
		case string:
			return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		case []byte:
			// The MySQL driver hands back []byte for a numeric column read
			// through the binary protocol, which is what preparedStatements
			// turns on — so the same projection behaved differently depending
			// on a flag that has nothing to do with types.
			return strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to integer", raw)
		}

	case crispv1alpha1.FieldTypeNumber:
		switch v := raw.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case uint64:
			return float64(v), nil
		case string:
			// A number field is a float64, which is what JSON has. PostgreSQL
			// hands back NUMERIC as a string and MySQL DECIMAL as bytes, and
			// both can hold more digits than a float64 keeps — so a currency
			// column mapped here loses its last places silently. Map such a
			// column as type: string to carry it exactly; the reference says so.
			return strconv.ParseFloat(strings.TrimSpace(v), 64)
		case []byte:
			return strconv.ParseFloat(strings.TrimSpace(string(v)), 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to number", raw)
		}

	case crispv1alpha1.FieldTypeBoolean:
		switch v := raw.(type) {
		case bool:
			return v, nil
		case int64:
			return v != 0, nil
		case string:
			return strconv.ParseBool(strings.TrimSpace(v))
		default:
			return nil, fmt.Errorf("cannot convert %T to boolean", raw)
		}

	case crispv1alpha1.FieldTypeTimestamp:
		// RFC3339Nano rather than RFC3339: a column recording microseconds is
		// common, and rounding it to the second on the way out — and again on
		// the way back in — loses data the row actually holds.
		switch v := raw.(type) {
		case time.Time:
			return v.UTC().Format(time.RFC3339Nano), nil
		case string:
			parsed, err := parseTimestamp(v)
			if err != nil {
				return nil, err
			}
			return parsed.UTC().Format(time.RFC3339Nano), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to timestamp", raw)
		}

	case crispv1alpha1.FieldTypeJSON:
		// The json_agg path hands back values that are already decoded.
		switch raw.(type) {
		case map[string]any, []any:
			return normalizeJSON(raw), nil
		}

		s, err := toString(raw)
		if err != nil {
			return nil, err
		}
		var decoded any
		if err := json.Unmarshal([]byte(s), &decoded); err != nil {
			return nil, fmt.Errorf("parsing JSON column: %w", err)
		}
		// json.Unmarshal yields float64 for every number; unstructured objects
		// require int64 for integral values.
		return normalizeJSON(decoded), nil

	default:
		return nil, fmt.Errorf("unknown field type %q", t)
	}
}

// parseTimestamp accepts the layouts drivers commonly hand back for text dates.
func parseTimestamp(s string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		// PostgreSQL renders a timestamptz as text with a two-digit offset.
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as a timestamp", s)
}

// normalizeJSON rewrites decoded JSON so that it only contains types the
// unstructured converter accepts.
func normalizeJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		// Copied rather than normalised in place: the row this came from can be
		// shared with other readers answering the same query, and an object
		// must never hold a reference into it.
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = normalizeJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = normalizeJSON(val)
		}
		return out
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	default:
		return v
	}
}

// decodeStrings reads a column holding a JSON array of strings. An empty or
// NULL column is no list at all rather than an error: most rows have none.
func decodeStrings(row crispsql.Row, col, field string) ([]string, error) {
	raw, ok := row[col]
	if !ok {
		return nil, fmt.Errorf("mapping.%s references column %q, which the query did not return", field, col)
	}
	if raw == nil {
		return nil, nil
	}

	// The json_agg path hands back a decoded value already.
	if decoded, ok := raw.([]any); ok {
		out := make([]string, 0, len(decoded))
		for _, item := range decoded {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("column %q holds %T where %s expects a string", col, item, field)
			}
			out = append(out, text)
		}
		return out, nil
	}

	text, err := toString(raw)
	if err != nil {
		return nil, fmt.Errorf("column %q: %w", col, err)
	}
	if strings.TrimSpace(text) == "" {
		return nil, nil
	}

	var out []string
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return nil, fmt.Errorf("column %q does not hold a JSON array of strings for %s: %w", col, field, err)
	}
	return out, nil
}

// decodeStringMap reads a column holding a JSON object of strings, which is
// what a table with per-row labels keeps them in. An empty column name means
// there is no such column, and the result is an empty map rather than an error.
func decodeStringMap(row crispsql.Row, col, field string) (map[string]string, error) {
	if col == "" {
		return map[string]string{}, nil
	}

	raw, ok := row[col]
	if !ok {
		return nil, fmt.Errorf("mapping.%s references column %q, which the query did not return", field, col)
	}
	if raw == nil {
		return map[string]string{}, nil
	}

	// The json_agg path and a jsonb column hand back a decoded value already.
	if decoded, ok := raw.(map[string]any); ok {
		out := make(map[string]string, len(decoded))
		for key, value := range decoded {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("column %q holds %T for key %q, where %s expects a string", col, value, key, field)
			}
			out[key] = text
		}
		return out, nil
	}

	text, err := toString(raw)
	if err != nil {
		return nil, fmt.Errorf("column %q: %w", col, err)
	}
	if strings.TrimSpace(text) == "" || strings.TrimSpace(text) == "null" {
		return map[string]string{}, nil
	}

	out := map[string]string{}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return nil, fmt.Errorf("column %q does not hold a JSON object of strings for %s: %w", col, field, err)
	}
	return out, nil
}

// decodeOwnerReferences reads a column holding metadata.ownerReferences.
//
// A malformed reference is refused rather than dropped: an owner reference the
// garbage collector cannot resolve is how objects get collected by surprise.
func decodeOwnerReferences(row crispsql.Row, col string) ([]metav1.OwnerReference, error) {
	raw, ok := row[col]
	if !ok {
		return nil, fmt.Errorf("mapping.ownerReferences references column %q, which the query did not return", col)
	}
	if raw == nil {
		return nil, nil
	}

	var encoded []byte
	switch value := raw.(type) {
	case []byte:
		encoded = value
	case string:
		encoded = []byte(value)
	default:
		// Already decoded, by json_agg or a jsonb column.
		remarshalled, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col, err)
		}
		encoded = remarshalled
	}
	if len(bytes.TrimSpace(encoded)) == 0 || string(bytes.TrimSpace(encoded)) == "null" {
		return nil, nil
	}

	var owners []metav1.OwnerReference
	if err := json.Unmarshal(encoded, &owners); err != nil {
		return nil, fmt.Errorf("column %q does not hold JSON ownerReferences: %w", col, err)
	}
	for i, owner := range owners {
		switch {
		case owner.APIVersion == "":
			return nil, fmt.Errorf("column %q: ownerReferences[%d] has no apiVersion", col, i)
		case owner.Kind == "":
			return nil, fmt.Errorf("column %q: ownerReferences[%d] has no kind", col, i)
		case owner.Name == "":
			return nil, fmt.Errorf("column %q: ownerReferences[%d] has no name", col, i)
		case owner.UID == "":
			// Without one the garbage collector cannot tell the named owner
			// from a later object with the same name.
			return nil, fmt.Errorf("column %q: ownerReferences[%d] has no uid", col, i)
		}
	}
	return owners, nil
}

// decodeManagedFields reads a column holding metadata.managedFields.
//
// Unlike ownerReferences these are not validated beyond being well-formed JSON:
// the apiserver's field manager writes them and reads them back, and this
// server is only storage. A malformed value is refused rather than dropped,
// because silently losing ownership is what makes an apply overwrite something
// it should have conflicted with.
func decodeManagedFields(row crispsql.Row, col string) ([]metav1.ManagedFieldsEntry, error) {
	raw, ok := row[col]
	if !ok {
		return nil, fmt.Errorf("mapping.managedFields references column %q, which the query did not return", col)
	}
	if raw == nil {
		return nil, nil
	}

	var encoded []byte
	switch value := raw.(type) {
	case []byte:
		encoded = value
	case string:
		encoded = []byte(value)
	default:
		// Already decoded, by json_agg or a jsonb column.
		remarshalled, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col, err)
		}
		encoded = remarshalled
	}
	if len(bytes.TrimSpace(encoded)) == 0 || string(bytes.TrimSpace(encoded)) == "null" {
		return nil, nil
	}

	var managed []metav1.ManagedFieldsEntry
	if err := json.Unmarshal(encoded, &managed); err != nil {
		return nil, fmt.Errorf("column %q does not hold JSON managedFields: %w", col, err)
	}
	return managed, nil
}

// toString renders a driver value as text.
//
// This is the busiest conversion in the mapper and not only the one behind
// type: string. mapping.name, namespace, uid and resourceVersion all reach it
// through requiredString, labels and annotations through the map decoders, and
// the JSON branch of coerce reads its column through it too. A width it does
// not know is therefore not a missing corner of one field type — it is every
// row of the projection failing to map, and under the default
// onUnmappableRow: Skip the collection reads empty with a warning.
//
// Which width a driver picks is not something a projection can see or say
// anything about. go-sql-driver's text protocol — what a list query with no
// bind parameters gets — returns uint64 for an unsigned BIGINT of any
// magnitude, and float32 for a FLOAT column, while its binary protocol returns
// int64 for the same column until the value passes MaxInt64. So a MySQL table
// keyed by `id BIGINT UNSIGNED`, which is the idiomatic MySQL primary key, has
// to work, and has to keep working when preparedStatements is turned on.
//
// It is also what makes the remedy the integer branch names above — "map this
// column as type: string" — true. A string is the one JSON type that holds the
// whole unsigned 64-bit range exactly, so the advice has to lead somewhere.
func toString(raw any) (string, error) {
	switch v := raw.(type) {
	case string:
		return v, nil
	case []byte:
		// Drivers hand back bytes for text, and for jsonb in particular: pgx
		// returns every jsonb column this way. coerce normalises them before it
		// does anything else, and the decoders that read a column directly have
		// to see the same thing.
		return string(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case uint64:
		// FormatUint rather than a detour through int64: the values that make
		// this branch necessary are exactly the ones an int64 cannot hold.
		return strconv.FormatUint(v, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case float32:
		// Formatted at the precision the value actually has. Widening a float32
		// to a float64 first and printing that prints the rounding error along
		// with it: 0.1 read from a MySQL FLOAT would render as 0.10000000149.
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case bool:
		return strconv.FormatBool(v), nil
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", fmt.Errorf("cannot convert %T to string", raw)
	}
}

func requiredString(row crispsql.Row, column, field string) (string, error) {
	raw, ok := row[column]
	if !ok {
		return "", fmt.Errorf("mapping.%s references column %q, which the query did not return", field, column)
	}
	if raw == nil {
		return "", fmt.Errorf("mapping.%s column %q was NULL", field, column)
	}
	s, err := coerce(raw, crispv1alpha1.FieldTypeString)
	if err != nil {
		return "", fmt.Errorf("mapping.%s column %q: %w", field, column, err)
	}
	return s.(string), nil
}

func optionalString(row crispsql.Row, column string) (string, error) {
	raw, ok := row[column]
	if !ok {
		return "", fmt.Errorf("mapping references column %q, which the query did not return", column)
	}
	if raw == nil {
		return "", nil
	}
	s, err := coerce(raw, crispv1alpha1.FieldTypeString)
	if err != nil {
		return "", fmt.Errorf("column %q: %w", column, err)
	}
	return s.(string), nil
}

// Params builds the bind arguments for a write, reversing the mapping: each
// mapped column is read back out of the submitted object.
//
// The parameter names are the column names, so an INSERT can be written as
// "INSERT INTO orders (id, tenant, customer) VALUES (:id, :tenant, :customer)"
// using exactly the columns the projection already declares.
func (m *Mapper) Params(obj *unstructured.Unstructured) (map[string]any, error) {
	args := map[string]any{
		"name":      obj.GetName(),
		"namespace": obj.GetNamespace(),
	}

	identity, err := m.SplitName(obj.GetName())
	if err != nil {
		return nil, err
	}
	for column, value := range identity {
		args[column] = value
	}
	if m.namespaced {
		args[m.mapping.Namespace] = obj.GetNamespace()
	}
	if col := m.mapping.UID; col != "" && obj.GetUID() != "" {
		args[col] = string(obj.GetUID())
	}
	if col := m.mapping.ResourceVersion; col != "" && obj.GetResourceVersion() != "" {
		args[col] = obj.GetResourceVersion()
	}

	if col := m.mapping.Finalizers; col != "" {
		encoded, err := json.Marshal(obj.GetFinalizers())
		if err != nil {
			return nil, fmt.Errorf("encoding finalizers: %w", err)
		}
		args[col] = string(encoded)
	}
	if col := m.mapping.OwnerReferences; col != "" {
		encoded, err := json.Marshal(obj.GetOwnerReferences())
		if err != nil {
			return nil, fmt.Errorf("encoding ownerReferences: %w", err)
		}
		args[col] = string(encoded)
	}

	// Whatever field management computed for this write. It is written back
	// verbatim: the apiserver owns this field, and a projection that stores it
	// is only giving it somewhere to live between requests.
	if col := m.mapping.ManagedFields; col != "" {
		managed := obj.GetManagedFields()
		if len(managed) == 0 {
			args[col] = nil
		} else {
			encoded, err := json.Marshal(managed)
			if err != nil {
				return nil, fmt.Errorf("encoding managedFields: %w", err)
			}
			args[col] = string(encoded)
		}
	}

	// A label the object does not carry binds NULL, not the empty string. The
	// two mean different things in a database, and mapped fields already make
	// that distinction — an absent one is not written as "".
	labels := obj.GetLabels()
	for key, col := range m.mapping.Labels {
		if value, ok := labels[key]; ok {
			args[col] = value
		} else {
			args[col] = nil
		}
	}
	annotations := obj.GetAnnotations()
	for key, col := range m.mapping.Annotations {
		if value, ok := annotations[key]; ok {
			args[col] = value
		} else {
			args[col] = nil
		}
	}

	// The JSON columns carry everything that does not have a column of its own,
	// so a key promoted into mapping.labels is not also written here — it would
	// be stored twice and the two could disagree.
	if col := m.mapping.LabelsFrom; col != "" {
		encoded, err := encodeStringMap(labels, m.mapping.Labels)
		if err != nil {
			return nil, fmt.Errorf("encoding labels: %w", err)
		}
		args[col] = encoded
	}
	if col := m.mapping.AnnotationsFrom; col != "" {
		encoded, err := encodeStringMap(annotations, m.mapping.Annotations)
		if err != nil {
			return nil, fmt.Errorf("encoding annotations: %w", err)
		}
		args[col] = encoded
	}

	for i := range m.fields {
		f := &m.fields[i]

		value, found, err := unstructured.NestedFieldNoCopy(obj.Object, f.path...)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", strings.Join(f.path, "."), err)
		}
		if !found || value == nil {
			args[f.column] = nil
			continue
		}

		bound, err := bindValue(value, f.fieldType)
		if err != nil {
			return nil, fmt.Errorf("binding %s to column %q: %w",
				strings.Join(f.path, "."), f.column, err)
		}
		args[f.column] = bound
	}

	return args, nil
}

// encodeStringMap renders the entries that have no column of their own, and
// reports NULL rather than "{}" for an object carrying none: an empty column
// and an empty map read back the same way, and NULL is the cheaper of the two
// to store and to index around.
func encodeStringMap(all map[string]string, owned map[string]string) (any, error) {
	remaining := make(map[string]string, len(all))
	for key, value := range all {
		if _, hasColumn := owned[key]; hasColumn {
			continue
		}
		remaining[key] = value
	}
	if len(remaining) == 0 {
		return nil, nil
	}

	encoded, err := json.Marshal(remaining)
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

// FieldValue reads one dotted path out of an object and coerces it, for
// query parameters declared with the Field source.
func FieldValue(obj *unstructured.Unstructured, path string, t crispv1alpha1.FieldType) (any, error) {
	value, found, err := unstructured.NestedFieldNoCopy(obj.Object, strings.Split(path, ".")...)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	if !found || value == nil {
		return nil, nil
	}
	return bindValue(value, t)
}

// bindValue converts a value taken from an API object into something a driver
// can bind. Structured values are re-encoded as JSON so they can be written to
// json or jsonb columns.
func bindValue(value any, t crispv1alpha1.FieldType) (any, error) {
	switch t {
	case crispv1alpha1.FieldTypeJSON:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encoding JSON: %w", err)
		}
		return string(encoded), nil

	case crispv1alpha1.FieldTypeInteger:
		return coerce(value, crispv1alpha1.FieldTypeInteger)

	case crispv1alpha1.FieldTypeNumber:
		return coerce(value, crispv1alpha1.FieldTypeNumber)

	case crispv1alpha1.FieldTypeBoolean:
		return coerce(value, crispv1alpha1.FieldTypeBoolean)

	case crispv1alpha1.FieldTypeTimestamp:
		return coerce(value, crispv1alpha1.FieldTypeTimestamp)

	default:
		// Strings, and anything the schema allowed through as a scalar.
		switch v := value.(type) {
		case map[string]any, []any:
			encoded, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("encoding value: %w", err)
			}
			return string(encoded), nil
		default:
			return coerce(value, crispv1alpha1.FieldTypeString)
		}
	}
}

// CoerceValue converts a literal declared on a query parameter into the type
// the statement expects.
func CoerceValue(value string, t crispv1alpha1.FieldType) (any, error) {
	if t == "" || t == crispv1alpha1.FieldTypeString {
		return value, nil
	}

	// A json literal binds as the text it already is, which is what the Field
	// source does with the value it encodes. The row conversion is the wrong
	// direction here: it exists to turn a column into part of an object, so it
	// decodes JSON into a Go map — and database/sql cannot bind one of those.
	// The parameter reached the driver as "unsupported type
	// map[string]interface {}", so every request through a query declaring one
	// failed, whatever the literal said.
	//
	// Parsed only to be sure it is JSON at all. A malformed literal sent on to
	// the database would be answered by the database, in its own words, about
	// a value the client never supplied and cannot change.
	if t == crispv1alpha1.FieldTypeJSON {
		if !json.Valid([]byte(value)) {
			return nil, fmt.Errorf("the declared value is not valid JSON")
		}
		return value, nil
	}

	return coerce(value, t)
}
