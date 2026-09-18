package projection

import (
	"fmt"
	"strings"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// CheckWriteBinds refuses a projection whose write statements name a bind
// parameter that nothing would supply.
//
// A write is bound from one flat map: the parameters the server fills in on
// every statement, every column the version's mapping reads, and whatever the
// query declares under parameters. A statement is free to name any of those.
// What it must not do is name something else — a column the mapping does not
// read, a typo, a parameter only a list binds — because the pool used to
// answer that with NULL and say nothing, and for a write NULL is a value:
// "SET total_cents = :total_cents" with nothing to bind there wipes the column
// and answers 200.
//
// The case that makes this a per-version check is a kind served at several
// versions. The write statements are shared and the mappings are not, so a
// secondary version that maps fewer columns than the statement sets writes NULL
// into every column it left out, on every write through it. conversion: None
// exists to let versions differ on purpose; it does not make that write mean
// anything. So the statements are checked against each served version's own
// mapping, and the refusal names the version, the verb and the parameter.
//
// deleteCollection is the one write with no object behind it, so it can name
// only a declared parameter or one of the server's.
func CheckWriteBinds(spec *crispv1alpha1.CustomResourceProjectionSpec) error {
	driver := spec.DataSource.Driver
	versions := writeVersions(spec)

	for _, write := range writeQueries(&spec.Queries) {
		for _, statement := range statementsOf(write.query) {
			_, names, err := crispsql.Rewrite(statement, driver)
			if err != nil {
				return fmt.Errorf("queries.%s: %w", write.verb, err)
			}
			names = distinct(names)

			if !write.hasObject {
				supplied := suppliedBinds(nil, write.query.Parameters)
				for _, name := range names {
					if supplied[name] {
						continue
					}
					return fmt.Errorf(
						"queries.%s names :%s, which nothing supplies: a %s statement runs with no object, so it can name a parameter declared under queries.%s.parameters or one the server binds (%s)",
						write.verb, name, write.verb, write.verb, strings.Join(ServerBinds(), ", "))
				}
				continue
			}

			for _, version := range versions {
				supplied := suppliedBinds(version.mapping, write.query.Parameters)
				for _, name := range names {
					if supplied[name] {
						continue
					}
					return fmt.Errorf(
						"version %s: queries.%s names :%s, which nothing supplies for that version, so the statement would write NULL there: a write statement can name a column the version's mapping reads, a parameter declared under queries.%s.parameters, or one the server binds (%s)",
						version.name, write.verb, name, write.verb, strings.Join(ServerBinds(), ", "))
				}
			}
		}
	}
	return nil
}

// writeVersion is one served version with the mapping its writes are bound
// through.
type writeVersion struct {
	name    string
	mapping *crispv1alpha1.Mapping
}

// writeVersions lists the served versions and the mapping each binds with. A
// version without a mapping of its own binds through the projection's, which
// is the same rule the apiserver compiles it by.
func writeVersions(spec *crispv1alpha1.CustomResourceProjectionSpec) []writeVersion {
	res := spec.Resource
	versions := []writeVersion{{name: res.Version, mapping: &spec.Mapping}}
	for i := range res.Versions {
		extra := &res.Versions[i]
		if extra.Served != nil && !*extra.Served {
			continue
		}
		mapping := extra.Mapping
		if mapping == nil {
			mapping = &spec.Mapping
		}
		versions = append(versions, writeVersion{name: extra.Name, mapping: mapping})
	}
	return versions
}

// writeQuery is one write verb's query, and whether an object is bound when it
// runs.
type writeQuery struct {
	verb      string
	query     *crispv1alpha1.Query
	hasObject bool
}

// writeQueries lists the write verbs a projection declares. Named one at a
// time, as the apiserver's statement check names them, so a write added to the
// API without a decision here is a compile error rather than a statement that
// is quietly not checked.
func writeQueries(qs *crispv1alpha1.Queries) []writeQuery {
	var out []writeQuery
	for _, candidate := range []writeQuery{
		{verb: "create", query: qs.Create, hasObject: true},
		{verb: "update", query: qs.Update, hasObject: true},
		{verb: "updateStatus", query: qs.UpdateStatus, hasObject: true},
		{verb: "delete", query: qs.Delete, hasObject: true},
		{verb: "markDeleted", query: qs.MarkDeleted, hasObject: true},
		{verb: "deleteCollection", query: qs.DeleteCollection, hasObject: false},
	} {
		if candidate.query != nil {
			out = append(out, candidate)
		}
	}
	return out
}

// statementsOf is every statement a query runs. A query that sets both, or
// neither, is refused when the registry compiles it; here it is simply read.
func statementsOf(query *crispv1alpha1.Query) []string {
	if len(query.Statements) > 0 {
		return query.Statements
	}
	if query.SQL == "" {
		return nil
	}
	return []string{query.SQL}
}

// suppliedBinds is every name a write bound through mapping can reference: the
// server's own, the mapped columns, and the declared parameters. A nil mapping
// is a write with no object behind it.
func suppliedBinds(mapping *crispv1alpha1.Mapping, params []crispv1alpha1.QueryParameter) map[string]bool {
	supplied := map[string]bool{}
	for _, name := range ServerBinds() {
		supplied[name] = true
	}
	for name := range MappingColumnNames(mapping) {
		supplied[name] = true
	}
	for _, p := range params {
		supplied[p.Name] = true
	}
	return supplied
}

// distinct keeps the first occurrence of each name. A driver with positional
// placeholders reports a parameter once per use, and a refusal should name it
// once.
func distinct(names []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, name)
	}
	return out
}
