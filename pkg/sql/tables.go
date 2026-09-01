package sql

import (
	"sort"
	"strings"
)

// Tables reports the tables a statement names.
//
// It exists so a projection can say what it needs from the database without
// anybody reading its SQL: the answer is reported in the projection's status,
// where it can be handed to whatever manages the schema. Nothing here creates
// or changes anything — this is a description of what is read, not a schema
// that will be applied.
//
// The scan is over statement text, sharing skipNonCode with the parameter
// rewriter, so a table name is never found inside a string literal, a comment,
// or a dollar-quoted body. It is deliberately conservative and deliberately
// incomplete:
//
//   - A common table expression is reported alongside the tables it reads, so
//     "WITH recent AS (SELECT ... FROM orders) SELECT * FROM recent" reports
//     both orders and recent. Naming something that is not a table is the safe
//     direction; failing to name one that is would not be.
//   - A table named only inside dynamic SQL, or reached through a view, is not
//     reported, because nothing in the statement text says so.
//   - What a call like extract(epoch FROM ts) reads is not reported, because
//     the scan steps over such calls whole. Their FROM is part of the
//     function's syntax rather than a clause, and reading it as one named a
//     column where a table belonged.
//
// Both are why the result is described as what the projection names rather than
// as a complete schema.
func Tables(stmt, driver string) []string {
	dialect := lexDialectFor(driver)

	var (
		found []string
		seen  = map[string]bool{}
	)
	add := func(name string) {
		name = strings.ToLower(name)
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		found = append(found, name)
	}

	for i := 0; i < len(stmt); {
		if end, ok := skipNonCode(stmt, i, dialect); ok {
			i = end
			continue
		}
		if !isNameStart(stmt[i]) {
			i++
			continue
		}

		j := i
		for j < len(stmt) && isNameChar(stmt[j]) {
			j++
		}
		keyword := stmt[i:j]

		// A handful of SQL functions spell an argument separator FROM, so the
		// FROM inside extract(epoch FROM f.last_update) introduces nothing.
		// Read as a clause it reported "f.last_update" as a table, which is a
		// column, in every projection deriving a resourceVersion from a
		// timestamp — the idiom the PostgreSQL tutorial recommends.
		//
		// The whole call is stepped over rather than the FROM alone. What these
		// take is an expression, so a table inside one would have to come from
		// a subquery nobody writes there, and skipping is the conservative
		// direction: a name missed is a name not invented.
		if fromTakingCall[strings.ToLower(keyword)] {
			if end, ok := skipCall(stmt, j, dialect); ok {
				i = end
				continue
			}
		}

		// INTO always introduces a table; FROM and JOIN may introduce a
		// subquery or a set-returning function instead.
		switch {
		case strings.EqualFold(keyword, "into"), strings.EqualFold(keyword, "update"):
			if name, next, ok := tableRef(stmt, j, dialect, false); ok {
				add(name)
				i = next
				continue
			}
		case strings.EqualFold(keyword, "from"), strings.EqualFold(keyword, "join"):
			if name, next, ok := tableRef(stmt, j, dialect, true); ok {
				add(name)
				i = next
				continue
			}
		}
		i = j
	}

	sort.Strings(found)
	return found
}

// tableRef reads the table reference following a keyword at i, reporting where
// it ended.
//
// rejectCalls distinguishes the two shapes a parenthesis can take. After FROM
// or JOIN, "generate_series(1, 10)" is a function and not a table. After
// INSERT INTO, "orders(id, tenant)" is a table followed by its column list, so
// the same punctuation means the opposite thing.
func tableRef(stmt string, i int, dialect lexDialect, rejectCalls bool) (string, int, bool) {
	for {
		i = skipSpace(stmt, i)
		if i >= len(stmt) {
			return "", i, false
		}

		// A quoted identifier is a name that needed quoting, so it is exactly
		// the name. skipNonCode steps over it as non-code, which is right for
		// every other reader and wrong here.
		if stmt[i] == '"' || stmt[i] == '`' {
			end := endOfDelimited(stmt, i, stmt[i])
			if end <= i+1 {
				return "", i, false
			}
			return strings.ReplaceAll(stmt[i+1:end-1], string(stmt[i])+string(stmt[i]), string(stmt[i])), end, true
		}

		if !isNameStart(stmt[i]) {
			// A subquery, a VALUES list, or punctuation. Either way there is no
			// name here to report.
			return "", i, false
		}

		j := i
		for j < len(stmt) && (isNameChar(stmt[j]) || stmt[j] == '.') {
			j++
		}
		word := stmt[i:j]

		switch {
		// ONLY qualifies the table that follows it rather than being one.
		case strings.EqualFold(word, "only"):
			i = j
			continue
		// A derived table, a lateral join, or a row constructor: whatever
		// follows is not a table name.
		case strings.EqualFold(word, "select"), strings.EqualFold(word, "lateral"),
			strings.EqualFold(word, "values"), strings.EqualFold(word, "unnest"):
			return "", j, false
		}

		if rejectCalls {
			if k := skipSpace(stmt, j); k < len(stmt) && stmt[k] == '(' {
				return "", j, false
			}
		}
		return word, j, true
	}
}

func skipSpace(stmt string, i int) int {
	for i < len(stmt) {
		switch stmt[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// fromTakingCall names the functions whose argument list contains a FROM that
// is part of their syntax rather than a clause: extract(field FROM source),
// substring(string FROM pattern), trim(side chars FROM string), and
// overlay(string placing string FROM int).
var fromTakingCall = map[string]bool{
	"extract":   true,
	"substring": true,
	"trim":      true,
	"overlay":   true,
}

// skipCall steps over a parenthesised argument list starting at or after i,
// reporting where it ended.
//
// Reports false when what follows is not a call at all, so a column named
// "extract" is still read as a name rather than swallowing the rest of the
// statement. Nesting is counted, and skipNonCode keeps a parenthesis inside a
// string or a comment from closing it.
func skipCall(stmt string, i int, dialect lexDialect) (int, bool) {
	i = skipSpace(stmt, i)
	if i >= len(stmt) || stmt[i] != '(' {
		return i, false
	}

	depth := 0
	for i < len(stmt) {
		if end, ok := skipNonCode(stmt, i, dialect); ok {
			i = end
			continue
		}
		switch stmt[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
		i++
	}

	// Unbalanced: the database will reject this statement anyway, and consuming
	// the rest of it would hide every table after the open parenthesis.
	return i, false
}
