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
//
// Both are why the result is described as what the projection names rather than
// as a complete schema.
func Tables(stmt, driver string) []string {
	// An unknown driver still has literals and comments; only dollar-quoting is
	// specific, and assuming it absent finds strictly fewer names.
	style, err := placeholderStyle(driver)
	if err != nil {
		style = PlaceholderQuestion
	}

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
		if end, ok := skipNonCode(stmt, i, style); ok {
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

		// INTO always introduces a table; FROM and JOIN may introduce a
		// subquery or a set-returning function instead.
		switch {
		case strings.EqualFold(keyword, "into"), strings.EqualFold(keyword, "update"):
			if name, next, ok := tableRef(stmt, j, style, false); ok {
				add(name)
				i = next
				continue
			}
		case strings.EqualFold(keyword, "from"), strings.EqualFold(keyword, "join"):
			if name, next, ok := tableRef(stmt, j, style, true); ok {
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
func tableRef(stmt string, i int, style PlaceholderStyle, rejectCalls bool) (string, int, bool) {
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
