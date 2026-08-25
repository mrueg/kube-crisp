package sql

import (
	"fmt"
	"strings"
)

func placeholderStyle(driver string) (PlaceholderStyle, error) {
	d, ok := Lookup(driver)
	if !ok {
		return 0, fmt.Errorf("unsupported driver %q; this build knows %s",
			driver, strings.Join(RegisteredDrivers(), ", "))
	}
	return d.Placeholders, nil
}

// Rewrite converts a statement written with :name bind parameters into the
// placeholder syntax of the target driver, returning the rewritten statement
// and the parameter names in positional order.
//
// Values are never interpolated into the statement: the returned names are
// resolved to driver arguments by the caller, so a request-supplied value can
// never change the shape of the query.
//
// String literals, quoted identifiers, comments, and PostgreSQL's :: cast
// operator are skipped rather than treated as parameters.
func Rewrite(stmt, driver string) (string, []string, error) {
	style, err := placeholderStyle(driver)
	if err != nil {
		return "", nil, err
	}

	var (
		out    strings.Builder
		names  []string
		index  = make(map[string]int)
		nextID = 1
	)

	for i := 0; i < len(stmt); {
		c := stmt[i]

		// Anything that is not statement text — a literal, a quoted identifier,
		// a comment — is copied across untouched, so nothing inside it is read
		// as a bind parameter.
		if end, ok := skipNonCode(stmt, i, style); ok {
			out.WriteString(stmt[i:end])
			i = end
			continue
		}

		switch {
		// PostgreSQL cast operator, not a bind parameter.
		case c == ':' && i+1 < len(stmt) && stmt[i+1] == ':':
			out.WriteString("::")
			i += 2
			continue

		// Named bind parameter.
		case c == ':' && i+1 < len(stmt) && isNameStart(stmt[i+1]):
			j := i + 1
			for j < len(stmt) && isNameChar(stmt[j]) {
				j++
			}
			name := stmt[i+1 : j]

			switch style {
			case PlaceholderDollar:
				pos, seen := index[name]
				if !seen {
					pos = nextID
					index[name] = pos
					nextID++
					names = append(names, name)
				}
				fmt.Fprintf(&out, "$%d", pos)
			default:
				// ? placeholders are positional, so a repeated parameter is
				// appended again rather than reused.
				out.WriteByte('?')
				names = append(names, name)
			}
			i = j
			continue
		}

		out.WriteByte(c)
		i++
	}

	return out.String(), names, nil
}

// dollarQuote reports where the dollar-quoted string starting at i ends.
//
// The opening tag is $ followed by an optional identifier and another $; the
// string runs to the next occurrence of that same tag.
//
// The tag follows PostgreSQL's rules for an unquoted identifier, which is what
// separates $tag$ from $1 — a tag cannot begin with a digit, so $1$ is the
// placeholder $1 followed by a $ and not the opening of a literal. That
// distinction matters because this rewriter's own output is full of $1, $2, and
// a second pass over it has to read them the same way.
func dollarQuote(stmt string, i int) (int, bool) {
	j := i + 1
	if j < len(stmt) && !isNameStart(stmt[j]) && stmt[j] != '$' {
		return 0, false
	}
	for j < len(stmt) && isNameChar(stmt[j]) {
		j++
	}
	if j >= len(stmt) || stmt[j] != '$' {
		return 0, false
	}

	tag := stmt[i : j+1]
	end := strings.Index(stmt[j+1:], tag)
	if end < 0 {
		// No closing tag, so this is not a literal — it is a malformed
		// statement the database will reject on its own. Treating the opener as
		// ordinary text keeps the rewrite unambiguous; swallowing the rest of
		// the statement instead would make the result depend on where an
		// already-emitted $1 happened to land.
		return 0, false
	}
	return j + 1 + end + len(tag), true
}

func isNameStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isNameChar(c byte) bool {
	return isNameStart(c) || (c >= '0' && c <= '9')
}

// skipNonCode reports where the region starting at i ends when that region is
// not statement text: a string literal, a quoted identifier, or a comment. It
// reports false when the statement at i is code.
//
// Every reader of a statement has to agree on this. A rewriter that treated a
// comment as code would bind a parameter that is not there; a scanner looking
// for a keyword and finding one inside a string literal would classify the
// statement as something it is not.
func skipNonCode(stmt string, i int, style PlaceholderStyle) (int, bool) {
	c := stmt[i]

	switch {
	// Single-quoted string literal, with '' escaping.
	//
	// Whether a backslash also escapes depends on the database. MySQL treats
	// \' that way by default. PostgreSQL does not: standard_conforming_strings
	// has been on since 9.1, so a backslash in an ordinary literal is data and
	// 'C:\' is a complete string. Treating it as an escape there ran the
	// scanner past the closing quote and swallowed the rest of the statement,
	// so every :name after it stopped being rewritten and PostgreSQL was handed
	// a literal colon to choke on.
	case c == '\'':
		backslashEscapes := style != PlaceholderDollar
		j := i + 1
		for j < len(stmt) {
			if backslashEscapes && stmt[j] == '\\' && j+1 < len(stmt) {
				j += 2
				continue
			}
			if stmt[j] == '\'' {
				if j+1 < len(stmt) && stmt[j+1] == '\'' {
					j += 2
					continue
				}
				j++
				break
			}
			j++
		}
		return j, true

	// PostgreSQL dollar-quoted string, $$...$$ or $tag$...$tag$. Function
	// bodies live in these, and a body is full of things that look like bind
	// parameters and are not.
	//
	// Anything else starting with $ — a hand-written positional placeholder, an
	// unclosed tag — is not a literal, so the text after it is statement text
	// and is scanned as such.
	case c == '$' && style == PlaceholderDollar:
		return dollarQuote(stmt, i)

	// Double-quoted identifier.
	case c == '"':
		return endOfDelimited(stmt, i, '"'), true

	// Backtick-quoted identifier (MySQL).
	case c == '`':
		return endOfDelimited(stmt, i, '`'), true

	// Line comment.
	//
	// MySQL requires whitespace after the two dashes: 1--2 is a subtraction of
	// a negative number, not a comment. Treating it as one swallowed the rest
	// of the line, and any :name in it stopped being rewritten. PostgreSQL has
	// no such rule, so -- always starts a comment there.
	case c == '-' && i+1 < len(stmt) && stmt[i+1] == '-' && startsLineComment(stmt, i, style):
		return endOfLine(stmt, i), true

	// MySQL line comment. Not a comment character in PostgreSQL, where # is
	// only ever an operator.
	case c == '#' && style == PlaceholderQuestion:
		return endOfLine(stmt, i), true

	// Block comment.
	case c == '/' && i+1 < len(stmt) && stmt[i+1] == '*':
		j := strings.Index(stmt[i:], "*/")
		if j < 0 {
			return len(stmt), true
		}
		return i + j + 2, true
	}

	return 0, false
}

// endOfDelimited reports where a region opened at i by delimiter and closed by
// the next one ends, or the end of the statement when it is never closed.
func endOfDelimited(stmt string, i int, delimiter byte) int {
	j := i + 1
	for j < len(stmt) && stmt[j] != delimiter {
		j++
	}
	if j < len(stmt) {
		j++
	}
	return j
}

// HasReturning reports whether a statement answers with the rows it wrote.
//
// The word has to appear as statement text, which is why this cannot be a
// search over the string: "-- returning the newest first" is a comment and
// 'returning to sender' is data. A write misread as returning rows is run as a
// query rather than for its effect; it succeeds, answers with no rows, and the
// client is told the object was not found or that something else changed it
// first — for a write that in fact landed.
func HasReturning(stmt, driver string) bool {
	// An unknown driver still has literals and comments; only dollar-quoting is
	// specific, and assuming it absent finds strictly fewer keywords.
	style, err := placeholderStyle(driver)
	if err != nil {
		style = PlaceholderQuestion
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

		// Consumed whole, so that a word merely ending in it — a column called
		// "not_returning" — is not mistaken for the keyword.
		j := i
		for j < len(stmt) && isNameChar(stmt[j]) {
			j++
		}
		if strings.EqualFold(stmt[i:j], "returning") {
			return true
		}
		i = j
	}
	return false
}

// startsLineComment reports whether a -- at i begins a comment.
//
// MySQL requires a space, tab, or newline after the dashes; PostgreSQL does
// not. The difference matters for arithmetic: 1--2 is 1 minus negative 2 on
// MySQL and a comment on PostgreSQL.
func startsLineComment(stmt string, i int, style PlaceholderStyle) bool {
	if style == PlaceholderDollar {
		return true
	}
	if i+2 >= len(stmt) {
		// Nothing follows the dashes, so there is no statement text left to
		// misread either way.
		return true
	}
	switch stmt[i+2] {
	case ' ', '\t', '\n', '\r':
		return true
	}
	return false
}

// endOfLine returns the index just past the next newline, or the end.
func endOfLine(stmt string, i int) int {
	if j := strings.IndexByte(stmt[i:], '\n'); j >= 0 {
		return i + j + 1
	}
	return len(stmt)
}
