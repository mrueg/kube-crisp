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
		dialect = lexDialectFor(driver)
		out     strings.Builder
		names   []string
		index   = make(map[string]int)
		nextID  = 1
	)

	for i := 0; i < len(stmt); {
		c := stmt[i]

		// Anything that is not statement text — a literal, a quoted identifier,
		// a comment — is copied across untouched, so nothing inside it is read
		// as a bind parameter.
		if end, ok := skipNonCode(stmt, i, dialect); ok {
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

// lexDialect is how a database reads the text of a statement: which characters
// open a string literal or a comment, and what ends one.
//
// It exists because PlaceholderStyle was standing in for this, and the two are
// not the same question. There are two placeholder styles and three lexical
// dialects: MySQL and SQLite both spell a parameter ?, and almost everything
// about how they read a literal differs. Keying the scanner off the placeholder
// style therefore handed SQLite MySQL's rules — a backslash escapes inside a
// literal, # opens a comment, -- opens one only when whitespace follows — and
// SQLite follows the standard on all three, exactly as PostgreSQL does. So
// 'C:\' ran the scanner past its own closing quote and every :name after it
// stopped being rewritten, while INSERT ... --returning id was read as a
// statement that answers with rows: run as a query it commits and returns
// nothing, and the client is told 404 for a row that is there.
//
// PlaceholderStyle stays what it is. It is registry API — a driver declares it
// and the README documents it — so the lexical rules are derived from the
// driver name here instead, the way sessionDialectFor derives the shape of a
// session-variable statement from it.
type lexDialect uint8

const (
	// lexConservative is what a driver this build does not recognise gets.
	//
	// Every rule that makes a region something other than code is turned on at
	// once, so this scan skips at least as much text as any named dialect below
	// would. That is the safe direction for all three readers, because each of
	// them is looking for something and the harm is in finding one that is not
	// there: a table name read out of a comment is invented, and a RETURNING
	// read out of one turns a write into a query that commits and answers with
	// no rows, which the registry reports as a 404 or a conflict for a write
	// that landed. Missing one only means a write is run for its effect, which
	// is what a write is for.
	//
	// Rewrite never sees this value. An unregistered driver has no placeholder
	// style either, so it is refused before the scan begins.
	lexConservative lexDialect = iota
	lexPostgres
	lexMySQL
	lexSQLite
)

// lexDialectFor reports how a driver's statement text is to be read.
//
// Derived from the driver name rather than declared in the registry, because
// this is a property of the database's grammar and not something a driver
// author has a choice about. CockroachDB is PostgreSQL's grammar as much as it
// is PostgreSQL's wire protocol, so it reads the same.
func lexDialectFor(driver string) lexDialect {
	switch driver {
	case "postgres", "cockroach":
		return lexPostgres
	case "mysql":
		return lexMySQL
	case "sqlite":
		return lexSQLite
	}
	return lexConservative
}

// backslashEscapes reports whether a backslash escapes the character after it
// inside an ordinary '...' literal.
//
// MySQL's default, and nobody else's. PostgreSQL has had
// standard_conforming_strings on since 9.1 and SQLite never had anything else,
// so in both a backslash is data and 'C:\' is a complete string. Reading it as
// an escape there ran the scanner past the closing quote and swallowed the rest
// of the statement, so every :name after it stopped being rewritten — which is
// how the idiomatic LIKE '%\_%' ESCAPE '\' silently lost the :namespace that
// scopes a query to one tenant.
func (d lexDialect) backslashEscapes() bool {
	return d == lexMySQL || d == lexConservative
}

// dollarQuotes reports whether $tag$...$tag$ opens a string literal. PostgreSQL
// function bodies live in these, and a body is full of things that look like
// bind parameters and are not.
func (d lexDialect) dollarQuotes() bool {
	return d == lexPostgres || d == lexConservative
}

// escapeStrings reports whether E'...' is a literal in which a backslash
// escapes the character after it.
//
// PostgreSQL's answer to standard_conforming_strings: the prefix is how you ask
// for the old behaviour for one literal, which is the whole reason anybody
// writes E'it\'s'. Read as an ordinary literal it ended at that escaped quote,
// and the scanner carried on through what is still string — so RETURNING after
// one went unseen and the :name after one went unbound.
//
// Only PostgreSQL needs to be asked. MySQL has no such prefix and escapes
// everywhere anyway, and lexConservative already escapes in every literal.
func (d lexDialect) escapeStrings() bool {
	return d == lexPostgres
}

// nestedBlockComments reports whether /* ... /* ... */ ... */ is one comment
// rather than one that ended at the first */.
//
// PostgreSQL nests, which is not a curiosity: nesting is what happens when you
// comment out a block that already had a comment in it. Ending at the first */
// left the tail of the outer comment — the half after the inner one closed —
// being read as statement text, which is where an invented RETURNING and an
// invented bind parameter came from.
func (d lexDialect) nestedBlockComments() bool {
	return d == lexPostgres || d == lexConservative
}

// hashComments reports whether # opens a line comment. MySQL only: in
// PostgreSQL # is an operator, and SQLite has no such comment at all.
func (d lexDialect) hashComments() bool {
	return d == lexMySQL || d == lexConservative
}

// lineCommentNeedsSpace reports whether -- opens a comment only when a space, a
// tab, or a newline follows it. MySQL requires that, which is what makes 1--2 a
// subtraction of a negative number there and a comment in PostgreSQL and
// SQLite.
func (d lexDialect) lineCommentNeedsSpace() bool {
	return d == lexMySQL
}

// skipNonCode reports where the region starting at i ends when that region is
// not statement text: a string literal, a quoted identifier, or a comment. It
// reports false when the statement at i is code.
//
// Every reader of a statement has to agree on this. A rewriter that treated a
// comment as code would bind a parameter that is not there; a scanner looking
// for a keyword and finding one inside a string literal would classify the
// statement as something it is not.
func skipNonCode(stmt string, i int, dialect lexDialect) (int, bool) {
	c := stmt[i]

	switch {
	// Single-quoted string literal.
	case c == '\'':
		return endOfString(stmt, i, dialect.backslashEscapes()), true

	// PostgreSQL escape string, E'...'.
	//
	// The prefix has to be a prefix and not the tail of a word: a column called
	// "type" followed by 'x' would otherwise have its e read as one. Nothing
	// but a name character can precede an identifier's last letter, so that is
	// the test.
	case (c == 'E' || c == 'e') && dialect.escapeStrings() &&
		i+1 < len(stmt) && stmt[i+1] == '\'' &&
		(i == 0 || !isNameChar(stmt[i-1])):
		return endOfString(stmt, i+1, true), true

	// PostgreSQL dollar-quoted string, $$...$$ or $tag$...$tag$.
	//
	// Anything else starting with $ — a hand-written positional placeholder, an
	// unclosed tag — is not a literal, so the text after it is statement text
	// and is scanned as such.
	case c == '$' && dialect.dollarQuotes():
		return dollarQuote(stmt, i)

	// Double-quoted identifier.
	case c == '"':
		return endOfDelimited(stmt, i, '"'), true

	// Backtick-quoted identifier (MySQL, and SQLite, which accepts them for
	// compatibility with it).
	case c == '`':
		return endOfDelimited(stmt, i, '`'), true

	// Line comment.
	case c == '-' && i+1 < len(stmt) && stmt[i+1] == '-' && startsLineComment(stmt, i, dialect):
		return endOfLine(stmt, i), true

	// MySQL line comment.
	case c == '#' && dialect.hashComments():
		return endOfLine(stmt, i), true

	// Block comment.
	case c == '/' && i+1 < len(stmt) && stmt[i+1] == '*':
		return endOfBlockComment(stmt, i, dialect.nestedBlockComments()), true
	}

	return 0, false
}

// endOfBlockComment reports where the /* comment opened at i ends, or the end
// of the statement when it is never closed.
//
// nested is the database's rule for what closes it. PostgreSQL counts opens, so
// the comment ends at the */ that balances the last of them; MySQL and SQLite
// end at the first one regardless.
//
// Either way the scan starts after the opening /*, so the * of the opener
// cannot also serve as the * of a closer: /*/ opens a comment that nothing has
// closed yet, and reading it as a closed one handed the rest of the statement
// back as code.
func endOfBlockComment(stmt string, i int, nested bool) int {
	depth := 1
	for j := i + 2; j+1 < len(stmt); {
		switch {
		case nested && stmt[j] == '/' && stmt[j+1] == '*':
			depth++
			j += 2
		case stmt[j] == '*' && stmt[j+1] == '/':
			depth--
			j += 2
			if depth == 0 {
				return j
			}
		default:
			j++
		}
	}
	return len(stmt)
}

// endOfString reports where the '...' literal opened at i ends, or the end of
// the statement when it is never closed.
//
// A doubled quote is an escaped one everywhere. Whether a backslash escapes as
// well is the database's business, which is why the caller answers that rather
// than this deciding it.
func endOfString(stmt string, i int, backslashEscapes bool) int {
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
	return j
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
	dialect := lexDialectFor(driver)

	for i := 0; i < len(stmt); {
		if end, ok := skipNonCode(stmt, i, dialect); ok {
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
// MySQL requires a space, tab, or newline after the dashes; PostgreSQL and
// SQLite do not. The difference matters for arithmetic: 1--2 is 1 minus
// negative 2 on MySQL and a comment on the other two.
func startsLineComment(stmt string, i int, dialect lexDialect) bool {
	if !dialect.lineCommentNeedsSpace() {
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
