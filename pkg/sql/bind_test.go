package sql

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestRewrite(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		driver   string
		wantSQL  string
		wantArgs []string
	}{
		{
			name:     "postgres numbers parameters in order",
			stmt:     "SELECT * FROM orders WHERE tenant = :namespace AND id = :name",
			driver:   "postgres",
			wantSQL:  "SELECT * FROM orders WHERE tenant = $1 AND id = $2",
			wantArgs: []string{"namespace", "name"},
		},
		{
			name:     "postgres reuses the placeholder for a repeated parameter",
			stmt:     "SELECT * FROM t WHERE a = :x OR b = :x",
			driver:   "postgres",
			wantSQL:  "SELECT * FROM t WHERE a = $1 OR b = $1",
			wantArgs: []string{"x"},
		},
		{
			name:     "mysql repeats positional placeholders",
			stmt:     "SELECT * FROM t WHERE a = :x OR b = :x",
			driver:   "mysql",
			wantSQL:  "SELECT * FROM t WHERE a = ? OR b = ?",
			wantArgs: []string{"x", "x"},
		},
		{
			name:     "cast operator is not a parameter",
			stmt:     "SELECT total::text FROM t WHERE id = :name",
			driver:   "postgres",
			wantSQL:  "SELECT total::text FROM t WHERE id = $1",
			wantArgs: []string{"name"},
		},
		{
			name:     "colons inside string literals are left alone",
			stmt:     "SELECT ':notaparam' AS lit FROM t WHERE id = :name",
			driver:   "postgres",
			wantSQL:  "SELECT ':notaparam' AS lit FROM t WHERE id = $1",
			wantArgs: []string{"name"},
		},
		{
			name:     "colons inside comments are left alone",
			stmt:     "SELECT 1 -- :nope\nFROM t WHERE id = :name",
			driver:   "postgres",
			wantSQL:  "SELECT 1 -- :nope\nFROM t WHERE id = $1",
			wantArgs: []string{"name"},
		},
		{
			name:     "quoted identifiers are left alone",
			stmt:     `SELECT "weird:column" FROM t WHERE id = :name`,
			driver:   "postgres",
			wantSQL:  `SELECT "weird:column" FROM t WHERE id = $1`,
			wantArgs: []string{"name"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSQL, gotArgs, err := Rewrite(tc.stmt, tc.driver)
			if err != nil {
				t.Fatalf("Rewrite() returned error: %v", err)
			}
			if gotSQL != tc.wantSQL {
				t.Errorf("SQL mismatch:\n got: %s\nwant: %s", gotSQL, tc.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tc.wantArgs) {
				t.Errorf("params = %v, want %v", gotArgs, tc.wantArgs)
			}
		})
	}
}

func TestRewriteUnsupportedDriver(t *testing.T) {
	if _, _, err := Rewrite("SELECT 1", "oracle"); err == nil {
		t.Fatal("expected an error for an unsupported driver")
	}
}

// TestRewriteLeavesDollarQuotedStringsAlone: a PostgreSQL function body lives
// inside $$...$$ and is full of things that look like bind parameters. Treating
// them as parameters would rewrite the body of the function being defined.
func TestRewriteLeavesDollarQuotedStringsAlone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stmt   string
		want   string
		params []string
	}{
		{
			name:   "anonymous tag",
			stmt:   `SELECT $$ :notaparam $$, :real`,
			want:   `SELECT $$ :notaparam $$, $1`,
			params: []string{"real"},
		},
		{
			name:   "named tag",
			stmt:   `SELECT $body$ :nope $body$ WHERE a = :name`,
			want:   `SELECT $body$ :nope $body$ WHERE a = $1`,
			params: []string{"name"},
		},
		{
			// A bare $n is not an opening tag, so it must not swallow the rest
			// of the statement as if it were a literal — the parameter after it
			// still has to be rewritten. (kube-crisp numbers its own
			// placeholders; a statement mixing hand-written $n with :named
			// parameters was never supported.)
			name:   "positional placeholder is not a dollar quote",
			stmt:   `SELECT $2 WHERE a = :name`,
			want:   `SELECT $2 WHERE a = $1`,
			params: []string{"name"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, params, err := Rewrite(tc.stmt, "postgres")
			if err != nil {
				t.Fatalf("Rewrite() returned error: %v", err)
			}
			if got != tc.want {
				t.Errorf("Rewrite() = %q, want %q", got, tc.want)
			}
			if len(params) != len(tc.params) {
				t.Fatalf("Rewrite() params = %v, want %v", params, tc.params)
			}
			for i := range params {
				if params[i] != tc.params[i] {
					t.Errorf("Rewrite() params = %v, want %v", params, tc.params)
				}
			}
		})
	}
}

// TestRewriteHonoursBackslashEscapes: MySQL escapes a quote with a backslash by
// default. Ending the literal at that quote would leave the rewriter reading
// the rest of the string as statement text.
func TestRewriteHonoursBackslashEscapes(t *testing.T) {
	stmt := `SELECT 'it\'s :notaparam' WHERE a = :name`

	got, params, err := Rewrite(stmt, "mysql")
	if err != nil {
		t.Fatalf("Rewrite() returned error: %v", err)
	}
	if want := `SELECT 'it\'s :notaparam' WHERE a = ?`; got != want {
		t.Errorf("Rewrite() = %q, want %q", got, want)
	}
	if len(params) != 1 || params[0] != "name" {
		t.Errorf("Rewrite() params = %v, want [name]", params)
	}
}

// TestHasReturning covers the distinction that decides whether a write is run
// as a query or for its effect: the keyword has to be statement text. A write
// misread as returning rows answers with nothing and is reported to the client
// as a conflict or a 404 for a write that succeeded.
func TestHasReturning(t *testing.T) {
	cases := []struct {
		name   string
		driver string
		stmt   string
		want   bool
	}{
		{
			name:   "returning clause",
			driver: "postgres",
			stmt:   "UPDATE orders SET status = :status WHERE id = :name RETURNING id, status",
			want:   true,
		},
		{
			name:   "lower case is the same keyword",
			driver: "postgres",
			stmt:   "INSERT INTO orders (id) VALUES (:id) returning id",
			want:   true,
		},
		{
			name:   "plain update",
			driver: "mysql",
			stmt:   "UPDATE orders SET status = :status WHERE id = :name",
			want:   false,
		},
		{
			name:   "the word in a line comment",
			driver: "postgres",
			stmt:   "-- returning the newest row first\nUPDATE orders SET status = :status WHERE id = :name",
			want:   false,
		},
		{
			name:   "the word in a block comment",
			driver: "postgres",
			stmt:   "/* not returning anything */ UPDATE orders SET status = :status WHERE id = :name",
			want:   false,
		},
		{
			name:   "the word in a string literal",
			driver: "mysql",
			stmt:   "UPDATE orders SET note = 'returning to sender' WHERE id = :name",
			want:   false,
		},
		{
			name:   "the word in a quoted identifier",
			driver: "postgres",
			stmt:   `UPDATE orders SET "returning" = :flag WHERE id = :name`,
			want:   false,
		},
		{
			name:   "the word in a backtick identifier",
			driver: "mysql",
			stmt:   "UPDATE orders SET `returning` = :flag WHERE id = :name",
			want:   false,
		},
		{
			name:   "the word inside a dollar-quoted body",
			driver: "postgres",
			stmt:   "UPDATE orders SET body = $fn$ select returning $fn$ WHERE id = :name",
			want:   false,
		},
		{
			name:   "a column merely ending in the word",
			driver: "postgres",
			stmt:   "UPDATE orders SET not_returning = :flag WHERE id = :name",
			want:   false,
		},
		{
			name:   "an unknown driver still skips literals",
			driver: "nonesuch",
			stmt:   "UPDATE orders SET note = 'returning' WHERE id = :name",
			want:   false,
		},
		{
			// An unknown driver is read with every comment rule at once, so a
			// keyword is never invented out of one. Inventing is the harmful
			// direction: the write below is run as a query, commits, and
			// answers with no rows, which the client is told is a 404.
			name:   "an unknown driver does not read the keyword out of a comment",
			driver: "nonesuch",
			stmt:   "INSERT INTO orders (id) VALUES (:name) --returning id",
			want:   false,
		},
		{
			// SQLite is not MySQL. A backslash in a literal is data there, so
			// this literal is closed and RETURNING is statement text. Read with
			// MySQL's escapes the scanner ran past the closing quote, saw the
			// rest of the statement as string, and answered false — so a write
			// that does return the row it wrote was run for its effect instead.
			name:   "sqlite backslash is data, so the word after the literal is code",
			driver: "sqlite",
			stmt:   `INSERT INTO items (id) VALUES ('C:\') RETURNING id`,
			want:   true,
		},
		{
			// The other direction, and the expensive one. SQLite needs no
			// whitespace after the dashes, so this is a comment; MySQL's rule
			// made it statement text. The write then ran as a query, committed,
			// and answered zero rows, which the registry reports as a 404 or a
			// conflict for a row that is in the table.
			name:   "sqlite needs no space after the dashes for a comment",
			driver: "sqlite",
			stmt:   "INSERT INTO items (id) VALUES (:name) --returning id",
			want:   false,
		},
		{
			// # is a comment on MySQL and nothing at all on SQLite, so the
			// keyword after one is still code.
			name:   "sqlite has no hash comment",
			driver: "sqlite",
			stmt:   "INSERT INTO items (id) VALUES (:name) # returning id",
			want:   true,
		},
		{
			// CockroachDB is PostgreSQL's grammar as well as its protocol.
			name:   "cockroach reads a literal the way postgres does",
			driver: "cockroach",
			stmt:   `INSERT INTO items (id) VALUES ('C:\') RETURNING id`,
			want:   true,
		},
		{
			// PostgreSQL block comments nest, which is what happens when you
			// comment out a block that already had a comment in it. Ending at
			// the first */ left "returning id */" being read as statement text,
			// so the write ran as a query, committed, and answered nothing.
			name:   "the word inside a nested block comment",
			driver: "postgres",
			stmt:   "INSERT INTO orders (id) VALUES (:name) /* outer /* inner */ returning id */",
			want:   false,
		},
		{
			// The same statement on a database whose comments do not nest. The
			// outer comment ends at the first */ there, so the keyword really
			// is statement text and MySQL really would return the row.
			name:   "mysql block comments do not nest",
			driver: "mysql",
			stmt:   "INSERT INTO orders (id) VALUES (:name) /* outer /* inner */ returning id */",
			want:   true,
		},
		{
			// An unknown driver counts the nesting, because that is the reading
			// that skips more and so invents fewer keywords.
			name:   "an unknown driver counts nested comments",
			driver: "nonesuch",
			stmt:   "INSERT INTO orders (id) VALUES (:name) /* outer /* inner */ returning id */",
			want:   false,
		},
		{
			// E'' asks PostgreSQL for backslash escapes in one literal, which
			// is the only reason to write it. Read as an ordinary literal it
			// ended at the escaped quote, and everything after it — RETURNING
			// included — was read as more string.
			name:   "the word after an escape string is still code",
			driver: "postgres",
			stmt:   `INSERT INTO orders (note) VALUES (E'it\'s') RETURNING id`,
			want:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasReturning(tc.stmt, tc.driver); got != tc.want {
				t.Errorf("HasReturning(%q, %q) = %v, want %v", tc.stmt, tc.driver, got, tc.want)
			}
		})
	}
}

// TestEveryBuiltInDriverIsReadWithItsOwnGrammar pins the mapping the scanner
// runs on.
//
// PlaceholderStyle used to stand in for it, and it cannot: there are two
// placeholder styles and three grammars, so SQLite was read as MySQL for as
// long as the two shared a ?. A built-in added here without being named in
// lexDialectFor would fall through to the conservative reading — safe, but not
// the reading of a database whose grammar this build does know.
func TestEveryBuiltInDriverIsReadWithItsOwnGrammar(t *testing.T) {
	want := map[string]lexDialect{
		// CockroachDB speaks PostgreSQL's grammar, not only its wire protocol.
		"postgres":  lexPostgres,
		"cockroach": lexPostgres,
		"mysql":     lexMySQL,
		"sqlite":    lexSQLite,
	}

	for name, expected := range want {
		if _, ok := Lookup(name); !ok {
			t.Errorf("the %s driver is not registered", name)
			continue
		}
		if got := lexDialectFor(name); got != expected {
			t.Errorf("lexDialectFor(%q) = %v, want %v", name, got, expected)
		}
	}

	// A driver registered by a build that links its own database/sql driver is
	// read conservatively, because nothing here knows its grammar.
	if got := lexDialectFor("nonesuch"); got != lexConservative {
		t.Errorf("lexDialectFor(%q) = %v, want the conservative reading", "nonesuch", got)
	}
}

// TestRewriteHandlesDriverSpecificSyntax covers the places where the scanner
// read one database's rules into another's. Each produced a statement the
// server then refused, with an error pointing at the wrong thing.
func TestRewriteHandlesDriverSpecificSyntax(t *testing.T) {
	for _, tc := range []struct {
		name       string
		driver     string
		sql        string
		wantSQL    string
		wantParams []string
	}{
		{
			// standard_conforming_strings has been on since PostgreSQL 9.1, so
			// a backslash in an ordinary literal is data and 'C:\' is closed.
			// Treating it as an escape ran past the closing quote and swallowed
			// the rest of the statement.
			name: "postgres backslash is data, not an escape", driver: "postgres",
			sql:     `SELECT 'C:\' AS prefix, :name AS n`,
			wantSQL: `SELECT 'C:\' AS prefix, $1 AS n`, wantParams: []string{"name"},
		},
		{
			// MySQL still escapes with a backslash by default.
			name: "mysql backslash still escapes", driver: "mysql",
			sql:     `SELECT 'it\'s' AS s, :name AS n`,
			wantSQL: `SELECT 'it\'s' AS s, ? AS n`, wantParams: []string{"name"},
		},
		{
			// # is a comment on MySQL, so the :namespace inside it is not a
			// parameter. Binding it produced two arguments for one placeholder.
			name: "mysql hash starts a comment", driver: "mysql",
			sql:        "SELECT id FROM widgets # by :namespace\nWHERE tenant = :namespace",
			wantSQL:    "SELECT id FROM widgets # by :namespace\nWHERE tenant = ?",
			wantParams: []string{"namespace"},
		},
		{
			// MySQL needs whitespace after the dashes; 1--2 is arithmetic.
			// Reading it as a comment swallowed the rest of the line.
			name: "mysql bare double dash is arithmetic", driver: "mysql",
			sql:     "SELECT 1--2 AS x, :name AS n",
			wantSQL: "SELECT 1--2 AS x, ? AS n", wantParams: []string{"name"},
		},
		{
			// With whitespace it is a comment on both.
			name: "mysql double dash and a space is a comment", driver: "mysql",
			sql:     "SELECT 1 AS x -- :ignored\n, :name AS n",
			wantSQL: "SELECT 1 AS x -- :ignored\n, ? AS n", wantParams: []string{"name"},
		},
		{
			// PostgreSQL has no whitespace rule, and # is only an operator.
			name: "postgres double dash needs no space", driver: "postgres",
			sql:     "SELECT 1--:ignored\n, :name AS n",
			wantSQL: "SELECT 1--:ignored\n, $1 AS n", wantParams: []string{"name"},
		},
		{
			// SQLite shares MySQL's ? placeholders and none of its lexical
			// rules. A backslash is data there, so this literal ends at the
			// quote; read as an escape the scanner ran on to the end of the
			// statement and :name was handed to SQLite as a named parameter of
			// its own — which admission accepts, because SQLite accepts it, and
			// which then fails at request time with a missing named argument.
			name: "sqlite backslash is data, not an escape", driver: "sqlite",
			sql:     `SELECT 'C:\' AS prefix, :name AS n`,
			wantSQL: `SELECT 'C:\' AS prefix, ? AS n`, wantParams: []string{"name"},
		},
		{
			// The same thing in the shape people actually write it: an escape
			// character for LIKE. What went missing was the parameter that
			// scopes the query to one namespace.
			name: "sqlite keeps the parameter after a LIKE escape clause", driver: "sqlite",
			sql:        `SELECT id FROM t WHERE name LIKE '%\_%' ESCAPE '\' AND tenant = :namespace`,
			wantSQL:    `SELECT id FROM t WHERE name LIKE '%\_%' ESCAPE '\' AND tenant = ?`,
			wantParams: []string{"namespace"},
		},
		{
			// SQLite needs no whitespace after the dashes either, so this is a
			// comment and the :not_a_param inside it is not a parameter. Bound
			// as one it produced two arguments for one placeholder.
			name: "sqlite double dash needs no space", driver: "sqlite",
			sql:     "SELECT id FROM t WHERE id = :name --:not_a_param",
			wantSQL: "SELECT id FROM t WHERE id = ? --:not_a_param", wantParams: []string{"name"},
		},
		{
			// SQLite has no # comment, so nothing after one is hidden and the
			// parameter is still rewritten. SQLite rejects the # itself, which
			// is a loud failure the author can see; dropping the parameter was
			// a quiet one they could not.
			name: "sqlite has no hash comment", driver: "sqlite",
			sql:     "SELECT id FROM t # :name",
			wantSQL: "SELECT id FROM t # ?", wantParams: []string{"name"},
		},
		{
			// E'' is how PostgreSQL is asked for backslash escapes in one
			// literal, so \' there does not end it. Read as an ordinary literal
			// it ended at that quote and the rewriter carried on through what
			// is still string — leaving the :b after it unbound, and
			// PostgreSQL a statement with a placeholder short.
			name: "postgres escape string escapes with a backslash", driver: "postgres",
			sql:     `INSERT INTO t (a, b) VALUES (E'it\'s', :b)`,
			wantSQL: `INSERT INTO t (a, b) VALUES (E'it\'s', $1)`, wantParams: []string{"b"},
		},
		{
			// The E has to be a prefix and not the last letter of a word.
			// PostgreSQL spells a typed constant date'...', so the character
			// before a literal's opening quote is routinely part of an
			// identifier, and reading one as a prefix would give that literal
			// escapes it does not have.
			name: "a type-prefixed literal is not an escape string", driver: "postgres",
			sql:     `SELECT date'a\' AS d, :name AS n`,
			wantSQL: `SELECT date'a\' AS d, $1 AS n`, wantParams: []string{"name"},
		},
		{
			// PostgreSQL comments nest, so :ghost is inside one and there is
			// one parameter here, not two. Ending the comment at the first */
			// bound both, and PostgreSQL answered "bind message supplies 2
			// parameters, but prepared statement requires 1".
			name: "postgres block comments nest", driver: "postgres",
			sql:     "/* outer /* inner */ :ghost */ SELECT :id",
			wantSQL: "/* outer /* inner */ :ghost */ SELECT $1", wantParams: []string{"id"},
		},
		{
			// MySQL's do not: its comment ended at the first */, so what
			// follows really is statement text.
			name: "mysql block comments do not nest", driver: "mysql",
			sql:     "/* outer /* inner */ :ghost */ SELECT :id",
			wantSQL: "/* outer /* inner */ ? */ SELECT ?", wantParams: []string{"ghost", "id"},
		},
		{
			// The * of the opener cannot also close it: /*/ is a comment
			// nobody has closed, so the rest of the statement is inside it.
			// Reading it as closed handed that text back as code.
			name: "an opening /*/ does not close itself", driver: "postgres",
			sql:     "SELECT 1 /*/ :ghost",
			wantSQL: "SELECT 1 /*/ :ghost", wantParams: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, params, err := Rewrite(tc.sql, tc.driver)
			if err != nil {
				t.Fatalf("Rewrite() returned error: %v", err)
			}
			if got != tc.wantSQL {
				t.Errorf("rewritten SQL:\n  got  %q\n  want %q", got, tc.wantSQL)
			}
			if len(params) != len(tc.wantParams) {
				t.Fatalf("params = %v, want %v", params, tc.wantParams)
			}
			for i := range params {
				if params[i] != tc.wantParams[i] {
					t.Errorf("params = %v, want %v", params, tc.wantParams)
					break
				}
			}
		})
	}
}

// TestSQLiteBindsAParameterAfterABackslashLiteral runs the statement against a
// real SQLite, because the argument is about what SQLite does and not about
// what the scanner believes.
//
// It insists on both halves: SQLite accepts 'C:\' as a complete literal, which
// is what makes the backslash data there, and the parameter that follows it is
// bound. Before this the parameter list came back empty and the :qty was left
// in the statement — where SQLite reads it as a named parameter of its own. So
// Pool.Check admitted a projection that could not run, and every request
// against it answered `missing named argument "qty"`.
func TestSQLiteBindsAParameterAfterABackslashLiteral(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	insert, err := pool.Prepare(`INSERT INTO items (id, qty) VALUES ('C:\', 7)`, time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Exec(ctx, insert, nil); err != nil {
		t.Fatalf("SQLite rejected 'C:\\' as a literal: %v", err)
	}

	stmt, err := pool.Prepare(`SELECT id FROM items WHERE id = 'C:\' AND qty = :qty`, time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if len(stmt.Params) != 1 || stmt.Params[0] != "qty" {
		t.Fatalf("prepared params = %v, want [qty]: the literal swallowed the rest of the statement", stmt.Params)
	}

	rows, err := pool.Query(ctx, stmt, map[string]any{"qty": int64(7)})
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Query() returned %d rows, want 1", len(rows))
	}
}

// TestSQLiteWriteWithACommentedKeywordIsRunForItsEffect is the failure that
// costs the most.
//
// SQLite ends a comment at two dashes with nothing after them, so this
// statement does not answer with rows. Read with MySQL's rule the keyword was
// statement text, the write was run as a query, and it committed and answered
// nothing — which the registry turns into a 404 or a conflict for a row that is
// now in the table.
func TestSQLiteWriteWithACommentedKeywordIsRunForItsEffect(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	const source = "INSERT INTO items (id, qty) VALUES (:id, 1) --returning id"
	if HasReturning(source, "sqlite") {
		t.Fatal("HasReturning read the keyword out of a comment, so the write would be run as a query")
	}

	stmt, err := pool.Prepare(source, time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	affected, err := pool.Exec(ctx, stmt, map[string]any{"id": "written"})
	if err != nil {
		t.Fatalf("Exec() returned error: %v", err)
	}
	if affected != 1 {
		t.Errorf("Exec() affected %d rows, want 1", affected)
	}
}
