package sql

import (
	"reflect"
	"testing"
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HasReturning(tc.stmt, tc.driver); got != tc.want {
				t.Errorf("HasReturning(%q, %q) = %v, want %v", tc.stmt, tc.driver, got, tc.want)
			}
		})
	}
}

// TestRewriteHandlesDriverSpecificSyntax covers three places where the scanner
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
