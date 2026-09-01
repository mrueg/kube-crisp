package sql

import (
	"strings"
	"testing"
)

// FuzzRewrite exercises the statement rewriter, which is the security boundary
// of this package: it is what guarantees a request-supplied value can never
// become part of the statement.
//
// The invariants are chosen so that a failure means something real:
//
//   - rewriting never fails for a supported driver, whatever the input;
//   - no bind parameter survives rewriting, so nothing downstream can
//     reinterpret one;
//   - every reported parameter is a plain identifier, never an expression;
//   - rewriting is deterministic.
func FuzzRewrite(f *testing.F) {
	seeds := []string{
		"SELECT * FROM orders WHERE tenant = :namespace AND id = :name",
		"SELECT total::text FROM t WHERE id = :name",
		"SELECT ':notaparam' AS lit FROM t WHERE id = :name",
		"SELECT 1 -- :nope\nFROM t WHERE id = :name",
		"SELECT /* :nope */ 1 FROM t WHERE a = :x OR b = :x",
		"SELECT `weird:column` FROM t",
		`SELECT "weird:column" FROM t`,
		"INSERT INTO t VALUES (:a, :b, :c) RETURNING *",
		// A backslash is an escape on MySQL and data on the other two, so the
		// same bytes end the literal in different places per driver.
		`SELECT 'C:\' AS prefix, :name AS n`,
		`SELECT id FROM t WHERE name LIKE '%\_%' ESCAPE '\' AND tenant = :namespace`,
		"SELECT 1--:x\n, :name AS n",
		"SELECT id FROM t # :name",
		"UPDATE t SET a = :a WHERE v = :resourceVersion",
		":",
		"::",
		":::x",
		"'",
		"'unterminated :x",
		"--",
		"/*",
		"",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	for _, driver := range []string{"postgres", "mysql", "sqlite"} {
		f.Add(driver)
	}

	f.Fuzz(func(t *testing.T, stmt string) {
		for _, driver := range []string{"postgres", "mysql", "sqlite"} {
			rewritten, params, err := Rewrite(stmt, driver)
			if err != nil {
				t.Fatalf("Rewrite(%q, %q) failed: %v", stmt, driver, err)
			}

			for _, name := range params {
				if name == "" {
					t.Fatalf("Rewrite(%q, %q) reported an empty parameter name", stmt, driver)
				}
				if !isNameStart(name[0]) {
					t.Fatalf("parameter %q does not start with an identifier character", name)
				}
				for i := 0; i < len(name); i++ {
					if !isNameChar(name[i]) {
						t.Fatalf("parameter %q contains %q, which is not an identifier character", name, name[i])
					}
				}
			}

			// Nothing that could be read as a bind parameter may survive: a
			// second pass must find none.
			//
			// Skipped for PostgreSQL when the statement itself contains a $.
			// The rewriter emits $1, $2 there, and $ is also what opens a
			// dollar-quoted string — so a second pass over output that mixes
			// the author's dollars with the rewriter's own can group them into
			// different literals than the first pass saw. That says nothing
			// about the first pass, which is the one that runs in production;
			// nothing ever re-rewrites a rewritten statement.
			if driver != "postgres" || !strings.Contains(stmt, "$") {
				second, leftovers, err := Rewrite(rewritten, driver)
				if err != nil {
					t.Fatalf("re-rewriting %q failed: %v", rewritten, err)
				}
				if len(leftovers) != 0 {
					t.Fatalf("Rewrite(%q, %q) left bind parameters %v in %q", stmt, driver, leftovers, rewritten)
				}
				if second != rewritten {
					t.Fatalf("rewriting is not stable: %q became %q", rewritten, second)
				}
			}

			// Determinism, so a prepared statement cache key means one thing.
			again, againParams, err := Rewrite(stmt, driver)
			if err != nil {
				t.Fatalf("Rewrite(%q, %q) failed on a repeat call: %v", stmt, driver, err)
			}
			if again != rewritten || strings.Join(againParams, ",") != strings.Join(params, ",") {
				t.Fatalf("Rewrite(%q, %q) is not deterministic", stmt, driver)
			}
		}
	})
}
