package sql

import (
	"strings"
	"testing"
)

func TestTables(t *testing.T) {
	for _, tc := range []struct {
		name   string
		driver string
		sql    string
		want   []string
	}{
		{
			// The bug this set exists for: EXTRACT spells an argument
			// separator FROM, and reading it as a clause reported a column as
			// a table. Every projection deriving a resourceVersion from a
			// timestamp does this, the PostgreSQL tutorial recommends it, and
			// every Pagila projection in examples/ was affected.
			name: "extract does not name a table",
			sql:  "SELECT id, (extract(epoch FROM f.last_update) * 1000000)::bigint AS version FROM film f",
			want: []string{"film"},
		},
		{
			name: "substring, trim and overlay do not either",
			sql: "SELECT substring(title FROM 1 FOR 3), trim(BOTH ' ' FROM title), " +
				"overlay(title placing 'x' FROM 2) FROM film",
			want: []string{"film"},
		},
		{
			name: "a call is stepped over whole, so a later table is still found",
			sql:  "SELECT extract(epoch FROM a.ts) FROM actor a JOIN film_actor fa ON fa.actor_id = a.actor_id",
			want: []string{"actor", "film_actor"},
		},
		{
			name: "nested parentheses inside the call do not end it early",
			sql:  "SELECT extract(epoch FROM (a.ts + interval '1 day')) FROM actor a",
			want: []string{"actor"},
		},
		{
			// The keyword only matters when a call follows it. A column that
			// happens to be named extract is read as a name.
			name: "a column named extract is not a call",
			sql:  "SELECT extract, id FROM orders",
			want: []string{"orders"},
		},
		{
			// Unbalanced parentheses are the database's to reject. Consuming
			// the rest of the statement would hide every table after them.
			name: "an unclosed call does not swallow the statement",
			sql:  "SELECT extract(epoch FROM ts FROM orders",
			want: []string{"orders", "ts"},
		},
		{
			name: "a plain select",
			sql:  "SELECT id, tenant FROM orders WHERE tenant = :namespace",
			want: []string{"orders"},
		},
		{
			name: "a join",
			sql:  "SELECT o.id FROM orders o JOIN order_events e ON e.id = o.id",
			want: []string{"order_events", "orders"},
		},
		{
			name: "an insert names its table despite the column list",
			sql:  "INSERT INTO orders (id, tenant) VALUES (:id, :tenant)",
			want: []string{"orders"},
		},
		{
			name: "an insert with no space before the column list",
			sql:  "INSERT INTO orders(id, tenant) VALUES (:id, :tenant)",
			want: []string{"orders"},
		},
		{
			name: "an update",
			sql:  "UPDATE orders SET customer = :customer WHERE id = :name",
			want: []string{"orders"},
		},
		{
			name: "a delete",
			sql:  "DELETE FROM order_tombstones WHERE deleted_at > :since",
			want: []string{"order_tombstones"},
		},
		{
			name: "a schema-qualified name keeps its schema",
			sql:  "SELECT 1 FROM public.orders",
			want: []string{"public.orders"},
		},
		{
			name: "a quoted identifier",
			sql:  `SELECT 1 FROM "order events"`,
			want: []string{"order events"},
		},
		// The scan is over statement text, so a name inside a literal or a
		// comment is data rather than a table. Getting this wrong is how a
		// status field starts telling an operator to create a table that does
		// not exist.
		{
			name: "a name inside a string literal is not a table",
			sql:  "SELECT 'from secrets' AS note FROM orders",
			want: []string{"orders"},
		},
		{
			name: "a name inside a comment is not a table",
			sql:  "SELECT 1 -- from secrets\nFROM orders",
			want: []string{"orders"},
		},
		{
			name:   "a name inside a dollar-quoted body is not a table",
			driver: "postgres",
			sql:    "SELECT $$ from secrets $$ FROM orders",
			want:   []string{"orders"},
		},
		// A set-returning function is not a table; a column list on an insert
		// is the same punctuation meaning the opposite thing.
		{
			name: "a set-returning function is not a table",
			sql:  "SELECT g FROM generate_series(1, 10) g",
			want: nil,
		},
		{
			name: "a derived table is not a table",
			sql:  "SELECT * FROM (SELECT id FROM orders) t",
			want: []string{"orders"},
		},
		{
			name: "ONLY qualifies the table rather than being one",
			sql:  "SELECT 1 FROM ONLY orders",
			want: []string{"orders"},
		},
		{
			name: "a column merely ending in a keyword is not one",
			sql:  "SELECT transform_into, id FROM orders",
			want: []string{"orders"},
		},
		{
			name: "reported once however often it is named",
			sql:  "SELECT 1 FROM orders UNION SELECT 1 FROM orders",
			want: []string{"orders"},
		},
		{
			name: "case is normalised",
			sql:  "SELECT 1 FROM Orders JOIN ORDERS o ON true",
			want: []string{"orders"},
		},
		{
			name: "nothing to find",
			sql:  "SELECT 1",
			want: nil,
		},
		// Documented and deliberate: a CTE is named alongside what it reads.
		// Naming something that is not a table is the safe direction.
		{
			name: "a common table expression is named alongside its source",
			sql:  "WITH recent AS (SELECT id FROM orders) SELECT * FROM recent",
			want: []string{"orders", "recent"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driver := tc.driver
			if driver == "" {
				driver = "sqlite"
			}
			got := Tables(tc.sql, driver)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("Tables()\n got: %v\nwant: %v", got, tc.want)
			}
		})
	}
}

// FuzzTables is here because this reads attacker-adjacent input: the SQL comes
// from a CustomResourceProjection, and the result is written into a status
// other people read. It must not panic on anything.
func FuzzTables(f *testing.F) {
	for _, seed := range []string{
		"SELECT 1 FROM orders",
		"INSERT INTO orders(id) VALUES (:id)",
		`SELECT 1 FROM "unterminated`,
		"SELECT 1 FROM",
		"FROM",
		"UPDATE",
		"SELECT $$ unterminated",
		"SELECT 1 -- comment",
		"''''''",
		"FROM `",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, stmt string) {
		for _, driver := range []string{"postgres", "mysql", "sqlite"} {
			_ = Tables(stmt, driver)
		}
	})
}
