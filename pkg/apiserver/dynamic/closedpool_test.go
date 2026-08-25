package dynamic

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestIsClosedPool separates a pool this server closed from anything the
// database said about a statement.
//
// The distinction decides whether a projection is refused. Pools are shared and
// released when no installed projection references them, and a projection being
// admitted is not installed — so a sync landing mid-check closes the pool the
// check is using, and reporting that as SQL the database cannot run refuses a
// perfectly good projection.
func TestIsClosedPool(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nothing went wrong", nil, false},
		{"the sentinel itself", sql.ErrConnDone, true},
		{"the sentinel, wrapped", fmt.Errorf("checking queries.list: %w", sql.ErrConnDone), true},
		{
			// What the pool actually answers, and what appeared in the denial
			// that started this: the message travels as text through the query
			// error rather than as the sentinel.
			name: "the message this server reports",
			err:  errors.New("queries.list: the database cannot run this statement: sql: database is closed"),
			want: true,
		},
		// The whole point of the webhook. These have to keep being refused.
		{
			name: "a table that does not exist",
			err:  errors.New(`queries.list: the database cannot run this statement: ERROR: relation "orders" does not exist (SQLSTATE 42P01)`),
			want: false,
		},
		{
			name: "a column that does not exist",
			err:  errors.New(`queries.list: the database cannot run this statement: ERROR: column "customer_name" does not exist (SQLSTATE 42703)`),
			want: false,
		},
		{
			name: "a projection that does not validate",
			err:  errors.New("mapping.name is required"),
			want: false,
		},
		// Close enough to be worth pinning: a closed *connection* is not a
		// closed pool, and says nothing about whether a retry would help.
		{
			name: "a closed result set",
			err:  errors.New("sql: Rows are closed"),
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isClosedPool(tc.err); got != tc.want {
				t.Errorf("isClosedPool(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestCheckSurvivesThePoolBeingClosedUnderneathIt is the failure this fixes,
// reproduced: the webhook denied a valid projection with "sql: database is
// closed" because a sync released the pool mid-check.
//
// Pools are shared and released when no installed projection references them
// any more. A projection being admitted is not installed, so it is exactly the
// one whose pool can go.
func TestCheckSurvivesThePoolBeingClosedUnderneathIt(t *testing.T) {
	ctx := context.Background()
	compiler := newTestCompiler(t)
	projection := testProjection()

	// Warm, and valid.
	if err := compiler.Check(ctx, projection); err != nil {
		t.Fatalf("Check() rejected a valid projection before anything went wrong: %v", err)
	}

	// Closed while still in the cache, which is what a check in flight meets:
	// it is holding the pool the release just closed, and the next lookup hands
	// out the same closed one. Closing it directly rather than through
	// RetainOnly, because RetainOnly also drops the entry — and then the next
	// lookup simply opens a fresh pool and there is nothing to recover from.
	prepared, err := compiler.Prepare(ctx, projection)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if err := prepared.Pool.Close(); err != nil {
		t.Fatalf("closing the pool: %v", err)
	}

	if err := compiler.Check(ctx, projection); err != nil {
		t.Errorf("Check() rejected a valid projection because its pool had been closed: %v", err)
	}
}

// TestCheckStillRefusesSQLTheDatabaseCannotRun is the other side: recovering
// from a closed pool must not turn into recovering from everything.
func TestCheckStillRefusesSQLTheDatabaseCannotRun(t *testing.T) {
	ctx := context.Background()
	compiler := newTestCompiler(t)

	projection := testProjection()
	projection.Spec.Queries.List.SQL = "SELECT id, tenant FROM no_such_table WHERE tenant = :namespace"

	err := compiler.Check(ctx, projection)
	if err == nil {
		t.Fatal("Check() accepted a projection whose table does not exist")
	}
	if !strings.Contains(err.Error(), "no_such_table") {
		t.Errorf("the refusal does not name what is wrong: %v", err)
	}
}
