package sql

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A parameter with no entry in args used to bind NULL: a map lookup on a
// missing key gives nil, and nil is a NULL to every driver. For a write that
// is a column overwritten with NULL and a 200. These pin the distinction the
// pool now draws — a key that is absent is refused, a key that is present with
// nil is the NULL it always was.

func TestExecRefusesAParameterNothingSupplies(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, true)

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, :qty)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	_, err = pool.Exec(ctx, insert, map[string]any{"id": "a"})
	if err == nil {
		t.Fatal("a statement naming :qty ran with no qty in args")
	}
	if !strings.Contains(err.Error(), ":qty") {
		t.Errorf("error %q does not name the parameter nothing supplied", err)
	}

	// Refused before anything ran, so the row is not there with a NULL in it.
	count, err := pool.Prepare("SELECT COUNT(*) AS n FROM items", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	rows, err := pool.Query(ctx, count, nil)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if got := rows[0]["n"]; got != int64(0) {
		t.Errorf("%v rows were written by a statement that should not have run", got)
	}
}

func TestQueryRefusesAParameterNothingSupplies(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, false)

	query, err := pool.Prepare("SELECT id FROM items WHERE id = :id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Query(ctx, query, map[string]any{}); err == nil || !strings.Contains(err.Error(), ":id") {
		t.Errorf("Query() with no id in args returned %v, want a refusal naming :id", err)
	}
}

func TestTransactRefusesBeforeAnyStatementRuns(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, true)

	first, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, 1)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	second, err := pool.Prepare("UPDATE items SET qty = :qty WHERE id = :id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	// The first statement could run; the second names something nobody
	// supplied. Neither may reach the database.
	if _, _, err := pool.Transact(ctx, []*Statement{first, second}, map[string]any{"id": "a"}); err == nil {
		t.Fatal("a transaction whose second statement names :qty ran with no qty in args")
	}

	count, err := pool.Prepare("SELECT COUNT(*) AS n FROM items", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	rows, err := pool.Query(ctx, count, nil)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if got := rows[0]["n"]; got != int64(0) {
		t.Errorf("the first statement of a refused transaction wrote %v row(s)", got)
	}
}

func TestANilValuedParameterBindsNULL(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t, true)

	insert, err := pool.Prepare("INSERT INTO items (id, qty) VALUES (:id, :qty)", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if _, err := pool.Exec(ctx, insert, map[string]any{"id": "a", "qty": nil}); err != nil {
		t.Fatalf("Exec() with a nil qty returned error: %v", err)
	}

	query, err := pool.Prepare("SELECT qty IS NULL AS unset FROM items WHERE id = :id", time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	rows, err := pool.Query(ctx, query, map[string]any{"id": "a"})
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(rows) != 1 || rows[0]["unset"] != int64(1) {
		t.Errorf("a nil-valued parameter was not stored as NULL: %v", rows)
	}
}

// TestBindDistinguishesAbsentFromNil is the same rule at the function itself,
// where the two cases are one map lookup apart.
func TestBindDistinguishesAbsentFromNil(t *testing.T) {
	stmt := &Statement{Params: []string{"id", "qty"}}

	values, err := bind(stmt, map[string]any{"id": "a", "qty": nil})
	if err != nil {
		t.Fatalf("bind() with a nil-valued key returned error: %v", err)
	}
	if len(values) != 2 || values[0] != "a" || values[1] != nil {
		t.Errorf("bind() = %v, want [a <nil>]", values)
	}

	if _, err := bind(stmt, map[string]any{"id": "a"}); err == nil {
		t.Error("bind() with a missing key returned no error")
	}
}
