package sql

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// wideTable opens a pool over a table of one wide text column.
func wideTable(t *testing.T, rows, width int) *Pool {
	t.Helper()

	pool, err := Open(PoolOptions{Driver: "sqlite", DSN: filepath.Join(t.TempDir(), "wide.db")})
	if err != nil {
		t.Fatalf("opening the pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	if _, err := pool.db.ExecContext(t.Context(),
		`CREATE TABLE wide (id INTEGER PRIMARY KEY, blob TEXT NOT NULL)`); err != nil {
		t.Fatalf("creating the table: %v", err)
	}
	padding := strings.Repeat("x", width)
	for i := 0; i < rows; i++ {
		if _, err := pool.db.ExecContext(t.Context(),
			`INSERT INTO wide (id, blob) VALUES (?, ?)`, i, padding); err != nil {
			t.Fatalf("seeding row %d: %v", i, err)
		}
	}
	return pool
}

// TestMaxBytesBoundsAReadThatMaxRowsCannot is the gap this closes: maxRows
// counts rows, and a row can be a megabyte.
//
// Ten rows is nothing; ten rows of 64KiB is 640KiB, and the same shape at
// production scale is gigabytes into a server every projection shares.
func TestMaxBytesBoundsAReadThatMaxRowsCannot(t *testing.T) {
	pool := wideTable(t, 10, 64<<10)

	statement, err := pool.Prepare("SELECT id, blob FROM wide", 0, 0)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}

	// Far above the row count, so nothing here is bounded by maxRows.
	statement.MaxRows = 1000

	statement.MaxBytes = 0
	all, err := pool.Query(context.Background(), statement, nil)
	if err != nil {
		t.Fatalf("unbounded read returned error: %v", err)
	}
	if len(all) != 10 {
		t.Fatalf("read %d rows, want 10", len(all))
	}

	statement.MaxBytes = 128 << 10
	_, err = pool.Query(context.Background(), statement, nil)
	if err == nil {
		t.Fatal("a read far over maxBytes was allowed; maxRows alone does not bound memory")
	}
	if !strings.Contains(err.Error(), "maxBytes") {
		t.Errorf("the refusal does not name the limit that stopped it: %v", err)
	}
	// Named so the fix is obvious: how far in it got says whether to narrow the
	// query or raise the limit.
	if !strings.Contains(err.Error(), "rows") {
		t.Errorf("the refusal does not say how many rows it managed: %v", err)
	}
}

// TestMaxBytesLeavesAnOrdinaryReadAlone: the limit has to be generous enough
// that a normal collection never meets it, or it is a limit nobody can keep.
func TestMaxBytesLeavesAnOrdinaryReadAlone(t *testing.T) {
	pool := wideTable(t, 200, 512)

	statement, err := pool.Prepare("SELECT id, blob FROM wide", 0, 0)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	if statement.MaxBytes != DefaultMaxBytes {
		t.Fatalf("a prepared statement carries maxBytes %d, want the default %d", statement.MaxBytes, DefaultMaxBytes)
	}

	rows, err := pool.Query(context.Background(), statement, nil)
	if err != nil {
		t.Fatalf("an ordinary read of 200 small rows was refused: %v", err)
	}
	if len(rows) != 200 {
		t.Errorf("read %d rows, want 200", len(rows))
	}
}

// TestValueSizeCountsWhatMakesAResultLarge: the variable-length types are the
// ones worth measuring, and the rest cost more to count exactly than they weigh.
func TestValueSizeCountsWhatMakesAResultLarge(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  int
	}{
		{"null", nil, 0},
		{"text", strings.Repeat("x", 1234), 1234},
		{"bytes", make([]byte, 4096), 4096},
		{"an integer", int64(42), 8},
		{"a bool", true, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := valueSize(tc.value); got != tc.want {
				t.Errorf("valueSize(%T) = %d, want %d", tc.value, got, tc.want)
			}
		})
	}
}
