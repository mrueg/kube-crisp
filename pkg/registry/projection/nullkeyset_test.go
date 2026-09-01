package projection

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// nullKeysetDB seeds a collection whose ordering column is NULL on one row,
// which is what a column added to an existing table looks like until somebody
// backfills it. SQLite sorts NULL first, so it is the row a first page reaches.
func nullKeysetDB(t *testing.T, rows int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "nullseq.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE orders (
		id TEXT PRIMARY KEY, tenant TEXT NOT NULL, customer TEXT NOT NULL,
		status TEXT NOT NULL, total_cents INTEGER NOT NULL,
		line_items TEXT NOT NULL, updated_at TEXT NOT NULL, seq INTEGER)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	for i := 0; i < rows; i++ {
		var seq any = i + 1
		if i == rows-1 {
			seq = nil
		}
		if _, err := db.Exec(`INSERT INTO orders VALUES (?, 'acme', 'ada', 'pending', 1, '[]', ?, ?)`,
			fmt.Sprintf("order-%06d", i), i+1, seq); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	return path
}

// newNullKeysetStorage pages on that column, and accepts a NULL namespace so
// the watch poll's own read can be exercised against it.
func newNullKeysetStorage(t *testing.T, rows int) *REST {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             "sqlite",
		DSN:                nullKeysetDB(t, rows),
		PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	spec := seqPagedSpec()
	spec.Queries.List.SQL = `SELECT id, tenant, customer, status, total_cents, line_items, updated_at, seq
		      FROM orders
		      WHERE (:namespace IS NULL OR tenant = :namespace) AND (:after IS NULL OR seq > :after)
		      ORDER BY seq
		      LIMIT :limit`

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.read
}

// TestAnUnpagedListDoesNotNeedAPagingKey is the bug. The key was derived for
// every row of every read, including reads that cannot hand back a continue
// token and never look at it — so one row whose keyset column is NULL, which no
// unpaged client is affected by, turned the whole list into a 500.
func TestAnUnpagedListDoesNotNeedAPagingKey(t *testing.T) {
	const rows = 5
	store := newNullKeysetStorage(t, rows)

	list, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got := len(list.(*unstructured.UnstructuredList).Items); got != rows {
		t.Errorf("List() returned %d items, want %d", got, rows)
	}
}

// TestTheWatchPollDoesNotNeedAPagingKey is why it mattered more than one
// client's list. listAllNamespaces reads through the same path with no options
// at all, so the same single row stopped the watch cache from ever priming and
// the projection could not be watched.
func TestTheWatchPollDoesNotNeedAPagingKey(t *testing.T) {
	const rows = 5
	store := newNullKeysetStorage(t, rows)

	items, err := store.listAllNamespaces(namespacedContext(""))
	if err != nil {
		t.Fatalf("listAllNamespaces() returned error: %v", err)
	}
	if got := len(items); got != rows {
		t.Errorf("the poll read %d rows, want %d", got, rows)
	}
}

// TestAPagedListStillRefusesANullPagingKey keeps the guard that made this
// visible. A NULL cannot anchor a page: a token built on one would resume
// somewhere the query cannot express, and rows would be skipped or repeated
// with nothing to say so. Failing the read is the honest answer there, and it
// was only ever wrong when the key was not going to be used.
func TestAPagedListStillRefusesANullPagingKey(t *testing.T) {
	store := newNullKeysetStorage(t, 5)

	_, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{Limit: 2})
	if !errors.IsInternalError(err) {
		t.Fatalf("a paged List() over a NULL paging key returned %v, want an internal error", err)
	}
}
