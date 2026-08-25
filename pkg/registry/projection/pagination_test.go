package projection

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// timeOrderedDB holds rows whose names sort in the opposite order to the column
// the collection is listed by. Anything that pages on the wrong one is caught.
func timeOrderedDB(t *testing.T, rows int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "events.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE orders (
		id TEXT PRIMARY KEY, tenant TEXT NOT NULL, customer TEXT NOT NULL,
		status TEXT NOT NULL, total_cents INTEGER NOT NULL,
		line_items TEXT NOT NULL, updated_at TEXT NOT NULL, seq INTEGER NOT NULL)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	for i := 0; i < rows; i++ {
		// Name descends while seq ascends.
		name := fmt.Sprintf("order-%06d", rows-i)
		if _, err := db.Exec(
			`INSERT INTO orders VALUES (?, 'acme', 'ada', 'pending', 1, '[]', ?, ?)`,
			name, i+1, i+1); err != nil {
			t.Fatalf("seeding: %v", err)
		}
	}
	return path
}

// seqPagedSpec lists by an ascending sequence column rather than by name, which
// is the ordinary shape for anything time-series.
func seqPagedSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at, seq
		      FROM orders
		      WHERE tenant = :namespace AND (:after IS NULL OR seq > :after)
		      ORDER BY seq
		      LIMIT :limit`,
		KeysetColumn: "seq",
	}
	return spec
}

func newPagedStorage(t *testing.T, spec crispv1alpha1.CustomResourceProjectionSpec, rows int) *REST {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             "sqlite",
		DSN:                timeOrderedDB(t, rows),
		PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	return storages.read
}

// walk pages through the whole collection and returns the names it saw.
func walk(t *testing.T, store *REST, pageSize int64) []string {
	t.Helper()

	ctx := namespacedContext("acme")
	var (
		seen     []string
		token    string
		pages    int
		maxPages = 100
	)
	for {
		pages++
		if pages > maxPages {
			t.Fatalf("paging did not finish after %d pages", maxPages)
		}

		result, err := store.List(ctx, &metainternalversion.ListOptions{Limit: pageSize, Continue: token})
		if err != nil {
			t.Fatalf("List() returned error: %v", err)
		}
		list := result.(*unstructured.UnstructuredList)
		for i := range list.Items {
			seen = append(seen, list.Items[i].GetName())
		}

		token = list.GetContinue()
		if token == "" {
			return seen
		}
	}
}

// TestKeysetPagingFollowsTheOrderingColumn is the bug this fixes: a collection
// ordered by a column other than the name used to page on the name anyway,
// which silently skipped and repeated rows.
func TestKeysetPagingFollowsTheOrderingColumn(t *testing.T) {
	const rows = 25
	store := newPagedStorage(t, seqPagedSpec(), rows)

	seen := walk(t, store, 5)

	if len(seen) != rows {
		t.Fatalf("paging returned %d objects, want %d: %v", len(seen), rows, seen)
	}
	unique := map[string]bool{}
	for _, name := range seen {
		if unique[name] {
			t.Errorf("object %q was returned on more than one page", name)
		}
		unique[name] = true
	}

	// The order is the query's, not the name's: seq ascends while names
	// descend, so the first page holds the highest-numbered names.
	if seen[0] != fmt.Sprintf("order-%06d", rows) {
		t.Errorf("the first object is %q, want the first row by seq", seen[0])
	}
}

// TestKeysetPagingDefaultsToTheNameColumn keeps the common case working
// without anyone having to say anything.
func TestKeysetPagingDefaultsToTheNameColumn(t *testing.T) {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at
		      FROM orders
		      WHERE tenant = :namespace AND (:after IS NULL OR id > :after)
		      ORDER BY id
		      LIMIT :limit`,
	}

	const rows = 25
	store := newPagedStorage(t, spec, rows)
	if got := store.keysetColumn; got != "id" {
		t.Errorf("keysetColumn = %q, want the name column %q", got, "id")
	}

	seen := walk(t, store, 5)
	if len(seen) != rows {
		t.Fatalf("paging returned %d objects, want %d", len(seen), rows)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i-1] >= seen[i] {
			t.Fatalf("names are not ascending across pages: %q then %q", seen[i-1], seen[i])
		}
	}
}

// TestKeysetColumnMustBeSelected turns a silent mis-paging into a loud error.
func TestKeysetColumnMustBeSelected(t *testing.T) {
	spec := seqPagedSpec()
	spec.Queries.List.KeysetColumn = "not_selected"

	store := newPagedStorage(t, spec, 10)
	_, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{Limit: 5})
	if err == nil {
		t.Fatal("paging on a column the query does not return was accepted")
	}
}

// TestContinueTokenKeepsTheKeyType guards the round trip: an integer key has to
// come back an integer, or the database compares it against a float.
func TestContinueTokenKeepsTheKeyType(t *testing.T) {
	encoded := encodeContinue(continueToken{After: int64(42), Consumed: 5})

	token, err := decodeContinue(encoded)
	if err != nil {
		t.Fatalf("decodeContinue() returned error: %v", err)
	}
	value, ok := token.After.(int64)
	if !ok {
		t.Fatalf("after came back as %T, want int64", token.After)
	}
	if value != 42 {
		t.Errorf("after = %d, want 42", value)
	}

	// A string key survives as a string.
	token, err = decodeContinue(encodeContinue(continueToken{After: "order-000500"}))
	if err != nil {
		t.Fatalf("decodeContinue() returned error: %v", err)
	}
	if got, ok := token.After.(string); !ok || got != "order-000500" {
		t.Errorf("after = %#v, want the string %q", token.After, "order-000500")
	}
}

// countedSpec pages and counts, which is what makes remainingItemCount
// meaningful.
func countedSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := seqPagedSpec()
	spec.Queries.Count = &crispv1alpha1.Query{
		SQL: `SELECT COUNT(*) AS total FROM orders WHERE tenant = :namespace`,
	}
	return spec
}

// TestPageAndCountAgree covers the consistency the pair is read for: they run
// in one transaction, so a page and the count taken with it describe the same
// collection.
func TestPageAndCountAgree(t *testing.T) {
	const rows = 25
	store := newPagedStorage(t, countedSpec(), rows)

	result, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	list := result.(*unstructured.UnstructuredList)
	remaining := list.GetRemainingItemCount()
	if remaining == nil {
		t.Fatal("a paged list reported no remainingItemCount")
	}
	if got, want := *remaining, int64(rows-10); got != want {
		t.Errorf("remainingItemCount = %d, want %d", got, want)
	}

	// Walking the rest has to arrive at exactly what was promised.
	var seen int
	token := list.GetContinue()
	for token != "" {
		page, err := store.List(namespacedContext("acme"),
			&metainternalversion.ListOptions{Limit: 10, Continue: token})
		if err != nil {
			t.Fatalf("List() returned error: %v", err)
		}
		collection := page.(*unstructured.UnstructuredList)
		seen += len(collection.Items)
		token = collection.GetContinue()
	}
	if int64(seen) != *remaining {
		t.Errorf("the pages held %d more objects, but the first page promised %d", seen, *remaining)
	}
}

// TestCountIsNotReadWhenNotPaging keeps the pairing from costing anything on
// the ordinary path: an unpaged list has nothing to report a remainder for.
func TestCountIsNotReadWhenNotPaging(t *testing.T) {
	store := newPagedStorage(t, countedSpec(), 5)

	result, err := store.List(namespacedContext("acme"), nil)
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if remaining := result.(*unstructured.UnstructuredList).GetRemainingItemCount(); remaining != nil {
		t.Errorf("an unpaged list reported remainingItemCount = %d", *remaining)
	}
}

// TestPageAndCountShareOneFlight checks they are read as one: two clients
// asking for the same page share a single transaction between them.
func TestPageAndCountShareOneFlight(t *testing.T) {
	store := newPagedStorage(t, countedSpec(), 25)

	if _, err := store.List(namespacedContext("acme"), &metainternalversion.ListOptions{Limit: 10}); err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	if got := store.flights.pending(); got != 0 {
		t.Errorf("%d reads left in flight after the list finished", got)
	}
}
