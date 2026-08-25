package projection

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// seededDB builds a database holding n orders, for measuring collection reads
// at a size where the cost of copying them is worth knowing.
func seededDB(tb testing.TB, n int) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "orders.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		tb.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE orders (
		id TEXT PRIMARY KEY, tenant TEXT NOT NULL, customer TEXT NOT NULL,
		status TEXT NOT NULL, total_cents INTEGER NOT NULL,
		line_items TEXT NOT NULL, updated_at TEXT NOT NULL)`); err != nil {
		tb.Fatalf("creating table: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		tb.Fatalf("beginning: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO orders VALUES (?, 'acme', ?, 'pending', ?, '[{"sku":"widget","qty":2}]', ?)`)
	if err != nil {
		tb.Fatalf("preparing: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := stmt.Exec(fmt.Sprintf("order-%06d", i), fmt.Sprintf("customer-%d", i%997), i*37, i+1); err != nil {
			tb.Fatalf("seeding: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatalf("committing: %v", err)
	}
	return path
}

func benchStorage(tb testing.TB, rows int, ttl time.Duration) *REST {
	tb.Helper()

	spec := testSpec()
	spec.Queries.List.MaxRows = ptr(int32(50000))
	if ttl > 0 {
		spec.CacheTTL = &metav1.Duration{Duration: ttl}
	}

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             "sqlite",
		DSN:                seededDB(tb, rows),
		PreparedStatements: true,
	})
	if err != nil {
		tb.Fatalf("opening pool: %v", err)
	}
	tb.Cleanup(func() { _ = pool.Close() })

	storages, err := New("orders", spec, pool, nil, nil)
	if err != nil {
		tb.Fatalf("New() returned error: %v", err)
	}
	return storages.read
}

// BenchmarkListUncached is the cost of answering a collection from the
// database: the query, the scan, and the mapping.
func BenchmarkListUncached(b *testing.B) {
	store := benchStorage(b, 10000, 0)
	ctx := namespacedContext("acme")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list, err := store.List(ctx, nil)
		if err != nil {
			b.Fatalf("List() returned error: %v", err)
		}
		if got := len(list.(*unstructured.UnstructuredList).Items); got != 10000 {
			b.Fatalf("listed %d objects", got)
		}
	}
}

// BenchmarkListCacheHit is the same collection answered from the cache, which
// is a deep copy of it and nothing else.
func BenchmarkListCacheHit(b *testing.B) {
	store := benchStorage(b, 10000, time.Hour)
	ctx := namespacedContext("acme")

	if _, err := store.List(ctx, nil); err != nil {
		b.Fatalf("priming the cache: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		list, err := store.List(ctx, nil)
		if err != nil {
			b.Fatalf("List() returned error: %v", err)
		}
		if got := len(list.(*unstructured.UnstructuredList).Items); got != 10000 {
			b.Fatalf("listed %d objects", got)
		}
	}
}

// BenchmarkListDeepCopy isolates the copy itself, which the cache pays twice:
// once storing the collection and once answering with it.
func BenchmarkListDeepCopy(b *testing.B) {
	store := benchStorage(b, 10000, 0)

	list, err := store.List(namespacedContext("acme"), nil)
	if err != nil {
		b.Fatalf("List() returned error: %v", err)
	}
	collection := list.(*unstructured.UnstructuredList)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = collection.DeepCopy()
	}
}

// BenchmarkGetCacheHit is the same question for a single object, where the copy
// is trivial and the round trip is not.
func BenchmarkGetCacheHit(b *testing.B) {
	store := benchStorage(b, 10000, time.Hour)
	ctx := namespacedContext("acme")

	if _, err := store.Get(ctx, "order-000001", &metav1.GetOptions{}); err != nil {
		b.Fatalf("priming the cache: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(ctx, "order-000001", &metav1.GetOptions{}); err != nil {
			b.Fatalf("Get() returned error: %v", err)
		}
	}
}

func BenchmarkGetUncached(b *testing.B) {
	store := benchStorage(b, 10000, 0)
	ctx := namespacedContext("acme")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(ctx, "order-000001", &metav1.GetOptions{}); err != nil {
			b.Fatalf("Get() returned error: %v", err)
		}
	}
}

var _ = crispv1alpha1.FieldTypeJSON
