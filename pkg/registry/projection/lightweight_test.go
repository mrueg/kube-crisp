package projection

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// richRow is a projected object with a body, so trimming it is measurable.
func richRow(namespace, name, version string) unstructured.Unstructured {
	obj := cachedItem(namespace, name, version)
	_ = unstructured.SetNestedField(obj.Object, "a customer name that occupies space", "spec", "customer")
	_ = unstructured.SetNestedField(obj.Object, int64(12345), "spec", "totalCents")
	_ = unstructured.SetNestedField(obj.Object, "pending", "status", "phase")
	return obj
}

func lightweightCache(t *testing.T, rows []unstructured.Unstructured, removed []cacheIdentity) *watchCache {
	t.Helper()

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	cache.lightweight = true
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		// The first poll reads everything; later ones would read forward.
		if since == "" {
			return rows, nil
		}
		return nil, nil
	}
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return removed, nil }

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("populating the cache: %v", err)
	}
	return cache
}

// TestLightweightCacheKeepsOnlyKeysAndVersions covers the ceiling on how large
// a table can be watched.
//
// A watched projection otherwise holds its whole collection in memory, which is
// why it needs maxRows above the row count. Keeping the key and the version is
// enough to diff, once a tombstone can describe what was deleted.
func TestLightweightCacheKeepsOnlyKeysAndVersions(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	cache := lightweightCache(t, rows, nil)

	cache.mu.Lock()
	held := cache.items["acme/order-1"]
	cache.mu.Unlock()

	if held == nil {
		t.Fatal("the row is not in the cache at all")
	}
	if got := held.GetResourceVersion(); got != "5" {
		t.Errorf("resourceVersion = %q, want 5 — the diff has nothing else to compare", got)
	}
	if _, found, _ := unstructured.NestedString(held.Object, "spec", "customer"); found {
		t.Error("the cache is still holding the row's body, which is the memory this exists to save")
	}
	if _, found, _ := unstructured.NestedString(held.Object, "status", "phase"); found {
		t.Error("the cache is still holding the row's status")
	}
}

// TestFullCacheStillHoldsObjects, since the saving is only safe where a
// tombstone can describe a deleted row.
func TestFullCacheStillHoldsObjects(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("populating the cache: %v", err)
	}

	cache.mu.Lock()
	held := cache.items["acme/order-1"]
	cache.mu.Unlock()

	if _, found, _ := unstructured.NestedString(held.Object, "spec", "customer"); !found {
		t.Error("a cache with no tombstones dropped the row's body, so a Deleted event would " +
			"name a row and describe nothing")
	}
}

// TestLightweightWatchStillSeesTheCollection: the initial state is read rather
// than remembered, and a watcher must not be able to tell.
func TestLightweightWatchStillSeesTheCollection(t *testing.T) {
	rows := []unstructured.Unstructured{
		richRow("acme", "order-1", "5"),
		richRow("acme", "order-2", "6"),
	}
	cache := lightweightCache(t, rows, nil)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	for i := range rows {
		select {
		case event := <-w.ResultChan():
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("event %d carried %T", i, event.Object)
			}
			// The whole object, not the trimmed one the cache holds.
			if _, found, _ := unstructured.NestedString(obj.Object, "spec", "customer"); !found {
				t.Errorf("%s arrived without its body; the initial state must be the real "+
					"collection, not what the cache kept", obj.GetName())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d initial events arrived", i, len(rows))
		}
	}
}

// TestLightweightDeletionDescribesTheRow is the property that makes all of this
// safe: the tombstone carries the mapped columns, so a deletion says what went
// away rather than only which key.
func TestLightweightDeletionDescribesTheRow(t *testing.T) {
	gone := richRow("acme", "order-gone", "9")

	// Built with no removals, so the first poll is the full resync that seeds
	// the cache. Tombstones are read by incremental polls, which is what the
	// second one below is.
	var removed []cacheIdentity
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	cache := lightweightCache(t, rows, nil)
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return removed, nil }

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	// Drain the initial state, then delete behind the cache's back.
	select {
	case <-w.ResultChan():
	case <-time.After(5 * time.Second):
		t.Fatal("the initial state never arrived")
	}

	removed = []cacheIdentity{{namespace: "acme", name: "order-gone", object: &gone}}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling after the deletion: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			if event.Type != watch.Deleted {
				continue
			}
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("the deletion carried %T", event.Object)
			}
			customer, found, _ := unstructured.NestedString(obj.Object, "spec", "customer")
			if !found {
				t.Fatal("the deletion named the row and described nothing, which is what the " +
					"tombstone columns exist to prevent")
			}
			if customer == "" {
				t.Error("the deleted row's body is empty")
			}
			return
		case <-deadline:
			t.Fatal("no deletion arrived")
		}
	}
}

// TestLightweightCacheIsSmaller puts a number on it.
func TestLightweightCacheIsSmaller(t *testing.T) {
	const n = 500
	rows := make([]unstructured.Unstructured, 0, n)
	for i := range n {
		rows = append(rows, richRow("acme", fmt.Sprintf("order-%04d", i), fmt.Sprint(i)))
	}

	full := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(full.Close)
	if err := full.poll(context.Background()); err != nil {
		t.Fatal(err)
	}

	light := lightweightCache(t, rows, nil)

	fullFields, lightFields := countFields(full), countFields(light)
	if fullFields == 0 || len(light.items) != n {
		t.Fatalf("the caches hold %d and %d rows; nothing is being compared",
			len(full.items), len(light.items))
	}
	if lightFields >= fullFields {
		t.Errorf("the lightweight cache holds %d fields against %d; it is not smaller",
			lightFields, fullFields)
	}
	t.Logf("%d rows: %d fields held in full, %d lightweight", n, fullFields, lightFields)
}

// countFields totals the mapped entries the cache is holding, as a stand-in for
// how much of each row it kept.
func countFields(c *watchCache) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	var total int
	for _, item := range c.items {
		total += countNested(item.Object)
	}
	return total
}

func countNested(value any) int {
	switch typed := value.(type) {
	case map[string]any:
		total := 0
		for _, v := range typed {
			total += 1 + countNested(v)
		}
		return total
	default:
		return 0
	}
}

// TestTrimmedEntriesStaySerialisable covers the failure that unit tests missed
// and the cluster found.
//
// A watch event carrying an object with no apiVersion or kind cannot be
// encoded. The stream dies part-way through the response, the client sees its
// channel close, and nothing is logged on either side — so the first symptom is
// a watch that stopped for no stated reason.
func TestTrimmedEntriesStaySerialisable(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	rows[0].SetLabels(map[string]string{"tier": "gold"})
	rows[0].SetGroupVersionKind(deletedTestGVK)

	cache := lightweightCache(t, rows, nil)

	cache.mu.Lock()
	held := cache.items["acme/order-1"]
	cache.mu.Unlock()

	if held.GetAPIVersion() == "" || held.GetKind() == "" {
		t.Errorf("the cache kept an object with no kind (%q/%q); a watch event carrying it "+
			"cannot be encoded and the stream dies mid-response",
			held.GetAPIVersion(), held.GetKind())
	}
	if got := held.GetLabels()["tier"]; got != "gold" {
		t.Errorf("labels = %v, want them kept — a watcher with a label selector filters "+
			"deletions on them and would silently miss removals it asked to see",
			held.GetLabels())
	}
}

// TestLightweightDeletionCarriesTheWholeRow: the cache is holding a trimmed
// entry, so a field selector over a mapped column could not be answered from
// it. The tombstone's own row can.
func TestLightweightDeletionCarriesTheWholeRow(t *testing.T) {
	gone := richRow("acme", "order-gone", "9")
	gone.SetGroupVersionKind(deletedTestGVK)

	var removed []cacheIdentity
	rows := []unstructured.Unstructured{richRow("acme", "order-gone", "9")}
	cache := lightweightCache(t, rows, nil)
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return removed, nil }

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	select {
	case <-w.ResultChan():
	case <-time.After(5 * time.Second):
		t.Fatal("the initial state never arrived")
	}

	// Now the row is both in the cache (trimmed) and tombstoned.
	removed = []cacheIdentity{{namespace: "acme", name: "order-gone", object: &gone}}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling after the deletion: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			if event.Type != watch.Deleted {
				continue
			}
			obj := event.Object.(*unstructured.Unstructured)
			if _, found, _ := unstructured.NestedString(obj.Object, "spec", "customer"); !found {
				t.Fatal("the deletion carried the trimmed cache entry rather than the " +
					"tombstone's row, so a field selector over a mapped column cannot match it")
			}
			return
		case <-deadline:
			t.Fatal("no deletion arrived")
		}
	}
}

// TestUnversionedRowsAreKeptWhole covers a row whose mapped version column is
// NULL.
//
// The diff compares versions when there are versions and compares the objects
// when there are not. A trimmed entry on one side of that comparison never
// matches the row on the other, so an unchanged row was reported as modified on
// every poll — an event per row per interval, for as long as anybody watched.
func TestUnversionedRowsAreKeptWhole(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "")}
	cache := lightweightCache(t, rows, nil)

	cache.mu.Lock()
	held := cache.items["acme/order-1"]
	cache.mu.Unlock()

	if _, found, _ := unstructured.NestedString(held.Object, "spec", "customer"); !found {
		t.Fatal("a row with no version was trimmed, so the diff compares a trimmed entry " +
			"against the full row and reports it modified on every poll")
	}
}

// TestUnchangedRowsProduceNoEvents is the behaviour that broke: polling a
// database where nothing happened has to produce nothing.
func TestUnchangedRowsProduceNoEvents(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version string
	}{
		{"with a version", "5"},
		{"with no version", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []unstructured.Unstructured{richRow("acme", "order-1", tc.version)}
			cache := lightweightCache(t, rows, nil)

			before := len(cache.history)
			for range 3 {
				if err := cache.poll(context.Background()); err != nil {
					t.Fatalf("polling: %v", err)
				}
			}

			if got := len(cache.history) - before; got != 0 {
				t.Errorf("three polls over an unchanged row produced %d event batches, want none", got)
			}
		})
	}
}

// TestLightweightWatchLosesNothingWhileConnecting covers the window between a
// watcher being registered and its initial state being read.
//
// A lightweight cache reads that state from the database rather than holding
// it, and the read has to happen after registration. Taken before, a poll
// landing in between would deliver its events to every watcher except this one
// — and the client would be told a resourceVersion covering changes it never
// received, so it would never ask for them again.
//
// Duplicates are the acceptable side of that trade: an object may arrive in
// both the snapshot and an event, which a client merges, where a missing change
// is invisible and permanent.
func TestLightweightWatchLosesNothingWhileConnecting(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "1")}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.lightweight = true
	// Reads forward, so a poll after the row is added actually delivers it.
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		if since == "" {
			return rows, nil
		}
		var out []unstructured.Unstructured
		for _, row := range rows {
			if movesForward(since, row.GetResourceVersion()) {
				out = append(out, row)
			}
		}
		return out, nil
	}
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return nil, nil }
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("populating the cache: %v", err)
	}

	// A poll that lands while the initial state is being read. The list hook is
	// what Watch calls for that read, so driving it from here puts the poll
	// exactly in the window under test.
	var once sync.Once
	cache.list = func(ctx context.Context) ([]unstructured.Unstructured, error) {
		// What this read returns, captured before the poll below adds to it —
		// so the new row reaches the watcher only as an event, never in the
		// snapshot. That is what makes this test able to tell the two
		// orderings apart.
		answer := append([]unstructured.Unstructured(nil), rows...)

		once.Do(func() {
			rows = append(rows, richRow("acme", "order-2", "2"))
			if err := cache.poll(ctx); err != nil {
				t.Errorf("polling mid-connect: %v", err)
			}
		})
		return answer, nil
	}

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	// Both rows have to reach the watcher, by whichever route.
	seen := map[string]bool{}
	deadline := time.After(10 * time.Second)
	for len(seen) < 2 {
		select {
		case event := <-w.ResultChan():
			if obj, ok := event.Object.(*unstructured.Unstructured); ok {
				seen[obj.GetName()] = true
			}
		case <-deadline:
			t.Fatalf("saw %v; a change delivered while this watcher was connecting was lost, "+
				"and the client would never ask for it again", seen)
		}
	}
}
