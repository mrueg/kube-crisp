package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// pollWithReadCache builds a watched projection that also caches reads, primed
// so that the next poll is the one under test.
//
// The priming matters. The first poll of an empty cache reports every row as an
// Added event, so it invalidates like any other change — which would make every
// assertion below pass for the wrong reason. The read cache is seeded after it.
func pollWithReadCache(t *testing.T, rows *[]unstructured.Unstructured) (*watchCache, *readCache) {
	t.Helper()

	cache := newWatchCache(time.Hour, "orders.store.example.com", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return *rows, nil })
	t.Cleanup(cache.Close)

	reads := newReadCache(time.Minute, "orders.store.example.com")
	cache.dropReadsOnChange(reads)

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("priming the watch cache: %v", err)
	}
	return cache, reads
}

// seedReads fills the read cache the way a set of clients listing each tenant,
// and one listing across all of them, would leave it.
func seedReads(reads *readCache) {
	reads.putList(listKey("acme", nil), "acme", &unstructured.UnstructuredList{})
	reads.putObject(objectKey("acme", "order-1"), "acme", &unstructured.Unstructured{Object: map[string]any{}})
	reads.putList(listKey("globex", nil), "globex", &unstructured.UnstructuredList{})
	reads.putList(listKey("", nil), "", &unstructured.UnstructuredList{})
}

// TestAPollThatObservesAChangeDropsTheReadCache is the whole point of hooking
// the poller.
//
// A write drops the read cache of the replica that served it and of no other,
// so on every other replica a cached read stays answerable for the full
// cacheTTL after the data changed underneath it. That looks to the client
// exactly like the write not having happened. There is no channel between
// replicas — but a watched projection polls on every replica already, so the
// poll that notices the change is where the stale entries can go.
func TestAPollThatObservesAChangeDropsTheReadCache(t *testing.T) {
	rows := []unstructured.Unstructured{
		richRow("acme", "order-1", "5"),
		richRow("globex", "order-9", "5"),
	}
	cache, reads := pollWithReadCache(t, &rows)

	seedReads(reads)
	if got, want := reads.Len(), 4; got != want {
		t.Fatalf("the read cache holds %d entries before the change, want %d", got, want)
	}

	// The write another replica served. This one only ever sees its effect on
	// the table.
	rows[0] = richRow("acme", "order-1", "6")
	rows[1] = richRow("globex", "order-9", "6")

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	if got := reads.Len(); got != 0 {
		t.Errorf("the read cache holds %d entries after a poll that saw both rows change, want 0; "+
			"a client reading from this replica is still being answered from before the write", got)
	}
}

// TestAPollThatObservesNothingKeepsTheReadCache is the other half, and the one
// that keeps cacheTTL meaning something.
//
// Dropping the cache on every poll regardless would make the TTL almost
// irrelevant — a watched projection polls every few seconds — and turn the
// poller into a source of exactly the database load the cache was configured to
// avoid.
func TestAPollThatObservesNothingKeepsTheReadCache(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	cache, reads := pollWithReadCache(t, &rows)

	seedReads(reads)

	// Nothing touched the table between the two polls.
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	for _, key := range []string{listKey("acme", nil), objectKey("acme", "order-1"), listKey("globex", nil), listKey("", nil)} {
		if _, ok := reads.lookup(key); !ok {
			t.Errorf("entry %q was dropped by a poll that observed no change", key)
		}
	}
}

// TestAPollDropsOnlyTheNamespacesItObservedChanging keeps the poller to the same
// rule a write follows: one tenant changing is not a reason to make every other
// tenant pay for a fresh read.
func TestAPollDropsOnlyTheNamespacesItObservedChanging(t *testing.T) {
	rows := []unstructured.Unstructured{
		richRow("acme", "order-1", "5"),
		richRow("globex", "order-9", "5"),
	}
	cache, reads := pollWithReadCache(t, &rows)

	seedReads(reads)

	// Only acme's row moves.
	rows[0] = richRow("acme", "order-1", "6")

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	for _, key := range []string{listKey("acme", nil), objectKey("acme", "order-1")} {
		if _, ok := reads.lookup(key); ok {
			t.Errorf("entry %q survived a poll that saw its namespace change", key)
		}
	}
	// A read across all namespaces contained the changed row by definition.
	if _, ok := reads.lookup(listKey("", nil)); ok {
		t.Error("the cluster-wide list survived a poll that saw a row inside it change")
	}
	if _, ok := reads.lookup(listKey("globex", nil)); !ok {
		t.Error("globex's list was dropped by a change in another tenant")
	}
}

// TestAClusterScopedChangeDropsEveryReadCacheEntry covers the projection with no
// tenants to separate. There is no namespace to narrow to, so narrowing would
// mean keeping an entry the poll has already shown to be stale.
func TestAClusterScopedChangeDropsEveryReadCacheEntry(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("", "region-eu", "5")}
	cache, reads := pollWithReadCache(t, &rows)

	seedReads(reads)

	rows[0] = richRow("", "region-eu", "6")

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	if got := reads.Len(); got != 0 {
		t.Errorf("the read cache holds %d entries after an unscoped change, want 0", got)
	}
}

// TestAFollowerPollDropsTheReadCache is the case that makes this worth doing at
// all.
//
// The replica that served a write already invalidated its own cache. The
// staleness is on the replicas that did not, and with leader election on those
// are followers: they poll at watch.followerPollInterval rather than the
// projection's own, but they do poll, through the same refresh path. If that
// path did not invalidate, the fix would only ever help the replica that needed
// no help.
func TestAFollowerPollDropsTheReadCache(t *testing.T) {
	SetLeadership(func() bool { return false })
	t.Cleanup(func() { SetLeadership(nil) })

	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	cache, reads := pollWithReadCache(t, &rows)

	// The follower interval only applies when it is genuinely slower than what
	// the projection asked for, so the projection has to ask for something
	// faster than a minute before this replica is polling as a follower at all.
	cache.interval = time.Second

	if got, want := cache.effectiveInterval(), DefaultFollowerPollInterval; got != want {
		t.Fatalf("this replica polls every %s, want the follower interval %s", got, want)
	}

	// refresh drops a cache with no watchers instead of polling it, which is
	// what the poll group relies on to stop polling an unwatched projection.
	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	seedReads(reads)

	rows[0] = richRow("acme", "order-1", "6")

	// The entry point the poll group uses on its timer, rather than poll
	// directly, so this covers the path a follower actually takes.
	if err := cache.refresh(context.Background()); err != nil {
		t.Fatalf("refresh() returned error: %v", err)
	}

	if _, ok := reads.lookup(listKey("acme", nil)); ok {
		t.Error("a follower poll that saw the row change left the stale entry in place; " +
			"the replica that did not serve the write is the one that needed dropping")
	}
}

// TestAProjectionWithNoReadCacheStillPolls guards the nil case. Caching is off
// unless a projection asks for it, and every read-cache method tolerates a nil
// receiver so the paths that use it do not have to ask.
func TestAProjectionWithNoReadCacheStillPolls(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}

	cache := newWatchCache(time.Hour, "orders.store.example.com", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	// What a projection without cacheTTL is wired with.
	cache.dropReadsOnChange(nil)

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
	rows[0] = richRow("acme", "order-1", "6")
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
}
