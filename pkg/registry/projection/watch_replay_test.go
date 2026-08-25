package projection

import (
	"context"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// replayCache builds a cache whose history ring holds nothing, so every resume
// has to come from the database or not at all.
func replayCache(t *testing.T, rows []unstructured.Unstructured, removed []cacheIdentity) *watchCache {
	t.Helper()

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	// Nothing remembered in memory: this is a restarted server, or a replica
	// the client has not spoken to before.
	cache.historySize = 0
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
	cache.deleted = func(_ context.Context, _ string) ([]cacheIdentity, error) { return removed, nil }

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("populating the cache: %v", err)
	}
	return cache
}

// TestWatchResumesFromTheDatabase covers what a client used to be told when it
// reconnected to a server that had restarted, or to a different replica: 410,
// and relist the whole collection.
//
// The history ring is in memory, dies with the process, and is different on
// every replica. The database is none of those things — for a projection that
// records versions and keeps tombstones it already holds what the client
// missed, so it can be answered instead of turned away.
func TestWatchResumesFromTheDatabase(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", "5"),
		cachedItem("acme", "order-2", "9"),
		cachedItem("acme", "order-3", "12"),
	}
	cache := replayCache(t, rows, nil)

	// Beyond anything the (empty) ring can cover.
	w, err := cache.Watch(context.Background(), "acme", nil, nil, "6", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("resuming from a version the ring cannot cover returned %v; the database "+
			"could have answered it", err)
	}
	defer w.Stop()

	// Only what changed after version 6, not the whole collection.
	seen := map[string]watch.EventType{}
	for range 2 {
		select {
		case event := <-w.ResultChan():
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("event carried %T, want an object", event.Object)
			}
			seen[obj.GetName()] = event.Type
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d event(s) arrived; want the two rows newer than the resumed version", len(seen))
		}
	}

	if _, replayed := seen["order-1"]; replayed {
		t.Error("order-1 is older than the resumed version and was replayed anyway, which is " +
			"the relist this exists to avoid")
	}
	for _, name := range []string{"order-2", "order-3"} {
		if _, ok := seen[name]; !ok {
			t.Errorf("%s changed after the resumed version and was not replayed", name)
		}
	}
}

// TestWatchReplaysDeletionsFromTombstones: a replay that reported every change
// and no removal would leave a client holding objects that are gone, silently,
// for as long as it stayed connected. That is worse than being asked to relist.
func TestWatchReplaysDeletionsFromTombstones(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "5")}
	removed := []cacheIdentity{{namespace: "acme", name: "order-gone"}}

	cache := replayCache(t, rows, removed)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "1", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	var deleted bool
	for range 2 {
		select {
		case event := <-w.ResultChan():
			if event.Type != watch.Deleted {
				continue
			}
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("the deletion carried %T, want an object", event.Object)
			}
			if obj.GetName() != "order-gone" {
				t.Errorf("the deletion names %q, want order-gone", obj.GetName())
			}
			deleted = true
		case <-time.After(5 * time.Second):
		}
	}

	if !deleted {
		t.Error("the tombstoned row was never reported as deleted, so a resumed client keeps it")
	}
}

// TestWatchWithoutTombstonesStillRefuses. A projection that cannot say what was
// deleted cannot be replayed correctly, and 410 is worse service and better
// behaviour than a client whose cache quietly disagrees with the table.
func TestWatchWithoutTombstonesStillRefuses(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "5")}
	cache := replayCache(t, rows, nil)
	cache.deleted = nil

	_, err := cache.Watch(context.Background(), "acme", nil, nil, "1", false, false, deletedTestGVK)
	if !apierrors.IsResourceExpired(err) {
		t.Errorf("resuming a projection with no deletion query returned %v, want 410 — without "+
			"tombstones a replay cannot report removals", err)
	}
}

// TestWatchPrefersRelistingOverAHugeReplay, since past a point replaying costs
// more than starting over and the client is better served by being told so.
func TestWatchPrefersRelistingOverAHugeReplay(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "5")}
	// Far more tombstones than objects, and past the floor below which the
	// comparison is not worth making.
	removed := make([]cacheIdentity, 0, minDatabaseReplay*2)
	for i := range minDatabaseReplay * 2 {
		removed = append(removed, cacheIdentity{namespace: "acme", name: fmt.Sprintf("gone-%d", i)})
	}
	cache := replayCache(t, rows, removed)

	_, err := cache.Watch(context.Background(), "acme", nil, nil, "1", false, false, deletedTestGVK)
	if !apierrors.IsResourceExpired(err) {
		t.Errorf("a replay larger than the collection returned %v, want 410 so the client "+
			"relists instead", err)
	}
}

// TestReplayedDeletionCarriesAKind covers the same defect that broke the watch
// stream once already, in the one place it was left.
//
// A Deleted event whose object has no apiVersion or kind cannot be encoded:
// the conversion fails, and the apiserver's watch loop ends the response
// mid-stream with no Error event and nothing logged. This is reachable exactly
// where the replay path exists for — a client resuming against a restarted
// server, where the cache holds nothing and the tombstone records only the
// identity, which is the documented minimum for a tombstone table.
func TestReplayedDeletionCarriesAKind(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "5")}
	// Identity only: no mapped columns, so nothing can describe the row.
	removed := []cacheIdentity{{namespace: "acme", name: "order-gone"}}

	cache := replayCache(t, rows, removed)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "1", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

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
			if obj.GetAPIVersion() == "" || obj.GetKind() == "" {
				t.Fatalf("the replayed deletion of %q carries apiVersion=%q kind=%q; it cannot "+
					"be encoded, so the watch response ends mid-stream with nothing logged",
					obj.GetName(), obj.GetAPIVersion(), obj.GetKind())
			}
			return
		case <-deadline:
			t.Fatal("no deletion arrived")
		}
	}
}
