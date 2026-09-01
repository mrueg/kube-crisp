package projection

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// TestADatabaseReplayCannotLoseAConcurrentPoll.
//
// A watcher resuming from beyond the in-memory ring is answered out of the
// database, and that read is a round trip: it cannot happen while the cache
// lock is held, because every List takes the same lock. What it can happen
// after is the watcher joining c.watchers, and until it does there is a window
// where a poll belongs to neither half — its targets were snapshotted without
// this watcher, so the broadcast goes past it, and its rows were committed
// after the replay's own query, so the replay does not carry them either. The
// watcher is admitted with no 410 and never hears about them.
//
// The lightweight snapshot path states the rule this used to break: a read
// taken before registration misses anything a poll delivered in between.
func TestADatabaseReplayCannotLoseAConcurrentPoll(t *testing.T) {
	var mu sync.Mutex
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", "1"),
		cachedItem("acme", "order-3", "3"),
	}
	snapshot := func() []unstructured.Unstructured {
		mu.Lock()
		defer mu.Unlock()
		return append([]unstructured.Unstructured(nil), rows...)
	}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return snapshot(), nil })
	t.Cleanup(cache.Close)

	// Nothing remembered in memory, so the resume below has to be answered
	// from the database or not at all.
	cache.historySize = 0
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		if since == "" {
			return snapshot(), nil
		}
		var out []unstructured.Unstructured
		for _, row := range snapshot() {
			if movesForward(since, row.GetResourceVersion()) {
				out = append(out, row)
			}
		}
		return out, nil
	}

	// The write lands in the window: after the replay has read the rows that
	// changed, and before the watcher asking for them is registered. Hooked
	// onto the deletion query because that is the replay's second read, so
	// order-5 is committed too late for the first one — which is what a row
	// committed a moment after the replay's query does on its own.
	var once sync.Once
	cache.deleted = func(_ context.Context, since string) ([]cacheIdentity, error) {
		// The interleaved poll asks this too, from the mark it has reached.
		if since != "1" {
			return nil, nil
		}
		once.Do(func() {
			mu.Lock()
			rows = append(rows, cachedItem("acme", "order-5", "5"))
			mu.Unlock()
			if err := cache.poll(context.Background()); err != nil {
				t.Errorf("the interleaved poll returned %v", err)
			}
		})
		return nil, nil
	}

	// Somebody is already watching, which is why the cache is polling at all.
	keepalive, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer keepalive.Stop()
	drain(t, keepalive, 2)

	resumed, err := cache.Watch(context.Background(), "acme", nil, nil, "1", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("resuming from version 1 returned %v", err)
	}
	defer resumed.Stop()

	cache.mu.Lock()
	_, landed := cache.items["acme/order-5"]
	cache.mu.Unlock()
	if !landed {
		t.Fatal("the interleaved poll never ran, so this test did not reproduce the window")
	}

	var seen []string
	deadline := time.After(10 * time.Second)
collect:
	for {
		select {
		case event := <-resumed.ResultChan():
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("an event carried %T, want an object", event.Object)
			}
			seen = append(seen, obj.GetName())
			if event.Type == watch.Deleted {
				t.Errorf("%s was reported as deleted; nothing was removed", obj.GetName())
			}
		case <-time.After(500 * time.Millisecond):
			break collect
		case <-deadline:
			break collect
		}
	}

	for _, name := range []string{"order-3", "order-5"} {
		if !slices.Contains(seen, name) {
			t.Errorf("%s changed after the resumed version and never reached the watcher, "+
				"which was admitted without a 410; it received %v", name, seen)
		}
	}
	if slices.Contains(seen, "order-1") {
		t.Errorf("order-1 is not newer than the resumed version and was replayed anyway; "+
			"the watcher received %v", seen)
	}
}
