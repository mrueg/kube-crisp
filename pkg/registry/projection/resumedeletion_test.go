package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// eventsFrom describes what a watcher was handed, as a client would read it:
// what happened, to which object, at which version.
func eventsFrom(t *testing.T, w watch.Interface) []string {
	t.Helper()

	var seen []string
	deadline := time.After(10 * time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("an event carried %T, want an object", event.Object)
			}
			seen = append(seen, string(event.Type)+"("+obj.GetName()+")")
		case <-time.After(500 * time.Millisecond):
			return seen
		case <-deadline:
			return seen
		}
	}
}

// TestAResumedWatchIsToldAboutADeletionThatDidNotMoveTheVersion.
//
// On a projection that maps a resourceVersion the cache's version comes from
// the rows, which is what lets several replicas agree on it. A deletion has no
// row to raise it: the resync notices order-1 stopped coming back, reports it,
// and the mark stays exactly where it was. The event is then recorded as having
// happened at the version it did not move.
//
// A client that listed at that version and reconnects to it looks identical to
// one that has already seen the deletion, and used to be admitted with nothing
// replayed and no 410 — so it kept a row that no longer exists for as long as
// it stayed connected, which is the guarantee in docs/reference.md that a
// resumed watch is handed what it missed.
func TestAResumedWatchIsToldAboutADeletionThatDidNotMoveTheVersion(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", "1"),
		cachedItem("acme", "order-2", "2"),
	}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	drain(t, w, 2)

	// What a list hands the client, and what its watch will come back with.
	listed := cache.ResourceVersion()
	if listed != "2" {
		t.Fatalf("the cache reports version %q, want 2 — the highest version in the rows", listed)
	}

	// The client goes away: an informer whose watch timed out and is about to
	// re-establish from the version it holds.
	w.Stop()

	// order-1 disappears from the table while nobody is connected. Nothing
	// raises the mark, because the row that would have is the one that is gone.
	rows = rows[1:]
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
	if after := cache.ResourceVersion(); after != listed {
		t.Fatalf("the deletion moved the version to %q; this test needs the case where it "+
			"cannot, which is what makes the resume ambiguous", after)
	}

	resumed, err := cache.Watch(context.Background(), "acme", nil, nil, listed, false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("resuming from %q returned %v", listed, err)
	}
	defer resumed.Stop()

	seen := eventsFrom(t, resumed)
	if len(seen) != 1 || seen[0] != "DELETED(order-1)" {
		t.Errorf("the resumed watch received %v, want just DELETED(order-1): without it the "+
			"client keeps a row that no longer exists and nothing ever tells it otherwise", seen)
	}
}

// TestAResumedWatchStillReplaysTheDeletionOnceTheVersionHasMovedOn covers the
// same deletion after later writes have carried the mark past it. The removal
// is still recorded at a version it did not move, so it is still invisible to
// the search for events strictly newer than the client's — and it has to come
// out in front of the changes that did move the version, which is the order
// they happened in.
func TestAResumedWatchStillReplaysTheDeletionOnceTheVersionHasMovedOn(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", "1"),
		cachedItem("acme", "order-2", "2"),
	}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	drain(t, w, 2)
	listed := cache.ResourceVersion()
	w.Stop()

	// The deletion the version cannot express, and then a write that moves it.
	rows = rows[1:]
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
	rows = append(rows, cachedItem("acme", "order-3", "7"))
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	resumed, err := cache.Watch(context.Background(), "acme", nil, nil, listed, false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("resuming from %q returned %v", listed, err)
	}
	defer resumed.Stop()

	seen := eventsFrom(t, resumed)
	if len(seen) != 2 || seen[0] != "DELETED(order-1)" || seen[1] != "ADDED(order-3)" {
		t.Errorf("the resumed watch received %v, want DELETED(order-1) then ADDED(order-3)", seen)
	}
}

// TestResumingAtTheCurrentVersionDoesNotReplayTheCollection. The priming poll
// records every row as an addition, at the mark it derives from those very
// rows — so replaying everything recorded at an unmoved version, rather than
// only the removals, would hand the whole collection back to every informer
// that lists and then watches. That is the case the cache is stamped for in the
// first place, and it has to stay free.
func TestResumingAtTheCurrentVersionDoesNotReplayTheCollection(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", "1"),
		cachedItem("acme", "order-2", "2"),
	}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	// A list, which primes the cache and stamps what the watch will send back.
	listed := cache.versionFor(context.Background())
	if listed != "2" {
		t.Fatalf("the list stamped %q, want 2", listed)
	}

	w, err := cache.Watch(context.Background(), "acme", nil, nil, listed, false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("watching from the version the list stamped returned %v", err)
	}
	defer w.Stop()

	if seen := eventsFrom(t, w); len(seen) != 0 {
		t.Errorf("watching from a list's own version replayed %v; the client already has all "+
			"of it, and this is the relist the stamped version exists to avoid", seen)
	}
}
