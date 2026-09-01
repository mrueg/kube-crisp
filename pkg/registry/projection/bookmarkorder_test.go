package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// streamAfter collects everything a watcher has to say until it goes quiet,
// describing each event the way a client would read it: what happened, to
// which object, at which version.
func streamAfter(t *testing.T, w watch.Interface) []string {
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
			seen = append(seen, string(event.Type)+"("+obj.GetName()+"@"+obj.GetResourceVersion()+")")
		case <-time.After(500 * time.Millisecond):
			return seen
		case <-deadline:
			return seen
		}
	}
}

// TestABookmarkArrivesAfterTheEventsThatMadeItsVersion.
//
// A bookmark says "you have seen everything up to this version", and a client
// keeps it as the point to reconnect from. Emitted from inside applyLocked it
// was queued before the poll's events, which are broadcast after the lock is
// released — so the watcher was told it was at version 5 and only then handed
// the row that made the version 5.
//
// Nothing about that is visible to the client: it reconnects at 5, is replayed
// what happened strictly after 5, and the row simply never arrives.
func TestABookmarkArrivesAfterTheEventsThatMadeItsVersion(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "1")}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	// Every poll bookmarks, so the one below is guaranteed to race the events
	// it is speaking for.
	cache.bookmarkInterval = time.Nanosecond

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, true, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	drain(t, w, 1)

	// A row appears, and with it the version the bookmark will carry.
	rows = append(rows, cachedItem("acme", "order-2", "5"))
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	seen := streamAfter(t, w)
	if len(seen) != 2 {
		t.Fatalf("the poll produced %v, want the added row and one bookmark", seen)
	}
	if seen[0] != "ADDED(order-2@5)" {
		t.Errorf("the first event after the poll was %s, want ADDED(order-2@5)", seen[0])
	}
	if seen[1] != "BOOKMARK(@5)" {
		t.Errorf("the second event after the poll was %s, want the bookmark last: a bookmark "+
			"ahead of the events at its own version promises a resume point that has not "+
			"been delivered, and the client loses them with no error and no gap", seen[1])
	}
}

// TestAnIdlePollStillSendsABookmark. The point of a periodic bookmark is the
// poll where nothing happened: a client sitting on an unchanging projection
// still needs a recent version to reconnect from, rather than replaying the
// collection it already has. Moving the emission behind the broadcast must not
// cost that, since a poll with no events broadcasts nothing at all.
func TestAnIdlePollStillSendsABookmark(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "1")}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.bookmarkInterval = time.Nanosecond

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, true, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	drain(t, w, 1)

	// Nothing changed.
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	seen := streamAfter(t, w)
	if len(seen) != 1 || seen[0] != "BOOKMARK(@1)" {
		t.Errorf("a poll that changed nothing produced %v, want one bookmark at the current "+
			"version", seen)
	}
}
