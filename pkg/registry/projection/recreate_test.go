package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// TestARecreatedRowSurvivesItsOwnTombstone covers a name deleted and created
// again inside one poll window.
//
// The poll reads the changed rows and the tombstones together. The re-created
// row is in the first list with a new version; the tombstone for the previous
// incarnation is in the second. Applying the tombstone unconditionally emits a
// Deleted for the row that exists and drops it from the cache — and an
// incremental poll will not return it again, because its version is no longer
// past :since. With fullResyncInterval: "0s" it stays invisible to every
// watcher while sitting in the table.
func TestARecreatedRowSurvivesItsOwnTombstone(t *testing.T) {
	// The row as it exists now: re-created, version 9.
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "9")}

	// The tombstone of the incarnation deleted at version 5.
	gone := cachedItem("acme", "order-1", "5")
	removed := []cacheIdentity{{namespace: "acme", name: "order-1", object: &gone}}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	// Returns the row on every poll, so it is in the cache before the stale
	// tombstone arrives — otherwise the assertion below measures nothing.
	cache.incremental = func(context.Context, string) ([]unstructured.Unstructured, error) {
		return rows, nil
	}
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return removed, nil }

	// Seed with the row present, then poll again with the stale tombstone in
	// play.
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	cache.mu.Lock()
	seeded := len(cache.items)
	cache.mu.Unlock()
	if seeded != 1 {
		t.Fatalf("the cache holds %d rows after seeding, want 1; the tombstone below would "+
			"have nothing to remove and this test would prove nothing", seeded)
	}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}

	cache.mu.Lock()
	_, present := cache.items["acme/order-1"]
	cache.mu.Unlock()

	if !present {
		t.Error("the re-created row was removed from the cache by the tombstone of the " +
			"incarnation it replaced; an incremental poll will not return it again, so it is " +
			"invisible to every watcher while sitting in the table")
	}
}

// TestARecreatedRowSurvivesAnIdentityOnlyTombstone is the same race with a
// tombstone that records only which row it was, so no version can say which
// incarnation it belongs to.
//
// The row was deleted and created again inside one poll interval. The poll
// reads it back at its new version and reads the tombstone of the old one in
// the same pass. Applied after the row, the tombstone emitted a Deleted for the
// row that exists and dropped it from the cache — and the next poll reads
// forward from the new version, so the row never comes back.
func TestARecreatedRowSurvivesAnIdentityOnlyTombstone(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "5")}
	var (
		changed []unstructured.Unstructured
		removed []cacheIdentity
	)

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.fullResyncInterval = 0
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		if since == "" {
			return rows, nil
		}
		return changed, nil
	}
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return removed, nil }

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	drain(t, w, 1)

	// Deleted and created again between two polls: the row is back at 8, and
	// the tombstone names it and nothing more.
	changed = []unstructured.Unstructured{cachedItem("acme", "order-1", "8")}
	removed = []cacheIdentity{{namespace: "acme", name: "order-1"}}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}

	if last := lastEventType(w); last == watch.Deleted {
		t.Error("the last event for the re-created row is a deletion, so an informer drops a " +
			"row that is sitting in the table")
	}

	cache.mu.Lock()
	_, present := cache.items["acme/order-1"]
	cache.mu.Unlock()
	if !present {
		t.Error("the re-created row was removed from the cache by an identity-only tombstone " +
			"read in the same poll; the next poll reads past its version and never returns it")
	}
}

// TestADatabaseReplayDoesNotDeleteARecreatedRow: a client away while a name was
// deleted and created again is owed the row as it is now, not a deletion of it.
//
// The replay reads the changed rows and the tombstones from the same version.
// Both carry the name, and the deletions were appended after the changes — so
// the client was handed the new row and then told it was gone, and its informer
// dropped a row that exists.
func TestADatabaseReplayDoesNotDeleteARecreatedRow(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "9")}
	gone := cachedItem("acme", "order-1", "7")
	removed := []cacheIdentity{{namespace: "acme", name: "order-1", object: &gone}}

	cache := replayCache(t, rows, removed)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "6", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	switch lastEventType(w) {
	case "":
		t.Fatal("the replay sent nothing; the re-created row changed after the resumed version")
	case watch.Deleted:
		t.Error("the replay ended with a deletion of a row that exists, so a resumed informer " +
			"drops it")
	}
}

// lastEventType drains a watch until it goes quiet and reports the type of the
// last event it delivered, or "" when there was none.
func lastEventType(w watch.Interface) watch.EventType {
	var last watch.EventType
	for {
		select {
		case event := <-w.ResultChan():
			last = event.Type
		case <-time.After(200 * time.Millisecond):
			return last
		}
	}
}
