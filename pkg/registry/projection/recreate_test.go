package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
