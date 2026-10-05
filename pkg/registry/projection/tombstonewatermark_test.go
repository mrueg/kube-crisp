package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// A tombstone is returned by every poll whose watermark it is still above, and
// the row it names is gone from the cache after the first one. It must not be
// reported deleted again on every poll for as long as it is returned.
func TestATombstoneIsReportedOnce(t *testing.T) {
	gone := richRow("acme", "order-gone", "9")
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}

	cache := lightweightCache(t, rows, nil)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	drain(t, w, 1)

	// What a tombstone query does: read forward from the watermark it is
	// given. Nothing else in this table is changing, so whether the watermark
	// moves is the whole question.
	cache.deleted = func(_ context.Context, since string) ([]cacheIdentity, error) {
		if since != "" && !movesForward(since, gone.GetResourceVersion()) {
			return nil, nil
		}
		return []cacheIdentity{{namespace: "acme", name: "order-gone", object: &gone}}, nil
	}

	deletions := 0
	for range 3 {
		if err := cache.poll(context.Background()); err != nil {
			t.Fatalf("polling: %v", err)
		}
		deletions += countDeletions(w)
	}

	if deletions != 1 {
		t.Errorf("the tombstone produced %d deletions across three polls, want 1", deletions)
	}
}

// TestATombstoneBelowTheMarkIsReportedOnce is the tombstone the tutorial and
// the e2e environment write: it carries the deleted row's last version as its
// mapped resourceVersion, while the deletion query filters on when it was
// deleted. The deletion time stays ahead of the mark the tombstone's own
// version can raise it to, so every poll returns the same tombstone, for as
// long as it is kept.
//
// The row was dropped from the cache by the first of them. Every later one
// found nothing in the cache and reported the deletion again anyway — and a
// lightweight cache took the tombstone's own row as the cached one, so it did
// not even look. Watchers were handed the same removal on every poll, and the
// history ring filled with copies of it until it held nothing else.
func TestATombstoneBelowTheMarkIsReportedOnce(t *testing.T) {
	for _, lightweight := range []bool{false, true} {
		name := "full"
		if lightweight {
			name = "lightweight"
		}
		t.Run(name, func(t *testing.T) {
			rows := []unstructured.Unstructured{
				richRow("acme", "order-1", "5"),
				richRow("acme", "order-2", "6"),
			}
			cache := newWatchCache(time.Hour, "orders", nil,
				func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
			t.Cleanup(cache.Close)
			cache.lightweight = lightweight
			cache.fullResyncInterval = 0
			cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
				if since == "" {
					return rows, nil
				}
				return nil, nil
			}
			var removed []cacheIdentity
			cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return removed, nil }

			w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
			if err != nil {
				t.Fatalf("Watch() returned error: %v", err)
			}
			defer w.Stop()
			drain(t, w, len(rows))

			// order-1 is deleted. Its tombstone carries the version the row
			// last had, below the mark order-2 set, and the query — filtering
			// on deleted_at, which is past every :since this test reaches —
			// hands it back on every poll whatever the mark is.
			gone := richRow("acme", "order-1", "5")
			removed = []cacheIdentity{{namespace: "acme", name: "order-1", object: &gone}}

			deletions := 0
			for range 3 {
				if err := cache.poll(context.Background()); err != nil {
					t.Fatalf("polling: %v", err)
				}
				deletions += countDeletions(w)
			}
			if deletions != 1 {
				t.Errorf("the tombstone produced %d deletions across three polls, want 1", deletions)
			}

			cache.mu.Lock()
			recorded := 0
			for _, event := range cache.history {
				if event.event.Type == watch.Deleted {
					recorded++
				}
			}
			cache.mu.Unlock()
			if recorded != 1 {
				t.Errorf("the history ring holds %d deletions of order-1, want 1", recorded)
			}
		})
	}
}

func drain(t *testing.T, w watch.Interface, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-w.ResultChan():
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d initial events arrived", i, n)
		}
	}
}

func countDeletions(w watch.Interface) int {
	deletions := 0
	for {
		select {
		case event := <-w.ResultChan():
			if event.Type == watch.Deleted {
				deletions++
			}
		case <-time.After(200 * time.Millisecond):
			return deletions
		}
	}
}
