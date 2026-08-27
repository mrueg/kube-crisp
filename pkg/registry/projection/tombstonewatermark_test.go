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
