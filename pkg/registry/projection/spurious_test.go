package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// A projection that maps no resourceVersion: supported, and the cache falls
// back to its own counter.
// TestIdleProjectionProducesNoEvents covers a projection that maps no
// resourceVersion, which is supported: the cache falls back to its own counter.
//
// The snapshot version was stamped onto the event objects in place, and those
// are the same objects the cache keeps. The next poll then compared a freshly
// mapped row carrying no version against a cached one carrying the counter —
// which differ, always — so every object was reported as modified on every
// poll, forever. Measured at 98 spurious events per second against an idle
// table, each one a DeepCopy per watcher, and the version advanced every poll
// so no client could ever resume.
func TestIdleProjectionProducesNoEvents(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", ""),
		cachedItem("acme", "order-2", ""),
	}
	cache := newWatchCache(20*time.Millisecond, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) {
			// Freshly mapped every poll, as the real list does.
			out := make([]unstructured.Unstructured, len(rows))
			for i := range rows {
				out[i] = *rows[i].DeepCopy()
			}
			return out, nil
		})
	t.Cleanup(cache.Close)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	counts := map[watch.EventType]int{}
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			counts[event.Type]++
		case <-deadline:
			t.Logf("in one second, with an idle table: %v", counts)
			if counts[watch.Modified] > 0 {
				t.Errorf("%d spurious MODIFIED events with nothing writing to the table",
					counts[watch.Modified])
			}
			return
		}
	}
}
