package projection

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestReplayDoesNotAliasTheHistoryRing covers a slice handed to a resuming
// watcher that the poller then overwrites underneath it.
//
// replayable returns c.history[i:] while holding the lock, and Watch iterates
// it after releasing it. record compacts the ring in place —
// append(c.history[:0], c.history[overflow:]...) — so once the ring is full the
// copy overwrites the very elements the departing Watch still holds. The client
// is handed events it has already seen and silently loses the one it was
// resuming from, with no 410 and nothing logged.
func TestReplayDoesNotAliasTheHistoryRing(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-0", "0")}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) {
			out := make([]unstructured.Unstructured, len(rows))
			for i := range rows {
				out[i] = *rows[i].DeepCopy()
			}
			return out, nil
		})
	t.Cleanup(cache.Close)
	cache.historySize = 4

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("populating: %v", err)
	}

	// Fill the ring so compaction is happening on every poll.
	for i := 1; i <= 8; i++ {
		rows = append(rows, cachedItem("acme", fmt.Sprintf("order-%d", i), fmt.Sprint(i)))
		if err := cache.poll(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}

	// A resuming watcher reads its replay while the poller keeps compacting.
	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 9; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			rows = append(rows, cachedItem("acme", fmt.Sprintf("order-%d", i), fmt.Sprint(i)))
			_ = cache.poll(context.Background())
		}
	}()

	for range 50 {
		cache.mu.Lock()
		var from string
		if len(cache.history) > 0 {
			from = cache.history[0].from
		}
		cache.mu.Unlock()
		if from == "" {
			continue
		}

		// The same shape Watch uses: take the replay under the lock, read it
		// after releasing.
		cache.mu.Lock()
		missed, ok := cache.replayable(from)
		cache.mu.Unlock()
		if !ok {
			continue
		}
		for _, recorded := range missed {
			_ = recorded.version
			_ = recorded.event.Type
		}
	}

	close(stop)
	wg.Wait()
}
