package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// resyncingCache builds an incrementally polled cache whose full reads can be
// made to fail on demand, primed so that the next poll is the one under test.
//
// The first poll of a cache is a full read too, and has to succeed for there to
// be anything to poll incrementally from — so failing is armed by the test,
// after priming, rather than from the start.
func resyncingCache(t *testing.T, rows *[]unstructured.Unstructured) (cache *watchCache, fullReads *int, failFull *bool) {
	t.Helper()

	cache = newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return *rows, nil })
	t.Cleanup(cache.Close)

	fullReads = new(int)
	failFull = new(bool)
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		if since == "" {
			*fullReads++
			if *failFull {
				// What a table that has outgrown the statement looks like.
				return nil, errors.New("canceling statement due to statement timeout")
			}
			return *rows, nil
		}
		var out []unstructured.Unstructured
		for _, row := range *rows {
			if movesForward(since, row.GetResourceVersion()) {
				out = append(out, row)
			}
		}
		return out, nil
	}

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("priming the cache: %v", err)
	}
	return cache, fullReads, failFull
}

// TestAFailingFullResyncDoesNotStarveIncrementalPolling is a regression test.
//
// A resync that failed left lastFullResync where it was, so the resync was due
// again on the next tick, and the next, and every one after it. Once a full
// read of the table took longer than the poll could wait — pollInterval: 1s
// over a table that takes six seconds to read and map — every poll from then on
// was a full one that failed the same way. No poll was ever incremental again,
// the watchers saw nothing move, and only the error counter said so.
func TestAFailingFullResyncDoesNotStarveIncrementalPolling(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "1")}
	cache, fullReads, failFull := resyncingCache(t, &rows)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	select {
	case <-w.ResultChan():
	case <-time.After(5 * time.Second):
		t.Fatal("the initial state never arrived")
	}

	// The periodic resync is due, and the table has become too large for it.
	cache.mu.Lock()
	cache.fullResyncInterval = 4 * time.Hour
	cache.lastFullResync = time.Time{}
	cache.mu.Unlock()
	*failFull = true
	primed := *fullReads

	if err := cache.poll(context.Background()); err == nil {
		t.Fatal("the resync succeeded though its full read failed")
	}
	if got := *fullReads - primed; got != 1 {
		t.Fatalf("the failing poll ran %d full reads, want 1", got)
	}

	cache.mu.Lock()
	notAdvanced := cache.lastFullResync.IsZero()
	deferred := time.Now().Before(cache.resyncRetryAt)
	backoff := cache.resyncBackoff
	cache.mu.Unlock()
	if !notAdvanced {
		t.Error("a failed resync advanced lastFullResync as if it had completed")
	}
	if !deferred {
		t.Fatal("a failed resync left the next attempt due immediately, which is one failing " +
			"full read per tick with nothing incremental in between")
	}
	if backoff != cache.interval {
		t.Errorf("the first deferral is %s, want the poll interval %s", backoff, cache.interval)
	}

	// The next ticks: a row changes, and the resync is still failing. Every
	// one of them has to be incremental, and the change has to reach the
	// watcher through them.
	rows = append(rows, cachedItem("acme", "order-2", "2"))
	for i := 0; i < 3; i++ {
		if err := cache.poll(context.Background()); err != nil {
			t.Fatalf("poll %d after the failed resync returned error: %v; it should have been "+
				"incremental, and incremental reads are working", i, err)
		}
	}
	if got := *fullReads - primed; got != 1 {
		t.Errorf("%d full reads ran while the retry was deferred, want 1; the resync is being "+
			"retried on every tick", got)
	}

	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Added || event.Object.(*unstructured.Unstructured).GetName() != "order-2" {
			t.Errorf("the watcher received %s %s, want ADDED order-2",
				event.Type, event.Object.(*unstructured.Unstructured).GetName())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the row added after the failed resync never reached the watcher")
	}

	// The deferral runs out and the resync is tried again, fails again, and is
	// deferred by twice as much this time.
	cache.mu.Lock()
	cache.resyncRetryAt = time.Time{}
	cache.mu.Unlock()
	if err := cache.poll(context.Background()); err == nil {
		t.Fatal("the retried resync succeeded though its full read still fails")
	}
	if got := *fullReads - primed; got != 2 {
		t.Fatalf("the retry ran %d full reads in total, want 2", got)
	}
	cache.mu.Lock()
	backoff = cache.resyncBackoff
	cache.mu.Unlock()
	if want := 2 * cache.interval; backoff != want {
		t.Errorf("the second deferral is %s, want %s: the retry does not back off", backoff, want)
	}

	// Doubling stops at the resync interval: a resync that has been failing
	// for a while is still tried as often as it was configured to run.
	for i := 0; i < 4; i++ {
		cache.mu.Lock()
		cache.resyncRetryAt = time.Time{}
		cache.mu.Unlock()
		_ = cache.poll(context.Background())
	}
	cache.mu.Lock()
	backoff = cache.resyncBackoff
	limit := cache.fullResyncInterval
	cache.mu.Unlock()
	if backoff != limit {
		t.Errorf("after repeated failures the deferral is %s, want it capped at the resync "+
			"interval %s", backoff, limit)
	}

	// The table shrinks back, or the statement is given the time it needs. The
	// next attempt completes, and the schedule is back to the ordinary one.
	*failFull = false
	cache.mu.Lock()
	cache.resyncRetryAt = time.Time{}
	cache.mu.Unlock()
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("the resync failed once its full read was working again: %v", err)
	}
	cache.mu.Lock()
	completed := !cache.lastFullResync.IsZero()
	backoff = cache.resyncBackoff
	retryAt := cache.resyncRetryAt
	cache.mu.Unlock()
	if !completed {
		t.Error("a resync that completed did not advance lastFullResync")
	}
	if backoff != 0 || !retryAt.IsZero() {
		t.Errorf("a resync that completed left a deferral of %s behind", backoff)
	}
}

// TestAFullResyncIsNotBoundByThePollInterval covers the other half of the same
// failure: the deadline that made a slow full read fail in the first place.
//
// A poll was given the poll interval plus a few seconds, whichever kind of read
// it ran. For an incremental read that is a fair bound. For a full one it tied
// how long the whole table may take to read to how often the projection wants
// to notice a change, which are unrelated — and the statement already has a
// timeout of its own, set by whoever wrote it with the table in mind.
func TestAFullResyncIsNotBoundByThePollInterval(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "1")}

	var fullDeadline, incrementalDeadline bool
	cache := newWatchCache(time.Second, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.incremental = func(ctx context.Context, since string) ([]unstructured.Unstructured, error) {
		_, bounded := ctx.Deadline()
		if since == "" {
			fullDeadline = bounded
			return rows, nil
		}
		incrementalDeadline = bounded
		return nil, nil
	}

	// The first poll is a full read; the second is incremental.
	for i := 0; i < 2; i++ {
		if err := cache.poll(context.Background()); err != nil {
			t.Fatalf("poll %d returned error: %v", i, err)
		}
	}

	if fullDeadline {
		t.Error("the full read ran under a deadline of the poll's own; the statement's timeout " +
			"is what should bound it")
	}
	if !incrementalDeadline {
		t.Error("the incremental read ran under no deadline at all")
	}
}
