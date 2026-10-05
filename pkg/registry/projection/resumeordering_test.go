package projection

import (
	"context"
	"fmt"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// TestAClientCutOffMidPollResumesTheRestOfIt covers a projection that maps no
// resourceVersion, where the cache's counter is the version.
//
// Every event of one poll was stamped with the same counter value — the one the
// poll ended at. A client that took the first of them and was then cut off, a
// slow consumer dropped by the broadcast being the usual way, resumed from that
// value, was told it was current, and never received the rest of the poll.
func TestAClientCutOffMidPollResumesTheRestOfIt(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-a", "")}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	first, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	drain(t, first, 1)

	rows = []unstructured.Unstructured{
		cachedItem("acme", "order-a", ""),
		cachedItem("acme", "order-b", ""),
		cachedItem("acme", "order-c", ""),
	}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}

	// The client takes one event and is cut off.
	var got *unstructured.Unstructured
	select {
	case event := <-first.ResultChan():
		got = event.Object.(*unstructured.Unstructured)
	case <-time.After(5 * time.Second):
		t.Fatal("the poll delivered nothing")
	}
	first.Stop()

	resumed, err := cache.Watch(context.Background(), "acme", nil, nil, got.GetResourceVersion(), false, false, deletedTestGVK)
	if err != nil {
		// A 410 would at least be honest; being admitted and told nothing is
		// the bug. Either way the rest of the poll has to reach the client.
		if apierrors.IsResourceExpired(err) {
			return
		}
		t.Fatalf("resuming: %v", err)
	}
	defer resumed.Stop()

	missing := map[string]bool{"order-b": true, "order-c": true}
	delete(missing, got.GetName())
	for {
		select {
		case event := <-resumed.ResultChan():
			delete(missing, event.Object.(*unstructured.Unstructured).GetName())
			if len(missing) == 0 {
				return
			}
			continue
		case <-time.After(500 * time.Millisecond):
		}
		break
	}
	t.Errorf("resumed at %s after receiving %s, and was never sent %v", got.GetResourceVersion(),
		got.GetName(), missing)
}

// TestAFullPollIsDeliveredInVersionOrder covers a projection that maps a
// resourceVersion and has no incremental query, so every poll is a full read.
//
// Its events came out of a map, in no order. A client that took b@19 first and
// was cut off before a@10 resumed from 19 — the version the poll ended at — was
// told it was current, and never heard about a. In version order it cannot have
// seen 19 without everything below it, and deletions, which carry the version
// the row last had, go after every row that is still there.
func TestAFullPollIsDeliveredInVersionOrder(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-old", "1")}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	drain(t, w, 1)

	// order-old goes, and ten rows arrive in one poll.
	rows = nil
	for i := range 10 {
		rows = append(rows, cachedItem("acme", fmt.Sprintf("order-%d", i), fmt.Sprint(10+i)))
	}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}

	var events []watch.Event
	for len(events) < len(rows)+1 {
		select {
		case event := <-w.ResultChan():
			events = append(events, event)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d events arrived", len(events), len(rows)+1)
		}
	}

	if events[len(events)-1].Type != watch.Deleted {
		t.Errorf("the deletion arrived before rows that are newer than the version it carries")
	}
	previous := ""
	for _, event := range events {
		if event.Type == watch.Deleted {
			continue
		}
		version := event.Object.(*unstructured.Unstructured).GetResourceVersion()
		if previous != "" && !movesForward(previous, version) {
			t.Fatalf("version %s was delivered after %s; a client cut off between them resumes "+
				"past the one it never received", version, previous)
		}
		previous = version
	}
}

// TestTheRingIsNotTrimmedIntoAPoll covers a history ring that fills up in the
// middle of a poll, on a projection versioned by the cache's counter.
//
// Every entry of a poll recorded where the poll started, so once the ring had
// dropped the first of them its oldest entry still claimed that starting point
// was covered. A client sitting there was admitted and replayed the poll
// without its first event, with no 410 to tell it anything was missing.
func TestTheRingIsNotTrimmedIntoAPoll(t *testing.T) {
	rows := []unstructured.Unstructured{cachedItem("acme", "order-old", "")}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.historySize = 3

	listed := cache.versionFor(context.Background())

	// One poll with more changes than the ring holds.
	rows = append(rows,
		cachedItem("acme", "order-a", ""),
		cachedItem("acme", "order-b", ""),
		cachedItem("acme", "order-c", ""),
		cachedItem("acme", "order-d", ""),
	)
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}

	assertResumeIsWholeOrRefused(t, cache, listed, "order-a", "order-b", "order-c", "order-d")
}

// TestAForgottenDeletionIsNotReplayedInPart covers deletions that did not move
// a mapped version, which are replayed to every client sitting at that version
// because it may or may not have seen them. Once the ring has dropped one of
// them, a client at that version can no longer be told everything it may have
// missed, and the answer is 410 rather than the deletions that are left.
func TestAForgottenDeletionIsNotReplayedInPart(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-x", "1"),
		cachedItem("acme", "order-y", "2"),
		cachedItem("acme", "order-z", "2"),
	}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.historySize = 1

	listed := cache.versionFor(context.Background())
	if listed != "2" {
		t.Fatalf("listed at %q, want 2", listed)
	}

	// Two rows go and the mark stays where it was.
	rows = rows[2:]
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}

	assertResumeIsWholeOrRefused(t, cache, listed, "order-x", "order-y")
}

// assertResumeIsWholeOrRefused resumes from a version and requires either a 410
// or every one of the named objects.
func assertResumeIsWholeOrRefused(t *testing.T, cache *watchCache, from string, names ...string) {
	t.Helper()

	w, err := cache.Watch(context.Background(), "acme", nil, nil, from, false, false, deletedTestGVK)
	if err != nil {
		if apierrors.IsResourceExpired(err) {
			return
		}
		t.Fatalf("resuming: %v", err)
	}
	defer w.Stop()

	seen := map[string]bool{}
	for {
		select {
		case event := <-w.ResultChan():
			seen[event.Object.(*unstructured.Unstructured).GetName()] = true
			continue
		case <-time.After(500 * time.Millisecond):
		}
		break
	}
	for _, name := range names {
		if !seen[name] {
			t.Errorf("a client resuming from %s was admitted and never sent %s; the ring no "+
				"longer held all of it, and the answer is 410", from, name)
		}
	}
}
