package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestPollDoesNotBlockReaders is a regression test.
//
// The poll used to hold the cache mutex across its database round trip, so
// every List — which stamps its resourceVersion from this cache — waited for
// the query to come back. On a full resync over a large table that is the whole
// poll's latency added to an unrelated read.
func TestPollDoesNotBlockReaders(t *testing.T) {
	querying := make(chan struct{})
	release := make(chan struct{})

	cache := newWatchCache(time.Hour, "orders", nil, func(context.Context) ([]unstructured.Unstructured, error) {
		close(querying)
		<-release
		return nil, nil
	})

	done := make(chan error, 1)
	go func() { done <- cache.poll(context.Background()) }()

	<-querying

	// The query is in flight. Everything that takes the cache lock has to still
	// be answerable, or a poll and a read are serialised against each other.
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		cache.ResourceVersion()
	}()

	select {
	case <-answered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("ResourceVersion() blocked behind an in-flight poll")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
}

// TestPollsDoNotOverlap checks the ordering the diff depends on: two snapshots
// compared against each other only mean anything if one poll finishes before
// the next starts.
func TestPollsDoNotOverlap(t *testing.T) {
	var inFlight, maxInFlight int
	entered := make(chan struct{}, 2)

	cache := newWatchCache(time.Hour, "orders", nil, func(context.Context) ([]unstructured.Unstructured, error) {
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		entered <- struct{}{}
		time.Sleep(20 * time.Millisecond)
		inFlight--
		return nil, nil
	})

	go func() { _ = cache.poll(context.Background()) }()
	go func() { _ = cache.poll(context.Background()) }()

	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("a poll never ran")
		}
	}

	if maxInFlight > 1 {
		t.Errorf("%d polls ran at once; consecutive snapshots cannot be diffed if they overlap", maxInFlight)
	}
}
