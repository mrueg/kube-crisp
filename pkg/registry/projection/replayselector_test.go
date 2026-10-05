package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
)

// deletionsOf collects the names a watch reports deleted until it goes quiet.
func deletionsOf(w watch.Interface) map[string]bool {
	gone := map[string]bool{}
	for {
		select {
		case event := <-w.ResultChan():
			if event.Type == watch.Deleted {
				gone[event.Object.(*unstructured.Unstructured).GetName()] = true
			}
		case <-time.After(300 * time.Millisecond):
			return gone
		}
	}
}

// TestAReplayedBareDeletionReachesASelectorWatcher covers a database replay
// whose tombstone records only the identity columns.
//
// The deletion carries a name and a namespace and nothing else, so a label
// selector has nothing to match and rejected it. The watcher kept the row — one
// it may well have held, since it matched when the watcher last saw it — for as
// long as it stayed connected. What the object cannot answer has to count as a
// possible match, the rule matchedBefore applies to a modification whose
// previous state is unknown.
func TestAReplayedBareDeletionReachesASelectorWatcher(t *testing.T) {
	rows := []unstructured.Unstructured{
		labelledItem("acme", "order-1", "5", map[string]string{"tier": "gold"}),
	}
	removed := []cacheIdentity{
		{namespace: "acme", name: "order-gone"},
		{namespace: "elsewhere", name: "order-foreign"},
	}
	cache := replayCache(t, rows, removed)

	w, err := cache.Watch(context.Background(), "acme", goldSelector(), nil, "1", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	gone := deletionsOf(w)
	if !gone["order-gone"] {
		t.Error("a watcher with a label selector was never told about a deletion whose tombstone " +
			"carries no labels, so it keeps the row")
	}
	if gone["order-foreign"] {
		t.Error("a watcher scoped to acme was told about a deletion in another namespace")
	}
}

// TestATrimmedDeletionReachesAFieldSelectorWatcher covers a lightweight cache
// noticing a row is gone with no tombstone to describe it, so the deletion
// carries the trimmed entry: identity, version and labels, and no mapped field.
// A field selector over a mapped column cannot be answered from that, and must
// not be taken as a no.
func TestATrimmedDeletionReachesAFieldSelectorWatcher(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.lightweight = true
	cache.matchFields = func(obj *unstructured.Unstructured, selector fields.Selector) bool {
		if selector == nil || selector.Empty() {
			return true
		}
		phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase")
		return selector.Matches(fields.Set{"status.phase": phase})
	}

	w, err := cache.Watch(context.Background(), "acme", nil,
		fields.OneTermEqualSelector("status.phase", "pending"), "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	drain(t, w, 1)

	rows = nil
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("polling: %v", err)
	}

	if !deletionsOf(w)["order-1"] {
		t.Error("a watcher with a field selector was never told the row it holds is gone; the " +
			"trimmed entry has no field to match, which is not the same as not matching")
	}
}
