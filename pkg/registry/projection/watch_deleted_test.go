package projection

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

var deletedTestGVK = schema.GroupVersionKind{
	Group: "store.example.com", Version: "v1alpha1", Kind: "Order",
}

// cachedItem builds an object at a version, as a poll would produce it.
func cachedItem(namespace, name, version string) unstructured.Unstructured {
	obj := unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(deletedTestGVK)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	obj.SetResourceVersion(version)
	return obj
}

// TestDeletionsWithoutAFullResync: an incremental poll reads forward, so a row
// that is gone simply stops coming back — indistinguishable from one that did
// not change. The full resync exists only to close that gap, and re-reading the
// whole collection on a timer is what makes a large table expensive to watch. A
// deletion query closes it without the scan.
func TestDeletionsWithoutAFullResync(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", "1"),
		cachedItem("acme", "order-2", "2"),
	}
	var removed []cacheIdentity

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		if since == "" {
			return rows, nil
		}
		return nil, nil
	}
	cache.deleted = func(_ context.Context, _ string) ([]cacheIdentity, error) { return removed, nil }
	// Off, so the deletion query is the only thing looking for removals.
	cache.fullResyncInterval = 0

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	for i := 0; i < len(rows); i++ {
		select {
		case <-w.ResultChan():
		case <-time.After(5 * time.Second):
			t.Fatal("the initial replay never arrived")
		}
	}

	// One row disappears. No full read runs, so only the deletion query can
	// report it.
	removed = []cacheIdentity{{namespace: "acme", name: "order-1"}}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Deleted {
			t.Fatalf("event type = %v, want Deleted", event.Type)
		}
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok {
			t.Fatalf("event carried %T", event.Object)
		}
		if obj.GetName() != "order-1" {
			t.Errorf("deleted %q, want order-1", obj.GetName())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no Deleted event; an incremental poll cannot see a removal on its own")
	}

	cache.mu.Lock()
	_, still := cache.items["acme/order-1"]
	cache.mu.Unlock()
	if still {
		t.Error("the deleted object is still in the cache")
	}
}

// TestFullResyncStillSeesDeletionsWithoutAQuery is the other half of the
// contract: a projection that has not been given a deletion query keeps the
// behaviour it had, where the periodic full read is what notices.
func TestFullResyncStillSeesDeletionsWithoutAQuery(t *testing.T) {
	rows := []unstructured.Unstructured{
		cachedItem("acme", "order-1", "1"),
		cachedItem("acme", "order-2", "2"),
	}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	for i := 0; i < len(rows); i++ {
		select {
		case <-w.ResultChan():
		case <-time.After(5 * time.Second):
			t.Fatal("the initial replay never arrived")
		}
	}

	rows = rows[1:]
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	select {
	case event := <-w.ResultChan():
		if event.Type != watch.Deleted {
			t.Fatalf("event type = %v, want Deleted", event.Type)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a full poll did not report the removal")
	}
}

// TestInitialEventsBookmarkNeedsBothConditions is a regression test for a crash.
//
// The endpoint defines a WatchList as sendInitialEvents *and*
// allowWatchBookmarks, and only wires up the hook that consumes the completion
// bookmark when both are set. Sending that bookmark to a watcher which asked
// for neither made the handler call a nil function, panic, and drop the
// connection — which is what an ordinary `kubectl get --watch` received.
func TestInitialEventsBookmarkNeedsBothConditions(t *testing.T) {
	for _, tc := range []struct {
		name              string
		sendInitialEvents bool
		allowBookmarks    bool
		wantBookmark      bool
	}{
		{"a plain watch", false, false, false},
		{"bookmarks alone", false, true, false},
		{"initial events alone", true, false, false},
		{"a real WatchList", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []unstructured.Unstructured{cachedItem("acme", "order-1", "1")}
			cache := newWatchCache(time.Hour, "orders", nil,
				func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })

			w, err := cache.Watch(context.Background(), "acme", nil, nil, "",
				tc.sendInitialEvents, tc.allowBookmarks, deletedTestGVK)
			if err != nil {
				t.Fatalf("Watch() returned error: %v", err)
			}
			defer w.Stop()

			var annotated bool
			deadline := time.After(2 * time.Second)
		collect:
			for {
				select {
				case event := <-w.ResultChan():
					if event.Type != watch.Bookmark {
						continue
					}
					obj, ok := event.Object.(*unstructured.Unstructured)
					if ok && obj.GetAnnotations()[metav1.InitialEventsAnnotationKey] == "true" {
						annotated = true
						break collect
					}
				case <-deadline:
					break collect
				}
			}

			if annotated != tc.wantBookmark {
				t.Errorf("initial-events bookmark sent = %v, want %v", annotated, tc.wantBookmark)
			}
		})
	}
}
