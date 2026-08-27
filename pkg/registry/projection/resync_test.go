package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
)

// A full resync of a lightweight cache must broadcast the rows it read, not the
// trimmed entries it keeps. An informer stores what the event carries.
func TestFullResyncOfALightweightCacheBroadcastsWholeRows(t *testing.T) {
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5")}
	cache := lightweightCache(t, rows, nil)

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

	// The row changes, and the next poll is the periodic full resync rather
	// than an incremental read.
	changed := richRow("acme", "order-1", "6")
	rows[0] = changed
	cache.mu.Lock()
	cache.fullResyncInterval = time.Nanosecond
	cache.lastFullResync = time.Time{}
	cache.mu.Unlock()

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("resyncing: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			if event.Type != watch.Modified {
				continue
			}
			obj := event.Object.(*unstructured.Unstructured)
			if _, found, _ := unstructured.NestedString(obj.Object, "spec", "customer"); !found {
				t.Fatal("the resync broadcast the trimmed cache entry; a watcher that stores it " +
					"holds an object with no spec and no status")
			}
			return
		case <-deadline:
			t.Fatal("no modification arrived")
		}
	}
}

// A row a full resync finds missing is a deletion like any other, and it still
// has to describe what went away. The cache cannot: its entry is trimmed. The
// tombstone can, so a lightweight resync asks for tombstones too.
func TestFullResyncOfALightweightCacheDescribesWhatItLost(t *testing.T) {
	gone := richRow("acme", "order-2", "6")
	rows := []unstructured.Unstructured{richRow("acme", "order-1", "5"), gone}
	cache := lightweightCache(t, rows, nil)

	w, err := cache.Watch(context.Background(), "acme", nil, nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()

	for range rows {
		select {
		case <-w.ResultChan():
		case <-time.After(5 * time.Second):
			t.Fatal("the initial state never arrived")
		}
	}

	// Armed only now: the poll that registering a watcher triggers would
	// otherwise consume the tombstone on the incremental path, which is not the
	// path under test.
	remaining := rows[:1]
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		if since == "" {
			return remaining, nil
		}
		return nil, nil
	}
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) {
		return []cacheIdentity{{namespace: "acme", name: "order-2", object: &gone}}, nil
	}
	cache.mu.Lock()
	cache.fullResyncInterval = time.Nanosecond
	cache.lastFullResync = time.Time{}
	cache.mu.Unlock()

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("resyncing: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case event := <-w.ResultChan():
			if event.Type != watch.Deleted {
				continue
			}
			obj := event.Object.(*unstructured.Unstructured)
			if _, found, _ := unstructured.NestedString(obj.Object, "spec", "customer"); !found {
				t.Fatalf("the resync deleted %s and described nothing, which is what the "+
					"tombstone columns exist to prevent", obj.GetName())
			}
			return
		case <-deadline:
			t.Fatal("no deletion arrived")
		}
	}
}
