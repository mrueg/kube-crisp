package projection

import (
	"sync"
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// writeWhileReading lands an update to order-1001 after the next read has its
// rows and before it stores them — the window a write used to be able to fall
// into unnoticed.
func writeWhileReading(t *testing.T, store *WritableREST, customer string) {
	t.Helper()

	var once sync.Once
	store.cache.beforeStore = func() {
		once.Do(func() {
			if _, _, err := store.Update(namespacedContext("acme"), "order-1001",
				restUpdate(newOrder("order-1001", customer, 1)), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
				t.Errorf("the update landed inside the read returned error: %v", err)
			}
		})
	}
}

func cachedSpecStore(t *testing.T) *WritableREST {
	t.Helper()
	spec := writableSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}
	return newStorage(t, spec).(*WritableREST)
}

// TestAListThatRacesAWriteDoesNotCacheThePreWriteRows is a regression test.
//
// A write drops the cache entries it makes stale, but a list whose query ran
// before the write and whose result arrived after it stored the pre-write rows
// straight back — and every list for the rest of the TTL was answered from
// them, including the one from the client that had just been told its write
// succeeded. Read-after-write held for a replica only when nothing else was
// reading.
func TestAListThatRacesAWriteDoesNotCacheThePreWriteRows(t *testing.T) {
	store := cachedSpecStore(t)
	ctx := namespacedContext("acme")
	writeWhileReading(t, store, "grace")

	if _, err := store.List(ctx, &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	after, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() after the write returned error: %v", err)
	}
	for _, item := range after.(*unstructured.UnstructuredList).Items {
		if item.GetName() != "order-1001" {
			continue
		}
		if got := customerIn(t, &item); got != "grace" {
			t.Errorf("a list after the write answered spec.customer = %q, want %q: "+
				"the racing read put the pre-write rows back in the cache", got, "grace")
		}
		return
	}
	t.Fatal("order-1001 is missing from the list")
}

// TestAGetThatRacesAWriteDoesNotCacheThePreWriteObject is the same for a
// single object.
func TestAGetThatRacesAWriteDoesNotCacheThePreWriteObject(t *testing.T) {
	store := cachedSpecStore(t)
	ctx := namespacedContext("acme")
	writeWhileReading(t, store, "grace")

	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	after, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() after the write returned error: %v", err)
	}
	if got := customerIn(t, after); got != "grace" {
		t.Errorf("a get after the write answered spec.customer = %q, want %q: "+
			"the racing read put the pre-write object back in the cache", got, "grace")
	}
}

// TestACacheStoreIsRefusedOnlyByAnInvalidationItCouldDescribe keeps the guard
// as narrow as the invalidation it answers to: a write to one tenant does not
// stop another tenant's reads from being cached, while a cluster-wide read,
// which spans them all, is held to every write.
func TestACacheStoreIsRefusedOnlyByAnInvalidationItCouldDescribe(t *testing.T) {
	for name, tc := range map[string]struct {
		invalidated string
		namespace   string
		stored      bool
	}{
		"a write to the same namespace":        {"acme", "acme", false},
		"a write to another namespace":         {"globex", "acme", true},
		"a cluster-wide read, any write":       {"globex", "", false},
		"an unscoped write, a namespaced read": {"", "acme", false},
	} {
		t.Run(name, func(t *testing.T) {
			c := newReadCache(time.Minute, "orders")
			since := c.epoch()
			c.invalidate(tc.invalidated)

			c.putObject("k", tc.namespace, &unstructured.Unstructured{Object: map[string]any{}}, since)
			if _, ok := c.lookup("k"); ok != tc.stored {
				t.Errorf("stored = %v after invalidating %q, want %v", ok, tc.invalidated, tc.stored)
			}

			// A read that started after the invalidation is current, and stores.
			c.putObject("later", tc.namespace, &unstructured.Unstructured{Object: map[string]any{}}, c.epoch())
			if _, ok := c.lookup("later"); !ok {
				t.Error("a read taken after the invalidation was not stored")
			}
		})
	}
}
