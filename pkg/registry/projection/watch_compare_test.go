package projection

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestEqualValuesMatchesDeepEqual is the whole contract: equalValues exists to
// be faster than reflect.DeepEqual, not to answer differently. Every case here
// is checked against it rather than against a hand-written expectation.
func TestEqualValuesMatchesDeepEqual(t *testing.T) {
	object := func() map[string]any {
		return map[string]any{
			"apiVersion": "store.example.com/v1alpha1",
			"kind":       "Order",
			"metadata": map[string]any{
				"name":      "order-1001",
				"namespace": "acme",
				"labels":    map[string]any{"store.example.com/status": "shipped"},
			},
			"spec": map[string]any{
				"customer":   "ada",
				"totalCents": int64(4999),
				"rate":       1.5,
				"paid":       true,
				"note":       nil,
				"lineItems": []any{
					map[string]any{"sku": "widget", "qty": int64(2)},
					map[string]any{"sku": "gizmo", "qty": int64(1)},
				},
			},
		}
	}

	for _, tc := range []struct {
		name   string
		mutate func(m map[string]any)
	}{
		{name: "identical", mutate: func(map[string]any) {}},
		{name: "scalar changed", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["customer"] = "grace"
		}},
		{name: "integer changed", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["totalCents"] = int64(5000)
		}},
		{name: "float changed", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["rate"] = 1.6
		}},
		{name: "bool changed", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["paid"] = false
		}},
		{name: "nil became a value", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["note"] = "late"
		}},
		{name: "value became nil", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["customer"] = nil
		}},
		{name: "key added", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["currency"] = "EUR"
		}},
		{name: "key removed", mutate: func(m map[string]any) {
			delete(m["spec"].(map[string]any), "customer")
		}},
		{name: "nested key changed", mutate: func(m map[string]any) {
			m["metadata"].(map[string]any)["labels"].(map[string]any)["store.example.com/status"] = "pending"
		}},
		{name: "list element changed", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["lineItems"].([]any)[0].(map[string]any)["qty"] = int64(3)
		}},
		{name: "list shortened", mutate: func(m map[string]any) {
			items := m["spec"].(map[string]any)["lineItems"].([]any)
			m["spec"].(map[string]any)["lineItems"] = items[:1]
		}},
		{name: "list reordered", mutate: func(m map[string]any) {
			items := m["spec"].(map[string]any)["lineItems"].([]any)
			items[0], items[1] = items[1], items[0]
		}},
		{name: "type changed", mutate: func(m map[string]any) {
			m["spec"].(map[string]any)["totalCents"] = "4999"
		}},
		{name: "map became a list", mutate: func(m map[string]any) {
			m["spec"] = []any{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous, next := object(), object()
			tc.mutate(next)

			want := reflect.DeepEqual(previous, next)
			if got := equalValues(previous, next); got != want {
				t.Errorf("equalValues = %v, reflect.DeepEqual = %v", got, want)
			}
			// Symmetric, since a resync compares in whichever order the map
			// iteration produced.
			if got := equalValues(next, previous); got != want {
				t.Errorf("equalValues reversed = %v, reflect.DeepEqual = %v", got, want)
			}
		})
	}
}

// TestEqualValuesFallsBackForUnexpectedTypes: an unstructured object should only
// hold what the JSON converter produces, but a value that arrived some other way
// has to be compared rather than declared different.
func TestEqualValuesFallsBackForUnexpectedTypes(t *testing.T) {
	type custom struct{ N int }

	if !equalValues(custom{N: 1}, custom{N: 1}) {
		t.Error("two equal values of an unexpected type compared unequal")
	}
	if equalValues(custom{N: 1}, custom{N: 2}) {
		t.Error("two different values of an unexpected type compared equal")
	}
	// int is not a type unstructured produces; it must still not equal int64.
	if equalValues(1, int64(1)) {
		t.Error("an int compared equal to an int64")
	}
}

// TestChangedUsesTheResourceVersionWhenThereIsOne keeps the cheap path cheap:
// comparing one column beats comparing two objects, and is why a projection that
// maps a version can afford a large collection.
func TestChangedUsesTheResourceVersionWhenThereIsOne(t *testing.T) {
	withVersion := func(rv, customer string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "order-1001", "resourceVersion": rv},
			"spec":     map[string]any{"customer": customer},
		}}
	}

	// Same version, different body: the version is authoritative.
	if changed(withVersion("7", "ada"), withVersion("7", "grace")) {
		t.Error("a matching resourceVersion was overridden by comparing the objects")
	}
	if !changed(withVersion("7", "ada"), withVersion("8", "ada")) {
		t.Error("a moved resourceVersion was not treated as a change")
	}

	// One side unversioned still compares versions, so a projection that starts
	// reporting one is not read as "nothing changed".
	if !changed(withVersion("", "ada"), withVersion("8", "ada")) {
		t.Error("gaining a resourceVersion was not treated as a change")
	}
}

func benchmarkObject(i int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata": map[string]any{
			"name":      fmt.Sprintf("order-%05d", i),
			"namespace": "acme",
			"labels":    map[string]any{"store.example.com/status": "shipped"},
		},
		"spec": map[string]any{
			"customer":   "ada",
			"totalCents": int64(4999),
			"lineItems": []any{
				map[string]any{"sku": "widget", "qty": int64(2)},
				map[string]any{"sku": "gizmo", "qty": int64(1)},
			},
		},
		"status": map[string]any{"phase": "shipped"},
	}}
}

// BenchmarkChangedWithoutAVersion measures the comparison a full resync makes
// for every object of a projection that maps no resourceVersion.
func BenchmarkChangedWithoutAVersion(b *testing.B) {
	previous, next := benchmarkObject(1), benchmarkObject(1)

	b.ReportAllocs()
	for b.Loop() {
		if changed(previous, next) {
			b.Fatal("identical objects compared as changed")
		}
	}
}

// BenchmarkDeepEqualWithoutAVersion is the same comparison through reflection,
// for the comparison this replaced.
func BenchmarkDeepEqualWithoutAVersion(b *testing.B) {
	previous, next := benchmarkObject(1), benchmarkObject(1)

	b.ReportAllocs()
	for b.Loop() {
		if !reflect.DeepEqual(previous.Object, next.Object) {
			b.Fatal("identical objects compared as changed")
		}
	}
}

// BenchmarkSortedItems measures the replay a new watcher pays for.
func BenchmarkSortedItems(b *testing.B) {
	cache := newWatchCache(time.Second, "orders", nil, nil)
	for i := range 10000 {
		item := benchmarkObject(i)
		cache.items[cacheKey(item)] = item
	}

	b.ReportAllocs()
	for b.Loop() {
		if got := len(cache.sortedItemsLocked()); got != 10000 {
			b.Fatalf("sorted %d items", got)
		}
	}
}

// seededCache returns a cache holding n objects, populated the way a real one
// is: by polling. Seeding c.items directly is not enough, because the first
// Watch polls and a poll that returns nothing empties the cache before the
// snapshot is taken.
func seededCache(t *testing.T, n int) *watchCache {
	t.Helper()

	items := make([]unstructured.Unstructured, 0, n)
	for i := range n {
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "store.example.com/v1alpha1",
			"kind":       "Order",
			"metadata": map[string]any{
				"name":      fmt.Sprintf("order-%06d", i),
				"namespace": "acme",
			},
		}}
		items = append(items, *obj)
	}

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return items, nil })
	t.Cleanup(cache.Close)

	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("populating the cache: %v", err)
	}
	if got := len(cache.items); got != n {
		t.Fatalf("the cache holds %d objects, want %d", got, n)
	}
	return cache
}

// TestWatchDoesNotHoldTheCacheLockWhileCopying is the contention this replay
// path used to create.
//
// Every List stamps its resourceVersion from the cache, taking the same lock,
// so copying a whole collection under it made a watcher connecting to a large
// projection block every read on that projection for the duration.
func TestWatchDoesNotHoldTheCacheLockWhileCopying(t *testing.T) {
	cache := seededCache(t, 5000)

	// A reader doing what List does: taking the lock to read the version.
	var reads atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = cache.ResourceVersion()
			reads.Add(1)
		}
	}()

	// Let the reader establish a rate before the watcher arrives.
	time.Sleep(50 * time.Millisecond)
	before := reads.Load()

	watcher, err := cache.Watch(context.Background(), "", nil, nil, "", true, false, schema.GroupVersionKind{})
	if err != nil {
		close(stop)
		t.Fatalf("Watch() returned error: %v", err)
	}
	t.Cleanup(watcher.Stop)

	during := reads.Load() - before
	close(stop)

	if during == 0 {
		t.Error("no read completed while a watcher was replaying 5000 objects: " +
			"the copy is happening under the lock every List takes")
	}
	t.Logf("%d reads completed while the replay was being built", during)
}

// TestWatchReplayPrecedesLiveEvents: registering the watcher before building
// the replay is what stops a poll landing in between from being delivered past
// it. The order still has to be the collection first, then what happened to it.
func TestWatchReplayPrecedesLiveEvents(t *testing.T) {
	cache := seededCache(t, 1)

	var existing *unstructured.Unstructured
	for _, item := range cache.items {
		existing = item
	}

	watcher, err := cache.Watch(context.Background(), "", nil, nil, "", true, false, schema.GroupVersionKind{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	t.Cleanup(watcher.Stop)

	// A change after the watcher connected.
	added := existing.DeepCopy()
	added.SetName("order-new")
	cache.mu.Lock()
	events, targets := cache.applyLocked(
		[]unstructured.Unstructured{*existing, *added}, true, "", nil)
	cache.mu.Unlock()
	cache.broadcast(events, targets)

	seen := make([]string, 0, 2)
	deadline := time.After(10 * time.Second)
	for len(seen) < 2 {
		select {
		case event := <-watcher.ResultChan():
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			seen = append(seen, string(event.Type)+"/"+obj.GetName())
		case <-deadline:
			t.Fatalf("only saw %v", seen)
		}
	}

	if seen[0] != "ADDED/"+existing.GetName() {
		t.Errorf("the first event was %s, want the replayed %s first", seen[0], existing.GetName())
	}
	if seen[1] != "ADDED/order-new" {
		t.Errorf("the second event was %s, want the live one after the replay", seen[1])
	}
}

// TestWatchBacklogIsBoundedAcrossWatchers: the per-watcher bound says nothing
// about how many watchers there are, and the replay is the whole collection —
// so fifty informers reconnecting at once used to queue fifty copies of it with
// nothing counting.
func TestWatchBacklogIsBoundedAcrossWatchers(t *testing.T) {
	// Big enough that a handful of replays exceeds the cache-wide bound while
	// no single one exceeds the per-watcher bound.
	const objects = maxPendingEvents - 1
	cache := seededCache(t, objects)

	var admitted, refused int
	for range 10 {
		w, err := cache.Watch(context.Background(), "", nil, nil, "", true, false, schema.GroupVersionKind{})
		if err != nil {
			refused++
			continue
		}
		admitted++
		t.Cleanup(w.Stop)
	}

	t.Logf("%d watchers admitted, %d refused; %d events queued across the projection",
		admitted, refused, cache.pending.Load())

	if refused == 0 {
		t.Errorf("all %d watchers were admitted, each holding a copy of %d objects: "+
			"nothing is bounding the total", admitted, objects)
	}
	if admitted == 0 {
		t.Error("no watcher was admitted at all")
	}
	if got := cache.pending.Load(); got > maxCachePendingEvents {
		t.Errorf("%d events are queued, past the bound of %d", got, maxCachePendingEvents)
	}
}

// TestWatchBacklogIsReleasedWhenAWatcherLeaves, or a client that disconnects
// mid-replay reserves its share of the projection's backlog forever and the
// bound closes in on itself.
func TestWatchBacklogIsReleasedWhenAWatcherLeaves(t *testing.T) {
	cache := seededCache(t, 100)

	watcher, err := cache.Watch(context.Background(), "", nil, nil, "", true, false, schema.GroupVersionKind{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	if cache.pending.Load() == 0 {
		t.Fatal("a replay of 100 objects counted nothing against the backlog")
	}

	watcher.Stop()

	deadline := time.After(10 * time.Second)
	for cache.pending.Load() != 0 {
		select {
		case <-deadline:
			t.Fatalf("%d events are still counted after the watcher left", cache.pending.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
}
