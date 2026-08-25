package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// twoVersions builds the same rows served under two versions of one
// projection, the way the compiler does, sharing a pool and the state that
// makes them one projection rather than two.
func twoVersions(t *testing.T) (*WritableREST, *WritableREST, *Shared) {
	t.Helper()

	pool, err := crispsql.Open(crispsql.PoolOptions{
		Driver:             "sqlite",
		DSN:                newTestDB(t),
		PreparedStatements: true,
	})
	if err != nil {
		t.Fatalf("opening pool: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	shared := NewShared("orders", "orders.store.example.com", nil)

	build := func(version, path string) *WritableREST {
		spec := watchableSpec()
		spec.Resource.Version = version
		// The second version maps the same column somewhere else entirely,
		// which is the whole reason the versions cannot share a cache of
		// objects — only the rows underneath them.
		for i := range spec.Mapping.Fields {
			if spec.Mapping.Fields[i].Column == "total_cents" {
				spec.Mapping.Fields[i].Path = path
			}
		}

		storages, err := New("orders", spec, pool, nil, shared)
		if err != nil {
			t.Fatalf("New(%s) returned error: %v", version, err)
		}
		return storages.writable
	}

	return build("v1alpha1", "spec.totalCents"), build("v1beta1", "spec.amount.cents"), shared
}

// TestVersionsShareOnePoll is the point of the poll group: a kind served at two
// versions polls its table once, not once per version.
func TestVersionsShareOnePoll(t *testing.T) {
	alpha, beta, shared := twoVersions(t)
	ctx := namespacedContext("acme")

	before := coalesced(t, shared.flights)

	alphaWatch, err := alpha.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("watching v1alpha1: %v", err)
	}
	defer alphaWatch.Stop()

	betaWatch, err := beta.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("watching v1beta1: %v", err)
	}
	defer betaWatch.Stop()

	if got := shared.polls.size(); got != 2 {
		t.Fatalf("%d caches are being polled, want both versions on one timer", got)
	}

	// Both versions are due on the same tick, so the second one's query joins
	// the first one's instead of reading the table again.
	waitFor(t, func() bool { return coalesced(t, shared.flights) > before })
}

// TestVersionsMapSharedRowsIndependently guards the risk the sharing
// introduces: one read, two different objects, and neither may see the other's.
func TestVersionsMapSharedRowsIndependently(t *testing.T) {
	alpha, beta, _ := twoVersions(t)
	ctx := namespacedContext("acme")

	first, err := alpha.Get(ctx, "order-1001", nil)
	if err != nil {
		t.Fatalf("reading v1alpha1: %v", err)
	}
	second, err := beta.Get(ctx, "order-1001", nil)
	if err != nil {
		t.Fatalf("reading v1beta1: %v", err)
	}

	if got := nestedInt(t, first, "spec", "totalCents"); got != 4999 {
		t.Errorf("v1alpha1 spec.totalCents = %d, want 4999", got)
	}
	if got := nestedInt(t, second, "spec", "amount", "cents"); got != 4999 {
		t.Errorf("v1beta1 spec.amount.cents = %d, want 4999", got)
	}
	if _, found, _ := nested(t, second, "spec", "totalCents"); found {
		t.Error("v1beta1 carries v1alpha1's field; the versions are sharing an object, not rows")
	}
}

func TestPollGroupStopsWhenTheLastWatcherLeaves(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	w, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	waitFor(t, func() bool { return store.watch.group.size() == 1 })

	w.Stop()
	waitFor(t, func() bool { return store.watch.group.size() == 0 })
}

// TestPollGroupTicksAtTheFastestInterval covers the mixed case: a cache that
// asked for a slower poll skips the ticks that come too soon for it.
func TestPollGroupTicksAtTheFastestInterval(t *testing.T) {
	group := newPollGroup(nil, "orders.store.example.com")

	fast := &watchCache{interval: 20 * time.Millisecond, group: group}
	slow := &watchCache{interval: time.Hour, group: group}
	group.add(fast)
	group.add(slow)

	group.mu.Lock()
	tick := group.tick
	group.mu.Unlock()
	if tick != 20*time.Millisecond {
		t.Errorf("the group ticks every %s, want the fastest member's %s", tick, 20*time.Millisecond)
	}

	if !fast.due(tick) || !slow.due(tick) {
		t.Fatal("a cache that has never polled is not due")
	}
	fast.lastPoll = time.Now()
	slow.lastPoll = time.Now()
	if fast.due(tick) {
		t.Error("a cache polled just now is due again immediately")
	}
	if slow.due(tick) {
		t.Error("an hourly cache is due on a 20ms tick")
	}

	group.remove(fast)
	group.remove(slow)
	if got := group.size(); got != 0 {
		t.Errorf("%d members left after removing both", got)
	}
}

// TestSharedIsOptional keeps the storage usable on its own, which is what the
// tests and any single-version caller rely on.
func TestSharedIsOptional(t *testing.T) {
	spec := watchableSpec()
	storages, err := New("orders", spec, newTestPoolFor(t, spec), nil, nil)
	if err != nil {
		t.Fatalf("New() with no shared state returned error: %v", err)
	}

	store := storages.writable
	if store.flights == nil || store.watch.group == nil {
		t.Fatal("storage built without shared state has no flight group or poll group")
	}
	if _, err := store.List(namespacedContext("acme"), nil); err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
}

// nested reads a path out of a projected object.
func nested(t *testing.T, obj runtime.Object, path ...string) (any, bool, error) {
	t.Helper()
	return unstructured.NestedFieldNoCopy(obj.(*unstructured.Unstructured).Object, path...)
}

func nestedInt(t *testing.T, obj runtime.Object, path ...string) int64 {
	t.Helper()

	value, found, err := unstructured.NestedInt64(obj.(*unstructured.Unstructured).Object, path...)
	if err != nil || !found {
		t.Fatalf("reading %v: found=%v err=%v", path, found, err)
	}
	return value
}

// TestANotificationWakesEveryMember is the point of the whole feature: a change
// makes the poll happen now rather than at the next tick.
//
// Every member, not only the ones the timer would call due — a notification says
// the data moved, and a cache that skipped this round would sit on a snapshot it
// has been told is stale.
func TestANotificationWakesEveryMember(t *testing.T) {
	notifications := make(chan struct{}, 1)
	group := newPollGroup(
		func(context.Context) (<-chan struct{}, error) { return notifications, nil },
		"orders.store.example.com",
	)

	polls := make(chan string, 16)
	member := func(name string, interval time.Duration) *watchCache {
		cache := newWatchCache(interval, name, group, func(context.Context) ([]unstructured.Unstructured, error) {
			select {
			case polls <- name:
			default:
			}
			return nil, nil
		})
		// A watcher, so the cache does not remove itself on the first refresh.
		cache.watchers[1] = newCacheWatcher(1, cache)
		t.Cleanup(cache.Close)
		return cache
	}

	// An hour apart, so nothing here is due and only a notification can produce
	// a poll.
	group.add(member("quick", time.Hour))
	group.add(member("slow", time.Hour))

	notifications <- struct{}{}

	seen := map[string]bool{}
	deadline := time.After(10 * time.Second)
	for len(seen) < 2 {
		select {
		case name := <-polls:
			seen[name] = true
		case <-deadline:
			t.Fatalf("only %v polled after a notification, want both", seen)
		}
	}
}

// TestASubscriptionThatFailsLeavesTheTimerPolling: a notification only ever made
// a poll happen sooner, so losing one costs latency rather than events.
func TestASubscriptionThatFailsLeavesTheTimerPolling(t *testing.T) {
	group := newPollGroup(
		func(context.Context) (<-chan struct{}, error) {
			return nil, errors.New("no connection to listen on")
		},
		"orders.store.example.com",
	)

	polled := make(chan struct{}, 1)
	cache := newWatchCache(50*time.Millisecond, "orders", group, func(context.Context) ([]unstructured.Unstructured, error) {
		select {
		case polled <- struct{}{}:
		default:
		}
		return nil, nil
	})
	cache.watchers[1] = newCacheWatcher(1, cache)
	t.Cleanup(cache.Close)

	group.add(cache)

	select {
	case <-polled:
	case <-time.After(10 * time.Second):
		t.Error("the timer stopped polling because the subscription could not be established")
	}
}

// TestNotificationsStopWithTheLastWatcher: a subscription holds a connection, so
// it has to have the same lifetime as the polling it drives — nothing for a
// projection nobody is watching.
func TestNotificationsStopWithTheLastWatcher(t *testing.T) {
	subscribed := make(chan context.Context, 4)
	group := newPollGroup(
		func(ctx context.Context) (<-chan struct{}, error) {
			subscribed <- ctx
			return make(chan struct{}), nil
		},
		"orders.store.example.com",
	)

	cache := newWatchCache(time.Hour, "orders", group, func(context.Context) ([]unstructured.Unstructured, error) {
		return nil, nil
	})
	t.Cleanup(cache.Close)

	group.add(cache)

	var ctx context.Context
	select {
	case ctx = <-subscribed:
	case <-time.After(10 * time.Second):
		t.Fatal("no subscription was made when the first watcher arrived")
	}
	if ctx.Err() != nil {
		t.Fatal("the subscription was cancelled before it started")
	}

	group.remove(cache)

	deadline := time.After(10 * time.Second)
	for ctx.Err() == nil {
		select {
		case <-deadline:
			t.Fatal("the subscription outlived the last watcher, holding a connection for nobody")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
