package projection

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"
)

// labelledItem is cachedItem with labels, which is what a selector reads.
func labelledItem(namespace, name, version string, set map[string]string) unstructured.Unstructured {
	obj := cachedItem(namespace, name, version)
	obj.SetLabels(set)
	return obj
}

// goldSelector is the selector every test here watches with.
func goldSelector() labels.Selector {
	return labels.SelectorFromSet(labels.Set{"tier": "gold"})
}

// TestAWatcherIsToldWhenARowCrossesItsSelector covers a change to a row that
// moves it into or out of a watcher's label selector.
//
// A modification used to be filtered on the new object alone. A row whose
// labels stopped matching therefore produced no event for the watcher that
// had been sent it, and that watcher kept the object in its store for as long
// as it stayed connected — nothing on either side could have said otherwise.
// The apiserver's own cacher compares both sides: leaving the selector is a
// deletion to that watcher, and arriving in it is an addition.
//
// The deletion carries the object as it was, at the version of the change
// that took it out — the cacher re-stamps it the same way, because a reflector
// takes the point it has synced to from every event object, and one left at
// the old version would resume from before its own departure.
func TestAWatcherIsToldWhenARowCrossesItsSelector(t *testing.T) {
	rows := []unstructured.Unstructured{
		labelledItem("acme", "order-1", "1", map[string]string{"tier": "gold"}),
	}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	w, err := cache.Watch(context.Background(), "acme", goldSelector(), nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	drain(t, w, 1)

	for _, step := range []struct {
		name string
		// The row after this step: its version, and its labels.
		version string
		tier    string
		// What the watcher is told, and the tier of the object it is handed —
		// a deletion carries the object as it was, at the version it left.
		want     watch.EventType
		wantTier string
	}{
		{"leaving the selector", "2", "silver", watch.Deleted, "gold"},
		{"arriving in it again", "3", "gold", watch.Added, "gold"},
		{"changing within it", "4", "gold", watch.Modified, "gold"},
	} {
		t.Run(step.name, func(t *testing.T) {
			rows = []unstructured.Unstructured{
				labelledItem("acme", "order-1", step.version, map[string]string{"tier": step.tier}),
			}
			if err := cache.poll(context.Background()); err != nil {
				t.Fatalf("poll() returned error: %v", err)
			}

			event := nextEvent(t, w)
			if event.Type != step.want {
				t.Fatalf("the watcher was told %s, want %s", event.Type, step.want)
			}
			obj, ok := event.Object.(*unstructured.Unstructured)
			if !ok {
				t.Fatalf("the event carried %T, want an object", event.Object)
			}
			if got := obj.GetLabels()["tier"]; got != step.wantTier {
				t.Errorf("the event carries tier=%q, want %q", got, step.wantTier)
			}
			if got := obj.GetResourceVersion(); got != step.version {
				t.Errorf("the event carries version %q, want %q: a client resuming from it "+
					"would otherwise start from before this change", got, step.version)
			}
		})
	}

	// A change to a row outside the selector, before and after, is nobody's
	// business here.
	rows = []unstructured.Unstructured{
		labelledItem("acme", "order-1", "5", map[string]string{"tier": "silver"}),
	}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
	drain(t, w, 1)
	rows = []unstructured.Unstructured{
		labelledItem("acme", "order-1", "6", map[string]string{"tier": "bronze"}),
	}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
	if seen := eventsFrom(t, w); len(seen) != 0 {
		t.Errorf("a row that matched neither before nor after produced %v", seen)
	}
}

// TestAResumedWatchIsToldWhenARowCrossedItsSelectorWhileItWasAway: the history
// ring replays what a client missed, filtered the way the live stream is — so
// it has to remember what each row changed from, or a resumed client is left
// holding the row that left its selector just as a connected one used to be.
func TestAResumedWatchIsToldWhenARowCrossedItsSelectorWhileItWasAway(t *testing.T) {
	rows := []unstructured.Unstructured{
		labelledItem("acme", "order-1", "1", map[string]string{"tier": "gold"}),
		labelledItem("acme", "order-2", "2", map[string]string{"tier": "silver"}),
	}
	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)

	w, err := cache.Watch(context.Background(), "acme", goldSelector(), nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	drain(t, w, 1)
	listed := cache.ResourceVersion()
	w.Stop()

	// While nobody is connected, the two rows trade places. Two polls, so the
	// ring records them in that order.
	rows[0] = labelledItem("acme", "order-1", "3", map[string]string{"tier": "silver"})
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}
	rows[1] = labelledItem("acme", "order-2", "4", map[string]string{"tier": "gold"})
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	resumed, err := cache.Watch(context.Background(), "acme", goldSelector(), nil, listed, false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("resuming from %q returned %v", listed, err)
	}
	defer resumed.Stop()

	seen := eventsFrom(t, resumed)
	if len(seen) != 2 || seen[0] != "DELETED(order-1)" || seen[1] != "ADDED(order-2)" {
		t.Errorf("the resumed watch received %v, want DELETED(order-1) then ADDED(order-2): "+
			"the row that left its selector while it was away has to be taken back", seen)
	}
}

// TestADatabaseReplayReportsARowThatNoLongerMatchesAsDeleted.
//
// A replay read out of the database has the row as it is now and nothing of
// what it was, so whether the client had it is not something the replay can
// answer. It is told a deletion anyway: a client that held the row needs
// exactly that, and one that never did discards a deletion of an object it
// does not know — which a watch is allowed to do, and losing the row is not.
func TestADatabaseReplayReportsARowThatNoLongerMatchesAsDeleted(t *testing.T) {
	rows := []unstructured.Unstructured{
		labelledItem("acme", "order-1", "2", map[string]string{"tier": "silver"}),
	}
	cache := replayCache(t, rows, nil)

	// Resuming from before the change, with a selector the row no longer
	// matches.
	w, err := cache.Watch(context.Background(), "acme", goldSelector(), nil, "1", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("resuming from the database returned %v", err)
	}
	defer w.Stop()

	seen := eventsFrom(t, w)
	if len(seen) != 1 || seen[0] != "DELETED(order-1)" {
		t.Errorf("the resumed watch received %v, want DELETED(order-1): a client that held "+
			"the row before it changed would otherwise keep it", seen)
	}
}

// TestALightweightCacheStillReportsARowLeavingALabelSelector: a lightweight
// cache holds a trimmed entry in place of the previous object, and the labels
// it keeps are what make this transition decidable at all. The entry is what
// the deletion carries, re-stamped with the version of the change that took
// the row out.
func TestALightweightCacheStillReportsARowLeavingALabelSelector(t *testing.T) {
	rows := []unstructured.Unstructured{
		labelledItem("acme", "order-1", "1", map[string]string{"tier": "gold"}),
	}
	var changes []unstructured.Unstructured

	cache := newWatchCache(time.Hour, "orders", nil,
		func(context.Context) ([]unstructured.Unstructured, error) { return rows, nil })
	t.Cleanup(cache.Close)
	cache.lightweight = true
	cache.incremental = func(_ context.Context, since string) ([]unstructured.Unstructured, error) {
		if since == "" {
			return rows, nil
		}
		return changes, nil
	}
	cache.deleted = func(context.Context, string) ([]cacheIdentity, error) { return nil, nil }

	w, err := cache.Watch(context.Background(), "acme", goldSelector(), nil, "", false, false, deletedTestGVK)
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer w.Stop()
	drain(t, w, 1)

	changes = []unstructured.Unstructured{
		labelledItem("acme", "order-1", "2", map[string]string{"tier": "silver"}),
	}
	if err := cache.poll(context.Background()); err != nil {
		t.Fatalf("poll() returned error: %v", err)
	}

	event := nextEvent(t, w)
	if event.Type != watch.Deleted {
		t.Fatalf("the watcher was told %s, want Deleted", event.Type)
	}
	obj, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		t.Fatalf("the event carried %T, want an object", event.Object)
	}
	if obj.GetLabels()["tier"] != "gold" || obj.GetResourceVersion() != "2" {
		t.Errorf("the deletion carries tier=%q at version %q, want the entry as it was at the "+
			"version that took it out: gold at 2", obj.GetLabels()["tier"], obj.GetResourceVersion())
	}
	if obj.GetAPIVersion() == "" || obj.GetKind() == "" {
		t.Errorf("the deletion carries apiVersion=%q kind=%q and cannot be encoded",
			obj.GetAPIVersion(), obj.GetKind())
	}
}
