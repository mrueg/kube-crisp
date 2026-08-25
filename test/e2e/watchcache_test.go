//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
)

var (
	replayedOrdersGVR     = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "replayedorders"}
	unreplayableOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "unreplayableorders"}
	strictOrdersGVR       = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "strictorders"}
	lenientOrdersGVR      = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "lenientorders"}
)

// TestDeletionCarriesTheRowFromTheTombstone covers what makes the lightweight
// watch cache safe.
//
// The cache keeps only the identity, version, kind and labels, so it has
// nothing left to describe a row that is gone. The tombstone does — and a
// Deleted event that named a row and described nothing would break every client
// that reads the deleted object, which is most of them.
func TestDeletionCarriesTheRowFromTheTombstone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client := dynamicClient.Resource(tombstonedOrdersGVR).Namespace(acmeNamespace)

	const name = "doomed-2"
	execSQL(t, fmt.Sprintf(
		"INSERT INTO tombstoned_orders (id, tenant, customer, updated_at) "+
			"VALUES ('%s', 'acme', 'grace', clock_timestamp()::text) "+
			"ON CONFLICT (id) DO UPDATE SET customer = 'grace'", name))
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf("DELETE FROM tombstoned_orders WHERE id = '%s'", name))
		execSQL(t, fmt.Sprintf("DELETE FROM order_tombstones WHERE id = '%s'", name))
	})

	watcher, err := client.Watch(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer watcher.Stop()
	drainFor(watcher, 5*time.Second)

	if err := client.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	deleted := awaitDeletion(t, watcher, name, 60*time.Second)
	customer, found, _ := unstructured.NestedString(deleted.Object, "spec", "customer")
	if !found {
		t.Fatal("the deletion named the row and described nothing; the cache holds no object " +
			"for it, so this has to come from the tombstone's own columns")
	}
	if customer != "grace" {
		t.Errorf("spec.customer = %q, want grace — the row as it was when it was removed", customer)
	}
}

// TestWatchResumesFromTheDatabaseInACluster covers a client reconnecting to a
// server that has forgotten what it missed.
//
// The history ring dies with the process and differs on every replica, so a
// rolling restart used to make every informer relist. Where the projection maps
// a version and keeps tombstones, the database still knows.
func TestWatchResumesFromTheDatabaseInACluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	client := dynamicClient.Resource(replayedOrdersGVR).Namespace(acmeNamespace)

	names := []string{"replay-1", "replay-2", "replay-3"}
	t.Cleanup(func() {
		for _, name := range names {
			execSQL(t, fmt.Sprintf("DELETE FROM tombstoned_orders WHERE id = '%s'", name))
		}
	})

	// The version to resume from, taken before any of the changes below.
	list, err := client.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	from := list.GetResourceVersion()
	if from == "" {
		t.Fatal("the list carries no resourceVersion, so there is nothing to resume from")
	}

	// More changes than a ring of one can hold, written in SQL so the server
	// learns of them only by polling.
	for _, name := range names {
		execSQL(t, fmt.Sprintf(
			"INSERT INTO tombstoned_orders (id, tenant, customer, updated_at) "+
				"VALUES ('%s', 'acme', 'ada', clock_timestamp()::text) "+
				"ON CONFLICT (id) DO UPDATE SET updated_at = clock_timestamp()::text", name))
	}
	// Long enough for the 1s poll to have taken them in and pushed them past
	// the one entry the ring keeps.
	time.Sleep(6 * time.Second)

	watcher, err := client.Watch(ctx, metav1.ListOptions{ResourceVersion: from})
	if err != nil {
		t.Fatalf("resuming from a version the ring cannot cover returned %v; the database "+
			"could have answered it", err)
	}
	defer watcher.Stop()

	// Proof it came from the database rather than from a ring that happened to
	// cover the version. Without this the test passes either way, which is the
	// failure mode it exists to rule out.
	if log := apiserverLog(t); !strings.Contains(log, "replayed watch history from the database") ||
		!strings.Contains(log, replayedOrdersGVR.Resource) {
		t.Error("the server logged no database replay for this resource, so the history ring " +
			"answered and this test is measuring nothing")
	}

	seen := map[string]bool{}
	deadline := time.After(30 * time.Second)
	for len(seen) < len(names) {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				t.Fatalf("the watch closed after replaying %d of %d changes", len(seen), len(names))
			}
			obj, isObject := event.Object.(*unstructured.Unstructured)
			if !isObject {
				continue
			}
			if strings.HasPrefix(obj.GetName(), "replay-") {
				seen[obj.GetName()] = true
			}
		case <-deadline:
			t.Fatalf("replayed %d of %d changes: %v", len(seen), len(names), seen)
		}
	}
}

// TestWatchWithoutTombstonesIsStillRefused. A projection that cannot say what
// was deleted cannot be replayed correctly, and a client whose cache quietly
// disagrees with the table is worse off than one told to start again.
func TestWatchWithoutTombstonesIsStillRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client := dynamicClient.Resource(unreplayableOrdersGVR).Namespace(acmeNamespace)

	list, err := client.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	from := list.GetResourceVersion()

	const name = "unreplayable-1"
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf("DELETE FROM tombstoned_orders WHERE id = '%s'", name))
	})
	for range 3 {
		execSQL(t, fmt.Sprintf(
			"INSERT INTO tombstoned_orders (id, tenant, customer, updated_at) "+
				"VALUES ('%s', 'acme', 'ada', clock_timestamp()::text) "+
				"ON CONFLICT (id) DO UPDATE SET updated_at = clock_timestamp()::text", name))
	}
	time.Sleep(6 * time.Second)

	watcher, err := client.Watch(ctx, metav1.ListOptions{ResourceVersion: from})

	// However it refuses, it must not have replayed: a projection with no
	// deletion query cannot account for removals.
	if log := apiserverLog(t); strings.Contains(log, "replayed watch history from the database") &&
		strings.Contains(log, unreplayableOrdersGVR.Resource) {
		t.Error("a projection with no deletion query was replayed from the database anyway")
	}

	if err == nil {
		defer watcher.Stop()
		// A watch that opened may still expire on its first event.
		select {
		case event, ok := <-watcher.ResultChan():
			if ok && event.Type != watch.Error {
				t.Fatal("a projection with no deletion query replayed a version it cannot " +
					"account for removals across")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the watch neither replayed nor expired")
		}
		return
	}
	if !apierrors.IsResourceExpired(err) {
		t.Errorf("Watch() returned %v, want 410 Expired", err)
	}
}

// TestUnmappableRowsFailWhenAsked covers mapping.onUnmappableRow, over a pair
// of projections differing in nothing else.
//
// The default leaves a bad row out, which is right when the rest of the table
// is still worth having. Fail is for the case where it is not: a collection
// that quietly omits rows is one a client cannot tell from a smaller
// collection, so a controller reconciling towards it deletes what it cannot
// see.
func TestUnmappableRowsFailWhenAsked(t *testing.T) {
	ctx := context.Background()

	// Skip: the rows that cannot be named are left out, the rest come back.
	lenient, err := dynamicClient.Resource(lenientOrdersGVR).Namespace(acmeNamespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("the default policy failed the whole read: %v", err)
	}
	if len(lenient.Items) == 0 {
		t.Fatal("no rows came back at all, so this proves nothing about skipping some")
	}
	for _, item := range lenient.Items {
		if customer, _, _ := unstructured.NestedString(item.Object, "spec", "customer"); customer == "ada" {
			t.Errorf("%s has customer=ada, which is the row the query makes unnameable; it "+
				"should have been skipped", item.GetName())
		}
	}

	// Fail: the same rows take the whole read down, naming the setting.
	_, err = dynamicClient.Resource(strictOrdersGVR).Namespace(acmeNamespace).
		List(ctx, metav1.ListOptions{})
	if err == nil {
		t.Fatal("onUnmappableRow: Fail returned a collection that is quietly missing rows")
	}
	if !strings.Contains(err.Error(), "onUnmappableRow") {
		t.Errorf("the failure does not name the setting that caused it: %v", err)
	}
}

// TestARecreatedObjectCanBeDeletedAgain covers a tombstone table keyed on the
// name alone.
//
// A name can be deleted, created again and deleted again. With the tombstone
// keyed on id, the second delete fails with a duplicate key for as long as the
// first tombstone exists — which is forever — so the object can never be
// deleted. Reproduced against PostgreSQL before the key was changed:
// "duplicate key value violates unique constraint order_tombstones_pkey".
func TestARecreatedObjectCanBeDeletedAgain(t *testing.T) {
	ctx := context.Background()
	client := dynamicClient.Resource(tombstonedOrdersGVR).Namespace(acmeNamespace)

	const name = "recreated-1"
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf("DELETE FROM tombstoned_orders WHERE id = '%s'", name))
		execSQL(t, fmt.Sprintf("DELETE FROM order_tombstones WHERE id = '%s'", name))
	})

	create := func(round string) {
		t.Helper()
		execSQL(t, fmt.Sprintf(
			"INSERT INTO tombstoned_orders (id, tenant, customer, updated_at) "+
				"VALUES ('%s', 'acme', '%s', clock_timestamp()::text) "+
				"ON CONFLICT (id) DO UPDATE SET customer = '%s'", name, round, round))
	}

	// Delete, recreate, delete. The second one is what used to be impossible.
	for _, round := range []string{"first", "second"} {
		create(round)

		if err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true,
			func(ctx context.Context) (bool, error) {
				_, err := client.Get(ctx, name, metav1.GetOptions{})
				return err == nil, nil
			}); err != nil {
			t.Fatalf("the %s incarnation never became visible: %v", round, err)
		}

		if err := client.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("deleting the %s incarnation: %v", round, err)
		}
		if _, err := client.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("after deleting the %s incarnation, get returned %v, want NotFound", round, err)
		}
	}
}

func drainFor(w watch.Interface, d time.Duration) {
	settled := time.After(d)
	for {
		select {
		case <-w.ResultChan():
		case <-settled:
			return
		}
	}
}

func awaitDeletion(t *testing.T, w watch.Interface, name string, within time.Duration) *unstructured.Unstructured {
	t.Helper()

	deadline := time.After(within)
	for {
		select {
		case event, ok := <-w.ResultChan():
			if !ok {
				t.Fatal("the watch closed before the deletion arrived")
			}
			if event.Type != watch.Deleted {
				continue
			}
			obj, isObject := event.Object.(*unstructured.Unstructured)
			if isObject && obj.GetName() == name {
				return obj
			}
		case <-deadline:
			t.Fatalf("no deletion of %s arrived within %v", name, within)
		}
	}
}
