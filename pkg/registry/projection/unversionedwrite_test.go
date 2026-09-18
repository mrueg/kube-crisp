package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// unversionedWatchableSpec is the watchable fixture with no resourceVersion
// mapped: the rows carry no version, and the watch cache stamps its own
// counter onto the events it delivers.
func unversionedWatchableSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := watchableSpec()
	spec.Mapping.ResourceVersion = ""
	return spec
}

// modifiedFromWatch lists, watches from the list's version the way an
// informer does, changes a row through the API, and hands back the object the
// watch delivered for it — the copy a controller then writes back.
func modifiedFromWatch(t *testing.T, store *WritableREST) *unstructured.Unstructured {
	t.Helper()
	ctx := namespacedContext("acme")

	listed, err := store.List(ctx, &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	w, err := store.Watch(ctx, &metainternalversion.ListOptions{
		ResourceVersion: listed.(*unstructured.UnstructuredList).GetResourceVersion(),
	})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	t.Cleanup(w.Stop)

	// Asserting nothing: this write is only here to make the row move.
	changed := newOrder("order-1001", "grace", 42)
	changed.SetResourceVersion("")
	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(changed), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() behind the watcher returned error: %v", err)
	}

	event := nextEvent(t, w)
	if event.Type != watch.Modified {
		t.Fatalf("watch delivered %s, want %s", event.Type, watch.Modified)
	}
	delivered := event.Object.(*unstructured.Unstructured)
	if delivered.GetResourceVersion() == "" {
		t.Fatal("the event carries no resourceVersion; the premise of this test is that it carries the cache's counter")
	}
	return delivered
}

// TestUpdateOfAWatchDeliveredObjectSucceedsWithoutAVersionColumn is the
// livelock. A projection with no mapped resourceVersion stores none, so Get
// and List answer with an empty one — but the watch stamps its counter onto
// every event, and that is the object an informer holds. A controller that
// writes it back sends the counter, applyUpdate compared it against the empty
// stored version, and answered 409. Nothing about a retry changes either side,
// so the controller was refused forever.
func TestUpdateOfAWatchDeliveredObjectSucceedsWithoutAVersionColumn(t *testing.T) {
	store := newStorage(t, unversionedWatchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	held := modifiedFromWatch(t, store)

	edit := held.DeepCopy()
	if err := unstructured.SetNestedField(edit.Object, "hopper", "spec", "customer"); err != nil {
		t.Fatalf("setting spec.customer: %v", err)
	}
	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(edit), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() with the object the watch delivered: error = %v", err)
	}

	// The same cached copy again, which is what a controller retries with.
	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(edit), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("second Update() with the same object: error = %v", err)
	}

	got, err := store.Get(ctx, "order-1001", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	customer, _, _ := unstructured.NestedString(got.(*unstructured.Unstructured).Object, "spec", "customer")
	if customer != "hopper" {
		t.Errorf("spec.customer = %q, want the value the write carried", customer)
	}
}

// TestDeleteWithAWatchDeliveredVersionSucceedsWithoutAVersionColumn is the
// same value on the other verb: a delete whose precondition quotes the version
// off an event has nothing in the record to be compared with, and is honoured
// rather than refused.
func TestDeleteWithAWatchDeliveredVersionSucceedsWithoutAVersionColumn(t *testing.T) {
	store := newStorage(t, unversionedWatchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	held := modifiedFromWatch(t, store)
	version := held.GetResourceVersion()

	if _, _, err := store.Delete(ctx, "order-1001", nil, &metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{ResourceVersion: &version},
	}); err != nil {
		t.Fatalf("Delete() with the version the watch delivered: error = %v", err)
	}
	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Get() after the delete: error = %v, want NotFound", err)
	}
}

// TestAMappedVersionColumnStillRejectsAStaleUpdate keeps the other side
// honest: skipping the check is for a record that has no version, not for
// every client that sends one. With the column mapped, a stale version is
// still a conflict.
func TestAMappedVersionColumnStillRejectsAStaleUpdate(t *testing.T) {
	store := newStorage(t, watchableSpec()).(*WritableREST)
	ctx := namespacedContext("acme")

	held := modifiedFromWatch(t, store)

	// Moved on behind the client's back, so the version it holds is stale.
	behind := newOrder("order-1001", "carol", 7)
	behind.SetResourceVersion("")
	if _, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(behind), nil, nil, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() behind the client returned error: %v", err)
	}

	_, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(held), nil, nil, false, &metav1.UpdateOptions{})
	if !errors.IsConflict(err) {
		t.Fatalf("Update() with a stale mapped version: error = %v, want Conflict", err)
	}
}
