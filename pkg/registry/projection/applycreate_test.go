package projection

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"
)

// Server-side apply creates what is not there yet, and it asks for that through
// Update's forceAllowCreate. kubectl apply --server-side is how most objects are
// written now, so a projection that can create must not answer it with 404.
func TestApplyCreatesAnObjectThatIsNotThere(t *testing.T) {
	store := newWritableREST(t)
	ctx := namespacedContext("acme")

	incoming := newOrder("order-4242", "hopper", 500)
	incoming.SetResourceVersion("")

	result, created, err := store.Update(ctx, "order-4242",
		rest.DefaultUpdatedObjectInfo(incoming), nil, nil, true, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() with forceAllowCreate returned error: %v", err)
	}
	if !created {
		t.Error("Update() did not report the object as created")
	}

	customer, _, _ := unstructured.NestedString(result.(*unstructured.Unstructured).Object, "spec", "customer")
	if customer != "hopper" {
		t.Errorf("spec.customer = %q, want %q", customer, "hopper")
	}

	if _, err := store.Get(ctx, "order-4242", &metav1.GetOptions{}); err != nil {
		t.Errorf("the created row cannot be read back: %v", err)
	}
}

// A projection that declares no create statement still refuses. Apply cannot
// conjure a row into a table the projection was never told how to write to.
func TestApplyDoesNotCreateWithoutACreateQuery(t *testing.T) {
	spec := writableSpec()
	spec.Queries.Create = nil

	store := newStorage(t, spec).(*WritableREST)

	incoming := newOrder("order-4243", "hopper", 500)
	incoming.SetResourceVersion("")

	_, created, err := store.Update(namespacedContext("acme"), "order-4243",
		rest.DefaultUpdatedObjectInfo(incoming), nil, nil, true, &metav1.UpdateOptions{})
	if created {
		t.Fatal("Update() created a row on a projection with no create statement")
	}
	if !errors.IsNotFound(err) {
		t.Fatalf("Update() error = %v, want NotFound", err)
	}
}
