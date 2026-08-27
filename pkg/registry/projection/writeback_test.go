package projection

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apiserver/pkg/registry/rest"
)

// A write holds a limiter slot for the whole of write(), including the read
// back that produces the response. At maxConcurrentQueries: 1 that read has
// nowhere to run.
func TestWriteReadBackDoesNotWaitOnItsOwnSlot(t *testing.T) {
	spec := writableSpec()
	spec.DataSource.MaxConcurrentQueries = ptr(int32(1))

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	start := time.Now()
	_, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(newOrder("order-1001", "grace", 8888)), nil, nil, false, &metav1.UpdateOptions{})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Update() error = %v, want success (the row was written)", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Update() took %v; it waited on a slot it was holding itself", elapsed)
	}
}

// The read back after a write must not be answered from the cache the write
// invalidates.
func TestWriteReadBackIsNotAnsweredFromTheStaleCache(t *testing.T) {
	spec := writableSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}

	store := newStorage(t, spec).(*WritableREST)
	ctx := namespacedContext("acme")

	// Populate the cache with the pre-write object.
	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
		t.Fatalf("priming Get() error: %v", err)
	}

	result, _, err := store.Update(ctx, "order-1001",
		rest.DefaultUpdatedObjectInfo(newOrder("order-1001", "grace", 8888)), nil, nil, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	customer, _, _ := unstructured.NestedString(result.(*unstructured.Unstructured).Object, "spec", "customer")
	if customer != "grace" {
		t.Errorf("Update() answered with spec.customer = %q, want %q: the read back saw the pre-write cache entry", customer, "grace")
	}
}
