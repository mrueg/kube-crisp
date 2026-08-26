//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

var namespacesGVR = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}

// TestNamespaceDeletionCompletes is the case the suite never had: it created
// namespaces and never deleted one.
//
// Projected objects are unstructured and cannot be encoded to protobuf. While
// this server offered protobuf anyway, the namespace controller's metadata
// client negotiated it for every deletecollection it issues while emptying a
// namespace, and got "object *unstructured.UnstructuredList does not implement
// the protobuf marshalling interface" back. It retries forever, so the
// namespace never left Terminating.
//
// And not only namespaces holding projected objects. The controller sweeps
// every resource that advertises deletecollection, so a namespace with nothing
// in it but a ConfigMap was stuck exactly the same way — which made this a
// cluster-wide failure caused by installing kube-crisp at all.
func TestNamespaceDeletionCompletes(t *testing.T) {
	ctx := context.Background()
	namespaces := dynamicClient.Resource(namespacesGVR)

	// Nothing projected in it. That is the point: if the sweep is broken, an
	// empty namespace is stuck too, and this is the cheapest way to show it.
	const name = "e2e-namespace-deletion"
	if _, err := namespaces.Create(ctx, newNamespace(name), metav1.CreateOptions{}); err != nil &&
		!apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the namespace: %v", err)
	}

	if err := namespaces.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the namespace: %v", err)
	}

	// Generous: emptying a namespace is a sweep over every resource in
	// discovery, and this cluster has a lot of projections.
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := namespaces.Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, nil
		})
	if err != nil {
		live, getErr := namespaces.Get(ctx, name, metav1.GetOptions{})
		if getErr == nil {
			conditions, _, _ := unstructured.NestedSlice(live.Object, "status", "conditions")
			t.Fatalf("the namespace never finished terminating; conditions: %v", conditions)
		}
		t.Fatalf("the namespace never finished terminating: %v", err)
	}
}

func newNamespace(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": name},
	}}
}
