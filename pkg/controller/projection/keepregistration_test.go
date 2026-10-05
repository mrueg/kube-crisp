package projection

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// registeredBefore is the APIService this server wrote for a group version on
// an earlier run, or that another replica wrote: labelled as kube-crisp's and
// routed to the fixture's Service.
func registeredBefore(group, version string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata": map[string]any{
			"name":   version + "." + group,
			"labels": map[string]any{managedByLabel: managedByValue},
		},
		"spec": map[string]any{
			"group":                 group,
			"version":               version,
			"insecureSkipTLSVerify": true,
			"service": map[string]any{
				"name": "kube-crisp-apiserver", "namespace": "kube-crisp", "port": int64(443),
			},
		},
	}}
}

// A projection that fails on the first sync after a restart keeps the
// registration it had.
//
// failed() keeps whatever a projection is already serving so that a blip -- a
// Secret that cannot be read for a moment, a borrowed schema the kube-apiserver
// did not answer for -- does not withdraw its group. That only held within one
// process. A replica with no previous compilation to fall back on left the
// group out of what it wanted, and pruned the APIService: a single replica
// restarting into a transient failure deleted the registration outright, and
// two replicas, one still serving from an earlier compilation, deleted and
// recreated it on each other's informer events for as long as the failure
// lasted.
func TestAFailedProjectionWithNothingToFallBackOnKeepsItsRegistration(t *testing.T) {
	broken := projectionObject("bins", "bins")
	// A namespaced projection with no namespace column cannot be compiled.
	broken.Spec.Mapping = crispv1alpha1.Mapping{Name: "id"}

	f := newFixture(t, []runtime.Object{broken}, registeredBefore("warehouse.example.com", "v1alpha1"))
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if _, found := f.apiService(t, "v1alpha1.warehouse.example.com"); !found {
		t.Fatal("a projection that failed to compile had its registration withdrawn")
	}

	// Deleting it is still a withdrawal.
	if err := f.client.CrispV1alpha1().CustomResourceProjections().
		Delete(context.Background(), "bins", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the projection: %v", err)
	}
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 0 })

	if _, found := f.apiService(t, "v1alpha1.warehouse.example.com"); found {
		t.Error("the registration outlived the projection that declared its group")
	}
}

// The same for a file-backed projection, which is the one a restart always
// starts without a previous compilation for.
func TestAFailedFileWithNothingToFallBackOnKeepsItsRegistration(t *testing.T) {
	dir := t.TempDir()
	// A plural the endpoint installer reads as a subresource, refused when the
	// projection is prepared.
	writeStaticResource(t, dir, "bins", `    group: warehouse.example.com
    version: v1alpha1
    kind: Bin
    plural: bins/status
    scope: Namespaced
    schema:
      type: object`)

	f := newFixture(t, nil, registeredBefore("warehouse.example.com", "v1alpha1"))
	f.controller.staticDir = dir
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	if degraded := f.controller.Degraded(); len(degraded) != 1 {
		t.Fatalf("Degraded() = %v, want the broken file", degraded)
	}

	if _, found := f.apiService(t, "v1alpha1.warehouse.example.com"); !found {
		t.Error("a file-backed projection that failed to compile had its registration withdrawn")
	}
}

// A cluster object refused its name declares nothing.
//
// It is not served and never will be while the file holds the name, so the
// group it names is not one this server is keeping for it: a registration
// there is withdrawn like any other nobody declares.
func TestAReservedObjectKeepsNoRegistration(t *testing.T) {
	dir := t.TempDir()
	writeStaticProjection(t, dir, "orders", "orders")

	object := projectionObject("orders", "crates")
	object.Spec.Resource.Group = "elsewhere.example.com"

	f := newFixture(t, []runtime.Object{object}, registeredBefore("elsewhere.example.com", "v1alpha1"))
	f.controller.staticDir = dir
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if _, found := f.apiService(t, "v1alpha1.elsewhere.example.com"); found {
		t.Error("an object refused its name kept a registration for its group")
	}
	if _, found := f.apiService(t, "v1alpha1.warehouse.example.com"); !found {
		t.Error("the file serving the name was not registered")
	}
}
