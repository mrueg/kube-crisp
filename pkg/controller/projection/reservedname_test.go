package projection

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// readyCondition is the Ready condition the controller last wrote for a
// cluster object.
func readyCondition(t *testing.T, f *fixture, name string) metav1.Condition {
	t.Helper()

	obj, err := f.client.CrispV1alpha1().CustomResourceProjections().
		Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading projection %s: %v", name, err)
	}
	ready := apimeta.FindStatusCondition(obj.Status.Conditions, crispv1alpha1.ConditionReady)
	if ready == nil {
		t.Fatalf("projection %s has no Ready condition: %+v", name, obj.Status)
	}
	return *ready
}

// A cluster object cannot take a file-backed projection's name.
//
// The two sets were merged by name, so an object bearing the name of a file
// replaced it wholesale: whoever could create a CustomResourceProjection could
// read the operator's projection name from the log, point an object of that
// name at any opted-in Secret with statements of their own, and the file's
// consumers were served the object's rows -- while the file was retired
// without a word, and the claim resolver saw nothing, since it settles claims
// between projections and only one of that name was left.
//
// The file keeps serving. The object is failed the way a losing claim is:
// not compiled, not installed, named by Degraded, and told why in its Ready
// condition and in a warning Event, so it is never silently dropped either.
func TestAClusterObjectCannotTakeAFileBackedProjectionsName(t *testing.T) {
	dir := t.TempDir()
	writeStaticProjection(t, dir, "orders", "orders")

	// The same name, a different resource and a Secret of its own.
	object := projectionObject("orders", "crates")
	kube := k8sfake.NewSimpleClientset()
	f := newFixtureWithEvents(t, kube, []runtime.Object{object})
	f.controller.staticDir = dir
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/orders]"; got != want {
		t.Errorf("served paths = %s, want the file alone %s", got, want)
	}
	if degraded := f.controller.Degraded(); degraded[0] != "orders" {
		t.Errorf("Degraded() = %v, want [orders]: the object is defined and not served", degraded)
	}

	ready := readyCondition(t, f, "orders")
	if ready.Status != metav1.ConditionFalse || ready.Reason != "NameReservedByFile" {
		t.Errorf("Ready = %s (%s), want False (NameReservedByFile)", ready.Status, ready.Reason)
	}
	if !strings.Contains(ready.Message, dir) || !strings.Contains(ready.Message, "orders.warehouse.example.com") {
		t.Errorf("Ready message = %q; it should name the directory and what the file serves", ready.Message)
	}

	var announced bool
	for _, event := range recordedEvents(t, kube) {
		if event.Reason == "NameReservedByFile" && event.Type == corev1.EventTypeWarning {
			announced = true
		}
	}
	if !announced {
		t.Error("the object was refused its name and no Event said so")
	}
}

// Removing the file hands the name to the cluster object on the next sync.
func TestARemovedFileHandsItsNameToTheClusterObject(t *testing.T) {
	dir := t.TempDir()
	writeStaticProjection(t, dir, "orders", "orders")

	f := newFixture(t, []runtime.Object{projectionObject("orders", "crates")})
	f.controller.staticDir = dir
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if err := os.Remove(filepath.Join(dir, "orders.yaml")); err != nil {
		t.Fatalf("removing the file: %v", err)
	}
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() after the file was removed returned error: %v", err)
	}

	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/crates]"; got != want {
		t.Errorf("served paths = %s, want the object alone %s", got, want)
	}
	if degraded := f.controller.Degraded(); len(degraded) != 0 {
		t.Errorf("Degraded() = %v, want nothing once the file is gone", degraded)
	}
	if ready := readyCondition(t, f, "orders"); ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %s (%s: %s), want True", ready.Status, ready.Reason, ready.Message)
	}
}

// A file added under a name a cluster object already uses takes the name over.
//
// The operator controls the directory, so it is the object that gives way, not
// the file -- whichever of them arrived first. The registration the object
// owned is the file's now and is left unowned, or deleting the object would
// collect it from under the file.
func TestAFileAddedUnderAClusterObjectsNameTakesItOver(t *testing.T) {
	dir := t.TempDir()

	// With a UID, so that it owns the registration to begin with.
	object := projectionObject("orders", "crates")
	object.UID = "11111111-2222-3333-4444-555555555555"
	f := newFixture(t, []runtime.Object{object})
	f.controller.staticDir = dir
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })
	if ready := readyCondition(t, f, "orders"); ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %s (%s), want True before the file exists", ready.Status, ready.Reason)
	}
	if registration, found := f.apiService(t, "v1alpha1.warehouse.example.com"); !found {
		t.Fatal("the group's APIService was never registered")
	} else if owners := registration.GetOwnerReferences(); len(owners) != 1 || owners[0].UID != object.UID {
		t.Fatalf("the APIService is owned by %v, want the object before the file exists", owners)
	}

	writeStaticProjection(t, dir, "orders", "orders")
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() after the file was added returned error: %v", err)
	}

	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/orders]"; got != want {
		t.Errorf("served paths = %s, want the file alone %s", got, want)
	}
	if degraded := f.controller.Degraded(); len(degraded) != 1 || degraded[0] != "orders" {
		t.Errorf("Degraded() = %v, want [orders]", degraded)
	}
	if ready := readyCondition(t, f, "orders"); ready.Status != metav1.ConditionFalse || ready.Reason != "NameReservedByFile" {
		t.Errorf("Ready = %s (%s), want False (NameReservedByFile)", ready.Status, ready.Reason)
	}

	registration, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("the group's APIService is gone")
	}
	if owners := registration.GetOwnerReferences(); len(owners) != 0 {
		t.Errorf("the APIService is owned by %v; the object serves nothing through it", owners)
	}
}

// A file that takes a name over and fails to compile does not fall back to
// what the cluster object compiled.
//
// The previous configuration a broken projection keeps serving is its own. A
// file and an object of one name are two projections, so once the file holds
// the name the object's rows are not served under it, whatever state the file
// is in; the same rule keeps a file the operator removed from being served on
// behalf of a broken object that took its name.
func TestATakenOverNameDoesNotServeTheClusterObjectsPreviousConfiguration(t *testing.T) {
	dir := t.TempDir()

	f := newFixture(t, []runtime.Object{projectionObject("orders", "crates")})
	f.controller.staticDir = dir
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	// A plural naming a subresource is refused where the file is prepared.
	writeStaticProjection(t, dir, "orders", "orders/status")
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() after the file was added returned error: %v", err)
	}

	if got := servedPaths(f.router); got != "[]" {
		t.Errorf("served paths = %s, want nothing: the object's storage outlived its claim to the name", got)
	}
	if degraded := f.controller.Degraded(); len(degraded) != 1 || degraded[0] != "orders" {
		t.Errorf("Degraded() = %v, want [orders]", degraded)
	}
}
