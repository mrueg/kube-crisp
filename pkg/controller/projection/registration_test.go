package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/dynamicinformer"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
	crispfake "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned/fake"
	crispinformers "github.com/mrueg/kube-crisp/pkg/generated/informers/externalversions"
)

// reportAvailability plays the part of the aggregation layer: it writes an
// Available condition onto the APIService this server registered.
//
// Onto the live object rather than a hand-built one, because that is the real
// sequence — the registration is created first and dialled afterwards — and
// because a hand-built spec that drifts from what the manager writes would make
// the controller correct it and quietly erase the status under test.
func reportAvailability(t *testing.T, f *fixture, name, status, reason, message string) {
	t.Helper()

	live, found := f.apiService(t, name)
	if !found {
		t.Fatalf("no APIService %s to report on", name)
	}
	if err := unstructured.SetNestedSlice(live.Object, []any{
		map[string]any{
			"type":    "Available",
			"status":  status,
			"reason":  reason,
			"message": message,
		},
	}, "status", "conditions"); err != nil {
		t.Fatalf("setting the Available condition: %v", err)
	}
	if _, err := f.dynamic.Resource(APIServiceGVR).Update(
		context.Background(), live, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("writing APIService status: %v", err)
	}
}

func conditionOf(t *testing.T, f *fixture, name, conditionType string) *metav1.Condition {
	t.Helper()

	obj, err := f.client.CrispV1alpha1().CustomResourceProjections().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the projection: %v", err)
	}
	condition := apimeta.FindStatusCondition(obj.Status.Conditions, conditionType)
	if condition == nil {
		t.Fatalf("no %s condition in %v", conditionType, obj.Status.Conditions)
	}
	return condition
}

// TestProjectionIsNotReadyWhenTheAggregatorCannotReachIt is the defect this
// covers: a projection that compiled reported Ready=True on the strength of the
// compile alone. Whether anything could actually reach the API it claimed to be
// serving was never consulted, so an APIService the aggregator had marked
// unavailable — a Service with no endpoints, a stale CA bundle, a certificate
// that does not name the Service — left every projection saying "Serving
// /apis/..." while requests for it returned NotFound.
func TestProjectionIsNotReadyWhenTheAggregatorCannotReachIt(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	reportAvailability(t, f, "v1alpha1.warehouse.example.com", "False", "ServiceNotFound",
		"service/kube-crisp-apiserver in kube-crisp is not present")
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync after the aggregator reported: %v", err)
	}

	registered := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered)
	if registered.Status != metav1.ConditionFalse {
		t.Errorf("Registered = %v, want False", registered.Status)
	}
	if registered.Reason != "NotRouted" {
		t.Errorf("Registered reason = %q, want NotRouted", registered.Reason)
	}

	ready := conditionOf(t, f, "bins", crispv1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready = %v, want False when nothing can reach the projection", ready.Status)
	}
}

// TestProjectionIsReadyWhenTheAggregatorRoutesToIt is the other side: the same
// path with Available=True has to leave Ready alone.
func TestProjectionIsReadyWhenTheAggregatorRoutesToIt(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	reportAvailability(t, f, "v1alpha1.warehouse.example.com", "True", "Passed", "all checks passed")
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync after the aggregator reported: %v", err)
	}

	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered).Status; got != metav1.ConditionTrue {
		t.Errorf("Registered = %v, want True", got)
	}
	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionReady).Status; got != metav1.ConditionTrue {
		t.Errorf("Ready = %v, want True", got)
	}
}

// TestRegistrationNotYetConfirmedIsUnknownRatherThanFailed keeps a projection
// created a moment ago from flapping.
//
// The aggregator sets Available on its own schedule, and there are environments
// with no aggregation layer at all. Neither is a failure, so the wait shows up
// as Registered=Unknown while Ready stands on what this server is responsible
// for.
func TestRegistrationNotYetConfirmedIsUnknownRatherThanFailed(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	registered := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered)
	if registered.Status != metav1.ConditionUnknown {
		t.Errorf("Registered = %v, want Unknown before anything has reported", registered.Status)
	}
	if registered.Reason != "Pending" {
		t.Errorf("Registered reason = %q, want Pending", registered.Reason)
	}
	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionReady).Status; got != metav1.ConditionTrue {
		t.Errorf("Ready = %v, want True while registration is merely unconfirmed", got)
	}
}

// TestAPIServiceChangeQueuesASync covers the informer that nothing listened to.
//
// It was a read cache and no more, so an APIService someone deleted, or one the
// aggregator had just marked unavailable, was neither repaired nor reported
// until the next resync.
func TestAPIServiceChangeQueuesASync(t *testing.T) {
	existing := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata": map[string]any{
			"name":   "v1alpha1.warehouse.example.com",
			"labels": map[string]any{managedByLabel: managedByValue},
		},
		"spec": map[string]any{"group": "warehouse.example.com", "version": "v1alpha1"},
	}}

	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{APIServiceGVR: "APIServiceList"},
		existing,
	)

	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)
	informer := factory.ForResource(APIServiceGVR).Informer()
	controller := New(Options{
		Client:             crispfake.NewSimpleClientset(),
		DynamicClient:      dynamicClient,
		Factory:            crispinformers.NewSharedInformerFactory(crispfake.NewSimpleClientset(), 0),
		APIServiceInformer: informer,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		t.Fatal("the APIService cache never synced")
	}

	// The initial listing enqueues too; take that one out of the way so the
	// next item can only have come from the change below.
	take(t, controller)

	broken := existing.DeepCopy()
	_ = unstructured.SetNestedSlice(broken.Object, []any{
		map[string]any{
			"type":    "Available",
			"status":  "False",
			"reason":  "FailedDiscoveryCheck",
			"message": "failing or missing response from the backend",
		},
	}, "status", "conditions")

	if _, err := dynamicClient.Resource(APIServiceGVR).Update(ctx, broken, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("marking the APIService unavailable: %v", err)
	}

	take(t, controller)
}

// TestRegisteredAPIServiceIsOwnedByItsProjection is what makes an uninstall
// clean up after itself.
//
// Without owner references the registrations outlive everything: cluster-scoped
// objects pointing at a Service that no longer exists, which the aggregation
// layer goes on dialling. Owned, deleting the projection — or the CRD, which
// takes every projection with it — collects them.
func TestRegisteredAPIServiceIsOwnedByItsProjection(t *testing.T) {
	projection := projectionObject("bins", "bins")
	projection.UID = "11111111-2222-3333-4444-555555555555"

	f := newFixture(t, []runtime.Object{projection})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	apiService, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("no APIService was created")
	}

	owners := apiService.GetOwnerReferences()
	if len(owners) != 1 {
		t.Fatalf("APIService has %d owner references, want 1: %+v", len(owners), owners)
	}
	if owners[0].Kind != "CustomResourceProjection" {
		t.Errorf("owner kind = %q, want CustomResourceProjection", owners[0].Kind)
	}
	if owners[0].Name != "bins" {
		t.Errorf("owner name = %q, want bins", owners[0].Name)
	}
	if owners[0].UID != projection.UID {
		t.Errorf("owner UID = %q, want %q", owners[0].UID, projection.UID)
	}
}

// TestAPIServiceOwnersSkipFileBasedProjections keeps a registration from being
// collected out from under a projection that has no object to be deleted.
//
// Kubernetes removes a dependent once all of its owners are gone. A group
// version served by both a stored projection and one loaded from a file would,
// if owned by the stored one alone, lose its registration the moment that
// object was deleted — while the file-based projection went on serving through
// it.
func TestAPIServiceOwnersSkipFileBasedProjections(t *testing.T) {
	stored := projectionObject("bins", "bins")
	stored.UID = "11111111-2222-3333-4444-555555555555"
	fromFile := projectionObject("bins-from-file", "crates")

	gv := schema.GroupVersion{Group: "warehouse.example.com", Version: "v1alpha1"}
	controller := New(Options{
		Client:  crispfake.NewSimpleClientset(),
		Factory: crispinformers.NewSharedInformerFactory(crispfake.NewSimpleClientset(), 0),
	})

	sharedGroup := []apidynamic.Resource{
		{Group: gv.Group, Version: gv.Version, Plural: "bins", ProjectionName: "bins"},
		{Group: gv.Group, Version: gv.Version, Plural: "crates", ProjectionName: "bins-from-file"},
	}

	// Both stored: the group version is owned by both.
	both := controller.apiServiceOwners([]projectionCandidate{
		{projection: stored, stored: stored},
		{projection: fromFile, stored: withUID(fromFile, "66666666-7777-8888-9999-000000000000")},
	}, sharedGroup)
	if got := len(both[gv]); got != 2 {
		t.Errorf("two stored projections produced %d owners, want 2", got)
	}

	// One of them from a file: nothing owns it, so nothing collects it.
	mixed := controller.apiServiceOwners([]projectionCandidate{
		{projection: stored, stored: stored},
		{projection: fromFile, stored: nil},
	}, sharedGroup)
	if got := len(mixed[gv]); got != 0 {
		t.Errorf("a file-based projection in the group produced %d owners, want none: %+v", got, mixed[gv])
	}
}

func withUID(p *crispv1alpha1.CustomResourceProjection, uid string) *crispv1alpha1.CustomResourceProjection {
	out := p.DeepCopy()
	out.UID = types.UID(uid)
	return out
}

// shortenRegistrationRecheck makes the re-check interval short enough to assert
// on, and restores it afterwards.
func shortenRegistrationRecheck(t *testing.T, d time.Duration) {
	t.Helper()
	previous := registrationRecheckInterval
	registrationRecheckInterval = d
	t.Cleanup(func() { registrationRecheckInterval = previous })
}

// TestUnconfirmedRegistrationIsRecheckedWithoutAnEvent is what stops a
// projection reporting an outage that is over.
//
// The aggregator's verdict reaches this controller as a change to an APIService,
// and the informer turns that into a sync. But a verdict that lands *during* a
// sync is spent on one that has already read the cache, so the condition it
// writes is out of date and nothing is left to correct it. Observed on a
// rollout: the registration became available in the same second the controller
// read it as unavailable, and the projection reported the failure for ten
// minutes, until an unrelated change queued the next sync.
func TestUnconfirmedRegistrationIsRecheckedWithoutAnEvent(t *testing.T) {
	shortenRegistrationRecheck(t, 250*time.Millisecond)

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	// Nothing has reported on the registration in this fixture, so it is
	// Pending — unresolved, and therefore worth looking at again.
	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered).Status; got != metav1.ConditionUnknown {
		t.Fatalf("Registered = %v, want Unknown for this fixture", got)
	}

	// Drain whatever the informer queued, so what is left can only have been
	// queued by the sync itself.
	for f.controller.queue.Len() > 0 {
		item, _ := f.controller.queue.Get()
		f.controller.queue.Done(item)
	}

	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}

	// AddAfter, so it arrives on a delay rather than immediately.
	deadline := time.Now().Add(registrationRecheckInterval + 10*time.Second)
	for f.controller.queue.Len() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("an unresolved registration queued no re-check, so nothing would correct a verdict that arrived during the sync")
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestConfirmedRegistrationQueuesNoRecheck is the other half: a healthy server
// must still sync only when something changes, or this becomes a busy loop over
// every projection every fifteen seconds.
func TestConfirmedRegistrationQueuesNoRecheck(t *testing.T) {
	shortenRegistrationRecheck(t, 250*time.Millisecond)

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	reportAvailability(t, f, "v1alpha1.warehouse.example.com", "True", "Passed", "all checks passed")
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync after the aggregator reported: %v", err)
	}
	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered).Status; got != metav1.ConditionTrue {
		t.Fatalf("Registered = %v, want True", got)
	}

	// The syncs above ran while the registration was still Pending, and each
	// scheduled a re-check of its own. Let those land before draining, or what
	// is drained here is their backlog and the assertion below is vacuous.
	time.Sleep(registrationRecheckInterval * 4)
	for f.controller.queue.Len() > 0 {
		item, _ := f.controller.queue.Get()
		f.controller.queue.Done(item)
	}

	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}

	// Long enough that a re-check would have landed.
	time.Sleep(registrationRecheckInterval * 4)
	if n := f.controller.queue.Len(); n != 0 {
		t.Errorf("a fully registered projection queued %d re-check(s); it should sync only on change", n)
	}
}

// TestStatusWriteRetriesOnConflict covers the write that used to be logged and
// dropped.
//
// The object is written by whoever applied it and by this controller, so a
// conflict here is ordinary. Dropping one leaves the projection describing a
// state it is no longer in until something else queues a sync — observed on a
// rollout, where the dropped write was the one that would have cleared a
// registration failure that had already recovered.
func TestStatusWriteRetriesOnConflict(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})

	fake, ok := f.client.(*crispfake.Clientset)
	if !ok {
		t.Fatalf("the fixture holds a %T, which cannot be given a reactor", f.client)
	}

	var attempts int
	fake.PrependReactor("update", "customresourceprojections",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.GetSubresource() != "status" {
				return false, nil, nil
			}
			attempts++
			if attempts == 1 {
				return true, nil, apierrors.NewConflict(
					schema.GroupResource{Group: "crisp.kubecrisp.io", Resource: "customresourceprojections"},
					"bins", errors.New("the object has been modified"))
			}
			return false, nil, nil
		})

	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if attempts < 2 {
		t.Fatalf("the status was written %d time(s); a conflict should have been retried", attempts)
	}

	// And the retry actually persisted something, rather than merely being
	// attempted.
	obj, err := f.client.CrispV1alpha1().CustomResourceProjections().Get(
		context.Background(), "bins", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the projection: %v", err)
	}
	if len(obj.Status.Conditions) == 0 {
		t.Error("the projection has no conditions, so the write that conflicted was never retried through")
	}
}
