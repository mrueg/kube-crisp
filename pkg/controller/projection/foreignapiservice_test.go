package projection

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
)

// availableAPIService builds an APIService for the fixture's projected group
// version that the aggregation layer has already marked Available, registered
// by something other than this server.
//
// Available from the start, because that is the shape of the defect: the
// aggregator reports on whatever the APIService points at, and what it points
// at is answering.
func availableAPIService(labels, service map[string]any) *unstructured.Unstructured {
	spec := map[string]any{
		"group":   "warehouse.example.com",
		"version": "v1alpha1",
	}
	if service != nil {
		spec["service"] = service
	}
	metadata := map[string]any{"name": "v1alpha1.warehouse.example.com"}
	if labels != nil {
		metadata["labels"] = labels
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata":   metadata,
		"spec":       spec,
		"status": map[string]any{
			"conditions": []any{map[string]any{
				"type":    "Available",
				"status":  "True",
				"reason":  "Passed",
				"message": "all checks passed",
			}},
		},
	}}
}

// crdAPIService is what the kube-apiserver registers for a group a
// CustomResourceDefinition serves: no Service, since it answers itself, and
// available for exactly as long as it is running.
func crdAPIService() *unstructured.Unstructured {
	return availableAPIService(map[string]any{"kube-aggregator.kubernetes.io/automanaged": "true"}, nil)
}

// TestAGroupServedByACRDIsNotReportedAsRegistered is the defect this covers: a
// projection claiming a group a CustomResourceDefinition already owns reported
// Registered=True and Ready=True.
//
// The APIService for that group version exists and is not this server's, so it
// was rightly left alone -- and then its Available condition, which describes
// the kube-apiserver serving the CRD, was read as the projection's own. Every
// request for the projection went to the CRD; nothing said so.
func TestAGroupServedByACRDIsNotReportedAsRegistered(t *testing.T) {
	kube := k8sfake.NewSimpleClientset()
	f := newFixtureWithEvents(t, kube, []runtime.Object{projectionObject("bins", "bins")}, crdAPIService())
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	registered := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered)
	if registered.Status != metav1.ConditionFalse {
		t.Errorf("Registered = %v, want False for a group a CRD serves", registered.Status)
	}
	if registered.Reason != "GroupAlreadyServed" {
		t.Errorf("Registered reason = %q, want GroupAlreadyServed", registered.Reason)
	}
	for _, want := range []string{
		"v1alpha1.warehouse.example.com",
		"not managed by kube-crisp",
		"CustomResourceDefinition",
		"another group",
	} {
		if !strings.Contains(registered.Message, want) {
			t.Errorf("Registered message does not mention %q: %s", want, registered.Message)
		}
	}

	ready := conditionOf(t, f, "bins", crispv1alpha1.ConditionReady)
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready = %v, want False when the group routes to a CRD", ready.Status)
	}
	if ready.Reason != "NotRegistered" {
		t.Errorf("Ready reason = %q, want NotRegistered", ready.Reason)
	}

	// Still not adopted: refusing to report on it is not a licence to take it.
	apiService, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("the CRD's APIService disappeared")
	}
	if apiService.GetLabels()[managedByLabel] == managedByValue {
		t.Error("the CRD's APIService was adopted")
	}
	if _, found, _ := unstructured.NestedMap(apiService.Object, "spec", "service"); found {
		t.Error("the CRD's APIService was rewritten to point here")
	}

	events := recordedEvents(t, kube)
	if len(events) == 0 {
		t.Fatal("a projection whose group is served elsewhere said nothing about it")
	}
	if events[0].Reason != "GroupAlreadyServed" {
		t.Errorf("event reason = %q, want GroupAlreadyServed", events[0].Reason)
	}
}

// TestAGroupRoutedToAnotherServerIsNotReportedAsRegistered is the same defect
// with another aggregated server behind the group: its APIService names who
// manages it and where it goes, and the projection's message has to say both.
func TestAGroupRoutedToAnotherServerIsNotReportedAsRegistered(t *testing.T) {
	foreign := availableAPIService(
		map[string]any{managedByLabel: "somebody-else"},
		map[string]any{"name": "other", "namespace": "elsewhere", "port": int64(443)},
	)
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")}, foreign)
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	registered := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered)
	if registered.Status != metav1.ConditionFalse || registered.Reason != "GroupAlreadyServed" {
		t.Errorf("Registered = %v (%s), want False (GroupAlreadyServed)", registered.Status, registered.Reason)
	}
	for _, want := range []string{`"somebody-else"`, "Service elsewhere/other"} {
		if !strings.Contains(registered.Message, want) {
			t.Errorf("Registered message does not mention %s: %s", want, registered.Message)
		}
	}
	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionReady).Status; got != metav1.ConditionFalse {
		t.Errorf("Ready = %v, want False when the group routes to another server", got)
	}
}

// TestARegistrationWrittenByHandAgainstThisServerCounts keeps the documented
// manual path working. examples/apiservice.yaml carries no managed-by label,
// and an operator who applied it before turning management on has a
// registration that routes here in every way that matters. It is not adopted,
// and it is not a conflict either.
func TestARegistrationWrittenByHandAgainstThisServerCounts(t *testing.T) {
	byHand := availableAPIService(nil, map[string]any{
		"name": "kube-crisp-apiserver", "namespace": "kube-crisp", "port": int64(443),
	})
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")}, byHand)
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered); got.Status != metav1.ConditionTrue {
		t.Errorf("Registered = %v (%s: %s), want True for a hand-written registration pointing here",
			got.Status, got.Reason, got.Message)
	}
	if got := conditionOf(t, f, "bins", crispv1alpha1.ConditionReady).Status; got != metav1.ConditionTrue {
		t.Errorf("Ready = %v, want True", got)
	}

	apiService, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("the hand-written APIService disappeared")
	}
	if apiService.GetLabels()[managedByLabel] == managedByValue {
		t.Error("the hand-written APIService was adopted")
	}
}

// TestRoutableRequiresARegistrationThatRoutesHere pins the check at the point
// that reads the aggregator's verdict, independently of ensure having run
// first: Available on an APIService that points elsewhere is not routable.
func TestRoutableRequiresARegistrationThatRoutesHere(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(crdAPIService()); err != nil {
		t.Fatalf("seeding the cache: %v", err)
	}
	manager := newAPIServiceManager(
		dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
			runtime.NewScheme(),
			map[schema.GroupVersionResource]string{APIServiceGVR: "APIServiceList"},
		),
		APIServiceOptions{Enabled: true, ServiceName: "kube-crisp-apiserver", ServiceNamespace: "kube-crisp", Port: 443},
		indexer,
	)

	err := manager.routable(context.Background(), "v1alpha1.warehouse.example.com")
	if err == nil {
		t.Fatal("an Available APIService routing to a CRD was reported as routing here")
	}
	if !errors.Is(err, errGroupServedElsewhere) {
		t.Errorf("the error is not the conflict: %v", err)
	}
	if errors.Is(err, errRegistrationPending) {
		t.Error("a conflict was reported as pending, which would leave Ready standing")
	}

	// And the whole reconcile reports it against the group version, which is
	// how the projection behind it finds out.
	unregistered, err := manager.reconcile(context.Background(),
		[]apidynamic.Resource{{Group: "warehouse.example.com", Version: "v1alpha1", Plural: "bins"}}, nil)
	if err != nil {
		t.Fatalf("reconcile() returned error: %v", err)
	}
	gv := schema.GroupVersion{Group: "warehouse.example.com", Version: "v1alpha1"}
	if !errors.Is(unregistered[gv], errGroupServedElsewhere) {
		t.Errorf("reconcile reported %v for the taken group version, want the conflict", unregistered[gv])
	}
}
