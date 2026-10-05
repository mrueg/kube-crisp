package projection

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// certManagerProjection claims a group whose operator is not this server.
func certManagerProjection() *crispv1alpha1.CustomResourceProjection {
	p := projectionObject("certificates", "certificates")
	p.Spec.Resource.Group = "cert-manager.io"
	p.Spec.Resource.Version = "v1"
	return p
}

// A group the operator has since put outside --projection-group-suffixes is
// withdrawn, not merely reported.
//
// A projection that took cert-manager.io/v1 before the suffixes were set had
// its group version added to what the server wanted all the same, so prune
// left the APIService it had written in place, ensure refused to touch it,
// and the router went on installing the projection: the group stayed routed
// here and answered, while the projection reported Registered=False. That is
// the claim the flag exists to stop, kept for good.
func TestADisallowedGroupIsWithdrawnAndNotServed(t *testing.T) {
	f := newFixture(t, []runtime.Object{certManagerProjection()},
		registeredBefore("cert-manager.io", "v1"))
	f.controller.apiServices.options.AllowedGroupSuffixes = []string{"example.com"}
	f.syncUntil(t, func() bool { return len(f.controller.Degraded()) == 1 })

	if paths := f.router.ServedPaths(); len(paths) != 0 {
		t.Errorf("served paths = %v, want nothing: the group may not be registered", paths)
	}
	if _, found := f.apiService(t, "v1.cert-manager.io"); found {
		t.Error("the registration of a group outside the permitted suffixes was kept")
	}

	ready := readyCondition(t, f, "certificates")
	if ready.Status != metav1.ConditionFalse || ready.Reason != "GroupNotAllowed" {
		t.Errorf("Ready = %s (%s), want False (GroupNotAllowed)", ready.Status, ready.Reason)
	}
	if !strings.Contains(ready.Message, "cert-manager.io") || !strings.Contains(ready.Message, "example.com") {
		t.Errorf("Ready message = %q; it should name the group and the permitted suffixes", ready.Message)
	}
}

// The same when the projection was already serving: what it compiled to is not
// kept as a previous configuration, the way a compile failure keeps it, since
// serving it is exactly what is not allowed.
func TestADisallowedGroupStopsBeingServed(t *testing.T) {
	f := newFixture(t, []runtime.Object{certManagerProjection()})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })
	if _, found := f.apiService(t, "v1.cert-manager.io"); !found {
		t.Fatal("the group was not registered while every group was allowed")
	}

	f.controller.apiServices.options.AllowedGroupSuffixes = []string{"example.com"}
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}

	if paths := f.router.ServedPaths(); len(paths) != 0 {
		t.Errorf("served paths = %v, want nothing once the group is outside the suffixes", paths)
	}
	if _, found := f.apiService(t, "v1.cert-manager.io"); found {
		t.Error("the registration outlived the suffixes that disallow it")
	}
	if got := f.pools.Len(); got != 0 {
		t.Errorf("%d connection pools are still open, want 0", got)
	}
}
