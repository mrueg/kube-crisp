package projection

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// kubeCrispAPIService is an APIService as a kube-crisp installation writes
// one: labelled as kube-crisp's, and routed to that installation's Service.
func kubeCrispAPIService(group, version, serviceNamespace string, port int64) *unstructured.Unstructured {
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
				"name": "kube-crisp-apiserver", "namespace": serviceNamespace, "port": port,
			},
		},
	}}
}

// Another kube-crisp installation's registrations are not this one's to prune.
//
// Both carry the same label, and the label was all that decided ownership, so
// each installation pruned every group the other served -- and the other put
// it back on its next sync, for as long as both ran.
func TestAnotherInstallationsRegistrationIsNotPruned(t *testing.T) {
	theirs := kubeCrispAPIService("other.example.com", "v1", "other-install", 443)

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")}, theirs)
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if _, found := f.apiService(t, "v1.other.example.com"); !found {
		t.Error("another installation's registration was pruned")
	}
}

// Nor is one for a group version both want this one's to take over.
//
// ensure rewrote spec.service on anything labelled as kube-crisp's, so two
// installations serving one group version took turns pointing it at
// themselves. The one that got there first keeps it, and the other reports the
// group as served elsewhere, the way it would for any other server's
// registration.
func TestAnotherInstallationsRegistrationIsNotTakenOver(t *testing.T) {
	theirs := kubeCrispAPIService("warehouse.example.com", "v1alpha1", "other-install", 443)

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")}, theirs)
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	apiService, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("another installation's registration was removed")
	}
	if namespace, _, _ := unstructured.NestedString(apiService.Object, "spec", "service", "namespace"); namespace != "other-install" {
		t.Errorf("the registration was pointed at namespace %q, want it left on other-install", namespace)
	}

	registered := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered)
	if registered.Status != metav1.ConditionFalse || registered.Reason != "GroupAlreadyServed" {
		t.Errorf("Registered = %v (%s), want False (GroupAlreadyServed)", registered.Status, registered.Reason)
	}
	for _, want := range []string{"another kube-crisp installation", "Service other-install/kube-crisp-apiserver"} {
		if !strings.Contains(registered.Message, want) {
			t.Errorf("Registered message does not mention %s: %s", want, registered.Message)
		}
	}
}

// A registration this installation wrote before still is one: the label and
// the Service both say so, and it is corrected rather than reported.
func TestThisInstallationsRegistrationIsStillCorrected(t *testing.T) {
	ours := kubeCrispAPIService("warehouse.example.com", "v1alpha1", "kube-crisp", 8443)

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")}, ours)
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	apiService, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("this installation's own registration was removed")
	}
	if port, _, _ := unstructured.NestedInt64(apiService.Object, "spec", "service", "port"); port != 443 {
		t.Errorf("port = %d, want it corrected to 443", port)
	}
	if registered := conditionOf(t, f, "bins", crispv1alpha1.ConditionRegistered); registered.Reason == "GroupAlreadyServed" {
		t.Errorf("this installation's own registration was reported as served elsewhere: %s", registered.Message)
	}
}
