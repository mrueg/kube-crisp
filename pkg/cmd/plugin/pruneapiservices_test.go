package plugin

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispfake "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned/fake"
)

// apiService builds a registration as the server writes it. available is nil
// for one the aggregation layer has not judged.
func apiService(group, version string, managed bool, available *bool) *unstructured.Unstructured {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata": map[string]any{
			"name": version + "." + group,
		},
		"spec": map[string]any{
			"group":   group,
			"version": version,
			"service": map[string]any{"namespace": "kube-crisp", "name": "kube-crisp-apiserver"},
		},
	}}
	if managed {
		object.SetLabels(map[string]string{managedByLabel: managedByKubeCris})
	}
	if available != nil {
		status := "False"
		message := "service/kube-crisp-apiserver in kube-crisp not found"
		if *available {
			status = "True"
			message = ""
		}
		_ = unstructured.SetNestedSlice(object.Object, []any{
			map[string]any{"type": "Available", "status": status, "message": message},
		}, "status", "conditions")
	}
	return object
}

func boolPtr(v bool) *bool { return &v }

func projectionFor(name, group, version string) *crispv1alpha1.CustomResourceProjection {
	p := &crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Spec.Resource.Group = group
	p.Spec.Resource.Version = version
	p.Spec.Resource.Plural = "bins"
	return p
}

func fakeDynamic(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{apiServiceGVR: "APIServiceList"},
		objects...,
	)
}

// apiServerService is the Service the registrations above route to, for the
// cases where the server behind them is installed and only stopped.
func apiServerService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-crisp", Name: "kube-crisp-apiserver"},
	}
}

// The case this exists for: the server was uninstalled with a group still
// served from --projection-dir, so nothing collected the registration and the
// aggregation layer is still dialling a Service that is gone.
func TestAStrandedRegistrationIsFound(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	dyn := fakeDynamic(apiService("gone.example.com", "v1alpha1", true, boolPtr(false)))

	survey, err := surveyAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 1 || survey.stranded[0].name != "v1alpha1.gone.example.com" {
		t.Fatalf("stranded = %+v, want the one registration", survey.stranded)
	}
	if survey.stranded[0].groupVersion != "gone.example.com/v1alpha1" {
		t.Errorf("groupVersion = %q, want it read from the spec", survey.stranded[0].groupVersion)
	}
	if survey.stranded[0].service != "kube-crisp/kube-crisp-apiserver" {
		t.Errorf("service = %q, want the missing Service named", survey.stranded[0].service)
	}
	// Stranded for this half, and still served for the other: the group is
	// counted for as long as the registration exists, since from here an
	// unavailable registration is indistinguishable from a restarting
	// server's, and removing it is what lets the roles for its group go.
	if !survey.served.Has("gone.example.com") {
		t.Errorf("served = %v, want the group counted while its registration exists", sets.List(survey.served))
	}
}

// The case that must not be found. A group served only from --projection-dir
// has no projection to claim it, so while its server restarts, crashloops or
// is upgraded its registration is unclaimed and unavailable — and removing it
// then would let the plain run take its roles and every binding on them. The
// Service it routes to still existing is what says the server is installed.
func TestAnUnavailableRegistrationWhoseServiceExistsIsLeftAlone(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	kube := kubefake.NewSimpleClientset(apiServerService())
	dyn := fakeDynamic(apiService("files.example.com", "v1alpha1", true, boolPtr(false)))

	survey, err := surveyAPIServices(context.Background(), crisp, kube, dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 0 {
		t.Fatalf("stranded = %+v, want none: the Service it routes to still exists", survey.stranded)
	}
	if len(survey.unavailable) != 1 || survey.unavailable[0].name != "v1alpha1.files.example.com" {
		t.Fatalf("unavailable = %+v, want the registration kept aside", survey.unavailable)
	}

	var out, errOut bytes.Buffer
	o := &pruneOptions{delete: true}
	if err := o.pruneAPIServices(context.Background(), crisp, kube, dyn, &out, &errOut); err != nil {
		t.Fatalf("pruneAPIServices() returned error: %v", err)
	}
	for _, action := range dyn.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("deleted the registration of a server that may only be restarting")
		}
	}
	if !strings.Contains(errOut.String(), "v1alpha1.files.example.com") ||
		!strings.Contains(errOut.String(), "still exists") ||
		!strings.Contains(errOut.String(), "--include-unavailable") {
		t.Errorf("stderr = %q, want the registration named, why it was kept, and how to take it", errOut.String())
	}
}

// --include-unavailable is the way to remove a registration whose server was
// stopped for good with its Service left in place.
func TestIncludeUnavailableTakesThemToo(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	kube := kubefake.NewSimpleClientset(apiServerService())
	dyn := fakeDynamic(
		apiService("files.example.com", "v1alpha1", true, boolPtr(false)),
		apiService("serving.example.com", "v1alpha1", true, boolPtr(true)),
	)

	var out, errOut bytes.Buffer
	o := &pruneOptions{delete: true, includeUnavailable: true}
	if err := o.pruneAPIServices(context.Background(), crisp, kube, dyn, &out, &errOut); err != nil {
		t.Fatalf("pruneAPIServices() returned error: %v", err)
	}
	var deleted []string
	for _, action := range dyn.Actions() {
		if action.GetVerb() == "delete" {
			deleted = append(deleted, action.(k8stesting.DeleteAction).GetName())
		}
	}
	if len(deleted) != 1 || deleted[0] != "v1alpha1.files.example.com" {
		t.Fatalf("deleted %v, want only the unavailable registration", deleted)
	}
}

// A Service that cannot be read says nothing about whether the server is gone,
// and the caller commonly may not read Services in the server's namespace.
// Treating that as gone would be guessing, with every binding on the group's
// roles as the stake.
func TestAServiceThatCannotBeReadIsNotEvidence(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(corev1.Resource("services"), "kube-crisp-apiserver",
			errors.New(`User "dev" cannot get resource "services"`))
	})
	dyn := fakeDynamic(apiService("files.example.com", "v1alpha1", true, boolPtr(false)))

	survey, err := surveyAPIServices(context.Background(), crisp, kube, dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 0 || len(survey.unavailable) != 1 {
		t.Fatalf("survey = %+v, want the registration kept aside rather than stranded", survey)
	}
	if !strings.Contains(survey.unavailable[0].note, "could not be read") {
		t.Errorf("note = %q, want the unreadable Service named as the reason", survey.unavailable[0].note)
	}
}

// A registration that routes to no Service leaves nothing to look for, so
// nothing says its server is gone.
func TestARegistrationWithNoServiceIsNotStranded(t *testing.T) {
	object := apiService("local.example.com", "v1alpha1", true, boolPtr(false))
	unstructured.RemoveNestedField(object.Object, "spec", "service")

	survey, err := surveyAPIServices(context.Background(),
		crispfake.NewSimpleClientset(), kubefake.NewSimpleClientset(), fakeDynamic(object))
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 0 || len(survey.unavailable) != 1 {
		t.Fatalf("survey = %+v, want the registration kept aside rather than stranded", survey)
	}
}

// A registration a projection still claims is in use, whatever the aggregation
// layer currently says about it — a database outage makes a live projection
// unavailable and is not a reason to unregister it.
func TestARegistrationAProjectionClaimsIsNeverACandidate(t *testing.T) {
	crisp := crispfake.NewSimpleClientset(projectionFor("bins", "live.example.com", "v1alpha1"))
	dyn := fakeDynamic(apiService("live.example.com", "v1alpha1", true, boolPtr(false)))

	survey, err := surveyAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 0 {
		t.Errorf("stranded = %+v, want none: a projection still declares that group", survey.stranded)
	}
}

// The rule that makes this safe for --projection-dir.
//
// A file-backed projection is invisible from the cluster, so "no projection
// claims it" is true of a registration that is being served right now. What
// settles it is the aggregation layer: an available registration is one a
// running server is answering, and it is never a candidate.
func TestAnAvailableRegistrationIsLeftAloneAndSaidSo(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	dyn := fakeDynamic(apiService("files.example.com", "v1alpha1", true, boolPtr(true)))

	survey, err := surveyAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 0 {
		t.Fatalf("stranded = %+v, want none: something is serving that group", survey.stranded)
	}
	if len(survey.serving) != 1 {
		t.Fatalf("serving = %v, want the registration reported as left alone", survey.serving)
	}

	// Reported, because a registration left alone silently is
	// indistinguishable from one the command failed to notice.
	var out, errOut bytes.Buffer
	o := &pruneOptions{}
	if err := o.pruneAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn, &out, &errOut); err != nil {
		t.Fatalf("pruneAPIServices() returned error: %v", err)
	}
	if !strings.Contains(errOut.String(), "--projection-dir") {
		t.Errorf("stderr = %q, want the file-backed case explained", errOut.String())
	}
}

// A registration the aggregation layer has not judged yet is not one that
// failed. Deleting it would unregister a group that was about to come up.
func TestAnUnjudgedRegistrationIsLeftAlone(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	dyn := fakeDynamic(apiService("new.example.com", "v1alpha1", true, nil))

	survey, err := surveyAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 0 {
		t.Errorf("stranded = %+v, want none for an unjudged registration", survey.stranded)
	}
	if len(survey.unjudged) != 1 {
		t.Errorf("unjudged = %v, want it reported", survey.unjudged)
	}
}

// Anything without the label was written by somebody else. The server applies
// the same rule before it touches one, and a hand-written APIService for an
// unrelated aggregated server must survive this command.
func TestAnUnlabelledRegistrationIsNeverConsidered(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	dyn := fakeDynamic(apiService("someone.example.com", "v1", false, boolPtr(false)))

	survey, err := surveyAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded)+len(survey.serving)+len(survey.unjudged) != 0 {
		t.Errorf("survey = %+v, want an unlabelled registration ignored entirely", survey)
	}
}

// Without --delete the command removes nothing, which for a command that can
// unregister an API group is the property worth pinning down.
func TestPrintingRemovesNothing(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	dyn := fakeDynamic(apiService("gone.example.com", "v1alpha1", true, boolPtr(false)))

	var out, errOut bytes.Buffer
	o := &pruneOptions{}
	if err := o.pruneAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn, &out, &errOut); err != nil {
		t.Fatalf("pruneAPIServices() returned error: %v", err)
	}

	for _, action := range dyn.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("something was deleted without --delete")
		}
	}
	if !strings.Contains(out.String(), "v1alpha1.gone.example.com") {
		t.Errorf("stdout = %q, want the registration named", out.String())
	}
	if !strings.Contains(errOut.String(), "Pass --delete") {
		t.Errorf("stderr = %q, want the hint", errOut.String())
	}
}

func TestDeleteRemovesOnlyTheStrandedOnes(t *testing.T) {
	crisp := crispfake.NewSimpleClientset(projectionFor("bins", "live.example.com", "v1alpha1"))
	dyn := fakeDynamic(
		apiService("gone.example.com", "v1alpha1", true, boolPtr(false)),
		apiService("live.example.com", "v1alpha1", true, boolPtr(false)),
		apiService("files.example.com", "v1alpha1", true, boolPtr(true)),
		apiService("someone.example.com", "v1", false, boolPtr(false)),
	)

	var out, errOut bytes.Buffer
	o := &pruneOptions{delete: true}
	if err := o.pruneAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn, &out, &errOut); err != nil {
		t.Fatalf("pruneAPIServices() returned error: %v", err)
	}

	var deleted []string
	for _, action := range dyn.Actions() {
		if action.GetVerb() != "delete" {
			continue
		}
		deleted = append(deleted, action.(k8stesting.DeleteAction).GetName())
	}
	if len(deleted) != 1 || deleted[0] != "v1alpha1.gone.example.com" {
		t.Fatalf("deleted %v, want only the stranded registration", deleted)
	}
	if !strings.Contains(out.String(), "apiservice.apiregistration.k8s.io/v1alpha1.gone.example.com deleted") {
		t.Errorf("stdout = %q, want the deletion reported the way kubectl reports one", out.String())
	}
}

// A registration removed between the survey and the delete is the outcome that
// was asked for, not an error to fail the run on.
func TestADisappearingRegistrationIsNotAFailure(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	dyn := fakeDynamic(apiService("gone.example.com", "v1alpha1", true, boolPtr(false)))
	dyn.PrependReactor("delete", "apiservices",
		func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrorsNewNotFound("v1alpha1.gone.example.com")
		})

	var out, errOut bytes.Buffer
	o := &pruneOptions{delete: true}
	if err := o.pruneAPIServices(context.Background(), crisp, kubefake.NewSimpleClientset(), dyn, &out, &errOut); err != nil {
		t.Fatalf("pruneAPIServices() returned error: %v", err)
	}
}

// An APIService whose spec names no group is not one this server wrote,
// whatever label it carries.
func TestARegistrationWithNoGroupInItsSpecIsIgnored(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata":   map[string]any{"name": "v1.mystery"},
		"spec":       map[string]any{},
	}}
	object.SetLabels(map[string]string{managedByLabel: managedByKubeCris})

	survey, err := surveyAPIServices(context.Background(), crispfake.NewSimpleClientset(), kubefake.NewSimpleClientset(), fakeDynamic(object))
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded)+len(survey.serving)+len(survey.unjudged) != 0 {
		t.Errorf("survey = %+v, want it ignored", survey)
	}
}

func TestNothingStrandedSaysSo(t *testing.T) {
	var out, errOut bytes.Buffer
	o := &pruneOptions{}
	err := o.pruneAPIServices(context.Background(),
		crispfake.NewSimpleClientset(), kubefake.NewSimpleClientset(), fakeDynamic(), &out, &errOut)
	if err != nil {
		t.Fatalf("pruneAPIServices() returned error: %v", err)
	}
	if !strings.Contains(errOut.String(), "no stranded APIServices") {
		t.Errorf("stderr = %q", errOut.String())
	}
	if out.Len() != 0 {
		t.Errorf("stdout = %q, want nothing", out.String())
	}
}

func TestGroupVersionIsReadFromTheSpecNotTheName(t *testing.T) {
	// A name that disagrees with the spec: the spec is what the aggregation
	// layer routes on, so it is what must be believed.
	object := apiService("real.example.com", "v1alpha1", true, boolPtr(false))
	object.SetName("misleading.name")

	got, ok := apiServiceGroupVersion(object)
	if !ok || got != "real.example.com/v1alpha1" {
		t.Errorf("apiServiceGroupVersion() = %q/%v, want real.example.com/v1alpha1", got, ok)
	}
}

// apierrorsNewNotFound keeps the reactor above readable.
func apierrorsNewNotFound(name string) error {
	return apierrors.NewNotFound(apiServiceGVR.GroupResource(), name)
}
