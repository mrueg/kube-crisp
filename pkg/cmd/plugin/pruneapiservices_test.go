package plugin

import (
	"bytes"
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
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
		"spec": map[string]any{"group": group, "version": version},
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

// The case this exists for: a projection was deleted from --projection-dir
// while the server was down, so nothing collected the registration and the
// aggregation layer is still dialling it.
func TestAStrandedRegistrationIsFound(t *testing.T) {
	crisp := crispfake.NewSimpleClientset()
	dyn := fakeDynamic(apiService("gone.example.com", "v1alpha1", true, boolPtr(false)))

	survey, err := surveyAPIServices(context.Background(), crisp, dyn)
	if err != nil {
		t.Fatalf("surveyAPIServices() returned error: %v", err)
	}
	if len(survey.stranded) != 1 || survey.stranded[0].name != "v1alpha1.gone.example.com" {
		t.Fatalf("stranded = %+v, want the one registration", survey.stranded)
	}
	if survey.stranded[0].groupVersion != "gone.example.com/v1alpha1" {
		t.Errorf("groupVersion = %q, want it read from the spec", survey.stranded[0].groupVersion)
	}
}

// A registration a projection still claims is in use, whatever the aggregation
// layer currently says about it — a database outage makes a live projection
// unavailable and is not a reason to unregister it.
func TestARegistrationAProjectionClaimsIsNeverACandidate(t *testing.T) {
	crisp := crispfake.NewSimpleClientset(projectionFor("bins", "live.example.com", "v1alpha1"))
	dyn := fakeDynamic(apiService("live.example.com", "v1alpha1", true, boolPtr(false)))

	survey, err := surveyAPIServices(context.Background(), crisp, dyn)
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

	survey, err := surveyAPIServices(context.Background(), crisp, dyn)
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
	if err := o.pruneAPIServices(context.Background(), crisp, dyn, &out, &errOut); err != nil {
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

	survey, err := surveyAPIServices(context.Background(), crisp, dyn)
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

	survey, err := surveyAPIServices(context.Background(), crisp, dyn)
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
	if err := o.pruneAPIServices(context.Background(), crisp, dyn, &out, &errOut); err != nil {
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
	if err := o.pruneAPIServices(context.Background(), crisp, dyn, &out, &errOut); err != nil {
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
	if err := o.pruneAPIServices(context.Background(), crisp, dyn, &out, &errOut); err != nil {
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

	survey, err := surveyAPIServices(context.Background(), crispfake.NewSimpleClientset(), fakeDynamic(object))
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
		crispfake.NewSimpleClientset(), fakeDynamic(), &out, &errOut)
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
