package plugin

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

func statusProjection(name string, conditions ...metav1.Condition) *crispv1alpha1.CustomResourceProjection {
	p := &crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Generation = 1
	p.Spec.Resource.Group = "warehouse.example.com"
	p.Spec.Resource.Version = "v1alpha1"
	p.Spec.Resource.Plural = "bins"
	p.Status.ObservedGeneration = 1
	p.Status.Conditions = conditions
	p.Status.ServedPaths = []string{"/apis/warehouse.example.com/v1alpha1/bins"}
	return p
}

func ready(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type: crispv1alpha1.ConditionReady, Status: status, Reason: reason, Message: message,
	}
}

func available(gv string, value bool, reason, message string) map[string]registrationReport {
	return map[string]registrationReport{
		gv: {GroupVersion: gv, Available: &value, Reason: reason, Message: message},
	}
}

// A projection that is serving has nothing to report, which is what makes the
// default output worth reading: anything printed is something to look at.
func TestAServingProjectionReportsNothing(t *testing.T) {
	p := statusProjection("bins", ready(metav1.ConditionTrue, "Serving", ""))
	got := report(p, available("warehouse.example.com/v1alpha1", true, "", ""))

	if !got.Healthy || len(got.Findings) != 0 {
		t.Errorf("findings = %v, want none", got.Findings)
	}
}

// An empty status is a statement about the server, not about the projection.
//
// The conditions are written by kube-crisp and by nothing else, so a projection
// that has never been reconciled looks exactly like one nobody applied — and
// `kubectl get` shows a blank Ready column for both.
func TestAProjectionWithNoStatusSaysTheServerIsMissing(t *testing.T) {
	p := statusProjection("bins")
	p.Status.Conditions = nil
	p.Status.ObservedGeneration = 0
	p.Status.ServedPaths = nil

	got := report(p, nil)
	if len(got.Findings) != 1 {
		t.Fatalf("findings = %v, want exactly one", got.Findings)
	}
	if !strings.Contains(got.Findings[0], "no kube-crisp server has reconciled") {
		t.Errorf("finding = %q, want it to name the server rather than the projection", got.Findings[0])
	}
}

// Requests succeed and the spec answering them is not the spec that was
// applied. Ready is true, the rows come back, and only these two numbers
// disagree.
func TestGenerationDriftIsReportedWhileReady(t *testing.T) {
	p := statusProjection("bins", ready(metav1.ConditionTrue, "Serving", ""))
	p.Generation = 4
	p.Status.ObservedGeneration = 2

	got := report(p, available("warehouse.example.com/v1alpha1", true, "", ""))
	if got.Healthy {
		t.Error("a projection serving a superseded spec was reported as healthy")
	}
	if len(got.Findings) != 1 || !strings.Contains(got.Findings[0], "generation 2 while 4 is applied") {
		t.Fatalf("findings = %v, want the two generations named", got.Findings)
	}
}

// Compiled here, reachable from nowhere. The projection's own conditions can
// all be true while the aggregation layer routes nothing to it.
func TestAnUnavailableAPIServiceIsReported(t *testing.T) {
	p := statusProjection("bins", ready(metav1.ConditionTrue, "Serving", ""))
	got := report(p, available("warehouse.example.com/v1alpha1", false,
		"ServiceNotFound", "service/kube-crisp-apiserver in kube-crisp not found"))

	if got.Healthy {
		t.Error("a projection nothing routes to was reported as healthy")
	}
	joined := strings.Join(got.Findings, "\n")
	if !strings.Contains(joined, "not reachable through the aggregation layer") {
		t.Errorf("findings = %v, want the aggregation layer named", got.Findings)
	}
	// The aggregator's own message, kept verbatim: it is about the Service and
	// the CA bundle, which is where the next step is.
	if !strings.Contains(joined, "service/kube-crisp-apiserver in kube-crisp not found") {
		t.Errorf("findings = %v, want the aggregator's message carried through", got.Findings)
	}
}

// A missing APIService is a different answer from an APIService reporting
// unavailable, and the reader needs to be able to tell them apart.
func TestAMissingAPIServiceSaysNothingRoutesTheGroup(t *testing.T) {
	registrations := map[string]registrationReport{
		"warehouse.example.com/v1alpha1": {
			GroupVersion: "warehouse.example.com/v1alpha1",
			Available:    func() *bool { v := false; return &v }(),
			Reason:       "NotRegistered",
			Message:      "no APIService v1alpha1.warehouse.example.com exists, so nothing routes this group here",
		},
	}
	got := report(statusProjection("bins", ready(metav1.ConditionTrue, "Serving", "")), registrations)
	if len(got.Findings) != 1 || !strings.Contains(got.Findings[0], "no APIService") {
		t.Fatalf("findings = %v, want the missing APIService named", got.Findings)
	}
}

// A condition that is false carries the server's message and gains the sentence
// saying what to do about it.
func TestAFalseConditionIsExplained(t *testing.T) {
	p := statusProjection("bins",
		ready(metav1.ConditionFalse, "CompilationFailed", `relation "bin" does not exist`),
		metav1.Condition{
			Type:    crispv1alpha1.ConditionDataSourceConnected,
			Status:  metav1.ConditionFalse,
			Reason:  "Unreachable",
			Message: "dial tcp: connection refused",
		},
	)

	got := report(p, available("warehouse.example.com/v1alpha1", true, "", ""))
	joined := strings.Join(got.Findings, "\n")
	if !strings.Contains(joined, `relation "bin" does not exist`) {
		t.Errorf("findings = %v, want the database's own message", got.Findings)
	}
	if !strings.Contains(joined, "answering with 503") {
		t.Errorf("findings = %v, want the DataSourceConnected explanation", got.Findings)
	}
}

// A version declared and not served is the one a client is told does not
// exist, while its siblings answer.
func TestADeclaredVersionThatIsNotServedIsReported(t *testing.T) {
	p := statusProjection("bins", ready(metav1.ConditionTrue, "Serving", ""))
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{Name: "v1beta1"}}

	registrations := available("warehouse.example.com/v1alpha1", true, "", "")
	registrations["warehouse.example.com/v1beta1"] = registrationReport{
		GroupVersion: "warehouse.example.com/v1beta1",
		Available:    func() *bool { v := true; return &v }(),
	}

	got := report(p, registrations)
	joined := strings.Join(got.Findings, "\n")
	if !strings.Contains(joined, "v1beta1 is declared and not served") {
		t.Fatalf("findings = %v, want the unserved version named", got.Findings)
	}
}

// A version turned off is not a version that is missing.
func TestAVersionThatIsNotServedOnPurposeIsNotReported(t *testing.T) {
	p := statusProjection("bins", ready(metav1.ConditionTrue, "Serving", ""))
	off := false
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{{Name: "v1beta1", Served: &off}}

	got := report(p, available("warehouse.example.com/v1alpha1", true, "", ""))
	if len(got.Findings) != 0 {
		t.Errorf("findings = %v, want none for a version deliberately turned off", got.Findings)
	}
}

// Whoever is diagnosing a projection often cannot read cluster-scoped objects.
// A command that refuses to say anything because half of it was forbidden is
// one nobody can use.
func TestAnUnreadableAPIServiceIsSaidRatherThanSwallowed(t *testing.T) {
	registrations := map[string]registrationReport{
		"warehouse.example.com/v1alpha1": {
			GroupVersion: "warehouse.example.com/v1alpha1",
			Error:        `apiservices.apiregistration.k8s.io is forbidden`,
		},
	}
	got := report(statusProjection("bins", ready(metav1.ConditionTrue, "Serving", "")), registrations)
	if len(got.Findings) != 1 || !strings.Contains(got.Findings[0], "unchecked") {
		t.Fatalf("findings = %v, want the gap reported as unchecked", got.Findings)
	}
}

func TestAPIServiceAvailabilityReadsTheCondition(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"conditions": []any{
				map[string]any{
					"type": "Available", "status": "False",
					"reason": "FailedDiscoveryCheck", "message": "no response",
				},
			},
		},
	}}

	value, reason, message := apiServiceAvailability(object)
	if value == nil || *value {
		t.Fatalf("available = %v, want false", value)
	}
	if reason != "FailedDiscoveryCheck" || message != "no response" {
		t.Errorf("reason/message = %q/%q, want the aggregator's own", reason, message)
	}
}

// An APIService the aggregator has not judged yet is not an APIService that
// failed, and reporting it as unavailable would send somebody looking for a
// fault that does not exist.
func TestAnUnjudgedAPIServiceIsNotReportedAsUnavailable(t *testing.T) {
	object := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{}}}
	value, reason, _ := apiServiceAvailability(object)
	if value != nil {
		t.Errorf("available = %v, want unknown", *value)
	}
	if reason != "NotYetJudged" {
		t.Errorf("reason = %q, want NotYetJudged", reason)
	}
}

func TestGroupVersionsSkipsVersionsTurnedOff(t *testing.T) {
	p := statusProjection("bins")
	off := false
	on := true
	p.Spec.Resource.Versions = []crispv1alpha1.ProjectedVersion{
		{Name: "v1beta1", Served: &on},
		{Name: "v1", Served: &off},
	}

	got := groupVersions(p)
	want := []string{"warehouse.example.com/v1alpha1", "warehouse.example.com/v1beta1"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("groupVersions() = %v, want %v", got, want)
	}
}

// The default output prints only what needs looking at. A status command whose
// output must be read in full before it can be dismissed is one nobody runs
// twice.
func TestTextPrintsOnlyWhatNeedsAttention(t *testing.T) {
	reports := []projectionReport{
		{Name: "healthy", Resources: []string{"bins.warehouse.example.com/v1alpha1"}, Healthy: true},
		{
			Name:      "broken",
			Resources: []string{"crates.warehouse.example.com/v1alpha1"},
			Findings:  []string{"Ready is False (CompilationFailed): relation does not exist"},
		},
	}

	var out, errOut strings.Builder
	o := &statusOptions{output: "text"}
	if err := o.text(&out, &errOut, reports); err != nil {
		t.Fatalf("text() returned error: %v", err)
	}

	if strings.Contains(out.String(), "healthy") {
		t.Errorf("the serving projection was printed:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "broken") ||
		!strings.Contains(out.String(), "CompilationFailed") {
		t.Errorf("the broken projection was not reported:\n%s", out.String())
	}
	// The count goes to stderr, so piping the report somewhere does not carry
	// it into the pipe.
	if !strings.Contains(errOut.String(), "2 projection(s), 1 serving, 1 with something to report") {
		t.Errorf("summary = %q", errOut.String())
	}
	if strings.Contains(out.String(), "projection(s)") {
		t.Error("the summary was written to stdout")
	}
}

// --verbose is for the case where the question is "what does it think is fine",
// which the default output deliberately cannot answer.
func TestVerbosePrintsEveryProjectionAndCondition(t *testing.T) {
	reports := []projectionReport{{
		Name:       "healthy",
		Resources:  []string{"bins.warehouse.example.com/v1alpha1"},
		Conditions: []conditionReport{{Type: "Ready", Status: "True", Reason: "Serving"}},
		Healthy:    true,
	}}

	var out, errOut strings.Builder
	o := &statusOptions{output: "text", verbose: true}
	if err := o.text(&out, &errOut, reports); err != nil {
		t.Fatalf("text() returned error: %v", err)
	}
	for _, want := range []string{"healthy", "Ready", "Serving"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("verbose output missing %q:\n%s", want, out.String())
		}
	}
}
