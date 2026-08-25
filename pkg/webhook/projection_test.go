package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

type stubChecker struct {
	err  error
	seen string
}

func (s *stubChecker) Check(_ context.Context, p *crispv1alpha1.CustomResourceProjection) error {
	s.seen = p.Name
	return s.err
}

func review(t *testing.T, handler *Handler, request *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	t.Helper()

	body, err := json.Marshal(admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request:  request,
	})
	if err != nil {
		t.Fatalf("encoding the review: %v", err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, Path, strings.NewReader(string(body))))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}

	var out admissionv1.AdmissionReview
	if err := json.Unmarshal(recorder.Body.Bytes(), &out); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}
	if out.Response == nil {
		t.Fatal("the response carries no AdmissionResponse")
	}
	return out.Response
}

func projectionRequest(t *testing.T, name string, op admissionv1.Operation) *admissionv1.AdmissionRequest {
	t.Helper()

	p := crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("encoding the projection: %v", err)
	}
	return &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Operation: op,
		Object:    runtime.RawExtension{Raw: raw},
	}
}

// TestAllowsAProjectionTheServerCouldServe, since a webhook that refused
// everything would pass a test that only fed it broken input.
func TestAllowsAProjectionTheServerCouldServe(t *testing.T) {
	checker := &stubChecker{}
	response := review(t, &Handler{Checker: checker}, projectionRequest(t, "orders", admissionv1.Create))

	if !response.Allowed {
		t.Errorf("a projection the server could serve was refused: %v", response.Result)
	}
	if checker.seen != "orders" {
		t.Errorf("the checker was given %q, want the projection from the request", checker.seen)
	}
	if response.UID != "test-uid" {
		t.Errorf("UID = %q, want the request's — the kube-apiserver matches responses by it", response.UID)
	}
}

// TestRefusesAndSaysWhy: the reason this runs at admission rather than being
// left to the status condition is that the message reaches the person applying.
func TestRefusesAndSaysWhy(t *testing.T) {
	checker := &stubChecker{err: errors.New(`queries.list: column "no_such_column" does not exist`)}
	response := review(t, &Handler{Checker: checker}, projectionRequest(t, "orders", admissionv1.Create))

	if response.Allowed {
		t.Fatal("a projection the server could not serve was accepted")
	}
	if response.Result == nil || !strings.Contains(response.Result.Message, "no_such_column") {
		t.Errorf("the refusal does not carry the database's own error: %+v", response.Result)
	}
}

// TestDeleteIsAllowed. A delete carries no object to check, and refusing one
// would leave a broken projection impossible to remove.
func TestDeleteIsAllowed(t *testing.T) {
	checker := &stubChecker{err: errors.New("this projection is broken")}
	response := review(t, &Handler{Checker: checker}, &admissionv1.AdmissionRequest{
		UID: "test-uid", Operation: admissionv1.Delete,
	})

	if !response.Allowed {
		t.Error("a delete was refused, so a broken projection could not be removed")
	}
	if checker.seen != "" {
		t.Error("the checker ran on a delete, which carries no object to check")
	}
}

// TestRejectsMalformedInput, rather than panicking on the request path.
func TestRejectsMalformedInput(t *testing.T) {
	handler := &Handler{Checker: &stubChecker{}}

	for _, tc := range []struct {
		name string
		body string
		code int
	}{
		{"not JSON", "{{{", http.StatusBadRequest},
		{"no request", `{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview"}`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, Path, strings.NewReader(tc.body)))
			if recorder.Code != tc.code {
				t.Errorf("status = %d, want %d", recorder.Code, tc.code)
			}
		})
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, Path, nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET returned %d, want 405", recorder.Code)
	}
}
