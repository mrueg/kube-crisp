// Package webhook serves the admission endpoint that checks a
// CustomResourceProjection before the cluster accepts it.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// Path is where the projection webhook is served.
const Path = "/admission/customresourceprojections"

// maxBody bounds what will be read from one admission request. An
// AdmissionReview holding a projection is a few kilobytes; the schema of a
// large one is tens.
const maxBody = 3 << 20

// Checker answers whether a projection could be served. dynamic.Compiler is the
// implementation, which is what keeps this from being able to accept something
// the server would then refuse.
type Checker interface {
	Check(ctx context.Context, p *crispv1alpha1.CustomResourceProjection) error
}

// Handler validates CustomResourceProjection objects at admission.
//
// This exists because the status condition arrives too late to be useful. A
// projection whose SQL has outlived its schema compiles, reports
// CompilationFailed, and is not served — but kubectl apply has already
// succeeded, so the author has to know to go and look. Answering here puts the
// database's own error where the mistake was made.
type Handler struct {
	Checker Checker
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "expected POST", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		http.Error(w, fmt.Sprintf("reading the request: %v", err), http.StatusBadRequest)
		return
	}

	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		http.Error(w, fmt.Sprintf("decoding the AdmissionReview: %v", err), http.StatusBadRequest)
		return
	}
	if review.Request == nil {
		http.Error(w, "the AdmissionReview carries no request", http.StatusBadRequest)
		return
	}

	response := h.review(r.Context(), review.Request)

	// The reply echoes the request's own apiVersion and kind, so a cluster
	// speaking either admission version gets an answer it can read.
	out := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: review.APIVersion,
			Kind:       review.Kind,
		},
		Response: response,
	}
	if out.APIVersion == "" {
		out.APIVersion = admissionv1.SchemeGroupVersion.String()
		out.Kind = "AdmissionReview"
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		klog.ErrorS(err, "writing the admission response")
	}
}

// review answers one admission request.
//
// Measured, because this path can fail without failing: the webhook's policy is
// Ignore, so a configuration the kube-apiserver cannot call means admission is
// skipped rather than erroring. A count that goes flat at zero is what that
// looks like from here, and nothing else shows it.
func (h *Handler) review(ctx context.Context, request *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	started := time.Now()
	result := crispmetrics.AdmissionAllowed
	defer func() {
		crispmetrics.AdmissionReviews.WithLabelValues(result).Inc()
		crispmetrics.AdmissionDuration.WithLabelValues(result).Observe(time.Since(started).Seconds())
	}()

	allowed := func() *admissionv1.AdmissionResponse {
		return &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
	}

	// A delete carries no object to check.
	if request.Operation == admissionv1.Delete {
		return allowed()
	}

	var p crispv1alpha1.CustomResourceProjection
	if err := json.Unmarshal(request.Object.Raw, &p); err != nil {
		// Not a refusal of the projection: there was no projection to refuse.
		result = crispmetrics.AdmissionError
		return denied(request.UID, fmt.Sprintf("this is not a CustomResourceProjection: %v", err))
	}

	if err := h.Checker.Check(ctx, &p); err != nil {
		klog.V(2).InfoS("rejecting a projection at admission",
			"projection", p.Name, "err", err)
		result = crispmetrics.AdmissionDenied
		return denied(request.UID, err.Error())
	}

	return allowed()
}

func denied(uid types.UID, message string) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		UID:     uid,
		Allowed: false,
		Result: &metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusUnprocessableEntity,
			Reason:  metav1.StatusReasonInvalid,
			Message: message,
		},
	}
}
