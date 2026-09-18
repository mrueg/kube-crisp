// Package webhook serves the admission endpoint that checks a
// CustomResourceProjection before the cluster accepts it.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	"github.com/mrueg/kube-crisp/pkg/projection"
)

// Path is where the projection webhook is served.
const Path = "/admission/customresourceprojections"

// maxBody bounds what will be read from one admission request. An
// AdmissionReview holding a projection is a few kilobytes; the schema of a
// large one is tens.
const maxBody = 3 << 20

// notPreparable is what a projection whose statements the database would not
// prepare is refused with. Deliberately the same for every such projection.
//
// The database's own error is what an author wants to see, and it used to be
// here — but it is also a description of the database. "relation users does not
// exist" against "allowed" says which tables exist; "column password_hash does
// not exist" says which columns; and the error for a Secret that is missing, a
// Secret that is not labelled, and a Secret without the key are three different
// sentences. Whoever can reach this endpoint could read the catalogue of every
// opted-in database one statement at a time, with no Kubernetes identity. The
// detail goes to the server log, where reading it takes the permissions it
// should.
const notPreparable = "the projection's statements could not be prepared against its data source; " +
	"the reason is in the kube-crisp server log"

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
// refusal where the mistake was made.
type Handler struct {
	Checker Checker

	// Authorizer decides whether the caller may have a projection checked.
	// The caller is the kube-apiserver — which presents whatever credentials
	// the cluster's admission kubeconfig gives it for this webhook, and nothing
	// at all without one — and it is allowed when that identity may write
	// customresourceprojections. Anything that can reach the Service can POST
	// an AdmissionReview, and a check prepares the statements in it against a
	// real database, so who is asking has to be settled before it runs.
	Authorizer authorizer.UnconditionalAuthorizer

	// AllowAnonymous answers callers that presented no credentials, and skips
	// the authorization above for every caller. It is what the endpoint did
	// before it asked who was calling, kept for a cluster whose kube-apiserver
	// has no admission kubeconfig yet, and unsafe for the reason in the
	// Authorizer comment.
	AllowAnonymous bool

	// warnOnce carries the first refusal of an anonymous caller to the log at
	// a level the operator will see. A kube-apiserver without credentials for
	// this webhook is refused every time, and with a failure policy of Ignore
	// it then skips admission rather than reporting it, so the one place that
	// says why is here. Once, because every refusal after the first says the
	// same thing, and whoever is probing the endpoint should not be able to
	// fill the log by doing so.
	warnOnce sync.Once
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

	// Before the projection is looked at, and as an HTTP status rather than
	// an admission response: a refused caller has not had anything reviewed,
	// and to the kube-apiserver a 403 is a webhook it could not call, which
	// the failure policy handles the same way as one it could not reach.
	if status, err := h.authorize(r.Context(), review.Request); err != nil {
		crispmetrics.AdmissionReviews.WithLabelValues(crispmetrics.AdmissionRefused).Inc()
		http.Error(w, "this endpoint answers the kube-apiserver, authenticated as an identity allowed "+
			"to write customresourceprojections", status)
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

// authorize settles who is calling before anything is checked on their behalf.
// It returns the HTTP status to answer with when the caller is refused.
//
// The identity comes from the server's own authentication filter, which runs
// on this path as on every other: a client certificate from the cluster's
// client CA, a token the cluster's TokenReview vouches for, or the front-proxy
// headers. A request carrying none of those is authenticated as anonymous, and
// anonymous is refused — an AdmissionReview names a Secret and a statement, and
// this would prepare the statement against the database behind the Secret for
// whoever sent it.
//
// The authorization is then the one the operation in the review would need
// against the resource the webhook exists for, not against whatever resource
// the review claims: the body is the caller's, so the resource it names is
// not evidence of anything.
func (h *Handler) authorize(ctx context.Context, request *admissionv1.AdmissionRequest) (int, error) {
	if h.AllowAnonymous {
		return 0, nil
	}

	caller, ok := genericapirequest.UserFrom(ctx)
	if !ok || caller.GetName() == user.Anonymous || slices.Contains(caller.GetGroups(), user.AllUnauthenticated) {
		h.warnOnce.Do(func() {
			klog.Warning("refused an anonymous call to the projection admission webhook: the kube-apiserver " +
				"presents no credentials to a webhook unless its AdmissionConfiguration names a kubeconfig " +
				"for this one, and with a failure policy of Ignore it then skips the check rather than " +
				"reporting it. Give it credentials that may write customresourceprojections, or pass " +
				"--projection-webhook-allow-anonymous to serve the check to anything that can reach the " +
				"Service. Further refusals are logged at -v=2")
		})
		klog.V(2).InfoS("refusing an anonymous call to the projection admission webhook",
			"projection", request.Name, "operation", request.Operation)
		return http.StatusUnauthorized, fmt.Errorf("the caller is anonymous")
	}

	if h.Authorizer == nil {
		// No authority to consult, which is a server running with no cluster
		// behind it. Nothing there calls a webhook; nothing that does should
		// be answered without a decision.
		klog.V(2).InfoS("refusing a call to the projection admission webhook: no authorizer to consult",
			"user", caller.GetName())
		return http.StatusForbidden, fmt.Errorf("no authorizer to consult")
	}

	decision, reason, err := h.Authorizer.Authorize(ctx, authorizer.AttributesRecord{
		User:            caller,
		Verb:            verb(request.Operation),
		APIGroup:        crispv1alpha1.GroupName,
		APIVersion:      crispv1alpha1.SchemeGroupVersion.Version,
		Resource:        "customresourceprojections",
		Name:            request.Name,
		ResourceRequest: true,
	})
	if err != nil {
		klog.ErrorS(err, "could not authorize a call to the projection admission webhook",
			"user", caller.GetName(), "projection", request.Name)
		return http.StatusInternalServerError, err
	}
	if decision != authorizer.DecisionAllow {
		klog.V(2).InfoS("refusing a call to the projection admission webhook",
			"user", caller.GetName(), "projection", request.Name, "operation", request.Operation, "reason", reason)
		return http.StatusForbidden, fmt.Errorf("the caller may not %s customresourceprojections", verb(request.Operation))
	}
	return 0, nil
}

// verb is the RBAC verb an admission operation stands for.
func verb(op admissionv1.Operation) string {
	switch op {
	case admissionv1.Create:
		return "create"
	case admissionv1.Update:
		return "update"
	case admissionv1.Delete:
		return "delete"
	case admissionv1.Connect:
		return "connect"
	}
	return strings.ToLower(string(op))
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

	// What is wrong with the object itself is said in full. Validate reads
	// nothing but the projection — the same check the validate command runs
	// on a file — so its message describes the request and nothing else, and
	// it is the message the checker below would have started with.
	if err := projection.Validate(&p); err != nil {
		result = crispmetrics.AdmissionDenied
		return denied(request.UID, err.Error())
	}

	// Everything past that point consulted something the caller does not
	// necessarily get to see: the Secret, the CustomResourceDefinition a schema
	// is borrowed from, and the database. Logged at the level a failed compile
	// is, since that is what it is; not sent back.
	if err := h.Checker.Check(ctx, &p); err != nil {
		klog.ErrorS(err, "refusing a projection at admission",
			"projection", p.Name, "requestedBy", request.UserInfo.Username)
		result = crispmetrics.AdmissionDenied
		return denied(request.UID, notPreparable)
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
