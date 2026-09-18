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
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	metricstestutil "k8s.io/component-base/metrics/testutil"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

type stubChecker struct {
	err  error
	seen string
}

func (s *stubChecker) Check(_ context.Context, p *crispv1alpha1.CustomResourceProjection) error {
	s.seen = p.Name
	return s.err
}

// stubAuthorizer allows the users it names to do anything and has no opinion
// on anyone else, which is what a delegating authorizer answers for an identity
// with no RBAC.
type stubAuthorizer struct {
	allowed []string
	seen    authorizer.Attributes
}

func (s *stubAuthorizer) Authorize(_ context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	s.seen = attrs
	for _, name := range s.allowed {
		if attrs.GetUser() != nil && attrs.GetUser().GetName() == name {
			return authorizer.DecisionAllow, "", nil
		}
	}
	return authorizer.DecisionNoOpinion, "no RBAC for this user", nil
}

// kubeAPIServer is the identity the cluster's admission kubeconfig gives the
// kube-apiserver for this webhook, and the one the tests allow.
const kubeAPIServer = "system:serviceaccount:kube-crisp:webhook-caller"

// as returns a request whose context carries what the server's authentication
// filter would have put there for that user; nil is a request that filter saw
// no credentials on.
func as(caller user.Info, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, Path, strings.NewReader(body))
	if caller == nil {
		caller = &user.DefaultInfo{Name: user.Anonymous, Groups: []string{user.AllUnauthenticated}}
	}
	return r.WithContext(genericapirequest.WithUser(r.Context(), caller))
}

// allowingHandler is one whose caller is the kube-apiserver with an identity
// allowed to write projections: the arrangement under which the check runs.
func allowingHandler(checker Checker) *Handler {
	return &Handler{Checker: checker, Authorizer: &stubAuthorizer{allowed: []string{kubeAPIServer}}}
}

func encode(t *testing.T, request *admissionv1.AdmissionRequest) string {
	t.Helper()

	body, err := json.Marshal(admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request:  request,
	})
	if err != nil {
		t.Fatalf("encoding the review: %v", err)
	}
	return string(body)
}

func review(t *testing.T, handler *Handler, request *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	t.Helper()

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, as(&user.DefaultInfo{Name: kubeAPIServer}, encode(t, request)))

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

// validProjection is one projection.Validate has no objection to, so that what
// a test sees is the checker's answer and not the structural check in front of
// it.
func validProjection(name string) *crispv1alpha1.CustomResourceProjection {
	p := &crispv1alpha1.CustomResourceProjection{}
	p.Name = name
	p.Spec.DataSource = crispv1alpha1.DataSource{Driver: "sqlite"}
	p.Spec.Resource = crispv1alpha1.ProjectedResource{
		Group:   "store.example.com",
		Version: "v1alpha1",
		Kind:    "Order",
		Plural:  "orders",
		Scope:   crispv1alpha1.NamespaceScoped,
		Schema:  &apiextensionsv1.JSONSchemaProps{Type: "object"},
	}
	p.Spec.Queries.List = crispv1alpha1.Query{SQL: "SELECT id, tenant FROM orders WHERE tenant = :namespace"}
	p.Spec.Mapping = crispv1alpha1.Mapping{Name: "id", Namespace: "tenant"}
	return p
}

func projectionRequest(t *testing.T, name string, op admissionv1.Operation) *admissionv1.AdmissionRequest {
	t.Helper()
	return requestFor(t, validProjection(name), op)
}

func requestFor(t *testing.T, p *crispv1alpha1.CustomResourceProjection, op admissionv1.Operation) *admissionv1.AdmissionRequest {
	t.Helper()

	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("encoding the projection: %v", err)
	}
	return &admissionv1.AdmissionRequest{
		UID:       "test-uid",
		Name:      p.Name,
		Operation: op,
		Object:    runtime.RawExtension{Raw: raw},
	}
}

// TestAllowsAProjectionTheServerCouldServe, since a webhook that refused
// everything would pass a test that only fed it broken input.
func TestAllowsAProjectionTheServerCouldServe(t *testing.T) {
	checker := &stubChecker{}
	response := review(t, allowingHandler(checker), projectionRequest(t, "orders", admissionv1.Create))

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

// TestRefusesWithoutRepeatingTheDatabase: a projection the database would not
// prepare is refused, and the refusal says so without saying what the database
// said.
//
// The database's error was in the response once, and it made the endpoint an
// oracle. "relation users does not exist" against "allowed" maps the tables of
// every opted-in database, the error text maps the columns, and the three
// messages for a Secret that is missing, unlabelled, or short of the key
// enumerate Secrets — for whoever could reach the Service, with no identity.
func TestRefusesWithoutRepeatingTheDatabase(t *testing.T) {
	for name, err := range map[string]error{
		"a column the database lacks":   errors.New(`queries.list: the database cannot run this statement: ERROR: column "password_hash" does not exist (SQLSTATE 42703)`),
		"a table the database lacks":    errors.New(`queries.list: the database cannot run this statement: ERROR: relation "users" does not exist (SQLSTATE 42P01)`),
		"a Secret that is missing":      errors.New(`connecting data source: resolving data source: reading secret kube-crisp/payments: secrets "payments" not found`),
		"a Secret that is not opted in": errors.New(`connecting data source: resolving data source: secret kube-crisp/payments is not marked as usable by kube-crisp`),
		"a Secret without the key":      errors.New(`connecting data source: resolving data source: secret kube-crisp/payments has no key "dsn"`),
	} {
		t.Run(name, func(t *testing.T) {
			checker := &stubChecker{err: err}
			response := review(t, allowingHandler(checker), projectionRequest(t, "orders", admissionv1.Create))

			if response.Allowed {
				t.Fatal("a projection the server could not serve was accepted")
			}
			if response.Result == nil {
				t.Fatal("the refusal carries no reason at all")
			}
			if response.Result.Message != notPreparable {
				t.Errorf("the refusal is %q, want the generic one", response.Result.Message)
			}
			for _, leaked := range []string{"password_hash", "users", "SQLSTATE", "payments", "secret", "not found", "dsn"} {
				if strings.Contains(response.Result.Message, leaked) {
					t.Errorf("the refusal repeats %q from the checker's error", leaked)
				}
			}
		})
	}
}

// TestRefusesAMalformedProjectionAndSaysWhat: what is wrong with the object
// itself is still said in full. Validate reads nothing but the projection, so
// its message describes the request and nothing behind it, and it is the same
// message the validate command gives for a file.
func TestRefusesAMalformedProjectionAndSaysWhat(t *testing.T) {
	checker := &stubChecker{err: errors.New("the checker should not have been consulted")}
	p := validProjection("orders")
	p.Spec.Resource.Group = ""

	response := review(t, allowingHandler(checker), requestFor(t, p, admissionv1.Create))

	if response.Allowed {
		t.Fatal("a projection without a group was accepted")
	}
	if response.Result == nil || !strings.Contains(response.Result.Message, "spec.resource.group is required") {
		t.Errorf("the refusal does not say what is wrong with the object: %+v", response.Result)
	}
	if checker.seen != "" {
		t.Error("the checker was consulted about a projection that does not validate, which reaches for its Secret")
	}
}

// TestRefusesAnAnonymousCaller. The kube-apiserver presents no credentials to a
// webhook unless its AdmissionConfiguration names a kubeconfig for it, and
// anything else that can reach the Service presents none either; answering the
// two alike is what made the endpoint an oracle.
func TestRefusesAnAnonymousCaller(t *testing.T) {
	crispmetrics.AdmissionReviews.Reset()
	t.Cleanup(crispmetrics.AdmissionReviews.Reset)

	checker := &stubChecker{}
	handler := allowingHandler(checker)
	body := encode(t, projectionRequest(t, "orders", admissionv1.Create))

	for name, r := range map[string]*http.Request{
		"no credentials":         as(nil, body),
		"no user in the context": httptest.NewRequest(http.MethodPost, Path, strings.NewReader(body)),
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, r)
			if recorder.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401: %s", recorder.Code, recorder.Body.String())
			}
			if checker.seen != "" {
				t.Error("the checker ran for an anonymous caller")
			}
		})
	}

	refused, err := metricstestutil.GetCounterMetricValue(
		crispmetrics.AdmissionReviews.WithLabelValues(crispmetrics.AdmissionRefused))
	if err != nil {
		t.Fatalf("reading admission_reviews_total{result=refused}: %v", err)
	}
	if refused != 2 {
		t.Errorf("refused reviews = %v, want 2: a kube-apiserver calling without credentials is otherwise invisible", refused)
	}

	// The flag keeps what the endpoint did before, explicitly.
	handler.AllowAnonymous = true
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, as(nil, body))
	if recorder.Code != http.StatusOK {
		t.Errorf("with AllowAnonymous, status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if checker.seen != "orders" {
		t.Error("with AllowAnonymous, the projection was not checked")
	}
}

// TestRefusesACallerWhoMayNotWriteProjections: authenticated is not enough. The
// question asked of the authorizer is the one the operation in the review would
// face at the kube-apiserver, against the resource this webhook is for — not
// against whatever resource the review claims to be about, since the review is
// the caller's to write.
func TestRefusesACallerWhoMayNotWriteProjections(t *testing.T) {
	checker := &stubChecker{}
	authz := &stubAuthorizer{allowed: []string{kubeAPIServer}}
	handler := &Handler{Checker: checker, Authorizer: authz}
	request := projectionRequest(t, "orders", admissionv1.Update)
	request.Resource = metav1.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, as(&user.DefaultInfo{Name: "system:serviceaccount:default:bystander"}, encode(t, request)))
	if recorder.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403: %s", recorder.Code, recorder.Body.String())
	}
	if checker.seen != "" {
		t.Error("the checker ran for a caller the authorizer had no opinion on")
	}

	if authz.seen == nil {
		t.Fatal("the authorizer was never asked")
	}
	if got, want := authz.seen.GetVerb(), "update"; got != want {
		t.Errorf("asked about verb %q, want %q — the operation in the review", got, want)
	}
	if got, want := authz.seen.GetResource(), "customresourceprojections"; got != want {
		t.Errorf("asked about resource %q, want %q, whatever the review claims", got, want)
	}
	if got, want := authz.seen.GetAPIGroup(), crispv1alpha1.GroupName; got != want {
		t.Errorf("asked about group %q, want %q", got, want)
	}
	if !authz.seen.IsResourceRequest() {
		t.Error("asked as a non-resource request; RBAC on the resource would not answer it")
	}

	// No authorizer at all is a server with no cluster to ask, and nothing
	// there should be answered on a guess.
	recorder = httptest.NewRecorder()
	(&Handler{Checker: checker}).ServeHTTP(recorder, as(&user.DefaultInfo{Name: kubeAPIServer}, encode(t, request)))
	if recorder.Code != http.StatusForbidden {
		t.Errorf("with no authorizer, status = %d, want 403", recorder.Code)
	}
}

// TestDeleteIsAllowed. A delete carries no object to check, and refusing one
// would leave a broken projection impossible to remove.
func TestDeleteIsAllowed(t *testing.T) {
	checker := &stubChecker{err: errors.New("this projection is broken")}
	response := review(t, allowingHandler(checker), &admissionv1.AdmissionRequest{
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
	handler := allowingHandler(&stubChecker{})

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

// TestAdmissionIsMeasured is why this path is instrumented at all.
//
// The webhook's failure policy is Ignore, so a configuration the kube-apiserver
// cannot call means admission is skipped rather than failing. Nothing errors,
// nothing logs, and a projection that would have been refused is accepted. A
// count that stays at zero is what that looks like from here, and it is the
// only thing that shows it.
func TestAdmissionIsMeasured(t *testing.T) {
	crispmetrics.AdmissionReviews.Reset()
	t.Cleanup(crispmetrics.AdmissionReviews.Reset)

	count := func(t *testing.T, result string) float64 {
		t.Helper()
		value, err := metricstestutil.GetCounterMetricValue(
			crispmetrics.AdmissionReviews.WithLabelValues(result))
		if err != nil {
			t.Fatalf("reading admission_reviews_total{result=%s}: %v", result, err)
		}
		return value
	}

	allowing := &Handler{Checker: checkerFunc(func(context.Context, *crispv1alpha1.CustomResourceProjection) error {
		return nil
	})}
	refusing := &Handler{Checker: checkerFunc(func(context.Context, *crispv1alpha1.CustomResourceProjection) error {
		return errors.New("queries.list: the database cannot run this statement")
	})}

	request := func(raw string) *admissionv1.AdmissionRequest {
		return &admissionv1.AdmissionRequest{
			UID:       "probe",
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: []byte(raw)},
		}
	}
	raw, err := json.Marshal(validProjection("bins"))
	if err != nil {
		t.Fatalf("encoding the projection: %v", err)
	}
	valid := string(raw)

	if response := allowing.review(context.Background(), request(valid)); !response.Allowed {
		t.Fatal("a projection the checker accepted was refused")
	}
	if got := count(t, crispmetrics.AdmissionAllowed); got != 1 {
		t.Errorf("allowed reviews = %v, want 1", got)
	}

	if response := refusing.review(context.Background(), request(valid)); response.Allowed {
		t.Fatal("a projection the checker rejected was allowed")
	}
	if got := count(t, crispmetrics.AdmissionDenied); got != 1 {
		t.Errorf("denied reviews = %v, want 1", got)
	}

	// A request that is not a projection at all is counted apart from one that
	// is and was refused: the first says nothing about any projection.
	if response := allowing.review(context.Background(), request(`{"this":`)); response.Allowed {
		t.Fatal("a malformed request was allowed")
	}
	if got := count(t, crispmetrics.AdmissionError); got != 1 {
		t.Errorf("errored reviews = %v, want 1", got)
	}
	if got := count(t, crispmetrics.AdmissionDenied); got != 1 {
		t.Errorf("a malformed request was counted as a denial; denied = %v, want 1", got)
	}
}

type checkerFunc func(context.Context, *crispv1alpha1.CustomResourceProjection) error

func (f checkerFunc) Check(ctx context.Context, p *crispv1alpha1.CustomResourceProjection) error {
	return f(ctx, p)
}
