package projection

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

func sessionSpec() crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.DataSource.Driver = "postgres"
	spec.DataSource.SessionVariables = []crispv1alpha1.SessionVariable{
		{Name: "app.tenant", From: crispv1alpha1.ParameterSourceRequestNamespace},
		{Name: "app.user", From: crispv1alpha1.ParameterSourceRequestUser},
		{Name: "app.mode", From: crispv1alpha1.ParameterSourceValue, Value: "readonly"},
	}
	spec.Watch = &crispv1alpha1.WatchSpec{Disabled: true}
	return spec
}

// userContext is a request from someone, in a namespace.
func userContext(namespace, name string) context.Context {
	ctx := namespacedContext(namespace)
	return genericapirequest.WithUser(ctx, &user.DefaultInfo{Name: name})
}

func TestSessionVariablesResolveFromTheRequest(t *testing.T) {
	store := &REST{sessionVariables: sessionSpec().DataSource.SessionVariables}

	session := store.session(userContext("acme", "ada"), "acme", "order-1")
	want := []crispsql.SessionVariable{
		{Name: "app.tenant", Value: "acme"},
		{Name: "app.user", Value: "ada"},
		{Name: "app.mode", Value: "readonly"},
	}
	if len(session) != len(want) {
		t.Fatalf("resolved %d variables, want %d", len(session), len(want))
	}
	for i := range want {
		if session[i] != want[i] {
			t.Errorf("variable %d = %+v, want %+v", i, session[i], want[i])
		}
	}
}

// TestSessionIsPartOfTheQueryKey is the isolation property: two requests that
// differ only in who made them must never share a query or a cache entry.
func TestSessionIsPartOfTheQueryKey(t *testing.T) {
	store := &REST{sessionVariables: sessionSpec().DataSource.SessionVariables}

	ada := sessionKey(store.session(userContext("acme", "ada"), "acme", ""))
	grace := sessionKey(store.session(userContext("acme", "grace"), "acme", ""))
	other := sessionKey(store.session(userContext("globex", "ada"), "globex", ""))

	if ada == grace {
		t.Error("two users produce the same key; one could be answered from the other's query")
	}
	if ada == other {
		t.Error("two namespaces produce the same key")
	}
	if sessionKey(nil) != "" {
		t.Error("a projection with no session variables should add nothing to the key")
	}
}

func TestSessionVariablesRejectedForSQLite(t *testing.T) {
	spec := sessionSpec()
	spec.DataSource.Driver = "sqlite"

	_, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "no session variables") {
		t.Fatalf("New() error = %v, want a rejection for sqlite", err)
	}
}

func TestSessionVariableNameIsValidated(t *testing.T) {
	for _, name := range []string{"app.tenant'; DROP TABLE orders; --", "app tenant", "", "1bad", "app..tenant"} {
		if err := crispsql.ValidateSessionVariableName(name); err == nil {
			t.Errorf("name %q was accepted", name)
		}
	}
	for _, name := range []string{"app.tenant", "tenant", "app.a_b.c1"} {
		if err := crispsql.ValidateSessionVariableName(name); err != nil {
			t.Errorf("name %q was rejected: %v", name, err)
		}
	}
}

func TestSessionVariableRejectedInAProjection(t *testing.T) {
	spec := sessionSpec()
	spec.DataSource.SessionVariables[0].Name = "app.tenant; DROP TABLE orders"

	if _, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil); err == nil {
		t.Fatal("a session variable name carrying SQL was accepted")
	}
}

func TestSessionVariableSourceIsChecked(t *testing.T) {
	spec := sessionSpec()
	spec.DataSource.SessionVariables[0].From = crispv1alpha1.ParameterSourceField

	_, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not a source") {
		t.Fatalf("New() error = %v, want a rejection of the Field source", err)
	}
}

// TestRequestDerivedSessionRejectsWatch covers the trap: a poll runs on a timer
// with no user and no namespace, so a policy keyed on either would show it
// nothing and the cache would read that as everything being deleted.
func TestRequestDerivedSessionRejectsWatch(t *testing.T) {
	spec := sessionSpec()
	spec.Watch = nil

	_, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "cannot be combined with watch") {
		t.Fatalf("New() error = %v, want a refusal to serve watch", err)
	}
}

// TestConstantSessionAllowsWatch keeps the safe case working: a setting that
// does not depend on the request is the same for a poll as for a request.
func TestConstantSessionAllowsWatch(t *testing.T) {
	spec := testSpec()
	spec.DataSource.Driver = "postgres"
	spec.DataSource.SessionVariables = []crispv1alpha1.SessionVariable{
		{Name: "app.mode", From: crispv1alpha1.ParameterSourceValue, Value: "readonly"},
	}

	if _, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil); err != nil {
		t.Fatalf("New() with a constant session and watch enabled returned error: %v", err)
	}
}

// TestSessionCacheKeysAreSeparate checks the read cache the same way: one
// user's rows must not be served to another from it.
func TestSessionCacheKeysAreSeparate(t *testing.T) {
	spec := sessionSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}
	// The fixture database is SQLite; only the key derivation is under test, so
	// the driver is switched back after the session declaration is accepted.
	store := &REST{
		sessionVariables: spec.DataSource.SessionVariables,
		cache:            newReadCache(time.Minute, "orders"),
	}

	adaKey := objectKey("acme", "order-1") + sessionKey(store.session(userContext("acme", "ada"), "acme", "order-1"))
	graceKey := objectKey("acme", "order-1") + sessionKey(store.session(userContext("acme", "grace"), "acme", "order-1"))
	if adaKey == graceKey {
		t.Fatal("two users share a cache key for the same object")
	}
}

var _ = metav1.GetOptions{}

// TestCallerArgsRenderTheWholeIdentity: a username answers "who is this" only
// as far as the authenticator's naming. A policy that has to survive a username
// being reassigned keys on the UID, and one that scopes rows the way RBAC
// scopes verbs keys on the groups.
func TestCallerArgsRenderTheWholeIdentity(t *testing.T) {
	ctx := genericapirequest.WithUser(context.Background(), &user.DefaultInfo{
		Name:   "ada",
		UID:    "uid-1",
		Groups: []string{"system:authenticated", "engineering"},
		Extra:  map[string][]string{"tenant": {"acme"}},
	})

	args := callerArgs(ctx)

	if got := args["user"]; got != "ada" {
		t.Errorf("user = %v, want ada", got)
	}
	if got := args["userUID"]; got != "uid-1" {
		t.Errorf("userUID = %v, want uid-1", got)
	}
	if got, want := args["userGroups"], `["system:authenticated","engineering"]`; got != want {
		t.Errorf("userGroups = %v, want %v", got, want)
	}
	if got, want := args["userExtra"], `{"tenant":["acme"]}`; got != want {
		t.Errorf("userExtra = %v, want %v", got, want)
	}
}

// TestCallerArgsWithoutAUser: an unauthenticated context binds NULL rather than
// the empty string, so a policy comparing against it does not match a row whose
// column is genuinely empty.
func TestCallerArgsWithoutAUser(t *testing.T) {
	for key, value := range callerArgs(context.Background()) {
		if value != nil {
			t.Errorf("%s = %v with no user in the context, want nil", key, value)
		}
	}
}

// TestSessionVariablesCarryTheWholeIdentity checks the same sources resolve for
// a session variable, which is what a row-level security policy reads.
func TestSessionVariablesCarryTheWholeIdentity(t *testing.T) {
	// Built directly rather than through a projection: SQLite has no session
	// state, and this is about how a request resolves into values, not about
	// what a driver does with them.
	store := &REST{sessionVariables: []crispv1alpha1.SessionVariable{
		{Name: "app.user", From: crispv1alpha1.ParameterSourceRequestUser},
		{Name: "app.uid", From: crispv1alpha1.ParameterSourceRequestUserUID},
		{Name: "app.groups", From: crispv1alpha1.ParameterSourceRequestUserGroups},
		{Name: "app.extra", From: crispv1alpha1.ParameterSourceRequestUserExtra},
	}}

	ctx := genericapirequest.WithUser(namespacedContext("acme"), &user.DefaultInfo{
		Name:   "grace",
		UID:    "uid-2",
		Groups: []string{"ops"},
		Extra:  map[string][]string{"scope": {"read"}},
	})

	got := map[string]string{}
	for _, variable := range store.session(ctx, "acme", "") {
		got[variable.Name] = variable.Value
	}

	for name, want := range map[string]string{
		"app.user":   "grace",
		"app.uid":    "uid-2",
		"app.groups": `["ops"]`,
		"app.extra":  `{"scope":["read"]}`,
	} {
		if got[name] != want {
			t.Errorf("%s = %q, want %q", name, got[name], want)
		}
	}
}

// The caller's identity is more than a username, and all of it is on the
// request by the time a connection is prepared.
//
// Authorization in Kubernetes is mostly by group, so a projection scoping rows
// the way RBAC scopes verbs wants the group list — which is what the PostgreSQL
// tutorial's row-level security policy is written against. That shape used to
// be accepted by the API server and then refused at compile, so the projection
// applied cleanly, reported CompilationFailed and never served.
func TestASessionVariableCanCarryTheWholeIdentity(t *testing.T) {
	for _, from := range []crispv1alpha1.ParameterSource{
		crispv1alpha1.ParameterSourceRequestUser,
		crispv1alpha1.ParameterSourceRequestUserUID,
		crispv1alpha1.ParameterSourceRequestUserGroups,
		crispv1alpha1.ParameterSourceRequestUserExtra,
	} {
		t.Run(string(from), func(t *testing.T) {
			spec := sessionSpec()
			spec.DataSource.SessionVariables = []crispv1alpha1.SessionVariable{
				{Name: "app.caller", From: from},
			}
			if _, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil); err != nil {
				t.Fatalf("New() refused a session variable from %s: %v", from, err)
			}
		})
	}
}

// Field and LabelSelector stay out, and for the reason the type's own rule
// gives: there is no object and no selector at the time the connection is
// prepared, so there is nothing for either to read.
func TestASessionVariableCannotComeFromTheObjectOrTheSelector(t *testing.T) {
	for _, from := range []crispv1alpha1.ParameterSource{
		crispv1alpha1.ParameterSourceField,
		crispv1alpha1.ParameterSourceLabelSelector,
	} {
		t.Run(string(from), func(t *testing.T) {
			spec := sessionSpec()
			spec.DataSource.SessionVariables = []crispv1alpha1.SessionVariable{
				{Name: "app.caller", From: from},
			}
			_, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil)
			if err == nil {
				t.Fatalf("New() accepted a session variable from %s", from)
			}
			if !strings.Contains(err.Error(), "not a source a session variable can use") {
				t.Errorf("the refusal does not say why: %v", err)
			}
		})
	}
}

// And the identity sources are still request-dependent, so a projection using
// one cannot also be watched: a poll runs on a timer with nobody behind it, and
// a policy keyed on the caller would show it nothing.
func TestAnIdentitySessionVariableStillCannotBeWatched(t *testing.T) {
	spec := sessionSpec()
	spec.DataSource.SessionVariables = []crispv1alpha1.SessionVariable{
		{Name: "app.groups", From: crispv1alpha1.ParameterSourceRequestUserGroups},
	}
	spec.Watch = nil

	_, err := New("orders", spec, newTestPoolFor(t, testSpec()), nil, nil)
	if err == nil {
		t.Fatal("a request-dependent session variable was combined with watch")
	}
	if !strings.Contains(err.Error(), "watch") {
		t.Errorf("the refusal does not mention watch: %v", err)
	}
}

// Two callers differing only in their groups must not share a cached page.
func TestGroupsReachTheQueryKey(t *testing.T) {
	store := &REST{sessionVariables: []crispv1alpha1.SessionVariable{
		{Name: "app.groups", From: crispv1alpha1.ParameterSourceRequestUserGroups},
	}}

	inTeamA := genericapirequest.WithUser(namespacedContext("acme"),
		&user.DefaultInfo{Name: "ada", Groups: []string{"team-a"}})
	inTeamB := genericapirequest.WithUser(namespacedContext("acme"),
		&user.DefaultInfo{Name: "ada", Groups: []string{"team-b"}})

	if sessionKey(store.session(inTeamA, "acme", "")) == sessionKey(store.session(inTeamB, "acme", "")) {
		t.Error("two group lists produce the same key; one caller could be answered from the other's query")
	}
}
