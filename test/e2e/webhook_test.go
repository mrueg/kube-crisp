//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var webhookGVR = schema.GroupVersionResource{
	Group:    "admissionregistration.k8s.io",
	Version:  "v1",
	Resource: "validatingwebhookconfigurations",
}

// webhookRefusal is what the webhook says about every projection its database
// would not prepare, whatever the database said. The detail is in the server
// log, because in the response it described the database to whoever asked.
const webhookRefusal = "could not be prepared against its data source"

// TestProjectionWebhookRefusesAnAnonymousCaller: the endpoint prepares the
// statements in a request against the database behind the Secret it names, so
// it answers only a caller it can name. The kube-apiserver here has credentials
// for it; anything else that reaches the Service does not, and gets no answer.
//
// Reached through the kube-apiserver's service proxy, which forwards the
// request without the caller's credentials — the same anonymous call any
// workload in the cluster could make directly.
func TestProjectionWebhookRefusesAnAnonymousCaller(t *testing.T) {
	ctx := context.Background()

	review := []byte(`{
	  "apiVersion": "admission.k8s.io/v1",
	  "kind": "AdmissionReview",
	  "request": {
	    "uid": "e2e-anonymous-probe",
	    "operation": "CREATE",
	    "name": "e2e-anonymous-probe",
	    "object": {
	      "apiVersion": "crisp.kubecrisp.io/v1alpha1",
	      "kind": "CustomResourceProjection",
	      "metadata": {"name": "e2e-anonymous-probe"},
	      "spec": {
	        "dataSource": {"driver": "postgres", "secretRef": {"name": "orders-db", "namespace": "kube-crisp"}},
	        "resource": {"group": "store.example.com", "version": "v1alpha1", "kind": "AnonymousProbe",
	                     "plural": "anonymousprobes", "scope": "Namespaced", "schema": {"type": "object"}},
	        "queries": {"list": {"sql": "SELECT id, tenant FROM no_such_table_for_the_probe WHERE tenant = :namespace"}},
	        "mapping": {"name": "id", "namespace": "tenant"}
	      }
	    }
	  }
	}`)

	var status int
	body, err := discoveryClient.RESTClient().Post().
		AbsPath("/api/v1/namespaces/kube-crisp/services/https:kube-crisp-apiserver:443/proxy/admission/customresourceprojections").
		SetHeader("Content-Type", "application/json").
		Body(review).
		Do(ctx).
		StatusCode(&status).
		Raw()
	if err == nil {
		t.Fatalf("an anonymous AdmissionReview was answered (status %d): %s", status, body)
	}
	if status != 401 && status != 403 {
		t.Errorf("status = %d, want 401 or 403: %v", status, err)
	}
	for _, leaked := range []string{`"allowed"`, "no_such_table_for_the_probe", "SQLSTATE"} {
		if strings.Contains(string(body), leaked) || strings.Contains(err.Error(), leaked) {
			t.Errorf("the refusal carries %q; an anonymous caller learned something about the database", leaked)
		}
	}
}

// TestProjectionWebhookRejectsSQLTheDatabaseCannotRun covers the whole point of
// the webhook: the mistake is reported where it was made.
//
// Without it a projection whose SQL has outlived its schema is accepted,
// reports Ready, appears in discovery, and fails every request with a 500 — the
// author gets no signal and the first person to find out is whoever called it.
//
// This is also the only test that can catch the webhook failing open. Its
// failure policy is Ignore, deliberately, so a webhook that cannot be reached
// lets everything through — which is what happened when the server's self-signed
// certificate named only localhost and not the Service. A test that checked
// valid projections were accepted would have passed throughout.
func TestProjectionWebhookRejectsSQLTheDatabaseCannotRun(t *testing.T) {
	ctx := context.Background()
	projections := dynamicClient.Resource(crpGVR)

	const name = "e2e-drifted-orders"
	t.Cleanup(func() {
		_ = projections.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	_, err := projections.Create(ctx, driftedProjection(name), metav1.CreateOptions{})
	if err == nil {
		t.Fatal("a projection selecting a column that does not exist was accepted; either the " +
			"webhook is not registered, or it could not be reached and failed open")
	}

	// Refused by the webhook, and refused without repeating the database. The
	// column name and the driver's error go to the server log: sent back, they
	// told whoever could reach the Service which tables and columns every
	// opted-in database has.
	message := err.Error()
	for _, want := range []string{"admission webhook", webhookRefusal} {
		if !strings.Contains(message, want) {
			t.Errorf("the rejection does not carry %q, so it is not the webhook's: %s", want, message)
		}
	}
	for _, leaked := range []string{"no_such_column", "SQLSTATE", "queries.list"} {
		if strings.Contains(message, leaked) {
			t.Errorf("the rejection repeats %q from the database's error: %s", leaked, message)
		}
	}

	// And nothing was written. A rejection that stored the object would leave a
	// projection in the cluster that the server then refuses to serve, which is
	// the state the webhook exists to prevent.
	if _, err := projections.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("get after a rejected create returned %v, want NotFound", err)
	}
}

// TestProjectionWebhookAcceptsAWorkingProjection, since a check that rejects
// everything would pass the test above.
func TestProjectionWebhookAcceptsAWorkingProjection(t *testing.T) {
	ctx := context.Background()
	projections := dynamicClient.Resource(crpGVR)

	const name = "e2e-webhook-accepted"
	t.Cleanup(func() {
		_ = projections.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	obj := driftedProjection(name)
	// The same projection with a column the table actually has.
	if err := unstructured.SetNestedField(obj.Object,
		"SELECT id, tenant FROM orders WHERE tenant = :namespace",
		"spec", "queries", "list", "sql"); err != nil {
		t.Fatalf("preparing the projection: %v", err)
	}
	if err := unstructured.SetNestedField(obj.Object, "AcceptedOrder", "spec", "resource", "kind"); err != nil {
		t.Fatalf("preparing the projection: %v", err)
	}
	if err := unstructured.SetNestedField(obj.Object, "acceptedorders", "spec", "resource", "plural"); err != nil {
		t.Fatalf("preparing the projection: %v", err)
	}

	if _, err := projections.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		t.Fatalf("a projection the database can serve was rejected: %v", err)
	}
}

// TestProjectionWebhookChecksUpdatesToo covers the half of the rule that a
// create-only test leaves unexercised.
//
// A projection is far more likely to break by being edited than by being
// created wrong: the schema moves underneath a working one, somebody adjusts a
// query. If the webhook only saw creates, the usual way to break a projection
// would be the way it does not catch.
func TestProjectionWebhookChecksUpdatesToo(t *testing.T) {
	ctx := context.Background()
	projections := dynamicClient.Resource(crpGVR)

	const name = "e2e-webhook-updated"
	t.Cleanup(func() {
		_ = projections.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	// Created working, so what follows tests the update and nothing else.
	obj := driftedProjection(name)
	if err := unstructured.SetNestedField(obj.Object,
		"SELECT id, tenant FROM orders WHERE tenant = :namespace",
		"spec", "queries", "list", "sql"); err != nil {
		t.Fatalf("preparing the projection: %v", err)
	}
	for path, value := range map[string]string{"kind": "UpdatedOrder", "plural": "updatedorders"} {
		if err := unstructured.SetNestedField(obj.Object, value, "spec", "resource", path); err != nil {
			t.Fatalf("preparing the projection: %v", err)
		}
	}

	created, err := projections.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a working projection: %v", err)
	}

	// Now break it the way a schema change would.
	broken := created.DeepCopy()
	if err := unstructured.SetNestedField(broken.Object,
		"SELECT id, tenant, no_such_column FROM orders WHERE tenant = :namespace",
		"spec", "queries", "list", "sql"); err != nil {
		t.Fatalf("preparing the update: %v", err)
	}

	if _, err := projections.Update(ctx, broken, metav1.UpdateOptions{}); err == nil {
		t.Fatal("an update to SQL the database cannot run was accepted; the webhook rule covers " +
			"UPDATE, so either it is not reaching this server or it failed open")
	} else if !strings.Contains(err.Error(), webhookRefusal) {
		t.Errorf("the refusal is not the webhook's: %v", err)
	}

	// And the stored projection is the one that works, not a half-applied edit.
	current, err := projections.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading after a refused update: %v", err)
	}
	sql, _, _ := unstructured.NestedString(current.Object, "spec", "queries", "list", "sql")
	if strings.Contains(sql, "no_such_column") {
		t.Error("the refused update was stored anyway, leaving a projection the server will " +
			"then refuse to serve")
	}
}

// TestProjectionWebhookIsRegisteredAndVerifiable covers the certificate, which
// is the part that failed silently.
//
// A ValidatingWebhookConfiguration has no insecureSkipTLSVerify — unlike an
// APIService, which is why this went unnoticed for as long as it did — so the
// caBundle has to verify a certificate that actually names the Service.
func TestProjectionWebhookIsRegisteredAndVerifiable(t *testing.T) {
	ctx := context.Background()

	configuration, err := dynamicClient.Resource(webhookGVR).
		Get(ctx, "kube-crisp-projections", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the webhook configuration the server registers for itself: %v", err)
	}

	webhooks, found, err := unstructured.NestedSlice(configuration.Object, "webhooks")
	if err != nil || !found || len(webhooks) == 0 {
		t.Fatalf("the configuration declares no webhooks: %v", err)
	}
	hook, ok := webhooks[0].(map[string]any)
	if !ok {
		t.Fatalf("webhook 0 is %T, want an object", webhooks[0])
	}

	if bundle, _, _ := unstructured.NestedString(hook, "clientConfig", "caBundle"); bundle == "" {
		t.Error("the webhook carries no caBundle, so the kube-apiserver has nothing to verify " +
			"this server's certificate against and every call fails open")
	}

	// Ignore rather than Fail, deliberately: this server serves the webhook, so
	// Fail would mean that while it is down nobody can create or fix a
	// projection.
	if policy, _, _ := unstructured.NestedString(hook, "failurePolicy"); policy != "Ignore" {
		t.Errorf("failurePolicy = %q, want Ignore — with Fail, a restart of this server blocks "+
			"every projection edit, including the one that would fix it", policy)
	}
}

// driftedProjection is a projection selecting a column the orders table does
// not have.
func driftedProjection(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "crisp.kubecrisp.io/v1alpha1",
		"kind":       "CustomResourceProjection",
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"dataSource": map[string]any{
				"driver":    "postgres",
				"secretRef": map[string]any{"name": "orders-db", "namespace": "kube-crisp"},
			},
			"resource": map[string]any{
				"group":   "drift.example.com",
				"version": "v1alpha1",
				"kind":    "DriftedOrder",
				"plural":  fmt.Sprintf("driftedorders%s", strings.TrimPrefix(name, "e2e-drifted-orders")),
				"scope":   "Namespaced",
				"schema":  map[string]any{"type": "object"},
			},
			"queries": map[string]any{
				"list": map[string]any{
					"sql": "SELECT id, tenant, no_such_column FROM orders WHERE tenant = :namespace",
				},
			},
			"watch":   map[string]any{"disabled": true},
			"mapping": map[string]any{"name": "id", "namespace": "tenant"},
		},
	}}
}
