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

	// The database's own words, so the author does not have to go and find out
	// which column.
	message := err.Error()
	for _, want := range []string{"queries.list", "no_such_column"} {
		if !strings.Contains(message, want) {
			t.Errorf("the rejection does not mention %q, so it does not say what to fix: %s",
				want, message)
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
	} else if !strings.Contains(err.Error(), "no_such_column") {
		t.Errorf("the refusal does not carry the database's own error: %v", err)
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
