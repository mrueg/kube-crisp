package apiserver

import (
	"context"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func webhookOptions(caBundle string) ProjectionWebhookOptions {
	return ProjectionWebhookOptions{
		Enabled:          true,
		Manage:           true,
		Name:             "kube-crisp-projections",
		ServiceName:      "kube-crisp-apiserver",
		ServiceNamespace: "kube-crisp",
		ServicePort:      443,
		CABundle:         []byte(caBundle),
	}
}

func liveBundle(t *testing.T, client *fake.Clientset, name string) string {
	t.Helper()

	live, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().
		Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the webhook configuration: %v", err)
	}
	if len(live.Webhooks) != 1 {
		t.Fatalf("the configuration has %d webhooks, want 1", len(live.Webhooks))
	}
	return string(live.Webhooks[0].ClientConfig.CABundle)
}

// TestWebhookConfigurationIsCorrectedWhenItTrustsTheWrongCertificate is the
// case that makes reconciling it worth repeating rather than doing once.
//
// A configuration can name one CA, and a server given none signs its own — so
// during a rolling update the pod on its way out can write its certificate
// after its replacement wrote theirs, leaving the cluster told to trust a
// certificate nothing serves. The policy is Ignore, so nothing fails: admission
// is skipped, and a projection this server would have refused is accepted.
func TestWebhookConfigurationIsCorrectedWhenItTrustsTheWrongCertificate(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()

	// The pod that got there first.
	if err := reconcileWebhookConfiguration(ctx, client, webhookOptions("cert-from-the-old-pod")); err != nil {
		t.Fatalf("registering: %v", err)
	}
	if got := liveBundle(t, client, "kube-crisp-projections"); got != "cert-from-the-old-pod" {
		t.Fatalf("caBundle = %q after registering", got)
	}

	// Its replacement, serving a certificate of its own.
	if err := reconcileWebhookConfiguration(ctx, client, webhookOptions("cert-from-the-new-pod")); err != nil {
		t.Fatalf("correcting: %v", err)
	}
	if got := liveBundle(t, client, "kube-crisp-projections"); got != "cert-from-the-new-pod" {
		t.Errorf("caBundle = %q, want the certificate this server actually serves", got)
	}
}

// TestWebhookConfigurationIsLeftAloneWhenItAlreadyAgrees keeps the repeat from
// writing on every pass, which would be a write to a cluster-scoped object
// every interval for the life of the process.
func TestWebhookConfigurationIsLeftAloneWhenItAlreadyAgrees(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	opts := webhookOptions("a-stable-certificate")

	if err := reconcileWebhookConfiguration(ctx, client, opts); err != nil {
		t.Fatalf("registering: %v", err)
	}

	var writes int
	client.PrependReactor("update", "validatingwebhookconfigurations",
		func(action k8stesting.Action) (bool, runtime.Object, error) {
			writes++
			return false, nil, nil
		})

	for i := 0; i < 5; i++ {
		if err := reconcileWebhookConfiguration(ctx, client, opts); err != nil {
			t.Fatalf("reconciling: %v", err)
		}
	}
	if writes != 0 {
		t.Errorf("an unchanged configuration was written %d time(s)", writes)
	}
}

// TestWebhookConfigurationSomebodyElseOwnsIsLeftAlone: correcting it would be
// taking over an object this server was never given, and an operator may have
// registered the webhook by hand precisely to control it.
func TestWebhookConfigurationSomebodyElseOwnsIsLeftAlone(t *testing.T) {
	ctx := context.Background()
	theirs := &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-crisp-projections"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name:         "projections.crisp.kubecrisp.io",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{CABundle: []byte("theirs")},
		}},
	}
	client := fake.NewSimpleClientset(theirs)

	if err := reconcileWebhookConfiguration(ctx, client, webhookOptions("ours")); err != nil {
		t.Fatalf("reconciling: %v", err)
	}
	if got := liveBundle(t, client, "kube-crisp-projections"); got != "theirs" {
		t.Errorf("caBundle = %q; an unmanaged configuration was taken over", got)
	}
}

// TestWebhookConfigurationNeedsACABundle: a ValidatingWebhookConfiguration has
// no insecureSkipTLSVerify, so registering one with nothing to verify against
// would be registering a webhook the cluster can never call.
func TestWebhookConfigurationNeedsACABundle(t *testing.T) {
	opts := webhookOptions("")
	if err := reconcileWebhookConfiguration(context.Background(), fake.NewSimpleClientset(), opts); err == nil {
		t.Error("a webhook configuration was registered with no CA bundle")
	}
}
