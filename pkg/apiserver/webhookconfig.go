package apiserver

import (
	"context"
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/webhook"
)

// Labels marking the configuration this server owns. Anything without them was
// created by somebody else and is left alone, the same rule the APIService
// reconciler follows.
const (
	webhookManagedByLabel = "app.kubernetes.io/managed-by"
	webhookManagedByValue = "kube-crisp"
)

// reconcileWebhookConfiguration creates or corrects the
// ValidatingWebhookConfiguration that points the cluster at this server.
//
// Registered by the server rather than shipped as a manifest because of the CA
// bundle. A webhook has no insecureSkipTLSVerify — the escape hatch the
// APIService path uses for self-signed certificates does not exist here — so
// the configuration cannot be written before the certificate it has to trust
// exists. The server knows its own certificate; a manifest author does not.
func reconcileWebhookConfiguration(
	ctx context.Context,
	client kubernetes.Interface,
	opts ProjectionWebhookOptions,
) error {
	if len(opts.CABundle) == 0 {
		return fmt.Errorf("the projection webhook needs a CA bundle: a ValidatingWebhookConfiguration " +
			"has no insecureSkipTLSVerify, so there is no way to register one the kube-apiserver " +
			"would trust")
	}

	desired := desiredWebhookConfiguration(opts)
	configurations := client.AdmissionregistrationV1().ValidatingWebhookConfigurations()

	existing, err := configurations.Get(ctx, opts.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := configurations.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("creating the projection webhook configuration: %w", err)
		}
		klog.InfoS("registered the projection admission webhook", "name", opts.Name)
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading the projection webhook configuration: %w", err)
	}

	// Somebody else's. Correcting it would be taking over an object this server
	// was never given, and the operator may have registered the webhook by hand
	// precisely to control it.
	if existing.Labels[webhookManagedByLabel] != webhookManagedByValue {
		klog.InfoS("leaving a ValidatingWebhookConfiguration alone: it is not labelled as managed by kube-crisp",
			"name", opts.Name)
		return nil
	}

	if apiequality.Semantic.DeepEqual(existing.Webhooks, desired.Webhooks) {
		return nil
	}

	updated := existing.DeepCopy()
	updated.Webhooks = desired.Webhooks
	updated.Labels = desired.Labels
	if _, err := configurations.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("correcting the projection webhook configuration: %w", err)
	}
	klog.InfoS("corrected the projection admission webhook", "name", opts.Name)
	return nil
}

func desiredWebhookConfiguration(opts ProjectionWebhookOptions) *admissionregistrationv1.ValidatingWebhookConfiguration {
	// Ignore, not Fail. This server is what serves the webhook, so a Fail
	// policy would mean that while kube-crisp is down or rolling, nobody can
	// create or edit a projection — including the edit that would fix it. The
	// check is a convenience that moves an error earlier; the server refuses to
	// serve a broken projection either way, so nothing depends on the webhook
	// having run.
	failurePolicy := admissionregistrationv1.Ignore
	sideEffects := admissionregistrationv1.SideEffectClassNone
	scope := admissionregistrationv1.AllScopes
	path := webhook.Path
	port := opts.ServicePort
	timeout := int32(10)

	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:   opts.Name,
			Labels: map[string]string{webhookManagedByLabel: webhookManagedByValue},
		},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "projections.crisp.kubecrisp.io",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service: &admissionregistrationv1.ServiceReference{
					Name:      opts.ServiceName,
					Namespace: opts.ServiceNamespace,
					Path:      &path,
					Port:      &port,
				},
				CABundle: opts.CABundle,
			},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{
					admissionregistrationv1.Create,
					admissionregistrationv1.Update,
				},
				Rule: admissionregistrationv1.Rule{
					APIGroups:   []string{crispv1alpha1.GroupName},
					APIVersions: []string{crispv1alpha1.SchemeGroupVersion.Version},
					Resources:   []string{"customresourceprojections"},
					Scope:       &scope,
				},
			}},
			FailurePolicy:           &failurePolicy,
			SideEffects:             &sideEffects,
			AdmissionReviewVersions: []string{"v1"},
			// Checking a projection connects to its database, which is slower
			// than an admission check has any business being if the database is
			// unhealthy. The failure policy above makes a timeout harmless.
			TimeoutSeconds: &timeout,
		}},
	}
}

// servingCertificate returns this server's own certificate, for use as the CA
// bundle the kube-apiserver verifies the webhook against.
//
// Correct for the self-signed certificate the server generates by default,
// since a self-signed certificate is its own issuer. With a certificate from a
// real CA this is the leaf rather than the issuer, which still verifies — a
// bundle is a set of trusted certificates, not necessarily roots — but pins the
// webhook to that one certificate, so it stops working when the certificate is
// rotated. Supply --projection-webhook-ca-bundle-file in that case, and this is
// never reached.
func servingCertificate(serving *genericapiserver.SecureServingInfo) ([]byte, error) {
	if serving == nil || serving.Cert == nil {
		return nil, fmt.Errorf("the projection webhook needs a serving certificate to point the " +
			"cluster at, and this server has none")
	}

	cert, _ := serving.Cert.CurrentCertKeyContent()
	if len(cert) == 0 {
		return nil, fmt.Errorf("this server's serving certificate is empty, so there is nothing " +
			"for the kube-apiserver to verify the webhook against")
	}
	return cert, nil
}
