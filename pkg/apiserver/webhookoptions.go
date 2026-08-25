package apiserver

// ProjectionWebhookOptions configures the admission webhook that checks a
// CustomResourceProjection before the cluster accepts it.
type ProjectionWebhookOptions struct {
	// Enabled serves the webhook endpoint and, unless Manage is false,
	// registers the ValidatingWebhookConfiguration that points at it.
	Enabled bool

	// Manage creates and updates the ValidatingWebhookConfiguration, the same
	// way APIServices are managed. Only a configuration labelled as managed by
	// kube-crisp is ever modified.
	Manage bool

	// Name of the ValidatingWebhookConfiguration.
	Name string

	// ServiceName and ServiceNamespace locate the Service that fronts this
	// server, which is what the kube-apiserver calls.
	ServiceName      string
	ServiceNamespace string
	ServicePort      int32

	// CABundle verifies this server's serving certificate.
	//
	// A webhook has no insecureSkipTLSVerify — unlike an APIService, which is
	// why this cannot simply be left empty the way the APIService path allows.
	// When nothing is supplied the server's own serving certificate is used,
	// which is correct for the self-signed certificate it generates by default:
	// a self-signed certificate is its own issuer.
	CABundle []byte
}
