// Package server assembles the command-line surface of kube-crisp-apiserver.
package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/spf13/cobra"
	generatedopenapi "k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/util/compatibility"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/apiserver"
	projectioncontroller "github.com/mrueg/kube-crisp/pkg/controller/projection"
	crispclient "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned"
	"github.com/mrueg/kube-crisp/pkg/projection"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
	"github.com/mrueg/kube-crisp/pkg/version"
	"github.com/mrueg/kube-crisp/pkg/webhook"
)

// defaultPort is the port the aggregated server listens on. The APIService
// registration in manifests/ must agree with the Service that fronts it.
const defaultPort = 6443

// CrispServerOptions holds everything needed to start the server.
type CrispServerOptions struct {
	RecommendedOptions *genericoptions.RecommendedOptions

	// ServerRun carries the serving limits the recommended set leaves out:
	// how many requests may be in flight, how long one may take, and — the one
	// that matters most for an aggregated server — how long to keep serving
	// after SIGTERM.
	//
	// Without it the server stops answering the instant it is told to shut
	// down, while the aggregation layer and the Service endpoints are still
	// routing to it, so every rolling update turns into a window of 503s on
	// each projected group.
	ServerRun *genericoptions.ServerRunOptions

	// ProjectionDir holds CustomResourceProjection manifests to serve.
	ProjectionDir string

	// ProjectionWebhook configures the admission webhook that checks a
	// projection before the cluster accepts it.
	ProjectionWebhook apiserver.ProjectionWebhookOptions

	// ProjectionWebhookCABundleFile holds the CA the kube-apiserver verifies
	// the webhook against.
	ProjectionWebhookCABundleFile string

	// LocalDSNFromEnv resolves data source credentials from environment
	// variables instead of Secrets, for running outside a cluster.
	LocalDSNFromEnv bool

	// WatchProjections enables watching CustomResourceProjection objects and
	// installing or removing API groups while the server runs.
	WatchProjections bool

	// APIServices controls whether the server registers the groups it serves
	// with the aggregation layer.
	APIServices projectioncontroller.APIServiceOptions

	// APIServiceCABundleFile holds the CA that verifies this server's serving
	// certificate, for the APIServices it creates.
	APIServiceCABundleFile string

	// RequireAllProjections gates readiness on every projection being served.
	RequireAllProjections bool

	// MaxOpenConnsPerDataSource is the ceiling a projection cannot raise, so
	// that many projections cannot together exhaust a database's connections.
	MaxOpenConnsPerDataSource int32

	// DataSourceNamespaces restricts where a projection's Secret may live.
	DataSourceNamespaces []string

	// RequireDataSourceOptIn demands the opt-in label on that Secret.
	RequireDataSourceOptIn bool

	// kubeClient is built when data source Secrets are read from the cluster,
	// and is reused to watch them for rotation.
	kubeClient kubernetes.Interface

	// LeaderElection decides which replica polls watched projections at the
	// configured interval; the others fall back to a slower one.
	LeaderElection apiserver.LeaderElectionOptions

	// EnableAdmission runs the admission chain for projected writes, so
	// ValidatingAdmissionPolicy, admission webhooks, and namespace lifecycle
	// apply to them as they do to any other resource.
	EnableAdmission bool

	StdOut io.Writer
	StdErr io.Writer
}

// NewCrispServerOptions returns options with sane defaults.
func NewCrispServerOptions(out, errOut io.Writer) *CrispServerOptions {
	o := &CrispServerOptions{
		// The empty resource config and nil etcd options below are the point
		// of this server: projected resources are never persisted, so there is
		// no storage backend to configure.
		RecommendedOptions: genericoptions.NewRecommendedOptions("", apiserver.Codecs.LegacyCodec()),
		ServerRun:          genericoptions.NewServerRunOptions(),
		StdOut:             out,
		StdErr:             errOut,
	}
	o.RecommendedOptions.Etcd = nil
	o.RecommendedOptions.SecureServing.BindPort = defaultPort

	// The webhook is off by default: it needs RBAC to register itself, and it
	// makes creating a projection depend on this server being up. Its defaults
	// are the conventional in-cluster deployment, so turning it on is one flag.
	o.ProjectionWebhook = apiserver.ProjectionWebhookOptions{
		Manage: true,
		Name:   "kube-crisp-projections",
	}

	// Allow running outside a cluster: without a kubeconfig the delegated
	// authentication and authorization clients are simply not built, which is
	// what makes `make run-local` work against a laptop database.
	o.RecommendedOptions.Authentication.RemoteKubeConfigFileOptional = true
	o.RecommendedOptions.Authorization.RemoteKubeConfigFileOptional = true

	// API priority-and-fairness is off unless asked for, through the generic
	// --enable-priority-and-fairness flag. Turning it on fair-queues requests
	// by FlowSchema, so one client cannot take a projection's whole capacity
	// and a slow database queues the requests that caused it rather than
	// everything. It watches FlowSchemas and PriorityLevelConfigurations,
	// which is a cluster-wide grant worth opting into rather than assuming:
	// manifests/optional/flowcontrol-rbac.yaml carries it.
	//
	// Without it the only backpressure is a projection's own concurrency
	// limit, which sheds whatever arrives when it is full rather than the
	// requests responsible for filling it.
	o.RecommendedOptions.Features.EnablePriorityAndFairness = false

	// Admission stays configured so its flags exist, but Config turns it off
	// unless --enable-admission is set: the plugins start cluster-wide
	// informers on webhook configurations, admission policies, and namespaces,
	// which is a grant worth opting into rather than assuming.

	// The aggregation layer fetches these documents itself, as the
	// kube-apiserver's own identity, which has no RBAC on this server. They
	// describe schemas rather than data, so serving them without an
	// authorization check is what upstream extension servers do too.
	// The webhook path joins the always-allowed set, because the kube-apiserver
	// calls a webhook without presenting credentials this server would
	// recognise: delegated authentication sees an anonymous request and
	// delegated authorization refuses it. Every webhook server in the ecosystem
	// has the same problem and the same answer.
	//
	// What is reachable without authorization is therefore an endpoint that
	// parses an AdmissionReview, resolves a Secret it is already allowed to
	// resolve — --datasource-namespaces and the opt-in label both still apply —
	// connects to that database, and answers allow or deny. It writes nothing.
	// The NetworkPolicy in manifests/optional is what limits who can reach it
	// at all.
	o.RecommendedOptions.Authorization.WithAlwaysAllowPaths(
		"/openapi/v2", "/openapi/v3", "/openapi/v3/*", webhook.Path)

	o.WatchProjections = true
	o.LeaderElection = apiserver.DefaultLeaderElectionOptions()
	o.APIServices = projectioncontroller.DefaultAPIServiceOptions()
	o.MaxOpenConnsPerDataSource = crispsql.DefaultMaxOpenConns
	o.RequireDataSourceOptIn = true

	// By default a projection may only use Secrets in the server's own
	// namespace, which is also the only namespace the shipped RBAC grants.
	if namespace := os.Getenv("POD_NAMESPACE"); namespace != "" {
		o.DataSourceNamespaces = []string{namespace}
	} else {
		o.DataSourceNamespaces = []string{"kube-crisp"}
	}

	return o
}

// NewCommandStartCrispServer builds the root cobra command.
func NewCommandStartCrispServer(ctx context.Context, defaults *CrispServerOptions) *cobra.Command {
	o := *defaults

	cmd := &cobra.Command{
		Short: "Serve SQL query results as Kubernetes custom resources",
		Long: "kube-crisp-apiserver is an aggregated Kubernetes API server that answers reads for\n" +
			"projected custom resources by executing SQL against a configured database. It never\n" +
			"writes to etcd: every request is served from the data source.",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			return o.RunCrispServer(c.Context())
		},
	}
	cmd.SetContext(ctx)

	flags := cmd.Flags()
	flags.StringVar(&o.ProjectionDir, "projection-dir", o.ProjectionDir,
		"Directory of CustomResourceProjection manifests to serve unconditionally, in addition to any found in the cluster. Re-read when it changes, so a projection here does not need a restart.")
	flags.BoolVar(&o.WatchProjections, "watch-projections", o.WatchProjections,
		"Watch CustomResourceProjection objects and install or remove projected API groups while running. Requires a kubeconfig.")
	flags.BoolVar(&o.APIServices.Enabled, "manage-apiservices", o.APIServices.Enabled,
		"Create and remove the APIService objects that delegate projected groups to this server. Only APIServices labelled as managed by kube-crisp are ever modified.")
	flags.StringVar(&o.APIServices.ServiceName, "apiservice-service-name", o.APIServices.ServiceName,
		"Name of the Service that fronts this server, used in the APIServices it creates.")
	flags.StringVar(&o.APIServices.ServiceNamespace, "apiservice-service-namespace", o.APIServices.ServiceNamespace,
		"Namespace of the Service that fronts this server. Defaults to $POD_NAMESPACE.")
	flags.Int32Var(&o.APIServices.Port, "apiservice-service-port", o.APIServices.Port,
		"Service port that fronts this server.")
	flags.BoolVar(&o.EnableAdmission, "enable-admission", o.EnableAdmission,
		"Run the admission chain for projected writes, so ValidatingAdmissionPolicy, admission webhooks, and namespace lifecycle apply to them. Requires the extra RBAC in manifests/optional/admission-rbac.yaml, since the plugins watch webhook configurations, admission policies, and namespaces.")
	flags.StringSliceVar(&o.DataSourceNamespaces, "datasource-namespaces", o.DataSourceNamespaces,
		"Namespaces a projection may take its data source Secret from. Defaults to $POD_NAMESPACE. Empty allows any namespace, which is only safe when this server's RBAC is already narrow.")
	flags.BoolVar(&o.RequireDataSourceOptIn, "require-datasource-optin", o.RequireDataSourceOptIn,
		"Require a data source Secret to carry "+projection.OptInLabel+"="+projection.OptInValue+". Since anyone who can create a CustomResourceProjection chooses both the Secret and the SQL, this keeps the decision to expose a database with whoever owns its credentials.")
	flags.BoolVar(&o.RequireAllProjections, "require-all-projections", o.RequireAllProjections,
		"Fail the readiness check when any projection cannot be served. By default a projection that fails to compile is reported at /healthz and by the kube_crisp_projections metric, while the server keeps serving the rest.")
	flags.Int32Var(&o.MaxOpenConnsPerDataSource, "max-open-conns-per-datasource", o.MaxOpenConnsPerDataSource,
		"Upper bound on connections to any one database, whatever a projection asks for. Every projection reaching the same connection string shares one pool, so this is the total this replica opens against that database.")
	flags.StringVar(&o.APIServiceCABundleFile, "apiservice-ca-bundle-file", o.APIServiceCABundleFile,
		"PEM file holding the CA that verifies this server's serving certificate. When empty, created APIServices set insecureSkipTLSVerify, which matches the self-signed certificates generated by default.")
	flags.BoolVar(&o.LeaderElection.Enabled, "enable-leader-election", o.LeaderElection.Enabled,
		"Elect one replica to poll watched projections at their configured interval; the others poll at watch.followerPollInterval instead. Polling is the only work a projection does with no request behind it, so it is the only load several replicas multiply for nothing. Needs the Lease permissions in manifests/20-rbac.yaml.")
	flags.StringVar(&o.LeaderElection.Namespace, "leader-election-namespace", o.LeaderElection.Namespace,
		"Namespace holding the Lease. Defaults to $POD_NAMESPACE.")
	flags.StringVar(&o.LeaderElection.Name, "leader-election-name", o.LeaderElection.Name,
		"Name of the Lease that decides which replica polls.")
	flags.BoolVar(&o.ProjectionWebhook.Enabled, "enable-projection-webhook", o.ProjectionWebhook.Enabled,
		"Serve an admission webhook that checks a CustomResourceProjection against its database before the cluster accepts it, so SQL the database cannot run is refused at kubectl apply rather than reported afterwards in the projection's status. Needs the RBAC in manifests/optional/webhook-rbac.yaml.")
	flags.BoolVar(&o.ProjectionWebhook.Manage, "manage-projection-webhook", o.ProjectionWebhook.Manage,
		"Create and correct the ValidatingWebhookConfiguration that points the cluster at this server, and keep correcting it: a generated certificate belongs to the pod that generated it, so a rolling update can otherwise leave the cluster trusting one nothing serves. Only a configuration labelled as managed by kube-crisp is ever modified. With more than one replica and no --projection-webhook-ca-bundle-file, every replica signs its own certificate and the configuration can name only one, so give them a shared certificate or run the webhook on a single replica.")
	flags.StringVar(&o.ProjectionWebhook.Name, "projection-webhook-name", o.ProjectionWebhook.Name,
		"Name of the ValidatingWebhookConfiguration.")
	flags.StringVar(&o.ProjectionWebhookCABundleFile, "projection-webhook-ca-bundle-file", o.ProjectionWebhookCABundleFile,
		"PEM file holding the CA that verifies this server's serving certificate, for the webhook configuration. A ValidatingWebhookConfiguration has no insecureSkipTLSVerify, so when this is empty the server's own serving certificate is used — correct for the self-signed certificate generated by default, and pinned to one certificate otherwise.")
	flags.BoolVar(&o.LocalDSNFromEnv, "local-dsn-from-env", o.LocalDSNFromEnv,
		"Resolve data source credentials from environment variables instead of Secrets. For local development only.")
	o.RecommendedOptions.AddFlags(flags)
	o.ServerRun.AddUniversalFlags(flags)
	logs.AddFlags(flags)

	return cmd
}

// Complete fills in defaults.
func (o *CrispServerOptions) Complete() error { return o.ServerRun.Complete() }

// Validate checks the options.
func (o *CrispServerOptions) Validate() error {
	var errs []error
	if o.ProjectionDir == "" && !o.WatchProjections {
		errs = append(errs, fmt.Errorf("either --projection-dir or --watch-projections is required, otherwise there is nothing to serve"))
	}
	errs = append(errs, o.RecommendedOptions.Validate()...)
	errs = append(errs, o.ServerRun.Validate()...)
	return utilerrors.NewAggregate(errs)
}

// servingDNSNames lists the names a client may reach this server at, for the
// subject alternative names of a generated certificate.
//
// Every form of the Service name, because which one is used depends on who is
// calling: the aggregation layer and an admission webhook both address the
// Service, and a cluster's resolver may or may not append the domain.
func (o *CrispServerOptions) servingDNSNames() []string {
	service, namespace := o.APIServices.ServiceName, o.APIServices.ServiceNamespace
	if service == "" || namespace == "" {
		return nil
	}
	return []string{
		service,
		service + "." + namespace,
		service + "." + namespace + ".svc",
		service + "." + namespace + ".svc.cluster.local",
	}
}

// Config builds the server configuration.
func (o *CrispServerOptions) Config() (*apiserver.Config, error) {
	// Self-signed certificates, generated for the names this server is actually
	// reached at.
	//
	// The Service names matter and used not to be here. The aggregation layer
	// tolerated their absence because an APIService can be created with
	// insecureSkipTLSVerify, so nothing verified the certificate and nothing
	// noticed it named only localhost. A ValidatingWebhookConfiguration has no
	// such escape hatch: the kube-apiserver verifies the name, and the webhook
	// failed with "certificate is valid for localhost, not
	// kube-crisp-apiserver.kube-crisp.svc" while failing open, which is a check
	// that silently is not running.
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts(
		"localhost", o.servingDNSNames(), []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("generating self-signed certificates: %w", err)
	}

	// Derived from the serving address, so --advertise-address only has to be
	// given when the server is reached at something other than where it binds.
	if err := o.ServerRun.DefaultAdvertiseAddress(o.RecommendedOptions.SecureServing.SecureServingOptions); err != nil {
		return nil, fmt.Errorf("defaulting the advertise address: %w", err)
	}

	// Admission is opt-in: without the flag the plugins never start, and the
	// informers they would need are never watched.
	if !o.EnableAdmission {
		o.RecommendedOptions.Admission = nil
	}

	// Serving with no cluster behind it is a supported way to run — a
	// --projection-dir and --local-dsn-from-env need no Kubernetes at all,
	// which is what makes trying a projection against a laptop database one
	// command. The recommended options do not allow for it on their own:
	// CoreAPIOptions.ApplyTo builds its client from --kubeconfig or, failing
	// that, from the in-cluster environment, and returns that error rather than
	// leaving the client unset. So every start outside a cluster stopped at
	// "unable to load in-cluster configuration" before any of the checks in
	// this file could say something more useful.
	//
	// Dropping the options when there is nothing for them to reach leaves
	// serverConfig.ClientConfig nil, which is the state dynamicClient,
	// metadataClient and dsnResolver already expect and already explain. An
	// explicit --kubeconfig is left alone: that is the operator asserting a
	// cluster, and failing to reach it should be loud.
	if o.standalone() {
		// The features that need a cluster say so here rather than failing
		// several layers down in a message about a nil shared informer.
		if o.EnableAdmission {
			return nil, fmt.Errorf("--enable-admission requires a kubeconfig: the plugins watch " +
				"webhook configurations, admission policies and namespaces, and there is no cluster here to watch")
		}
		if o.RecommendedOptions.Features.EnablePriorityAndFairness {
			return nil, fmt.Errorf("--enable-priority-and-fairness requires a kubeconfig: fair queueing " +
				"is driven by FlowSchemas and PriorityLevelConfigurations, and there is no cluster here to read them from")
		}

		o.RecommendedOptions.CoreAPI = nil

		// And with no cluster there is nothing to ask about a request either.
		// Delegated authorization answers a SubjectAccessReview; without a
		// client it holds no opinion on any resource request, and the fallback
		// is deny — so the server came up and refused every read, which is a
		// worse failure than not coming up at all because it looks like a
		// permissions problem the operator can fix. Nor can
		// --authorization-always-allow-paths cover it: the path authorizer
		// declines to decide resource requests by design.
		//
		// Nil means always allow. That is the honest description of a server
		// with no authority to consult, and it is why this is reached only when
		// there is no kubeconfig and no service account — never in a cluster,
		// and never when --kubeconfig was given.
		o.RecommendedOptions.Authorization = nil
		klog.Warning("no kubeconfig and no service account: serving without authentication or authorization, " +
			"because there is no cluster to delegate either to. Every request is allowed. " +
			"This is for running against a local database; do not expose the port.")
	}

	serverConfig := genericapiserver.NewRecommendedConfig(apiserver.Codecs)

	// Serving limits, request timeouts, and the graceful shutdown delay.
	if err := o.ServerRun.ApplyTo(&serverConfig.Config); err != nil {
		return nil, fmt.Errorf("applying server run options: %w", err)
	}

	// Completing the config dereferences EffectiveVersion, which only the
	// kube-apiserver sets for itself. ApplyTo above sets one from the component
	// registry, and this pins it regardless: without a value here an aggregated
	// server panics on startup rather than reporting a useful error.
	serverConfig.EffectiveVersion = compatibility.DefaultBuildEffectiveVersion()

	// Enabling the OpenAPI v3 endpoint is what gives the router somewhere to
	// publish projected schemas, which is in turn what makes kubectl explain
	// work. The definitions below cover the shared metadata types; the schemas
	// that matter are added per group version as projections are installed.
	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(
		generatedopenapi.GetOpenAPIDefinitions, openapinamer.NewDefinitionNamer(apiserver.Scheme))
	serverConfig.OpenAPIV3Config.Info.Title = "kube-crisp"
	serverConfig.OpenAPIV3Config.Info.Version = version.Version

	// The v2 endpoint carries no projected schemas, but the aggregation layer
	// downloads it regardless and logs an error for every attempt when it is
	// missing.
	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(
		generatedopenapi.GetOpenAPIDefinitions, openapinamer.NewDefinitionNamer(apiserver.Scheme))
	serverConfig.OpenAPIConfig.Info.Title = "kube-crisp"
	serverConfig.OpenAPIConfig.Info.Version = version.Version

	if err := o.RecommendedOptions.ApplyTo(serverConfig); err != nil {
		return nil, err
	}

	var projections []crispv1alpha1.CustomResourceProjection
	if o.ProjectionDir != "" {
		loaded, err := projection.LoadDir(o.ProjectionDir)
		if err != nil {
			return nil, err
		}
		projections = loaded
		if len(projections) == 0 && !o.WatchProjections {
			return nil, fmt.Errorf("no CustomResourceProjection manifests found in %s", o.ProjectionDir)
		}
	}

	resolver, err := o.dsnResolver(serverConfig)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := o.dynamicClient(serverConfig)
	if err != nil {
		return nil, err
	}

	metadataClient, err := o.metadataClient(serverConfig)
	if err != nil {
		return nil, err
	}

	var crispClient crispclient.Interface
	if dynamicClient != nil {
		if crispClient, err = crispclient.NewForConfig(serverConfig.ClientConfig); err != nil {
			return nil, fmt.Errorf("building the projection client: %w", err)
		}
	}

	apiServices := o.APIServices
	if o.APIServiceCABundleFile != "" {
		caBundle, err := os.ReadFile(o.APIServiceCABundleFile)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", o.APIServiceCABundleFile, err)
		}
		apiServices.CABundle = caBundle
	}

	// The webhook reaches this server through the same Service the aggregation
	// layer does, so it takes its location from the APIService options rather
	// than repeating them as four more flags.
	projectionWebhook := o.ProjectionWebhook
	projectionWebhook.ServiceName = apiServices.ServiceName
	projectionWebhook.ServiceNamespace = apiServices.ServiceNamespace
	projectionWebhook.ServicePort = apiServices.Port
	if o.ProjectionWebhookCABundleFile != "" {
		caBundle, err := os.ReadFile(o.ProjectionWebhookCABundleFile)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", o.ProjectionWebhookCABundleFile, err)
		}
		projectionWebhook.CABundle = caBundle
	}

	return &apiserver.Config{
		GenericConfig: serverConfig,
		ExtraConfig: apiserver.ExtraConfig{
			StaticProjections:         projections,
			ProjectionDir:             o.ProjectionDir,
			CrispClient:               crispClient,
			DynamicClient:             dynamicClient,
			MetadataClient:            metadataClient,
			DSNResolver:               resolver,
			Pools:                     crispsql.NewPoolCache(),
			APIServices:               apiServices,
			RequireAllProjections:     o.RequireAllProjections,
			MaxOpenConnsPerDataSource: int(o.MaxOpenConnsPerDataSource),
			KubeClient:                o.kubeClient,
			DataSourceNamespaces:      o.DataSourceNamespaces,
			LeaderElection:            o.LeaderElection,
			ProjectionWebhook:         projectionWebhook,
		},
	}, nil
}

// standalone reports whether this process has no Kubernetes to talk to: no
// --kubeconfig, and no service account mounted by a cluster.
//
// Only the absence of both counts. A kubeconfig that names an unreachable
// cluster is not standalone — it is a broken deployment, and the error belongs
// where it happens rather than silently downgraded to a server that serves
// files and cannot read a Secret.
func (o *CrispServerOptions) standalone() bool {
	if o.RecommendedOptions.CoreAPI == nil {
		return true
	}
	if o.RecommendedOptions.CoreAPI.CoreAPIKubeconfigPath != "" {
		return false
	}
	_, err := rest.InClusterConfig()
	return err != nil
}

// dynamicClient builds the client used to watch projections and report their
// status. It returns nil when watching is disabled.
func (o *CrispServerOptions) dynamicClient(serverConfig *genericapiserver.RecommendedConfig) (dynamic.Interface, error) {
	if !o.WatchProjections {
		return nil, nil
	}
	if serverConfig.ClientConfig == nil {
		return nil, fmt.Errorf("--watch-projections requires a kubeconfig; pass --watch-projections=false to serve only --projection-dir")
	}
	return dynamic.NewForConfig(serverConfig.ClientConfig)
}

// metadataClient builds the client that watches CustomResourceDefinitions for
// changes to a borrowed schema.
//
// Metadata only, and deliberately: the schema itself is read through the
// dynamic client when a projection is prepared, so this watch carries object
// metadata rather than a copy of every CRD schema in the cluster — which on a
// cluster running a few large operators is hundreds of megabytes held to notice
// an edit.
//
// It follows --watch-projections, since a server serving only --projection-dir
// has no cluster to watch.
func (o *CrispServerOptions) metadataClient(serverConfig *genericapiserver.RecommendedConfig) (metadata.Interface, error) {
	if !o.WatchProjections || serverConfig.ClientConfig == nil {
		return nil, nil
	}
	return metadata.NewForConfig(serverConfig.ClientConfig)
}

func (o *CrispServerOptions) dsnResolver(serverConfig *genericapiserver.RecommendedConfig) (projection.DSNResolver, error) {
	if o.LocalDSNFromEnv {
		return projection.EnvDSNResolver{}, nil
	}

	if serverConfig.ClientConfig == nil {
		return nil, fmt.Errorf("no kubeconfig available to read data source Secrets; pass --local-dsn-from-env when running outside a cluster")
	}
	client, err := kubernetes.NewForConfig(serverConfig.ClientConfig)
	if err != nil {
		return nil, fmt.Errorf("building client for Secret access: %w", err)
	}
	o.kubeClient = client
	return &projection.SecretDSNResolver{
		Client:            client,
		AllowedNamespaces: sets.New(o.DataSourceNamespaces...),
		RequireOptIn:      o.RequireDataSourceOptIn,
	}, nil
}

// RunCrispServer starts the server and blocks until the context is cancelled.
func (o *CrispServerOptions) RunCrispServer(ctx context.Context) error {
	config, err := o.Config()
	if err != nil {
		return err
	}

	server, err := config.Complete().New()
	if err != nil {
		return err
	}

	// After serving, not from a pre-shutdown hook: those run before in-flight
	// requests drain, and the requests still being answered need the database.
	defer server.ClosePools()

	return server.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}
