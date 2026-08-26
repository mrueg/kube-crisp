// Package apiserver wires projections into an aggregated Kubernetes API server.
package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	dynamicclient "k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/handler3"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
	"github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
	projectioncontroller "github.com/mrueg/kube-crisp/pkg/controller/projection"
	crispclient "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned"
	crispinformers "github.com/mrueg/kube-crisp/pkg/generated/informers/externalversions"
	"github.com/mrueg/kube-crisp/pkg/projection"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
	"github.com/mrueg/kube-crisp/pkg/webhook"
)

// Scheme holds every type this server serves, including the projected kinds
// that are registered as unstructured when they are compiled. Codecs is its
// codec factory.
var Scheme, Codecs = scheme.New()

// ExtraConfig carries everything kube-crisp adds on top of a generic server.
type ExtraConfig struct {
	// StaticProjections are served unconditionally. They come from
	// --projection-dir and are useful for bootstrapping or for running without
	// a cluster.
	StaticProjections []crispv1alpha1.CustomResourceProjection

	// ProjectionDir is where those were read from. Given one, the controller
	// re-reads it on every sync and watches it, so a file-backed projection
	// changes without a restart.
	ProjectionDir string

	// CrispClient watches CustomResourceProjection objects and writes their
	// status, which is what lets API groups be installed and removed while the
	// server runs. When nil, only the static projections are served.
	CrispClient crispclient.Interface

	// DynamicClient reads CustomResourceDefinitions for borrowed schemas and
	// manages the APIServices this server registers.
	DynamicClient dynamicclient.Interface

	// DSNResolver turns a data source reference into a connection string.
	DSNResolver projection.DSNResolver

	// Pools is the shared connection pool cache.
	Pools *crispsql.PoolCache

	// APIServices controls registration of served groups with the aggregation
	// layer.
	APIServices projectioncontroller.APIServiceOptions

	// RequireAllProjections makes a projection that cannot be served fail the
	// readiness check, rather than only the degradation check.
	RequireAllProjections bool

	// MaxOpenConnsPerDataSource bounds connections to any one database,
	// whatever a projection asks for.
	MaxOpenConnsPerDataSource int

	// KubeClient watches the Secrets that hold data source credentials.
	// Without it, a rotated credential is picked up at the next resync rather
	// than when it changes.
	KubeClient kubernetes.Interface

	// DataSourceNamespaces are the namespaces those Secrets may live in.
	DataSourceNamespaces []string

	// LeaderElection decides which replica polls watched projections at the
	// configured interval and which fall back to a slower one.
	LeaderElection LeaderElectionOptions

	// ProjectionWebhook registers an admission webhook that checks a
	// CustomResourceProjection before the cluster accepts it, so a projection
	// whose SQL the database cannot run is refused at kubectl apply rather than
	// reported afterwards in its status.
	ProjectionWebhook ProjectionWebhookOptions
}

// Config is the full server configuration.
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig is a Config with defaults applied.
type CompletedConfig struct {
	*completedConfig
}

// Complete fills in defaults.
func (c *Config) Complete() CompletedConfig {
	return CompletedConfig{&completedConfig{
		GenericConfig: c.GenericConfig.Complete(),
		ExtraConfig:   &c.ExtraConfig,
	}}
}

// CrispServer serves projected resources over the aggregation layer.
type CrispServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	router *dynamic.Router
	pools  *crispsql.PoolCache
}

// ClosePools releases every connection pool.
//
// Called after serving has finished rather than from a pre-shutdown hook.
// Pre-shutdown hooks run before in-flight requests drain — the sequence is
// hooks, then stop accepting, then wait for the request and watch wait groups
// — so closing pools there pulled the database out from under every request
// that was still being answered, and every watch poll until the last watcher
// went away. They got "sql: database is closed", which is not something a
// client can do anything about and not something a retry reaches.
//
// Shutting down gracefully is the whole point of the drain, and the drain needs
// the database.
func (s *CrispServer) ClosePools() {
	s.pools.Close()
}

// New builds the server and installs the projected API surface.
// webhookReconcileInterval is how often the projection webhook configuration is
// checked against what this server serves. Short, because until the two agree
// the webhook is being skipped rather than failing loudly.
const webhookReconcileInterval = 30 * time.Second

func (c CompletedConfig) New() (*CrispServer, error) {
	genericServer, err := c.GenericConfig.New("kube-crisp-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("building generic apiserver: %w", err)
	}

	router := dynamic.NewRouter(dynamic.Options{
		// Each generation of the API surface gets a scheme of its own rather
		// than sharing this package's: a projected kind has to be taught to a
		// scheme before it can be served, and runtime.Scheme has no locking, so
		// teaching one that is already answering requests races every read.
		NewScheme:  scheme.New,
		Authorizer: genericServer.Authorizer,
		// The server's admission chain, not a fresh empty one: projected
		// writes go through the same plugins as any other resource.
		Admit:                      c.GenericConfig.AdmissionControl,
		EquivalentResourceRegistry: genericServer.EquivalentResourceRegistry,
		DiscoveryManager:           genericServer.AggregatedDiscoveryGroupManager,
		OpenAPIV3Service:           func() *handler3.OpenAPIService { return genericServer.OpenAPIV3VersionedService },
		MinRequestTimeout:          time.Duration(c.GenericConfig.MinRequestTimeout) * time.Second,
	})

	compiler := &dynamic.Compiler{
		Pools:        c.ExtraConfig.Pools,
		Resolver:     c.ExtraConfig.DSNResolver,
		MaxOpenConns: c.ExtraConfig.MaxOpenConnsPerDataSource,
	}
	if c.ExtraConfig.DynamicClient != nil {
		compiler.Schemas = &projection.CRDSchemaResolver{Client: c.ExtraConfig.DynamicClient}
	}

	// Everything under /apis is served by the router rather than by statically
	// installed API groups, because the set of groups changes at runtime.
	genericServer.Handler.NonGoRestfulMux.HandlePrefix("/apis/", router)

	if c.ExtraConfig.ProjectionWebhook.Enabled {
		genericServer.Handler.NonGoRestfulMux.Handle(webhook.Path,
			&webhook.Handler{Checker: compiler})
		klog.InfoS("serving the projection admission webhook", "path", webhook.Path)
	}

	s := &CrispServer{
		GenericAPIServer: genericServer,
		router:           router,
		pools:            c.ExtraConfig.Pools,
	}

	if c.ExtraConfig.CrispClient != nil {
		if err := s.installController(c, compiler); err != nil {
			return nil, err
		}
	} else {
		// Without a client there is nothing to watch, so the static
		// projections are compiled once, here, and any failure is fatal.
		resources, err := compileAll(context.Background(), compiler, c.ExtraConfig.StaticProjections)
		if err != nil {
			return nil, err
		}
		if err := router.Rebuild(resources); err != nil {
			return nil, err
		}
		klog.InfoS("serving static projections", "resources", len(resources))
	}

	if c.ExtraConfig.ProjectionWebhook.Enabled && c.ExtraConfig.ProjectionWebhook.Manage {
		opts := c.ExtraConfig.ProjectionWebhook
		serving := c.GenericConfig.SecureServing
		client := c.ExtraConfig.KubeClient

		// A server on its way out must stop correcting the configuration.
		//
		// The certificate in it belongs to whichever pod wrote it, so a
		// terminating pod whose reconcile fires during a rolling update points
		// the cluster back at a certificate it is about to stop serving. Its
		// replacement is already serving a different one, so admission then
		// fails TLS — silently, because the policy is Ignore — until the next
		// reconcile happens to correct it again.
		//
		// Pre-shutdown hooks run before anything drains, which is exactly when
		// this should stop.
		leaving := make(chan struct{})
		genericServer.AddPreShutdownHookOrDie("kube-crisp-stop-webhook-reconcile", func() error {
			close(leaving)
			return nil
		})

		// Registered from a post-start hook rather than here: the certificate
		// is loaded as part of starting to serve, so before that there is
		// nothing to point the cluster at.
		genericServer.AddPostStartHookOrDie("kube-crisp-projection-webhook",
			func(hookCtx genericapiserver.PostStartHookContext) error {
				if len(opts.CABundle) == 0 {
					cert, err := servingCertificate(serving)
					if err != nil {
						return err
					}
					opts.CABundle = cert
				}
				if err := reconcileWebhookConfiguration(hookCtx.Context, client, opts); err != nil {
					return err
				}

				// And kept reconciled, not registered once.
				//
				// The configuration can name one CA, and a server that was not
				// given one signs its own — so during a rolling update the pod
				// on its way out can write its certificate after its
				// replacement wrote theirs, leaving the cluster told to trust a
				// certificate nothing serves any more. Because the policy is
				// Ignore, that is silent: admission is skipped, and a
				// projection this server would have refused is accepted.
				//
				// A no-op almost always: a read and a comparison, writing only
				// when the configuration has drifted from what this server can
				// actually answer.
				go wait.UntilWithContext(hookCtx.Context, func(ctx context.Context) {
					select {
					case <-leaving:
						return
					default:
					}
					if err := reconcileWebhookConfiguration(ctx, client, opts); err != nil {
						klog.V(2).InfoS("could not reconcile the projection webhook configuration", "err", err)
					}
				}, webhookReconcileInterval)
				return nil
			})
	}

	// The OpenAPI endpoint only exists once the server is prepared, so the
	// schemas installed before that are published again here.
	genericServer.AddPostStartHookOrDie("kube-crisp-openapi", func(genericapiserver.PostStartHookContext) error {
		s.router.PublishOpenAPI()
		return nil
	})

	return s, nil
}

// secretInformers builds one informer per namespace credentials may be read
// from, watching only the Secrets that have opted in.
//
// Watching every Secret in the cluster to notice a password change would be a
// poor trade. The opt-in label is already what makes a Secret readable by a
// projection, so it is also what the informer selects on: the server sees only
// the Secrets it was given.
func secretInformers(extra *ExtraConfig) ([]informers.SharedInformerFactory, []cache.SharedIndexInformer, *secretCache) {
	if extra.KubeClient == nil || len(extra.DataSourceNamespaces) == 0 {
		return nil, nil, nil
	}

	var (
		factories []informers.SharedInformerFactory
		watched   []cache.SharedIndexInformer
		listers   = map[string]corelisters.SecretNamespaceLister{}
	)
	for _, namespace := range extra.DataSourceNamespaces {
		factory := informers.NewSharedInformerFactoryWithOptions(
			extra.KubeClient,
			projectioncontroller.ResyncPeriod,
			informers.WithNamespace(namespace),
			informers.WithTweakListOptions(func(options *metav1.ListOptions) {
				options.LabelSelector = labels.Set{projection.OptInLabel: projection.OptInValue}.String()
			}),
		)
		secrets := factory.Core().V1().Secrets()
		factories = append(factories, factory)
		watched = append(watched, secrets.Informer())
		listers[namespace] = secrets.Lister().Secrets(namespace)
	}
	return factories, watched, &secretCache{listers: listers}
}

// secretCache answers data source lookups from the informers that are already
// watching those Secrets, so resolving a connection string on every sync costs
// nothing rather than a request per projection.
type secretCache struct {
	listers map[string]corelisters.SecretNamespaceLister
}

// Secret returns the cached Secret, reporting false when this namespace is not
// watched or the Secret is not in the cache yet. The caller falls back to the
// API server, which is also what makes an unlabelled Secret — one the informers
// deliberately do not select — behave as it always did.
func (c *secretCache) Secret(namespace, name string) (*corev1.Secret, bool) {
	lister, ok := c.listers[namespace]
	if !ok {
		return nil, false
	}
	secret, err := lister.Get(name)
	if err != nil {
		return nil, false
	}
	return secret, true
}

// installController starts the watch-driven path: projections are compiled and
// installed as they appear, and removed when they are deleted.
func (s *CrispServer) installController(c CompletedConfig, compiler *dynamic.Compiler) error {
	factory := crispinformers.NewSharedInformerFactory(
		c.ExtraConfig.CrispClient, projectioncontroller.ResyncPeriod)

	secretFactories, secretInformers, secrets := secretInformers(c.ExtraConfig)

	// The registrations this server manages, watched rather than re-read: the
	// reconciler otherwise makes one request per served group version, plus a
	// list, on every sync. Not label-filtered, because it also has to be able
	// to see an APIService someone else owns in order to leave it alone.
	var (
		apiServiceFactory  dynamicinformer.DynamicSharedInformerFactory
		apiServiceInformer cache.SharedIndexInformer
	)
	if c.ExtraConfig.APIServices.Enabled && c.ExtraConfig.DynamicClient != nil {
		apiServiceFactory = dynamicinformer.NewDynamicSharedInformerFactory(
			c.ExtraConfig.DynamicClient, projectioncontroller.ResyncPeriod)
		apiServiceInformer = apiServiceFactory.ForResource(projectioncontroller.APIServiceGVR).Informer()
	}

	// The resolver reads a connection string once per projection on every
	// sync. Backed by the informers above, that stops being a request each.
	if resolver, ok := c.ExtraConfig.DSNResolver.(*projection.SecretDSNResolver); ok && secrets != nil {
		resolver.Cache = secrets
	}

	controller := projectioncontroller.New(projectioncontroller.Options{
		SecretInformers:    secretInformers,
		APIServiceInformer: apiServiceInformer,
		Client:             c.ExtraConfig.CrispClient,
		EventClient:        c.ExtraConfig.KubeClient,
		DynamicClient:      c.ExtraConfig.DynamicClient,
		Factory:            factory,
		Compiler:           compiler,
		Router:             s.router,
		Pools:              c.ExtraConfig.Pools,
		Static:             c.ExtraConfig.StaticProjections,
		// Leader election is the operator saying there are peers; this server
		// has no way to count them itself.
		HasPeers:    c.ExtraConfig.LeaderElection.Enabled,
		StaticDir:   c.ExtraConfig.ProjectionDir,
		APIServices: c.ExtraConfig.APIServices,
	})

	if c.ExtraConfig.LeaderElection.Enabled && c.ExtraConfig.KubeClient != nil {
		s.GenericAPIServer.AddPostStartHookOrDie("kube-crisp-leader-election",
			func(hookCtx genericapiserver.PostStartHookContext) error {
				return runLeaderElection(hookCtx.Context, c.ExtraConfig.KubeClient, c.ExtraConfig.LeaderElection)
			})
	}

	s.GenericAPIServer.AddPostStartHookOrDie("kube-crisp-projections", func(hookCtx genericapiserver.PostStartHookContext) error {
		factory.Start(hookCtx.Done())
		for _, secrets := range secretFactories {
			secrets.Start(hookCtx.Done())
		}
		if apiServiceFactory != nil {
			apiServiceFactory.Start(hookCtx.Done())
		}
		go controller.Run(hookCtx.Context)
		return nil
	})

	// The server is not ready until the projected API surface exists, so that
	// the aggregation layer does not route requests to an empty server.
	if err := s.GenericAPIServer.AddReadyzChecks(healthz.NamedCheck("projections-synced", func(_ *http.Request) error {
		if !controller.HasSynced() {
			return fmt.Errorf("projections have not been installed yet")
		}
		return nil
	})); err != nil {
		return fmt.Errorf("registering the readiness check: %w", err)
	}

	// Serving some projections is not the same as serving all of them, and the
	// difference has to be visible — but not by taking down a server that is
	// still serving the healthy ones.
	//
	// It is deliberately not a health check. AddHealthChecks registers into
	// livez and readyz as well as healthz, so a projection with a typo in its
	// query would fail the liveness probe and have the kubelet restart the
	// server, taking every working projection with it. The signal lives in
	// kube_crisp_projections{state="failed"}, in each projection's status
	// conditions, and in the log.
	degraded := healthz.NamedCheck("projections-degraded", func(_ *http.Request) error {
		if broken := controller.Degraded(); len(broken) > 0 {
			return fmt.Errorf("%d projection(s) are defined but not served: %s",
				len(broken), strings.Join(broken, ", "))
		}
		return nil
	})

	// Operators who would rather a partially broken server stopped taking
	// traffic can ask for it, and then it is a readiness gate only: the server
	// leaves the endpoints, and nothing restarts it.
	if c.ExtraConfig.RequireAllProjections {
		if err := s.GenericAPIServer.AddReadyzChecks(degraded); err != nil {
			return fmt.Errorf("registering the degradation gate: %w", err)
		}
	}

	return nil
}

// compileAll compiles every projection, failing on the first error.
func compileAll(ctx context.Context, compiler *dynamic.Compiler, projections []crispv1alpha1.CustomResourceProjection) ([]dynamic.Resource, error) {
	var resources []dynamic.Resource
	for i := range projections {
		compiled, err := compiler.Compile(ctx, &projections[i])
		if err != nil {
			return nil, fmt.Errorf("projection %s: %w", projections[i].Name, err)
		}
		resources = append(resources, compiled...)
	}
	return resources, nil
}
