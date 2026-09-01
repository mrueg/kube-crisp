// Package projection watches CustomResourceProjection objects and keeps the
// served API surface in sync with them.
package projection

import (
	"bytes"
	"errors"
	"maps"

	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	corev1 "k8s.io/api/core/v1"

	"github.com/prometheus/client_golang/prometheus"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
	crispclient "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned"
	crispinformers "github.com/mrueg/kube-crisp/pkg/generated/informers/externalversions"
	crisplisters "github.com/mrueg/kube-crisp/pkg/generated/listers/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	"github.com/mrueg/kube-crisp/pkg/projection"
	projectionregistry "github.com/mrueg/kube-crisp/pkg/registry/projection"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// GVR is the resource the controller watches. The typed client knows this
// already; it is kept because the APIService reconciler and the tests address
// resources by GVR.
var GVR = schema.GroupVersionResource{
	Group:    crispv1alpha1.GroupName,
	Version:  "v1alpha1",
	Resource: "customresourceprojections",
}

// syncKey is the only key the queue carries. Projections are cheap to compile
// and the served surface is rebuilt as a whole, so there is nothing to gain
// from per-object keys and a lot of ordering trouble to avoid.
const syncKey = "sync"

// registrationRecheckInterval is how soon to look again while the aggregation
// layer has not confirmed a registration. Short, because it is the difference
// between a projection describing a recovery within seconds and describing an
// outage that is over until the next resync.
//
// A variable so tests can shorten it; nothing else assigns to it.
var registrationRecheckInterval = 15 * time.Second

// ResyncPeriod is how often the informer relists projections, as a backstop
// against a missed watch event.
const ResyncPeriod = 10 * time.Minute

// Controller keeps the router's resources in sync with the cluster.
type Controller struct {
	// client reads and writes projections; dynamicClient is only used for the
	// APIServices this server manages.
	client        crispclient.Interface
	dynamicClient dynamic.Interface

	pools    *crispsql.PoolCache
	informer cache.SharedIndexInformer
	lister   crisplisters.CustomResourceProjectionLister

	// secrets watch data source credentials. A change to one is a reason to
	// recompile: the pool is keyed by the connection string, so a projection
	// whose password moved gets a new pool and the old one is released.
	secrets []cache.SharedIndexInformer

	// apiServiceInformer backs the registration reconciler's reads.
	apiServiceInformer cache.SharedIndexInformer

	// crds watch the CustomResourceDefinitions projections borrow schemas
	// from. A change to one is a reason to recompile: the borrowed schema is
	// part of what identifies a compiled projection, so an edited CRD gets
	// fresh storage validating and explaining the new shape.
	crds cache.SharedIndexInformer

	compiler    *apidynamic.Compiler
	router      *apidynamic.Router
	apiServices *apiServiceManager

	// static projections come from --projection-dir and are always served,
	// regardless of what exists in the cluster. staticDir is where they were
	// read from, when they came from a directory at all.
	//
	// They are re-read on every sync rather than held from startup: a file
	// changing is the same kind of event as a projection changing in the
	// cluster, and needing a restart to pick one up made the directory the only
	// part of the configuration that could go stale. Only sync touches these.
	static    []crispv1alpha1.CustomResourceProjection
	staticDir string

	// lastStatus is the status this controller last wrote for each projection,
	// keyed by projection name.
	//
	// The lister's copy can lag a write this controller just made, and a status
	// write does not change the generation — which the update handler
	// deliberately ignores, so it queues no sync of its own. Deciding "this
	// status is already correct" from the lister therefore risks skipping the
	// one write that would have corrected it, leaving the object wrong until
	// something unrelated happens. What this controller last wrote is the
	// authoritative answer to that question.
	//
	// Only sync touches it, and sync runs on one worker.
	lastStatus map[string]crispv1alpha1.CustomResourceProjectionStatus

	// hasPeers is Options.HasPeers; warnedUnversioned keeps the warning above
	// from repeating on every sync.
	hasPeers          bool
	warnedUnversioned map[string]bool

	// warnedCacheUnshared keeps the cacheTTL warning to once per projection,
	// for the same reason.
	warnedCacheUnshared map[string]bool

	// reportedStates names the projections that currently have a
	// projection_state series, so the ones that go away take their series with
	// them. Only ever touched from sync.
	reportedStates []string

	// recorder writes Events against projections; nil when no client was given.
	// events remembers the last one written for each projection, so a state
	// that has not changed is not re-announced on every sync.
	recorder record.EventRecorder
	events   map[string]string

	// compiled remembers what each projection last compiled to, keyed by
	// projection name. Only sync touches it, and sync runs on one worker.
	//
	// Recompiling builds fresh storage, and fresh storage means an empty watch
	// cache, an empty read cache, and every watcher relisting. Doing that to
	// every projection whenever any one of them changes — or ten minutes pass —
	// is a lot of disruption for no change, so a projection whose spec and
	// credentials are unchanged keeps the storage it already has.
	compiled map[string]compilation

	queue     workqueue.TypedRateLimitingInterface[string]
	hasSynced atomic.Bool

	// degraded names the projections that could not be compiled on the last
	// sync. Serving three of five projections is not the same as serving all
	// five, and nothing else in the health surface says so.
	degraded atomic.Pointer[[]string]
}

// compilation is one projection's compiled resources and the fingerprint that
// says what they were compiled from.
type compilation struct {
	fingerprint string
	resources   []apidynamic.Resource
}

// Options configures a Controller.
type Options struct {
	// Client reads projections and writes their status.
	Client crispclient.Interface

	// DynamicClient is used for the APIServices this server registers.
	DynamicClient dynamic.Interface

	// Factory supplies the projection informer. The caller owns starting it.
	Factory crispinformers.SharedInformerFactory

	// SecretInformers watch the Secrets that hold data source credentials, so
	// a rotated password is picked up when it changes rather than at the next
	// resync. One per namespace credentials may be read from. Optional: with
	// none, rotation still lands within ResyncPeriod.
	SecretInformers []cache.SharedIndexInformer

	// APIServiceInformer watches the registrations this server manages, so
	// reconciling them reads from a cache rather than making a request per
	// served group version on every sync. Optional: without it the reconciler
	// reads through the client.
	APIServiceInformer cache.SharedIndexInformer

	// CRDInformer watches the CustomResourceDefinitions that projections
	// borrow schemas from, so an edited one is picked up when it changes
	// rather than at the next resync. Metadata only: the schema itself is read
	// through the client when a projection is prepared, and caching every
	// schema in the cluster is a cost this server has no reason to pay.
	// Optional: with none, an edit still lands within ResyncPeriod.
	CRDInformer cache.SharedIndexInformer

	// Compiler turns projections into servable resources; Router installs them.
	Compiler *apidynamic.Compiler
	Router   *apidynamic.Router
	// HasPeers reports whether this server expects to run alongside other
	// replicas, which is what makes a projection with no mapped resourceVersion
	// unsafe: the version a list reports then comes from a per-replica counter,
	// so two replicas hand the same client incompatible versions.
	HasPeers bool

	// Pools is the shared connection pool cache, trimmed as projections come
	// and go.
	Pools *crispsql.PoolCache

	// Static projections come from --projection-dir and are always served,
	// regardless of what exists in the cluster.
	Static []crispv1alpha1.CustomResourceProjection

	// StaticDir is where those came from. Given one, the controller re-reads it
	// on every sync and watches it for changes; without one, Static is fixed.
	StaticDir string

	// APIServices controls whether this server registers the groups it serves
	// with the aggregation layer, and how it describes itself when it does.
	APIServices APIServiceOptions

	// EventClient records Events against projections. Optional: without it the
	// controller reports through conditions and the log only.
	//
	// Conditions say what a projection's state is now; an Event says that it
	// changed and when. That is what kubectl describe shows, and what anything
	// watching for failures reacts to — neither of which a condition alone
	// reaches.
	EventClient kubernetes.Interface
}

// New builds a controller. The caller owns starting the informer factory.
func New(opts Options) *Controller {
	informer := opts.Factory.Crisp().V1alpha1().CustomResourceProjections()

	c := &Controller{
		apiServiceInformer: opts.APIServiceInformer,
		crds:               opts.CRDInformer,

		client:              opts.Client,
		dynamicClient:       opts.DynamicClient,
		pools:               opts.Pools,
		informer:            informer.Informer(),
		lister:              informer.Lister(),
		compiler:            opts.Compiler,
		router:              opts.Router,
		apiServices:         newAPIServiceManager(opts.DynamicClient, opts.APIServices, apiServiceIndexer(opts.APIServiceInformer)),
		static:              opts.Static,
		staticDir:           opts.StaticDir,
		secrets:             opts.SecretInformers,
		compiled:            map[string]compilation{},
		lastStatus:          map[string]crispv1alpha1.CustomResourceProjectionStatus{},
		hasPeers:            opts.HasPeers,
		warnedUnversioned:   map[string]bool{},
		warnedCacheUnshared: map[string]bool{},
		queue: workqueue.NewTypedRateLimitingQueue[string](
			workqueue.DefaultTypedControllerRateLimiter[string](),
		),
	}

	if opts.EventClient != nil {
		broadcaster := record.NewBroadcaster()
		broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
			Interface: opts.EventClient.CoreV1().Events(""),
		})
		projectionScheme, _ := crispscheme.New()
		c.recorder = broadcaster.NewRecorder(
			projectionScheme, corev1.EventSource{Component: "kube-crisp"})
		c.events = map[string]string{}
	}

	_, _ = c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { c.queue.Add(syncKey) },
		DeleteFunc: func(any) { c.queue.Add(syncKey) },
		UpdateFunc: func(old, new any) {
			// Ignore updates that only touched status, which the controller
			// writes itself and which would otherwise loop.
			oldObj, okOld := old.(*crispv1alpha1.CustomResourceProjection)
			newObj, okNew := new.(*crispv1alpha1.CustomResourceProjection)
			if okOld && okNew && oldObj.Generation == newObj.Generation {
				return
			}
			c.queue.Add(syncKey)
		},
	})

	// Nothing reacted to these before: the informer was a read cache and no
	// more, so an APIService someone deleted, or one the aggregator had just
	// marked unavailable, was neither repaired nor reported until the next
	// resync — up to ResyncPeriod of a projection claiming to serve an API that
	// answers NotFound.
	if c.apiServiceInformer != nil {
		_, _ = c.apiServiceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(any) { c.queue.Add(syncKey) },
			DeleteFunc: func(any) { c.queue.Add(syncKey) },
			UpdateFunc: func(old, new any) {
				// Only a changed registration is worth a sync. An APIService
				// relisted unchanged is not — and the resync period here is the
				// controller's own, so without this every relist would rebuild
				// the API surface.
				//
				// Content rather than resourceVersion: what matters is whether
				// the spec this server maintains, or the availability the
				// aggregator reports, actually moved.
				oldObj, okOld := old.(*unstructured.Unstructured)
				newObj, okNew := new.(*unstructured.Unstructured)
				if okOld && okNew && sameRegistration(oldObj, newObj) {
					return
				}
				c.queue.Add(syncKey)
			},
		})
	}

	if c.crds != nil {
		_, _ = c.crds.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { c.queueIfBorrowed(obj) },
			DeleteFunc: func(obj any) { c.queueIfBorrowed(obj) },
			UpdateFunc: func(old, new any) {
				// A relist is not a change. Compared on resourceVersion
				// because this informer carries metadata only, and every edit
				// to a CustomResourceDefinition moves it.
				oldMeta, okOld := old.(*metav1.PartialObjectMetadata)
				newMeta, okNew := new.(*metav1.PartialObjectMetadata)
				if okOld && okNew && oldMeta.ResourceVersion == newMeta.ResourceVersion {
					return
				}
				c.queueIfBorrowed(new)
			},
		})
	}

	for _, secrets := range c.secrets {
		_, _ = secrets.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(any) { c.queue.Add(syncKey) },
			DeleteFunc: func(any) { c.queue.Add(syncKey) },
			UpdateFunc: func(old, new any) {
				// Only a changed credential is worth recompiling for. A Secret
				// relisted unchanged, or relabelled, is not.
				oldSecret, okOld := old.(*corev1.Secret)
				newSecret, okNew := new.(*corev1.Secret)
				if okOld && okNew && maps.EqualFunc(oldSecret.Data, newSecret.Data, bytes.Equal) {
					return
				}
				c.queue.Add(syncKey)
			},
		})
	}

	return c
}

// queueIfBorrowed syncs when the CustomResourceDefinition that changed is one a
// projection takes its schema from.
//
// A cluster has a great many CRDs and they are edited by things that have
// nothing to do with this server, so syncing on all of them would re-prepare
// every projection — a read per data source — for changes none of them can see.
func (c *Controller) queueIfBorrowed(obj any) {
	meta, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown)
		if !isTombstone {
			return
		}
		if meta, ok = tombstone.Obj.(*metav1.PartialObjectMetadata); !ok {
			return
		}
	}

	// The static projections too: a file on disk borrows a schema exactly as an
	// object in the cluster does.
	for i := range c.static {
		if borrowsFrom(&c.static[i], meta.Name) {
			c.queue.Add(syncKey)
			return
		}
	}

	// Serving only --projection-dir: the static projections above are all there
	// are to check.
	if c.lister == nil {
		return
	}

	projections, err := c.lister.List(labels.Everything())
	if err != nil {
		// The cache is not readable, which the sync itself will report. Erring
		// towards a sync is the safe direction: an unnecessary one costs a
		// fingerprint comparison.
		c.queue.Add(syncKey)
		return
	}
	for _, p := range projections {
		if borrowsFrom(p, meta.Name) {
			c.queue.Add(syncKey)
			return
		}
	}
}

// borrowsFrom reports whether any version of a projection takes its schema from
// the named CustomResourceDefinition.
func borrowsFrom(p *crispv1alpha1.CustomResourceProjection, crd string) bool {
	if ref := p.Spec.Resource.SchemaFrom; ref != nil && ref.Name == crd {
		return true
	}
	for _, version := range p.Spec.Resource.Versions {
		if ref := version.SchemaFrom; ref != nil && ref.Name == crd {
			return true
		}
	}
	return false
}

// sameRegistration reports whether two versions of an APIService say the same
// thing about how the group version is routed and whether it is reachable.
func sameRegistration(old, new *unstructured.Unstructured) bool {
	for _, field := range []string{"spec", "status"} {
		oldValue, _, _ := unstructured.NestedMap(old.Object, field)
		newValue, _, _ := unstructured.NestedMap(new.Object, field)
		if !apiequality.Semantic.DeepEqual(oldValue, newValue) {
			return false
		}
	}
	return true
}

// apiServiceIndexer returns an informer's cache, or nil when there is no
// informer to read from.
func apiServiceIndexer(informer cache.SharedIndexInformer) cache.Indexer {
	if informer == nil {
		return nil
	}
	return informer.GetIndexer()
}

// HasSynced reports whether the controller has installed the API surface at
// least once, which is what makes the server ready to serve projections.
func (c *Controller) HasSynced() bool { return c.hasSynced.Load() }

// Degraded returns the projections that are defined but not being served,
// sorted. An empty result means every projection compiled.
func (c *Controller) Degraded() []string {
	names := c.degraded.Load()
	if names == nil {
		return nil
	}
	return *names
}

// Run processes queue items until the context is cancelled.
func (c *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.InfoS("starting projection controller")
	defer klog.InfoS("stopping projection controller")

	synced := []cache.InformerSynced{c.informer.HasSynced}

	for _, secrets := range c.secrets {
		synced = append(synced, secrets.HasSynced)
	}
	if c.apiServiceInformer != nil {
		// Reconciling reads this cache, and reading it before it has synced
		// would look like every registration is missing.
		synced = append(synced, c.apiServiceInformer.HasSynced)
	}
	if c.crds != nil {
		synced = append(synced, c.crds.HasSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), synced...) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}

	c.watchStaticDir(ctx)

	// Install whatever exists right now before serving traffic.
	c.queue.Add(syncKey)

	go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	<-ctx.Done()
}

func (c *Controller) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

func (c *Controller) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	if err := c.sync(ctx); err != nil {
		utilruntime.HandleError(fmt.Errorf("syncing projections: %w", err))
		c.queue.AddRateLimited(key)
		return true
	}

	c.queue.Forget(key)
	return true
}

// sync rebuilds the entire served surface from the current set of projections.
func (c *Controller) sync(ctx context.Context) error {
	objects, err := c.lister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("listing projections: %w", err)
	}

	static := c.staticProjections()
	candidates := make([]projectionCandidate, 0, len(objects)+len(static))
	for i := range static {
		candidates = append(candidates, projectionCandidate{projection: &static[i]})
	}
	for _, obj := range objects {
		candidates = append(candidates, projectionCandidate{projection: obj, stored: obj})
	}

	var (
		resources []apidynamic.Resource
		failures  = map[string]error{}

		// Separate from failures on purpose: a projection whose database is
		// down is still installed and still answers, with 503.
		unreachable = map[string]error{}

		// stale names the projections that failed to compile this time but are
		// still being served from what they compiled to last time.
		stale = map[string]struct{}{}
	)

	// Storage that is about to stop being served, released once the router no
	// longer routes to it: a projection that was replaced or deleted otherwise
	// keeps polling its table forever with nobody reading the result.
	var retired []apidynamic.Resource
	surviving := make(map[string]compilation, len(candidates))

	// failed records a projection that did not compile, and keeps whatever it is
	// already serving.
	//
	// A projection that exists and cannot be compiled is not a projection that
	// is gone. Reading its data source Secret can fail for a moment, and a
	// borrowed schema is a request to the kube-apiserver like any other — so
	// treating a failure as a deletion would withdraw the API group, delete its
	// APIService, and take discovery, RBAC, and every controller watching it
	// down with it, over a blip. That is the same reasoning that keeps a
	// projection installed when its database is unreachable; this is the other
	// half of it.
	//
	// Nothing is hidden by doing so: the projection is still counted as failed,
	// still named by Degraded, and still reports Ready=False for the generation
	// that did not compile.
	failed := func(name string, err error) {
		failures[name] = err

		previous, served := c.compiled[name]
		if !served {
			// Never compiled, so there is nothing to fall back to and no
			// registration to protect.
			klog.ErrorS(err, "projection is not servable", "projection", name)
			return
		}

		stale[name] = struct{}{}
		surviving[name] = previous
		resources = append(resources, previous.resources...)
		klog.ErrorS(err, "projection failed to compile; still serving its previous configuration",
			"projection", name)
	}

	// Creation times, for settling a resource two projections both claim. Kept
	// here because the candidate is the only place the object itself is in
	// hand; a compilation does not carry one.
	created := make(map[string]metav1.Time, len(candidates))

	for _, cand := range candidates {
		name := cand.projection.Name
		created[name] = cand.projection.CreationTimestamp

		prepared, err := c.compiler.Prepare(ctx, cand.projection)
		if err != nil {
			failed(name, err)
			continue
		}

		// Unchanged spec, unchanged credentials: keep the storage that is
		// already serving, along with its watch cache and everything watching
		// it. Only the data source is re-probed, since that can change without
		// the projection doing anything.
		if previous, ok := c.compiled[name]; ok && previous.fingerprint == prepared.Fingerprint {
			reachErr := prepared.Pool.Ping(ctx)
			for i := range previous.resources {
				previous.resources[i].DataSourceReady = reachErr == nil
				previous.resources[i].DataSourceError = reachErr
			}
			if reachErr != nil {
				unreachable[name] = reachErr
			}
			surviving[name] = previous
			resources = append(resources, previous.resources...)
			continue
		}

		compiled, err := c.compiler.CompileWith(ctx, cand.projection, prepared)
		if err != nil {
			failed(name, err)
			continue
		}
		for _, res := range compiled {
			if !res.DataSourceReady && res.DataSourceError != nil {
				unreachable[name] = res.DataSourceError
			}
		}
		if previous, ok := c.compiled[name]; ok {
			retired = append(retired, previous.resources...)
		}
		surviving[name] = compilation{fingerprint: prepared.Fingerprint, resources: compiled}
		resources = append(resources, compiled...)
	}

	// A resource two projections both claim used to fail the whole rebuild, and
	// with it every other projection: sync returned before c.compiled was
	// replaced and before hasSynced was set, so on a cold start the
	// projections-synced readiness gate never closed and the server served
	// nothing at all. One projection's mistake, and the only evidence a line in
	// the log.
	//
	// Settled here instead, the way a compile failure is: the projections that
	// lose a claim are failed by name and every other projection installs.
	// Before the retirement pass below, so that a loser that was serving has
	// its storage released rather than left polling its table with nobody
	// reading the result.
	for name, err := range resolveClaims(surviving, created, c.compiled) {
		delete(surviving, name)
		failures[name] = err
		klog.ErrorS(err, "projection claims a resource another projection serves", "projection", name)
	}

	// Anything gone from the cluster entirely keeps whatever it had until here
	// and then loses it. A projection that merely failed to compile is in
	// surviving and is not retired: it is still serving what it last compiled
	// to, which is what failed() arranges.
	for name, previous := range c.compiled {
		if _, still := surviving[name]; !still {
			retired = append(retired, previous.resources...)
		}
	}

	// Rebuilt from what survived rather than carried along, since a projection
	// that lost a claim has to take its resources out of the set with it.
	resources = resources[:0]
	for _, compiled := range surviving {
		resources = append(resources, compiled.resources...)
	}

	if err := c.router.Rebuild(resources); err != nil {
		return fmt.Errorf("installing projected resources: %w", err)
	}
	c.compiled = surviving
	c.hasSynced.Store(true)

	// A projection that is gone takes its remembered status with it, so a later
	// object of the same name is not compared against a stranger's.
	live := make(map[string]struct{}, len(candidates))
	for _, cand := range candidates {
		live[cand.projection.Name] = struct{}{}
	}
	for name := range c.lastStatus {
		if _, still := live[name]; !still {
			delete(c.lastStatus, name)
		}
	}

	// The same for the unversioned warning. A projection that has been deleted
	// is not a projection running unsafely, and leaving its gauge behind would
	// have an alert fire for something that no longer exists.
	for name := range c.warnedUnversioned {
		if _, still := live[name]; !still {
			delete(c.warnedUnversioned, name)
			crispmetrics.ProjectionsUnversioned.DeletePartialMatch(
				prometheus.Labels{"projection": name})
		}
	}

	// And for the cacheTTL warning.
	for name := range c.warnedCacheUnshared {
		if _, still := live[name]; !still {
			delete(c.warnedCacheUnshared, name)
			crispmetrics.ProjectionsCacheUnshared.DeletePartialMatch(
				prometheus.Labels{"projection": name})
		}
	}

	// After the rebuild, so a request in flight against the old surface is
	// never handed storage that has just been torn down.
	apidynamic.DestroyAll(retired)

	// Connections belonging to projections that are gone are released here;
	// otherwise a deleted projection would hold its pool open forever.
	inUse := make(map[string]struct{}, len(resources))
	for _, res := range resources {
		inUse[res.PoolKey] = struct{}{}
		if res.ReadPoolKey != "" {
			inUse[res.ReadPoolKey] = struct{}{}
		}
	}
	if evicted := c.pools.RetainOnly(inUse); evicted > 0 {
		klog.InfoS("released connection pools no projection references", "pools", evicted)
	}

	// Registration happens after the surface is installed: an APIService that
	// points at a group this server does not serve yet would be marked
	// unavailable by the aggregator.
	unregistered, err := c.apiServices.reconcile(ctx, resources, c.apiServiceOwners(candidates, resources))
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("reconciling APIServices: %w", err))
	}

	broken := make([]string, 0, len(failures))
	for name := range failures {
		broken = append(broken, name)
	}
	sort.Strings(broken)
	c.degraded.Store(&broken)

	crispmetrics.Projections.WithLabelValues(crispmetrics.ProjectionServing).Set(float64(len(resources)))
	crispmetrics.Projections.WithLabelValues(crispmetrics.ProjectionFailed).Set(float64(len(failures)))
	// Worth its own series: a projection serving configuration it can no longer
	// recompile looks healthy from the outside, and only stops being correct
	// when whatever broke the recompile is fixed. Alerting on this above zero
	// for more than a few minutes is how that gets noticed.
	crispmetrics.Projections.WithLabelValues(crispmetrics.ProjectionStale).Set(float64(len(stale)))
	// Compiled here and unreachable from outside. Counted separately because
	// nothing else would show it: the projection is installed, its queries
	// work, and every request for it stops at the aggregation layer. Only
	// group versions whose registration has actually failed — one merely
	// waiting to be dialled is not a fault.
	unrouted := 0
	for _, err := range unregistered {
		if !errors.Is(err, errRegistrationPending) {
			unrouted++
		}
	}
	crispmetrics.Projections.WithLabelValues(crispmetrics.ProjectionUnrouted).Set(float64(unrouted))

	c.reportProjectionStates(candidates, failures, stale, unregistered)
	c.recordProjectionEvents(candidates, failures, stale, unregistered)

	// Look again shortly while any group version is unregistered or waiting to
	// be dialled.
	//
	// The aggregator's verdict arrives as a change to an APIService, which the
	// informer turns into a sync — but only if the change lands after this sync
	// read the cache. When it lands during one, the sync writes a verdict that
	// is already out of date and the event that would have corrected it has
	// already been spent. Observed on a rollout: the aggregator marked the
	// registration available in the same second the controller read it as
	// unavailable, and the projection reported an outage that was over until
	// something unrelated queued the next sync ten minutes later.
	//
	// Only while something is unresolved, so a healthy server still syncs only
	// when something actually changes.
	if len(unregistered) > 0 {
		c.queue.AddAfter(syncKey, registrationRecheckInterval)
	}

	for _, cand := range candidates {
		c.warnIfUnversioned(cand.projection)
		c.warnIfCacheUnshared(cand.projection)
	}

	klog.InfoS("projections installed",
		"servable", len(resources), "failed", len(failures), "stale", len(stale))

	// Status is reported only for projections that exist in the cluster;
	// file-based ones have no object to write back to.
	for _, cand := range candidates {
		if cand.stored == nil {
			continue
		}
		name := cand.projection.Name
		_, servingPrevious := stale[name]
		if err := c.updateStatus(ctx, cand.stored, failures[name], unreachable[name],
			registrationError(cand.projection, unregistered), servingPrevious); err != nil {
			utilruntime.HandleError(fmt.Errorf("updating status of %s: %w", name, err))
		}
	}

	return nil
}

// staticProjections returns what --projection-dir holds now.
//
// A directory that cannot be read, or holds a projection that does not parse,
// keeps whatever was last read successfully. The alternative is that saving a
// half-written file takes every file-backed projection out of service, which is
// a worse answer to a mistake than carrying on with the last good one — and the
// same reasoning that keeps a projection serving when it fails to recompile.
func (c *Controller) staticProjections() []crispv1alpha1.CustomResourceProjection {
	if c.staticDir == "" {
		return c.static
	}

	loaded, err := projection.LoadDir(c.staticDir)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf(
			"re-reading %s; keeping the %d projection(s) last read from it: %w",
			c.staticDir, len(c.static), err))
		return c.static
	}

	c.static = loaded
	return loaded
}

// watchStaticDir queues a sync when the projection directory changes.
//
// The directory is watched rather than the files in it, which is what makes
// this work for a ConfigMap: a mount is updated by swapping a symlink, so the
// files themselves are never written to and only the directory sees the event.
//
// A watch that cannot be established is reported and not fatal — the resync
// still picks a change up, just not promptly.
func (c *Controller) watchStaticDir(ctx context.Context) {
	if c.staticDir == "" {
		return
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("watching %s: %w", c.staticDir, err))
		return
	}
	if err := watcher.Add(c.staticDir); err != nil {
		_ = watcher.Close()
		utilruntime.HandleError(fmt.Errorf("watching %s: %w", c.staticDir, err))
		return
	}

	klog.InfoS("watching the projection directory for changes", "dir", c.staticDir)

	go func() {
		defer func() { _ = watcher.Close() }()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-watcher.Events:
				if !ok {
					return
				}
				// Which file changed does not matter: the whole directory is
				// re-read, and the queue collapses a burst into one sync.
				c.queue.Add(syncKey)
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				utilruntime.HandleError(fmt.Errorf("watching %s: %w", c.staticDir, err))
			}
		}
	}()
}

// updateStatus reports whether a projection is being served.
// reportProjectionStates publishes the state of each projection by name.
//
// The aggregate counts answer whether anything is wrong; this answers which.
// An alert on the counts can only say that one projection failed, and the next
// step is always to go and look — while the name was here the whole time.
//
// Series for projections that have gone are removed, or a projection deleted
// while it was failing would go on reporting that it is, forever.
func (c *Controller) reportProjectionStates(
	candidates []projectionCandidate,
	failures map[string]error,
	stale map[string]struct{},
	unregistered map[schema.GroupVersion]error,
) {
	live := map[string]bool{}

	for _, cand := range candidates {
		name := cand.projection.Name
		live[name] = true
		resource := projectionregistry.ResourceLabel(cand.projection.Spec.Resource)

		state := crispmetrics.ProjectionServing
		switch {
		case failures[name] != nil:
			if _, servingPrevious := stale[name]; servingPrevious {
				state = crispmetrics.ProjectionStale
			} else {
				state = crispmetrics.ProjectionFailed
			}
		case registrationError(cand.projection, unregistered) != nil &&
			!errors.Is(registrationError(cand.projection, unregistered), errRegistrationPending):
			state = crispmetrics.ProjectionUnrouted
		}

		// One series per state with exactly one of them set, so a query can ask
		// for a state without knowing which states exist, and a transition
		// leaves nothing behind claiming the old one.
		for _, candidateState := range []string{
			crispmetrics.ProjectionServing, crispmetrics.ProjectionFailed,
			crispmetrics.ProjectionStale, crispmetrics.ProjectionUnrouted,
		} {
			value := 0.0
			if candidateState == state {
				value = 1
			}
			crispmetrics.ProjectionState.WithLabelValues(name, resource, candidateState).Set(value)
		}
	}

	for _, name := range c.reportedStates {
		if !live[name] {
			crispmetrics.ProjectionState.DeletePartialMatch(map[string]string{"projection": name})
		}
	}
	c.reportedStates = c.reportedStates[:0]
	for name := range live {
		c.reportedStates = append(c.reportedStates, name)
	}
}

// recordProjectionEvents announces a projection changing state.
//
// Conditions say what the state is now, which is what a controller reconciling
// against it needs. An Event says that it changed and when, which is what
// kubectl describe shows and what anything watching for failures reacts to —
// and a condition reaches neither.
//
// Only on a change. A sync runs every time anything moves and a projection's
// state is usually the same as it was, so announcing it each time would bury
// the one that matters under thousands that do not. Kubernetes aggregates
// repeated Events, but aggregation is not a reason to send them.
func (c *Controller) recordProjectionEvents(
	candidates []projectionCandidate,
	failures map[string]error,
	stale map[string]struct{},
	unregistered map[schema.GroupVersion]error,
) {
	if c.recorder == nil {
		return
	}

	live := map[string]bool{}
	for _, cand := range candidates {
		// A projection loaded from a file has no object in the cluster to hang
		// an Event on.
		if cand.stored == nil {
			continue
		}
		name := cand.projection.Name
		live[name] = true

		kind, reason, message := projectionEvent(cand.projection, failures[name], stale, unregistered)
		if c.events[name] == reason+message {
			continue
		}
		c.events[name] = reason + message
		c.recorder.Event(cand.stored, kind, reason, message)
	}

	for name := range c.events {
		if !live[name] {
			delete(c.events, name)
		}
	}
}

// projectionEvent describes a projection's state as an Event.
func projectionEvent(
	p *crispv1alpha1.CustomResourceProjection,
	failure error,
	stale map[string]struct{},
	unregistered map[schema.GroupVersion]error,
) (kind, reason, message string) {
	if failure != nil {
		if _, servingPrevious := stale[p.Name]; servingPrevious {
			return corev1.EventTypeWarning, "ServingPreviousConfiguration",
				fmt.Sprintf("%s. The previously compiled configuration is still being served, "+
					"so requests succeed and the spec answering them is not the spec that was applied.", failure)
		}
		return corev1.EventTypeWarning, "CompilationFailed", failure.Error()
	}

	if err := registrationError(p, unregistered); err != nil && !errors.Is(err, errRegistrationPending) {
		return corev1.EventTypeWarning, "NotRouted", err.Error()
	}

	return corev1.EventTypeNormal, "Serving",
		fmt.Sprintf("Serving %s.%s", p.Spec.Resource.Plural, p.Spec.Resource.Group)
}

// projectionCandidate is one projection under consideration for installation,
// from the cluster or from a file.
type projectionCandidate struct {
	projection *crispv1alpha1.CustomResourceProjection

	// stored is nil for file-based projections, which have no object to write
	// status back to.
	stored *crispv1alpha1.CustomResourceProjection
}

// apiServiceOwners works out which CustomResourceProjections should own each
// registered APIService, so that removing them removes it.
//
// Without owner references an uninstall leaves the registrations behind:
// cluster-scoped objects pointing at a Service that no longer exists, which the
// aggregation layer goes on dialling and failing to reach. Owned, they are
// collected when the last projection behind them is — including when the CRD
// itself is deleted, which is what `kubectl delete -f manifests/` does and what
// takes every projection with it.
//
// A group version is left unowned if any projection serving it came from a file
// rather than the cluster. Kubernetes deletes a dependent once *all* its owners
// are gone, so owning it by only the cluster-backed ones would collect the
// registration while a file-based projection was still serving through it.
func (c *Controller) apiServiceOwners(
	candidates []projectionCandidate,
	resources []apidynamic.Resource,
) map[schema.GroupVersion][]metav1.OwnerReference {
	stored := make(map[string]*crispv1alpha1.CustomResourceProjection, len(candidates))
	for _, cand := range candidates {
		stored[cand.projection.Name] = cand.stored
	}

	owners := map[schema.GroupVersion][]metav1.OwnerReference{}
	unownable := map[schema.GroupVersion]bool{}
	seen := map[schema.GroupVersion]map[types.UID]bool{}

	for _, res := range resources {
		gv := res.GroupVersion()
		object, known := stored[res.ProjectionName]
		if !known || object == nil || object.UID == "" {
			unownable[gv] = true
			continue
		}
		if seen[gv] == nil {
			seen[gv] = map[types.UID]bool{}
		}
		if seen[gv][object.UID] {
			continue
		}
		seen[gv][object.UID] = true
		owners[gv] = append(owners[gv], metav1.OwnerReference{
			APIVersion: crispv1alpha1.SchemeGroupVersion.String(),
			Kind:       "CustomResourceProjection",
			Name:       object.Name,
			UID:        object.UID,
		})
	}

	for gv := range unownable {
		delete(owners, gv)
	}
	return owners
}

// registrationError picks out the registration failure that applies to one
// projection, if any.
//
// A projection can serve several group versions, and it is not being routed
// unless all of them are. The first failure is the one reported: listing them
// all would make the condition message unreadable without saying anything the
// first does not.
func registrationError(p *crispv1alpha1.CustomResourceProjection, unregistered map[schema.GroupVersion]error) error {
	if len(unregistered) == 0 {
		return nil
	}

	versions := []string{p.Spec.Resource.Version}
	for _, version := range p.Spec.Resource.Versions {
		if version.Served != nil && !*version.Served {
			continue
		}
		versions = append(versions, version.Name)
	}

	for _, version := range versions {
		gv := schema.GroupVersion{Group: p.Spec.Resource.Group, Version: version}
		if err, ok := unregistered[gv]; ok {
			return err
		}
	}
	return nil
}

func (c *Controller) updateStatus(
	ctx context.Context,
	p *crispv1alpha1.CustomResourceProjection,
	failure error,
	unreachable error,
	unregistered error,
	servingPrevious bool,
) error {
	conditions := append([]metav1.Condition(nil), p.Status.Conditions...)
	generation := p.Generation

	setStatus := func(conditionType string, status metav1.ConditionStatus, reason, message string) {
		apimeta.SetStatusCondition(&conditions, metav1.Condition{
			Type:               conditionType,
			Status:             status,
			ObservedGeneration: generation,
			Reason:             reason,
			Message:            message,
		})
	}

	set := func(conditionType string, ok bool, reason, message string) {
		status := metav1.ConditionFalse
		if ok {
			status = metav1.ConditionTrue
		}
		setStatus(conditionType, status, reason, message)
	}

	// Unknown is for a condition nothing has answered yet, as distinct from one
	// answered in the negative.
	setUnknown := func(conditionType, reason, message string) {
		setStatus(conditionType, metav1.ConditionUnknown, reason, message)
	}

	var servedPaths []string
	if failure == nil {
		servedPaths = []string{fmt.Sprintf("/apis/%s/%s/%s",
			p.Spec.Resource.Group, p.Spec.Resource.Version, p.Spec.Resource.Plural)}
		for _, version := range p.Spec.Resource.Versions {
			if version.Served != nil && !*version.Served {
				continue
			}
			servedPaths = append(servedPaths, fmt.Sprintf("/apis/%s/%s/%s",
				p.Spec.Resource.Group, version.Name, p.Spec.Resource.Plural))
		}

		set(crispv1alpha1.ConditionSchemaResolved, true, "SchemaAccepted", "Projected schema accepted.")

		if unreachable == nil {
			set(crispv1alpha1.ConditionDataSourceConnected, true, "Connected",
				fmt.Sprintf("Connected to the %s data source.", p.Spec.DataSource.Driver))
		} else {
			// Served but not answering: requests get 503 until it recovers.
			set(crispv1alpha1.ConditionDataSourceConnected, false, "Unreachable", unreachable.Error())
		}

		switch {
		case unregistered == nil:
			set(crispv1alpha1.ConditionRegistered, true, "Routed",
				"The aggregation layer is routing the projected group version here.")
			set(crispv1alpha1.ConditionReady, true, "Serving", fmt.Sprintf("Serving %s.", servedPaths[0]))

		case errors.Is(unregistered, errRegistrationPending):
			// Registered, but nothing has confirmed it routes yet. Everything
			// this server is responsible for is done, so Ready stands; the
			// Registered condition is where the wait is visible.
			setUnknown(crispv1alpha1.ConditionRegistered, "Pending", unregistered.Error())
			set(crispv1alpha1.ConditionReady, true, "Serving", fmt.Sprintf("Serving %s.", servedPaths[0]))

		default:
			// Compiled and installed, but nothing can reach it. Ready used to
			// be true here on the strength of the compile alone, so a
			// projection whose APIService could not be created — or whose
			// Service the aggregator could not dial — reported "Serving
			// /apis/..." while every request for it returned NotFound.
			set(crispv1alpha1.ConditionRegistered, false, "NotRouted", unregistered.Error())
			set(crispv1alpha1.ConditionReady, false, "NotRegistered",
				fmt.Sprintf("Compiled, but not reachable through the aggregation layer: %s", unregistered.Error()))
		}
	} else if servingPrevious {
		// The generation in hand did not compile, so Ready is false for it —
		// but the API group has not gone anywhere, and saying only
		// "CompilationFailed" would send someone looking for an outage that is
		// not happening.
		set(crispv1alpha1.ConditionReady, false, "ServingPreviousConfiguration",
			fmt.Sprintf("%s. The previously compiled configuration is still being served.", failure.Error()))
	} else {
		set(crispv1alpha1.ConditionReady, false, "CompilationFailed", failure.Error())
	}

	status := crispv1alpha1.CustomResourceProjectionStatus{
		ObservedGeneration: generation,
		Conditions:         conditions,
		ServedPaths:        servedPaths,
		// Reported whatever the projection's serving state, because a
		// projection that failed to compile because its table is missing is
		// exactly the one whose required schema someone wants to read.
		RequiredSchema: projection.RequiredSchema(p.Spec),
	}

	// Against what this controller last wrote when it has written one, and only
	// against the lister's copy on the first sync for this projection — where
	// there is nothing else to go on, and the copy cannot be lagging a write
	// this controller has not made.
	if previous, written := c.lastStatus[p.Name]; written {
		if statusUnchanged(previous, status) {
			return nil
		}
	} else if statusUnchanged(p.Status, status) {
		c.lastStatus[p.Name] = status
		return nil
	}

	// Re-read and retry on conflict rather than reporting it and moving on. A
	// conflict here is ordinary — the object is written by whoever applied it
	// and by this controller — and dropping the write leaves the projection
	// describing a state it is no longer in until something else happens to
	// queue a sync. Observed doing exactly that: a rollout wrote
	// Registered=False from a registration that had already recovered, the
	// retry conflicted, and the projection reported an outage that was over.
	client := c.client.CrispV1alpha1().CustomResourceProjections()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.Get(ctx, p.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		// The conditions were computed against the generation this sync saw. If
		// the object has moved on since, the next sync describes the new one
		// and this write has nothing useful to say about it.
		if current.Generation != p.Generation {
			return nil
		}
		current.Status = status
		_, err = client.UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return err
	}
	c.lastStatus[p.Name] = status
	return nil
}

// statusUnchanged reports whether writing the new status would be a no-op.
// Without this check every status write would wake the informer and queue
// another sync.
func statusUnchanged(old, new crispv1alpha1.CustomResourceProjectionStatus) bool {
	if old.ObservedGeneration != new.ObservedGeneration {
		return false
	}
	if len(old.Conditions) != len(new.Conditions) || len(old.ServedPaths) != len(new.ServedPaths) {
		return false
	}
	for i := range old.ServedPaths {
		if old.ServedPaths[i] != new.ServedPaths[i] {
			return false
		}
	}
	for _, want := range new.Conditions {
		got := apimeta.FindStatusCondition(old.Conditions, want.Type)
		if got == nil ||
			got.Status != want.Status ||
			got.Reason != want.Reason ||
			got.Message != want.Message ||
			got.ObservedGeneration != want.ObservedGeneration {
			return false
		}
	}

	// Compared too, or a projection whose schema changed under an unchanged
	// generation would go on reporting the old one. Both sides are built in a
	// fixed order, so equality here is a comparison of content rather than of
	// whichever order a map happened to produce.
	return apiequality.Semantic.DeepEqual(old.RequiredSchema, new.RequiredSchema)
}

// warnIfCacheUnshared reports projections whose read cache cannot be
// invalidated across replicas.
//
// A write drops the entries it could have invalidated in the replica that
// served it. The cache is in process, nothing connects the replicas, and a
// client cannot tell which one it reached — so a read after a write can be
// answered by a replica that never saw the write, from an entry older than it,
// for as long as the TTL. Nothing about the response says so: it is not
// malformed, it is old, which looks exactly like not having written yet.
//
// Writes are not exposed to this. The row a write is based on is always read
// from the database rather than from the cache, so a client acting on a stale
// read sends the older resourceVersion and is refused with a conflict.
//
// A warning rather than a refusal, and gated the same way as the unversioned
// one: caching on a single replica is exactly correct, and this server cannot
// count its peers — leader election being on is the operator saying there are
// some. Making it an error would break a valid deployment; making it silent is
// what it has been.
func (c *Controller) warnIfCacheUnshared(p *crispv1alpha1.CustomResourceProjection) {
	if !c.hasPeers || p == nil {
		return
	}

	// Cleared when the cache is turned off, so an alert stops firing when the
	// projection is changed rather than when the process restarts.
	if p.Spec.CacheTTL == nil || p.Spec.CacheTTL.Duration <= 0 {
		if c.warnedCacheUnshared[p.Name] {
			delete(c.warnedCacheUnshared, p.Name)
			crispmetrics.ProjectionsCacheUnshared.DeleteLabelValues(
				p.Name, p.Spec.Resource.Plural+"."+p.Spec.Resource.Group)
		}
		return
	}

	resource := p.Spec.Resource.Plural + "." + p.Spec.Resource.Group
	crispmetrics.ProjectionsCacheUnshared.WithLabelValues(p.Name, resource).Set(1)

	if c.warnedCacheUnshared[p.Name] {
		return
	}
	c.warnedCacheUnshared[p.Name] = true

	klog.InfoS("projection caches reads and this server has peers; a write is only "+
		"invalidated in the replica that served it, so a read routed to another can be "+
		"answered from an entry older than that write",
		"projection", p.Name, "resource", resource,
		"cacheTTL", p.Spec.CacheTTL.Duration.String(),
		"fix", "remove spec.cacheTTL, or run a single replica, where the invalidation is complete")
}

// warnAboutUnversionedProjections reports projections that cannot safely be
// served by more than one replica.
//
// The resourceVersion a list reports is derived from the data when the
// projection maps a version column, so every replica reading the same rows
// reports the same thing. Without one it falls back to a counter that belongs
// to this process — and two replicas then hand the same client versions that
// mean different things, so a watch resumed against the other replica either
// replays what the client has or skips what it does not.
//
// Documented as a limitation for a long time and checked by nothing, which is
// the worst combination: the failure is silent, intermittent, and looks like a
// client bug. It is a warning rather than a refusal because a single-replica
// deployment is perfectly valid and this server cannot count its own peers —
// leader election being on is the operator saying there are some.
//
// Warned once per projection rather than on every sync, since a sync happens
// every time anything changes and this would otherwise be most of the log.
func (c *Controller) warnIfUnversioned(p *crispv1alpha1.CustomResourceProjection) {
	if !c.hasPeers || p == nil {
		return
	}

	// Cleared when the projection is fixed. A gauge that can only be set is a
	// gauge that cannot report that the condition ended, so an alert on it
	// would fire until the process restarted — long after the resourceVersion
	// column was added.
	if p.Spec.Mapping.ResourceVersion != "" {
		if c.warnedUnversioned[p.Name] {
			delete(c.warnedUnversioned, p.Name)
			crispmetrics.ProjectionsUnversioned.DeleteLabelValues(
				p.Name, p.Spec.Resource.Plural+"."+p.Spec.Resource.Group)
		}
		return
	}

	resource := p.Spec.Resource.Plural + "." + p.Spec.Resource.Group
	crispmetrics.ProjectionsUnversioned.WithLabelValues(p.Name, resource).Set(1)

	if c.warnedUnversioned[p.Name] {
		return
	}
	c.warnedUnversioned[p.Name] = true

	klog.InfoS("projection maps no resourceVersion and this server has peers; "+
		"the version a list reports comes from a per-replica counter, so two replicas "+
		"give the same client versions that mean different things",
		"projection", p.Name, "resource", resource,
		"fix", "map a column that advances on every write to mapping.resourceVersion, "+
			"or run a single replica")
}

// resourceClaim identifies the API path a projection asks to serve. Two
// projections naming the same one is the conflict resolveClaims settles.
type resourceClaim struct {
	group   string
	version string
	plural  string
}

func (k resourceClaim) String() string {
	return fmt.Sprintf("%s.%s/%s", k.plural, k.group, k.version)
}

// resolveClaims decides which projection serves a resource that more than one
// claims, and returns an error for each projection that loses.
//
// The router refuses to install two storages at one path, and rightly: the
// second would silently replace the first, so a request for somebody's rows
// would be answered from somebody else's table. What it cannot do is say which
// projection should give way, so it failed the rebuild -- taking every
// unrelated projection down with the pair that disagreed.
//
// Losing is decided so that the answer does not move:
//
//   - A projection already serving the resource keeps it. Applying a
//     conflicting projection must not take a working API group away from the
//     one that had it; the mistake is in the object just applied, and that is
//     the object that should fail.
//   - Otherwise the older projection wins, which on a cold start usually
//     re-elects whoever was serving before the restart.
//   - Then the name, so that two created in the same instant still settle the
//     same way in every replica.
//
// A projection loses whole. One that claims two resources and conflicts on one
// of them serves neither: half a projection is a surface whose absent half
// looks like a projection that was never applied.
func resolveClaims(
	surviving map[string]compilation,
	created map[string]metav1.Time,
	serving map[string]compilation,
) map[string]error {
	names := make([]string, 0, len(surviving))
	for name := range surviving {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := names[i], names[j]
		if _, ai := serving[a]; ai != mapHas(serving, b) {
			return ai
		}
		at, bt := created[a], created[b]
		if !at.Equal(&bt) {
			return at.Before(&bt)
		}
		return a < b
	})

	claimed := make(map[resourceClaim]string, len(surviving))
	losses := map[string]error{}
	for _, name := range names {
		held := false
		for _, res := range surviving[name].resources {
			claim := resourceClaim{group: res.Group, version: res.Version, plural: res.Plural}
			if other, taken := claimed[claim]; taken {
				losses[name] = fmt.Errorf(
					"resource %s is claimed by projection %q as well, which serves it", claim, other)
				held = true
				break
			}
		}
		if held {
			continue
		}
		for _, res := range surviving[name].resources {
			claimed[resourceClaim{group: res.Group, version: res.Version, plural: res.Plural}] = name
		}
	}
	return losses
}

// mapHas keeps the comparison in resolveClaims' sort readable.
func mapHas(m map[string]compilation, name string) bool {
	_, ok := m[name]
	return ok
}
