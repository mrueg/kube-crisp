package projection

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	apidynamic "github.com/mrueg/kube-crisp/pkg/apiserver/dynamic"
	crispscheme "github.com/mrueg/kube-crisp/pkg/apiserver/scheme"
	crispclient "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned"
	crispfake "github.com/mrueg/kube-crisp/pkg/generated/clientset/versioned/fake"
	crispinformers "github.com/mrueg/kube-crisp/pkg/generated/informers/externalversions"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"

	_ "modernc.org/sqlite"
)

// stubResolver points every data source at one SQLite file, so the controller
// can be exercised without a database server. The path can change, which is how
// credential rotation is simulated.
type stubResolver struct {
	mu  sync.Mutex
	dsn string
}

func (r *stubResolver) set(dsn string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dsn = dsn
}

func (r *stubResolver) Resolve(context.Context, crispv1alpha1.DataSource) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dsn, nil
}

func newTestDB(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "bins.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening sqlite: %v", err)
	}
	defer db.Close()

	for _, stmt := range []string{
		`CREATE TABLE bins (id TEXT PRIMARY KEY, tenant TEXT NOT NULL)`,
		`INSERT INTO bins VALUES ('bin-1', 'acme')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seeding the database: %v", err)
		}
	}
	return path
}

// projectionObject builds a CustomResourceProjection as it would arrive from
// the API.
func projectionObject(name, plural string) *crispv1alpha1.CustomResourceProjection {
	return &crispv1alpha1.CustomResourceProjection{
		ObjectMeta: metav1.ObjectMeta{Name: name, Generation: 1},
		Spec: crispv1alpha1.CustomResourceProjectionSpec{
			DataSource: crispv1alpha1.DataSource{
				Driver:    "sqlite",
				SecretRef: crispv1alpha1.SecretReference{Name: name, Namespace: "kube-crisp"},
			},
			Resource: crispv1alpha1.ProjectedResource{
				Group:   "warehouse.example.com",
				Version: "v1alpha1",
				Kind:    "Bin",
				Plural:  plural,
				Scope:   crispv1alpha1.NamespaceScoped,
				Schema:  &apiextensionsv1.JSONSchemaProps{Type: "object"},
			},
			Queries: crispv1alpha1.Queries{
				List: crispv1alpha1.Query{
					SQL: "SELECT id, tenant FROM bins WHERE (:namespace IS NULL OR tenant = :namespace)",
				},
			},
			Mapping: crispv1alpha1.Mapping{Name: "id", Namespace: "tenant"},
		},
	}
}

type fixture struct {
	controller *Controller
	client     crispclient.Interface
	dynamic    dynamic.Interface
	router     *apidynamic.Router
	pools      *crispsql.PoolCache
	resolver   *stubResolver
}

// newFixture wires the controller against fake clients: the generated one for
// projections, and the dynamic one for the APIServices it manages.
func newFixture(t *testing.T, projections []runtime.Object, others ...runtime.Object) *fixture {
	t.Helper()
	return newFixtureWithEvents(t, nil, projections, others...)
}

// newFixtureWithEvents is newFixture with a client to record Events against.
func newFixtureWithEvents(
	t *testing.T,
	events kubernetes.Interface,
	projections []runtime.Object,
	others ...runtime.Object,
) *fixture {
	t.Helper()

	client := crispfake.NewSimpleClientset(projections...)
	dynamicClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			APIServiceGVR: "APIServiceList",
		},
		others...,
	)

	pools := crispsql.NewPoolCache()
	t.Cleanup(pools.Close)

	router := apidynamic.NewRouter(apidynamic.Options{NewScheme: crispscheme.New})
	resolver := &stubResolver{dsn: newTestDB(t)}
	compiler := &apidynamic.Compiler{
		Pools:    pools,
		Resolver: resolver,
	}

	factory := crispinformers.NewSharedInformerFactory(client, 0)
	controller := New(Options{
		Client:        client,
		EventClient:   events,
		DynamicClient: dynamicClient,
		Factory:       factory,
		Compiler:      compiler,
		Router:        router,
		Pools:         pools,
		APIServices: APIServiceOptions{
			Enabled:          true,
			ServiceName:      "kube-crisp-apiserver",
			ServiceNamespace: "kube-crisp",
			Port:             443,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), controller.informer.HasSynced) {
		t.Fatal("the projection cache never synced")
	}

	return &fixture{
		controller: controller,
		client:     client,
		dynamic:    dynamicClient,
		router:     router,
		pools:      pools,
		resolver:   resolver,
	}
}

// waitForSync runs sync until the lister has caught up with the expectation,
// since the informer observes changes asynchronously.
func (f *fixture) syncUntil(t *testing.T, want func() bool) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := f.controller.sync(context.Background()); err != nil {
			t.Fatalf("sync() returned error: %v", err)
		}
		if want() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the controller never reached the expected state")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (f *fixture) apiService(t *testing.T, name string) (*unstructured.Unstructured, bool) {
	t.Helper()

	obj, err := f.dynamic.Resource(APIServiceGVR).Get(context.Background(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("reading APIService %s: %v", name, err)
	}
	return obj, true
}

func servedPaths(router *apidynamic.Router) string { return fmt.Sprint(router.ServedPaths()) }

func TestSyncInstallsProjectionAndRegistersAPIService(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})

	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/bins]"; got != want {
		t.Errorf("served paths = %s, want %s", got, want)
	}

	apiService, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("no APIService was created for the projected group")
	}
	if got := apiService.GetLabels()[managedByLabel]; got != managedByValue {
		t.Errorf("APIService label = %q, want %q", got, managedByValue)
	}
	service, _, _ := unstructured.NestedMap(apiService.Object, "spec", "service")
	if service["name"] != "kube-crisp-apiserver" || service["namespace"] != "kube-crisp" {
		t.Errorf("APIService points at %v, want the configured service", service)
	}
}

func TestSyncReportsReadyStatus(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	obj, err := f.client.CrispV1alpha1().CustomResourceProjections().Get(context.Background(), "bins", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the projection: %v", err)
	}

	ready := apimeta.FindStatusCondition(obj.Status.Conditions, crispv1alpha1.ConditionReady)
	if ready == nil {
		t.Fatalf("no Ready condition in %v", obj.Status.Conditions)
	}
	if ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %v, want True", ready.Status)
	}
	if len(obj.Status.ServedPaths) == 0 {
		t.Error("the projection reports no served paths")
	}
}

func TestSyncRemovesDeletedProjection(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if err := f.client.CrispV1alpha1().CustomResourceProjections().
		Delete(context.Background(), "bins", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the projection: %v", err)
	}

	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 0 })

	if _, found := f.apiService(t, "v1alpha1.warehouse.example.com"); found {
		t.Error("the APIService outlived its projection")
	}
	// The connection pool must be released too, not merely unreferenced.
	if got := f.pools.Len(); got != 0 {
		t.Errorf("%d connection pools are still open, want 0", got)
	}
}

// TestSyncKeepsServingWhenOneProjectionIsBroken is the isolation property: one
// bad projection must not take the working ones down with it.
func TestSyncKeepsServingWhenOneProjectionIsBroken(t *testing.T) {
	broken := projectionObject("broken", "broken")
	// A namespaced projection with no namespace column cannot be compiled.
	broken.Spec.Mapping = crispv1alpha1.Mapping{Name: "id"}

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins"), broken})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/bins]"; got != want {
		t.Errorf("served paths = %s, want only the healthy projection %s", got, want)
	}

	obj, err := f.client.CrispV1alpha1().CustomResourceProjections().Get(context.Background(), "broken", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the broken projection: %v", err)
	}

	ready := apimeta.FindStatusCondition(obj.Status.Conditions, crispv1alpha1.ConditionReady)
	if ready == nil {
		t.Fatalf("no Ready condition in %v", obj.Status.Conditions)
	}
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready = %v, want False", ready.Status)
	}
	if ready.Reason != "CompilationFailed" {
		t.Errorf("reason = %q, want CompilationFailed", ready.Reason)
	}
}

// TestAPIServiceOwnedByAnotherControllerIsLeftAlone: adopting an APIService
// this server did not create could redirect an unrelated API to it.
func TestAPIServiceOwnedByAnotherControllerIsLeftAlone(t *testing.T) {
	foreign := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata": map[string]any{
			"name":   "v1alpha1.warehouse.example.com",
			"labels": map[string]any{"app.kubernetes.io/managed-by": "somebody-else"},
		},
		"spec": map[string]any{
			"group":   "warehouse.example.com",
			"version": "v1alpha1",
			"service": map[string]any{"name": "other", "namespace": "other", "port": int64(443)},
		},
	}}

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")}, foreign)
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	apiService, found := f.apiService(t, "v1alpha1.warehouse.example.com")
	if !found {
		t.Fatal("the foreign APIService disappeared")
	}
	service, _, _ := unstructured.NestedMap(apiService.Object, "spec", "service")
	if service["name"] != "other" {
		t.Errorf("the foreign APIService was rewritten to %v", service)
	}
}

// TestSyncReplacesPoolWhenCredentialsRotate covers credential rotation: the
// pool is keyed by the connection string, so a rotated Secret produces a new
// pool and the old one is released rather than lingering with dead
// credentials.
func TestSyncReplacesPoolWhenCredentialsRotate(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if got := f.pools.Len(); got != 1 {
		t.Fatalf("%d pools are open, want 1", got)
	}

	// The Secret now resolves to a different database.
	rotated := newTestDB(t)
	f.resolver.set(rotated)

	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() after rotation returned error: %v", err)
	}

	if got := f.pools.Len(); got != 1 {
		t.Fatalf("%d pools are open after rotation, want 1: the old pool was not released", got)
	}
	if len(f.router.ServedPaths()) != 1 {
		t.Fatal("the projection stopped serving after the credentials rotated")
	}
}

// TestDegradedReportsUnservedProjections covers the signal that distinguishes
// "serving everything" from "serving some of it": without it, a server with
// half its projections broken looks entirely healthy.
func TestDegradedReportsUnservedProjections(t *testing.T) {
	broken := projectionObject("broken", "broken")
	broken.Spec.Mapping = crispv1alpha1.Mapping{Name: "id"}

	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins"), broken})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	degraded := f.controller.Degraded()
	if len(degraded) != 1 || degraded[0] != "broken" {
		t.Fatalf("Degraded() = %v, want [broken]", degraded)
	}

	// The healthy projection is still served, which is why this is a separate
	// signal rather than a readiness failure.
	if got := len(f.router.ServedPaths()); got != 1 {
		t.Errorf("served paths = %d, want the healthy projection to keep serving", got)
	}
}

func TestDegradedIsEmptyWhenEverythingServes(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if degraded := f.controller.Degraded(); len(degraded) != 0 {
		t.Errorf("Degraded() = %v, want nothing", degraded)
	}
}

// TestSecretChangeQueuesASync covers credential rotation: a changed Secret has
// to reach the controller when it changes, not at the next resync.
func TestSecretChangeQueuesASync(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-db", Namespace: "kube-crisp"},
		Data:       map[string][]byte{"dsn": []byte("postgres://old")},
	}

	kube := k8sfake.NewSimpleClientset(secret)
	factory := informers.NewSharedInformerFactoryWithOptions(kube, 0, informers.WithNamespace("kube-crisp"))
	secrets := factory.Core().V1().Secrets().Informer()

	controller := New(Options{
		Client:          crispfake.NewSimpleClientset(),
		Factory:         crispinformers.NewSharedInformerFactory(crispfake.NewSimpleClientset(), 0),
		SecretInformers: []cache.SharedIndexInformer{secrets},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), secrets.HasSynced) {
		t.Fatal("the Secret cache never synced")
	}

	// The initial listing enqueues too; take that one out of the way so the
	// next item can only have come from the change below.
	take(t, controller)

	rotated := secret.DeepCopy()
	rotated.Data["dsn"] = []byte("postgres://new")
	if _, err := kube.CoreV1().Secrets("kube-crisp").Update(ctx, rotated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("rotating the credential: %v", err)
	}

	take(t, controller)
}

// TestSecretResyncDoesNotQueueASync keeps the watch from turning every relist
// into a rebuild of the whole API surface.
func TestSecretResyncDoesNotQueueASync(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-db", Namespace: "kube-crisp"},
		Data:       map[string][]byte{"dsn": []byte("postgres://old")},
	}

	kube := k8sfake.NewSimpleClientset(secret)
	factory := informers.NewSharedInformerFactoryWithOptions(kube, 0, informers.WithNamespace("kube-crisp"))
	secrets := factory.Core().V1().Secrets().Informer()

	controller := New(Options{
		Client:          crispfake.NewSimpleClientset(),
		Factory:         crispinformers.NewSharedInformerFactory(crispfake.NewSimpleClientset(), 0),
		SecretInformers: []cache.SharedIndexInformer{secrets},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), secrets.HasSynced) {
		t.Fatal("the Secret cache never synced")
	}
	take(t, controller)

	// Same data, different labels: nothing a projection reads has changed.
	relabelled := secret.DeepCopy()
	relabelled.Labels = map[string]string{"note": "touched"}
	if _, err := kube.CoreV1().Secrets("kube-crisp").Update(ctx, relabelled, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating the Secret: %v", err)
	}

	// Give the handler a chance to run before concluding it did nothing.
	time.Sleep(200 * time.Millisecond)
	if got := controller.queue.Len(); got != 0 {
		t.Errorf("a Secret update that changed no credential queued %d syncs", got)
	}
}

// take waits for one item on the controller's queue, failing rather than
// hanging when nothing arrives.
func take(t *testing.T, c *Controller) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		key, shutdown := c.queue.Get()
		if !shutdown {
			c.queue.Done(key)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a sync to be queued")
	}
}

// TestSyncKeepsStorageForUnchangedProjections is a regression test. Every sync
// used to recompile every projection, which built fresh storage — an empty
// watch cache, an empty read cache, and every watcher relisting — for a
// projection nothing had touched. A ten-minute resync did it to all of them.
func TestSyncKeepsStorageForUnchangedProjections(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("orders", "orders")})

	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })
	first := f.controller.compiled["orders"]
	if first.resources == nil {
		t.Fatal("nothing was recorded as compiled")
	}

	// Nothing changed, so nothing should be rebuilt.
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	second := f.controller.compiled["orders"]

	if second.fingerprint != first.fingerprint {
		t.Errorf("fingerprint changed with no change to the projection: %s -> %s",
			first.fingerprint, second.fingerprint)
	}
	if &second.resources[0].Storage != &first.resources[0].Storage &&
		second.resources[0].Storage != first.resources[0].Storage {
		t.Error("the projection was recompiled, so its watch cache and every watcher were dropped for nothing")
	}
}

// TestSyncRecompilesWhenCredentialsRotate is the other half: the storage is
// bound to a pool, so a rotated connection string does have to rebuild it.
func TestSyncRecompilesWhenCredentialsRotate(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("orders", "orders")})

	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })
	before := f.controller.compiled["orders"]

	f.resolver.set(newTestDB(t))
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	after := f.controller.compiled["orders"]

	if after.fingerprint == before.fingerprint {
		t.Fatal("the fingerprint survived a credential rotation, so the projection kept its old pool")
	}
	if after.resources[0].Storage == before.resources[0].Storage {
		t.Error("the storage was reused after a rotation, so it is still reading through the old connection")
	}
}

// TestSyncReleasesStorageOfDeletedProjections checks the teardown that keeps a
// removed projection from polling its table forever with nobody listening.
func TestSyncReleasesStorageOfDeletedProjections(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("orders", "orders")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if err := f.client.CrispV1alpha1().CustomResourceProjections().
		Delete(context.Background(), "orders", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the projection: %v", err)
	}
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 0 })

	if _, still := f.controller.compiled["orders"]; still {
		t.Error("a deleted projection is still recorded as compiled, so its storage was never released")
	}
}

// countingIndexer records how often the reconciler consults the cache, so a
// test can tell a cached read from one that went to the API server.
type countingIndexer struct {
	cache.Indexer
	gets  int
	lists int
}

func (i *countingIndexer) GetByKey(key string) (any, bool, error) {
	i.gets++
	return i.Indexer.GetByKey(key)
}

func (i *countingIndexer) List() []any {
	i.lists++
	return i.Indexer.List()
}

// TestAPIServiceReconcileReadsThroughTheCache: reconciling used to make one
// request per served group version plus a list, on every sync, for objects this
// server already watches.
func TestAPIServiceReconcileReadsThroughTheCache(t *testing.T) {
	indexer := &countingIndexer{
		Indexer: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{}),
	}

	manager := newAPIServiceManager(
		dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
			runtime.NewScheme(),
			map[schema.GroupVersionResource]string{APIServiceGVR: "APIServiceList"},
		),
		APIServiceOptions{
			Enabled:          true,
			ServiceName:      "kube-crisp-apiserver",
			ServiceNamespace: "kube-crisp",
			Port:             443,
		},
		indexer,
	)

	resources := []apidynamic.Resource{{Group: "store.example.com", Version: "v1alpha1", Plural: "orders"}}
	if _, err := manager.reconcile(context.Background(), resources, nil); err != nil {
		t.Fatalf("reconcile() returned error: %v", err)
	}

	if indexer.gets == 0 {
		t.Error("the reconciler never consulted the cache, so it read through the client")
	}
	if indexer.lists == 0 {
		t.Error("pruning never consulted the cache, so it listed through the client")
	}

	// The cache was empty, so the registration has to have been created.
	created, err := manager.client.Resource(APIServiceGVR).
		Get(context.Background(), "v1alpha1.store.example.com", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the APIService was not created: %v", err)
	}
	if created.GetLabels()[managedByLabel] != managedByValue {
		t.Error("the created APIService is not labelled as managed by kube-crisp")
	}
}

// TestAPIServiceReconcileLeavesForeignRegistrationsAlone: an APIService this
// server did not create must survive a cached read the same way it survived an
// uncached one.
func TestAPIServiceReconcileLeavesForeignRegistrationsAlone(t *testing.T) {
	foreign := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiregistration.k8s.io/v1",
		"kind":       "APIService",
		"metadata": map[string]any{
			"name":   "v1alpha1.store.example.com",
			"labels": map[string]any{"app.kubernetes.io/managed-by": "someone-else"},
		},
		"spec": map[string]any{"group": "store.example.com", "version": "v1alpha1"},
	}}

	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(foreign); err != nil {
		t.Fatalf("seeding the cache: %v", err)
	}

	manager := newAPIServiceManager(
		dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
			runtime.NewScheme(),
			map[schema.GroupVersionResource]string{APIServiceGVR: "APIServiceList"},
			foreign,
		),
		APIServiceOptions{Enabled: true, ServiceName: "kube-crisp-apiserver", ServiceNamespace: "kube-crisp", Port: 443},
		indexer,
	)

	resources := []apidynamic.Resource{{Group: "store.example.com", Version: "v1alpha1", Plural: "orders"}}
	if _, err := manager.reconcile(context.Background(), resources, nil); err != nil {
		t.Fatalf("reconcile() returned error: %v", err)
	}

	after, err := manager.client.Resource(APIServiceGVR).
		Get(context.Background(), "v1alpha1.store.example.com", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the APIService: %v", err)
	}
	if after.GetLabels()["app.kubernetes.io/managed-by"] != "someone-else" {
		t.Error("an APIService owned by someone else was taken over")
	}
	if _, found, _ := unstructured.NestedMap(after.Object, "spec", "service"); found {
		t.Error("an APIService owned by someone else was rewritten to point here")
	}
}

// brokenResolver is a data source Secret read that fails, as it would during an
// API server blip or against a Secret that is momentarily missing.
type brokenResolver struct{}

func (brokenResolver) Resolve(context.Context, crispv1alpha1.DataSource) (string, error) {
	return "", fmt.Errorf("reading secret kube-crisp/bins-db: etcdserver: request timed out")
}

// TestSyncKeepsServingWhenCompilationStopsWorking is the resilience contract: a
// projection that is still in the cluster and stops compiling keeps serving
// what it last compiled to.
//
// Treating a failed compile as a deletion would withdraw the API group, delete
// its APIService, and take discovery, RBAC, and every controller watching it
// down — over a Secret read that failed once. That is the same outcome an
// unreachable database is deliberately spared.
func TestSyncKeepsServingWhenCompilationStopsWorking(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	const apiServiceName = "v1alpha1.warehouse.example.com"
	if _, found := f.apiService(t, apiServiceName); !found {
		t.Fatalf("no APIService after the first sync; served = %s", servedPaths(f.router))
	}

	// The projection itself is untouched. Only resolving its data source breaks.
	f.controller.compiler.Resolver = brokenResolver{}
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}

	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/bins]"; got != want {
		t.Errorf("served paths = %s, want the group still served %s", got, want)
	}
	if _, found := f.apiService(t, apiServiceName); !found {
		t.Error("the APIService was deleted by a failure to recompile")
	}

	// Still serving is not still healthy, and the difference has to be visible.
	if degraded := f.controller.Degraded(); len(degraded) != 1 || degraded[0] != "bins" {
		t.Errorf("Degraded() = %v, want [bins]", degraded)
	}

	obj, err := f.client.CrispV1alpha1().CustomResourceProjections().Get(context.Background(), "bins", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the projection: %v", err)
	}
	ready := apimeta.FindStatusCondition(obj.Status.Conditions, crispv1alpha1.ConditionReady)
	if ready == nil {
		t.Fatalf("no Ready condition in %v", obj.Status.Conditions)
	}
	if ready.Status != metav1.ConditionFalse {
		t.Errorf("Ready = %v, want False for the generation that did not compile", ready.Status)
	}
	if ready.Reason != "ServingPreviousConfiguration" {
		t.Errorf("reason = %q, want ServingPreviousConfiguration", ready.Reason)
	}
}

// TestSyncRecoversWhenCompilationStartsWorkingAgain: the fallback is not a
// one-way door. Once the data source resolves again the projection recompiles
// and reports itself ready.
func TestSyncRecoversWhenCompilationStartsWorkingAgain(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	working := f.controller.compiler.Resolver
	f.controller.compiler.Resolver = brokenResolver{}
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() with a broken resolver: %v", err)
	}
	if len(f.controller.Degraded()) != 1 {
		t.Fatalf("Degraded() = %v, want the projection reported broken", f.controller.Degraded())
	}

	f.controller.compiler.Resolver = working
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() after recovery: %v", err)
	}

	if degraded := f.controller.Degraded(); len(degraded) != 0 {
		t.Errorf("Degraded() = %v, want empty once the data source resolves again", degraded)
	}
	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/bins]"; got != want {
		t.Errorf("served paths = %s, want %s", got, want)
	}

	obj, err := f.client.CrispV1alpha1().CustomResourceProjections().Get(context.Background(), "bins", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the projection: %v", err)
	}
	ready := apimeta.FindStatusCondition(obj.Status.Conditions, crispv1alpha1.ConditionReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %v, want True after recovery", ready)
	}
}

// TestSyncDropsAProjectionThatIsActuallyDeleted guards the other side of
// keepPrevious: a projection removed from the cluster really does lose its
// API group and its registration, rather than being kept alive by the fallback.
func TestSyncDropsAProjectionThatIsActuallyDeleted(t *testing.T) {
	f := newFixture(t, []runtime.Object{projectionObject("bins", "bins")})
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 1 })

	if err := f.client.CrispV1alpha1().CustomResourceProjections().
		Delete(context.Background(), "bins", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the projection: %v", err)
	}
	f.syncUntil(t, func() bool { return len(f.router.ServedPaths()) == 0 })

	if _, found := f.apiService(t, "v1alpha1.warehouse.example.com"); found {
		t.Error("the APIService outlived the projection it was registered for")
	}
}

// writeStaticProjection puts one projection manifest in a directory.
func writeStaticProjection(t *testing.T, dir, name, plural string) {
	t.Helper()

	manifest := fmt.Sprintf(`apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: %s
spec:
  dataSource:
    driver: sqlite
    secretRef: {name: bins-db, namespace: kube-crisp}
  resource:
    group: warehouse.example.com
    version: v1alpha1
    kind: Bin
    plural: %s
    scope: Namespaced
    schema:
      type: object
  queries:
    list:
      sql: SELECT id, tenant FROM bins WHERE tenant = :namespace
  mapping:
    name: id
    namespace: tenant
`, name, plural)

	if err := os.WriteFile(filepath.Join(dir, name+".yaml"), []byte(manifest), 0o600); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
}

// TestStaticProjectionsAreRereadOnEverySync: a file-backed projection used to
// be read once at startup, which made --projection-dir the only part of the
// configuration that needed a restart to change.
func TestStaticProjectionsAreRereadOnEverySync(t *testing.T) {
	dir := t.TempDir()
	writeStaticProjection(t, dir, "bins", "bins")

	f := newFixture(t, nil)
	f.controller.staticDir = dir

	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/bins]"; got != want {
		t.Fatalf("served paths = %s, want %s", got, want)
	}

	// A second file appears without anything restarting.
	writeStaticProjection(t, dir, "crates", "crates")
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	if got := len(f.router.ServedPaths()); got != 2 {
		t.Errorf("%d paths are served after a projection was added: %s", got, servedPaths(f.router))
	}

	// And removing one takes its API group away again.
	if err := os.Remove(filepath.Join(dir, "crates.yaml")); err != nil {
		t.Fatalf("removing crates.yaml: %v", err)
	}
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	if got, want := servedPaths(f.router), "[/apis/warehouse.example.com/v1alpha1/bins]"; got != want {
		t.Errorf("served paths = %s, want %s", got, want)
	}
}

// TestABrokenFileKeepsTheLastGoodProjections: saving a half-written file must
// not take every file-backed projection out of service. The same reasoning that
// keeps a projection serving when it fails to recompile.
func TestABrokenFileKeepsTheLastGoodProjections(t *testing.T) {
	dir := t.TempDir()
	writeStaticProjection(t, dir, "bins", "bins")

	f := newFixture(t, nil)
	f.controller.staticDir = dir

	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	before := servedPaths(f.router)

	if err := os.WriteFile(filepath.Join(dir, "bins.yaml"), []byte("this: is: not: yaml\n"), 0o600); err != nil {
		t.Fatalf("writing a broken file: %v", err)
	}
	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}

	if got := servedPaths(f.router); got != before {
		t.Errorf("served paths = %s after a broken edit, want the last good %s", got, before)
	}
}

// TestNoStaticDirLeavesTheGivenProjectionsAlone keeps the other path intact:
// projections handed in directly are not re-read from anywhere.
func TestNoStaticDirLeavesTheGivenProjectionsAlone(t *testing.T) {
	f := newFixture(t, nil)
	f.controller.static = []crispv1alpha1.CustomResourceProjection{*projectionObject("bins", "bins")}

	if err := f.controller.sync(context.Background()); err != nil {
		t.Fatalf("sync() returned error: %v", err)
	}
	if got := len(f.router.ServedPaths()); got != 1 {
		t.Errorf("%d paths are served, want the one that was handed in", got)
	}
}
