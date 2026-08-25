//go:build e2e

// Package e2e exercises kube-crisp against a real cluster: a kind cluster
// running kube-crisp and PostgreSQL, brought up by hack/e2e-up.sh.
package e2e

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	restConfig      *rest.Config
	dynamicClient   dynamic.Interface
	discoveryClient *discovery.DiscoveryClient

	ordersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "orders"}
	benchGVR  = schema.GroupVersionResource{Group: "bench.example.com", Version: "v1alpha1", Resource: "benchorders"}
	crpGVR    = schema.GroupVersionResource{Group: "crisp.kubecrisp.io", Version: "v1alpha1", Resource: "customresourceprojections"}

	// jsonOrdersGVR is the same table projected through json_agg.
	jsonOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "jsonorders"}

	apiServiceGVR = schema.GroupVersionResource{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"}

	// widgetsGVR is projected from MySQL, itemsGVR from a SQLite file.
	widgetsGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "widgets"}
	itemsGVR   = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "items"}

	// The same orders through a second version, and through projections that
	// borrow a schema and cache reads. The concurrency-capped projection is
	// addressed by path, since that test issues raw requests.
	ordersBetaGVR     = schema.GroupVersionResource{Group: "store.example.com", Version: "v1beta1", Resource: "orders"}
	borrowedOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "borrowedorders"}
	cachedOrdersGVR   = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "cachedorders"}
	scalableOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "scalableorders"}
	auditedOrdersGVR  = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "auditedorders"}
	securedOrdersGVR  = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "securedorders"}
	shipmentsGVR      = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "shipments"}

	// The same benchmark collection from the other two drivers, so the
	// comparison is between backends rather than between datasets.
	mysqlOrdersGVR  = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "mysqlorders"}
	sqliteOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "sqliteorders"}

	// kubeconfigPath is kept so tests can shell out to kubectl.
	kubeconfigPath string
)

const (
	benchNamespace = "bench"
	acmeNamespace  = "acme"
)

// event is one informer callback, recorded so a test can assert on ordering.
type event struct {
	verb string
	name string
}

func TestMain(m *testing.M) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		wd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "determining working directory: %v\n", err)
			os.Exit(1)
		}
		kubeconfig = filepath.Join(wd, "..", "..", "hack", ".e2e-kubeconfig")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading kubeconfig %s: %v\nRun hack/e2e-up.sh first.\n", kubeconfig, err)
		os.Exit(1)
	}
	// The comparison is only meaningful if client-side throttling never kicks in.
	cfg.QPS = 500
	cfg.Burst = 1000

	kubeconfigPath = kubeconfig
	restConfig = cfg
	if dynamicClient, err = dynamic.NewForConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "building dynamic client: %v\n", err)
		os.Exit(1)
	}
	if discoveryClient, err = discovery.NewDiscoveryClientForConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "building discovery client: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestProjectedListAndGet(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing orders: %v", err)
	}
	if got, want := len(list.Items), 2; got != want {
		t.Fatalf("listed %d orders in %s, want %d", got, acmeNamespace, want)
	}

	order, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).Get(ctx, "order-acme-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting order-acme-1: %v", err)
	}

	customer, found, err := unstructured.NestedString(order.Object, "spec", "customer")
	if err != nil || !found {
		t.Fatalf("spec.customer missing: %v", err)
	}
	if customer != "ada" {
		t.Errorf("spec.customer = %q, want %q", customer, "ada")
	}

	total, found, err := unstructured.NestedInt64(order.Object, "spec", "totalCents")
	if err != nil || !found {
		t.Fatalf("spec.totalCents missing: %v", err)
	}
	if total != 4999 {
		t.Errorf("spec.totalCents = %d, want 4999", total)
	}

	if got, want := order.GetAPIVersion(), "store.example.com/v1alpha1"; got != want {
		t.Errorf("apiVersion = %q, want %q", got, want)
	}
	if got, want := order.GetLabels()["store.example.com/status"], "shipped"; got != want {
		t.Errorf("status label = %q, want %q", got, want)
	}
}

func TestProjectedNamespaceIsolation(t *testing.T) {
	ctx := context.Background()

	// order-acme-1 exists, but in the acme tenant.
	_, err := dynamicClient.Resource(ordersGVR).Namespace(benchNamespace).Get(ctx, "order-acme-1", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("cross-namespace get error = %v, want NotFound", err)
	}
}

func TestProjectedLabelSelector(t *testing.T) {
	ctx := context.Background()

	list, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "store.example.com/status=shipped",
	})
	if err != nil {
		t.Fatalf("listing with selector: %v", err)
	}
	if got, want := len(list.Items), 1; got != want {
		t.Fatalf("selector returned %d orders, want %d", got, want)
	}
	if got, want := list.Items[0].GetName(), "order-acme-1"; got != want {
		t.Errorf("order = %q, want %q", got, want)
	}
}

func TestProjectionReportsReadyStatus(t *testing.T) {
	ctx := context.Background()

	err := wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		p, err := dynamicClient.Resource(crpGVR).Get(ctx, "orders", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		conditions, found, err := unstructured.NestedSlice(p.Object, "status", "conditions")
		if err != nil || !found {
			// Status has not been written yet; keep polling.
			return false, nil //nolint:nilerr // waiting for status to appear
		}
		for _, raw := range conditions {
			c, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if c["type"] == "Ready" && c["status"] == "True" {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("projection never reported Ready: %v", err)
	}
}

// TestDynamicInstallAndRemoval is the point of the watch-driven path: a
// projection created after the server started must begin serving without a
// restart, and must stop serving when it is deleted.
func TestDynamicInstallAndRemoval(t *testing.T) {
	ctx := context.Background()

	projection := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "crisp.kubecrisp.io/v1alpha1",
		"kind":       "CustomResourceProjection",
		"metadata":   map[string]any{"name": "e2e-customers"},
		"spec": map[string]any{
			"dataSource": map[string]any{
				"driver":    "postgres",
				"dsnKey":    "dsn",
				"secretRef": map[string]any{"name": "orders-db", "namespace": "kube-crisp"},
			},
			"resource": map[string]any{
				"group":   "store.example.com",
				"version": "v1alpha1",
				"kind":    "Customer",
				"plural":  "customers",
				"scope":   "Namespaced",
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"spec": map[string]any{
							"type":       "object",
							"properties": map[string]any{"orders": map[string]any{"type": "integer", "format": "int64"}},
						},
					},
				},
			},
			"queries": map[string]any{
				"list": map[string]any{
					"sql": "SELECT customer AS name, tenant, COUNT(*) AS orders FROM orders " +
						"WHERE tenant = :namespace GROUP BY customer, tenant ORDER BY customer LIMIT COALESCE(:limit, 1000)",
				},
			},
			"mapping": map[string]any{
				"name":      "name",
				"namespace": "tenant",
				"fields": []any{
					map[string]any{"column": "orders", "path": "spec.orders", "type": "integer"},
				},
			},
		},
	}}

	customersGVR := schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "customers"}

	if _, err := dynamicClient.Resource(crpGVR).Create(ctx, projection, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating projection: %v", err)
	}
	t.Cleanup(func() {
		_ = dynamicClient.Resource(crpGVR).Delete(context.Background(), "e2e-customers", metav1.DeleteOptions{})
	})

	// The new resource must appear without restarting the server.
	err := wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		list, err := dynamicClient.Resource(customersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			// The projection is not being served yet; keep polling.
			return false, nil //nolint:nilerr // waiting for the API to appear
		}
		return len(list.Items) > 0, nil
	})
	if err != nil {
		t.Fatalf("projection never started serving: %v", err)
	}

	list, err := dynamicClient.Resource(customersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing customers: %v", err)
	}
	if got, want := len(list.Items), 2; got != want {
		t.Fatalf("listed %d customers, want %d", got, want)
	}

	// Discovery must advertise it too.
	resources, err := discoveryClient.ServerResourcesForGroupVersion("store.example.com/v1alpha1")
	if err != nil {
		t.Fatalf("discovering group version: %v", err)
	}
	var advertised bool
	for _, r := range resources.APIResources {
		if r.Name == "customers" {
			advertised = true
		}
	}
	if !advertised {
		t.Errorf("customers not advertised in discovery: %+v", resources.APIResources)
	}

	// Deleting the projection must take the API with it.
	if err := dynamicClient.Resource(crpGVR).Delete(ctx, "e2e-customers", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting projection: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := dynamicClient.Resource(customersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
		return err != nil, nil
	})
	if err != nil {
		t.Fatalf("projection kept serving after deletion: %v", err)
	}
}

// TestProjectedWriteRoundTrip exercises the write path end to end: an object
// created through the Kubernetes API must become a row in PostgreSQL, and the
// subsequent read must reflect it.
func TestProjectedWriteRoundTrip(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	const name = "order-e2e-write"
	t.Cleanup(func() {
		_ = orders.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"customer": "linus", "totalCents": int64(2500)},
		"status":     map[string]any{"phase": "pending"},
	}}

	created, err := orders.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating order: %v", err)
	}
	if got, want := created.GetName(), name; got != want {
		t.Errorf("created name = %q, want %q", got, want)
	}
	if customer, _, _ := unstructured.NestedString(created.Object, "spec", "customer"); customer != "linus" {
		t.Errorf("created spec.customer = %q, want %q", customer, "linus")
	}

	// Creating the same object twice must conflict, exactly as it would for a
	// native resource.
	if _, err := orders.Create(ctx, obj, metav1.CreateOptions{}); !apierrors.IsAlreadyExists(err) {
		t.Errorf("duplicate create error = %v, want AlreadyExists", err)
	}

	fetched, err := orders.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the created order: %v", err)
	}
	if total, _, _ := unstructured.NestedInt64(fetched.Object, "spec", "totalCents"); total != 2500 {
		t.Errorf("spec.totalCents = %d, want 2500", total)
	}

	updated := fetched.DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, int64(9900), "spec", "totalCents"); err != nil {
		t.Fatalf("setting spec.totalCents: %v", err)
	}

	result, err := orders.Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("updating order: %v", err)
	}
	if total, _, _ := unstructured.NestedInt64(result.Object, "spec", "totalCents"); total != 9900 {
		t.Errorf("updated spec.totalCents = %d, want 9900", total)
	}

	// This projection enables the status subresource, so the phase is written
	// through /status rather than alongside the spec.
	statusWrite := result.DeepCopy()
	if err := unstructured.SetNestedField(statusWrite.Object, "shipped", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	if _, err := orders.UpdateStatus(ctx, statusWrite, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating status: %v", err)
	}

	// The update must be visible to a fresh read, which proves it reached the
	// database rather than only the response.
	reread, err := orders.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-reading the order: %v", err)
	}
	if phase, _, _ := unstructured.NestedString(reread.Object, "status", "phase"); phase != "shipped" {
		t.Errorf("status.phase = %q, want %q", phase, "shipped")
	}
	if got, want := reread.GetLabels()["store.example.com/status"], "shipped"; got != want {
		t.Errorf("status label = %q, want %q", got, want)
	}

	if err := orders.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting order: %v", err)
	}
	if _, err := orders.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("get after delete error = %v, want NotFound", err)
	}
	if err := orders.Delete(ctx, name, metav1.DeleteOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("second delete error = %v, want NotFound", err)
	}
}

// TestProjectedListPagination checks that a limited list hands out a continue
// token instead of silently truncating the collection.
func TestProjectedListPagination(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(benchNamespace)

	first, err := orders.List(ctx, metav1.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("listing first page: %v", err)
	}
	if got, want := len(first.Items), 100; got != want {
		t.Fatalf("first page held %d items, want %d", got, want)
	}
	if first.GetContinue() == "" {
		t.Fatal("first page carried no continue token")
	}

	second, err := orders.List(ctx, metav1.ListOptions{Limit: 100, Continue: first.GetContinue()})
	if err != nil {
		t.Fatalf("listing second page: %v", err)
	}
	if got, want := len(second.Items), 100; got != want {
		t.Fatalf("second page held %d items, want %d", got, want)
	}
	if first.Items[0].GetName() == second.Items[0].GetName() {
		t.Error("second page repeated the first page")
	}
}

// TestJSONAggregationProjectionMatchesRows checks that the json_agg projection
// returns exactly what the row-scanning projection returns.
func TestJSONAggregationProjectionMatchesRows(t *testing.T) {
	ctx := context.Background()

	rows, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing orders: %v", err)
	}
	aggregated, err := dynamicClient.Resource(jsonOrdersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing jsonorders: %v", err)
	}

	if len(rows.Items) != len(aggregated.Items) {
		t.Fatalf("row scan returned %d items, JSON aggregation returned %d", len(rows.Items), len(aggregated.Items))
	}

	for i := range rows.Items {
		rowSpec, _, _ := unstructured.NestedMap(rows.Items[i].Object, "spec")
		aggSpec, _, _ := unstructured.NestedMap(aggregated.Items[i].Object, "spec")

		if rows.Items[i].GetName() != aggregated.Items[i].GetName() {
			t.Errorf("item %d: names differ: %q versus %q", i, rows.Items[i].GetName(), aggregated.Items[i].GetName())
		}
		if fmt.Sprint(rowSpec) != fmt.Sprint(aggSpec) {
			t.Errorf("item %d: specs differ:\n rows: %v\n json: %v", i, rowSpec, aggSpec)
		}
	}
}

// TestInformerReceivesWatchEvents is the reason watch exists: a standard
// client-go informer must be able to build and maintain a cache of projected
// objects, which requires LIST and WATCH to agree.
func TestInformerReceivesWatchEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)
	const name = "order-e2e-informer"

	t.Cleanup(func() {
		_ = orders.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	events := make(chan event, 64)

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		dynamicClient, 0, acmeNamespace, nil)
	informer := factory.ForResource(ordersGVR).Informer()

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			events <- event{"add", obj.(*unstructured.Unstructured).GetName()}
		},
		UpdateFunc: func(_, obj any) {
			events <- event{"update", obj.(*unstructured.Unstructured).GetName()}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			events <- event{"delete", obj.(*unstructured.Unstructured).GetName()}
		},
	}); err != nil {
		t.Fatalf("registering event handler: %v", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		t.Fatal("informer cache never synced")
	}

	// The initial sync must have found the seeded rows.
	if got := len(informer.GetStore().List()); got < 2 {
		t.Fatalf("informer cache held %d objects after sync, want at least 2", got)
	}
	drain(events)

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(100)},
		"status":     map[string]any{"phase": "pending"},
	}}
	if _, err := orders.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating order: %v", err)
	}
	awaitEvent(t, events, "add", name)

	created, err := orders.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the created order: %v", err)
	}
	if err := unstructured.SetNestedField(created.Object, int64(555), "spec", "totalCents"); err != nil {
		t.Fatalf("setting spec.totalCents: %v", err)
	}
	if _, err := orders.Update(ctx, created, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating order: %v", err)
	}
	awaitEvent(t, events, "update", name)

	if err := orders.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting order: %v", err)
	}
	awaitEvent(t, events, "delete", name)

	// The informer's cache must end up consistent with the database.
	err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 30*time.Second, true,
		func(context.Context) (bool, error) {
			_, exists, err := informer.GetStore().GetByKey(acmeNamespace + "/" + name)
			return !exists, err
		})
	if err != nil {
		t.Fatalf("informer cache still holds the deleted object: %v", err)
	}
}

// awaitEvent waits for a specific event, ignoring unrelated ones.
func awaitEvent(t *testing.T, events chan event, verb, name string) {
	t.Helper()

	deadline := time.After(60 * time.Second)
	for {
		select {
		case e := <-events:
			if e.verb == verb && e.name == name {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a %q event for %q", verb, name)
		}
	}
}

func drain(events chan event) {
	for {
		select {
		case <-events:
		default:
			return
		}
	}
}

// TestAPIServiceLifecycle covers registration with the aggregation layer: a
// projection in a group nobody has served before must get its own APIService,
// and deleting it must withdraw that registration. Without this the "no
// restart, no redeploy" story stops at the edge of an existing API group.
func TestAPIServiceLifecycle(t *testing.T) {
	ctx := context.Background()

	const (
		group          = "warehouse.example.com"
		projectionName = "e2e-bins"
		apiServiceName = "v1alpha1." + group
		managedByLabel = "app.kubernetes.io/managed-by"
		managedByValue = "kube-crisp"
	)
	binsGVR := schema.GroupVersionResource{Group: group, Version: "v1alpha1", Resource: "bins"}

	// The projection kube-crisp already serves must be registered and labelled.
	orders, err := dynamicClient.Resource(apiServiceGVR).Get(ctx, "v1alpha1.store.example.com", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the APIService for the orders group: %v", err)
	}
	if got := orders.GetLabels()[managedByLabel]; got != managedByValue {
		t.Errorf("APIService label %s = %q, want %q", managedByLabel, got, managedByValue)
	}

	projection := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "crisp.kubecrisp.io/v1alpha1",
		"kind":       "CustomResourceProjection",
		"metadata":   map[string]any{"name": projectionName},
		"spec": map[string]any{
			"dataSource": map[string]any{
				"driver":    "postgres",
				"dsnKey":    "dsn",
				"secretRef": map[string]any{"name": "orders-db", "namespace": "kube-crisp"},
			},
			"resource": map[string]any{
				"group":   group,
				"version": "v1alpha1",
				"kind":    "Bin",
				"plural":  "bins",
				"scope":   "Namespaced",
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"spec": map[string]any{
							"type":       "object",
							"properties": map[string]any{"orders": map[string]any{"type": "integer", "format": "int64"}},
						},
					},
				},
			},
			"queries": map[string]any{
				"list": map[string]any{
					"sql": "SELECT status AS name, tenant, COUNT(*) AS orders FROM orders " +
						"WHERE (:namespace::text IS NULL OR tenant = :namespace) " +
						"GROUP BY status, tenant ORDER BY status LIMIT COALESCE(:limit, 100)",
				},
			},
			"mapping": map[string]any{
				"name":      "name",
				"namespace": "tenant",
				"fields": []any{
					map[string]any{"column": "orders", "path": "spec.orders", "type": "integer"},
				},
			},
		},
	}}

	if _, err := dynamicClient.Resource(crpGVR).Create(ctx, projection, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the projection: %v", err)
	}
	t.Cleanup(func() {
		_ = dynamicClient.Resource(crpGVR).Delete(context.Background(), projectionName, metav1.DeleteOptions{})
	})

	// The APIService must appear without anyone applying one.
	err = wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		apiService, err := dynamicClient.Resource(apiServiceGVR).Get(ctx, apiServiceName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if got := apiService.GetLabels()[managedByLabel]; got != managedByValue {
			return false, fmt.Errorf("APIService %s is not labelled as managed by kube-crisp", apiServiceName)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("the APIService was never created: %v", err)
	}

	// And the group must actually serve once the aggregator picks it up.
	err = wait.PollUntilContextTimeout(ctx, time.Second, 120*time.Second, true, func(ctx context.Context) (bool, error) {
		list, err := dynamicClient.Resource(binsGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, nil //nolint:nilerr // waiting for the aggregation layer to route
		}
		return len(list.Items) > 0, nil
	})
	if err != nil {
		t.Fatalf("the newly registered group never served: %v", err)
	}

	// Deleting the projection must withdraw the registration.
	if err := dynamicClient.Resource(crpGVR).Delete(ctx, projectionName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the projection: %v", err)
	}

	err = wait.PollUntilContextTimeout(ctx, time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := dynamicClient.Resource(apiServiceGVR).Get(ctx, apiServiceName, metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	if err != nil {
		t.Fatalf("the APIService outlived its projection: %v", err)
	}
}

// currentDocumentURL reads the OpenAPI root and returns the URL it currently
// advertises for the projected group version, hash and all.
func currentDocumentURL(ctx context.Context) (*url.URL, error) {
	rootRaw, err := discoveryClient.RESTClient().Get().AbsPath("/openapi/v3").Do(ctx).Raw()
	if err != nil {
		return nil, err
	}

	var root struct {
		Paths map[string]struct {
			ServerRelativeURL string `json:"serverRelativeURL"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(rootRaw, &root); err != nil {
		return nil, err
	}

	found, listed := root.Paths["apis/store.example.com/v1alpha1"]
	if !listed {
		return nil, fmt.Errorf("the projected group is no longer listed in the OpenAPI root")
	}
	return url.Parse(found.ServerRelativeURL)
}

// TestProjectedSchemaIsPublished checks that the schema a projection declares
// reaches clients, which is what makes kubectl explain work.
func TestProjectedSchemaIsPublished(t *testing.T) {
	ctx := context.Background()

	// Clients discover the document's URL from the root, which carries a cache
	// busting hash, rather than assuming the path. The aggregation layer
	// downloads extension server documents on its own schedule, so this waits
	// rather than assuming the merge has already happened.
	var entry struct {
		ServerRelativeURL string `json:"serverRelativeURL"`
	}
	var listedPaths []string

	err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		rootRaw, err := discoveryClient.RESTClient().Get().AbsPath("/openapi/v3").Do(ctx).Raw()
		if err != nil {
			return false, err
		}

		var root struct {
			Paths map[string]struct {
				ServerRelativeURL string `json:"serverRelativeURL"`
			} `json:"paths"`
		}
		if err := json.Unmarshal(rootRaw, &root); err != nil {
			return false, err
		}

		listedPaths = listedPaths[:0]
		for path := range root.Paths {
			listedPaths = append(listedPaths, path)
		}
		sort.Strings(listedPaths)

		found, listed := root.Paths["apis/store.example.com/v1alpha1"]
		if !listed {
			return false, nil
		}
		entry.ServerRelativeURL = found.ServerRelativeURL
		return true, nil
	})
	if err != nil {
		t.Fatalf("the projected group never reached the aggregated OpenAPI root (%v); it lists %v", err, listedPaths)
	}

	// The advertised URL carries a cache-busting hash, which AbsPath would fold
	// into the path. The hash also goes stale the moment the document changes —
	// a projection registering is enough — and the endpoint answers a stale one
	// with a redirect the aggregation layer refuses to follow. So the root is
	// re-read and the document re-fetched until the two agree.
	var raw []byte
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, time.Minute, true, func(ctx context.Context) (bool, error) {
		url, err := currentDocumentURL(ctx)
		if err != nil {
			return false, err
		}

		request := discoveryClient.RESTClient().Get().AbsPath(url.Path)
		for key, values := range url.Query() {
			for _, value := range values {
				request = request.Param(key, value)
			}
		}

		raw, err = request.Do(ctx).Raw()
		if err != nil {
			if strings.Contains(err.Error(), "attempted to redirect") {
				// The document moved on while we were asking for it.
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("fetching the OpenAPI document at %s: %v", entry.ServerRelativeURL, err)
	}

	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decoding the OpenAPI document: %v", err)
	}

	components, _, _ := unstructured.NestedMap(document, "components", "schemas")
	var found string
	for name := range components {
		if strings.HasSuffix(name, "v1alpha1.Order") {
			found = name
		}
	}
	if found == "" {
		t.Fatalf("no schema for the Order kind was published; got %d schemas", len(components))
	}

	customer, exists, err := unstructured.NestedMap(components, found, "properties", "spec", "properties", "customer")
	if err != nil || !exists {
		t.Fatalf("the published schema does not describe spec.customer: %v", err)
	}
	if customer["type"] != "string" {
		t.Errorf("spec.customer type = %v, want string", customer["type"])
	}
}

// TestWriteRejectsSchemaViolation checks that the declared schema is enforced,
// not merely published.
func TestWriteRejectsSchemaViolation(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	const name = "order-e2e-invalid"
	t.Cleanup(func() {
		_ = orders.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	invalid := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		// totalCents is declared as an integer.
		"spec": map[string]any{"customer": "ada", "totalCents": "not-a-number"},
	}}

	_, err := orders.Create(ctx, invalid, metav1.CreateOptions{})
	if err == nil {
		t.Fatal("creating an object that violates the schema succeeded")
	}
	if !apierrors.IsInvalid(err) && !apierrors.IsBadRequest(err) {
		t.Fatalf("create error = %v, want Invalid", err)
	}
}

// TestOptimisticConcurrency checks that a stale write is rejected instead of
// silently overwriting a change someone else made.
func TestOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	const name = "order-e2e-conflict"
	t.Cleanup(func() {
		_ = orders.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	created, err := orders.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(10)},
		"status":     map[string]any{"phase": "pending"},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating the order: %v", err)
	}

	// Two clients read the same version; the first write wins.
	first := created.DeepCopy()
	second := created.DeepCopy()

	if err := unstructured.SetNestedField(first.Object, int64(20), "spec", "totalCents"); err != nil {
		t.Fatalf("setting spec.totalCents: %v", err)
	}
	if _, err := orders.Update(ctx, first, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("the first update failed: %v", err)
	}

	if err := unstructured.SetNestedField(second.Object, int64(30), "spec", "totalCents"); err != nil {
		t.Fatalf("setting spec.totalCents: %v", err)
	}
	if _, err := orders.Update(ctx, second, metav1.UpdateOptions{}); !apierrors.IsConflict(err) {
		t.Fatalf("the second update error = %v, want Conflict", err)
	}
}

// TestFieldSelectorIsHonouredOrRejected covers both halves of the contract.
func TestFieldSelectorIsHonouredOrRejected(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	list, err := orders.List(ctx, metav1.ListOptions{FieldSelector: "metadata.name=order-acme-1"})
	if err != nil {
		t.Fatalf("listing with a supported field selector: %v", err)
	}
	if got, want := len(list.Items), 1; got != want {
		t.Fatalf("selector returned %d items, want %d", got, want)
	}

	if _, err := orders.List(ctx, metav1.ListOptions{FieldSelector: "spec.customer=ada"}); err == nil {
		t.Fatal("an unsupported field selector was accepted; it must be rejected rather than ignored")
	}
}

// TestPrinterColumnsAreServed checks that kubectl prints the columns a
// projection declares, rather than only NAME.
func TestPrinterColumnsAreServed(t *testing.T) {
	ctx := context.Background()

	raw, err := discoveryClient.RESTClient().Get().
		AbsPath("/apis/store.example.com/v1alpha1/namespaces/"+acmeNamespace+"/orders").
		SetHeader("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io").
		Do(ctx).Raw()
	if err != nil {
		t.Fatalf("requesting the table form: %v", err)
	}

	var table struct {
		ColumnDefinitions []struct {
			Name string `json:"name"`
		} `json:"columnDefinitions"`
		Rows []struct {
			Cells []any `json:"cells"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatalf("decoding the table: %v", err)
	}

	var columns []string
	for _, definition := range table.ColumnDefinitions {
		columns = append(columns, definition.Name)
	}
	want := []string{"Name", "Customer", "Phase", "Total"}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("columns = %v, want %v", columns, want)
	}

	if len(table.Rows) == 0 {
		t.Fatal("the table has no rows")
	}
	if got := table.Rows[0].Cells[1]; got != "ada" && got != "grace" {
		t.Errorf("first row customer cell = %v, want a customer name", got)
	}
}

// TestDeleteCollection covers `kubectl delete orders --all`, which previously
// had no server-side support at all.
func TestDeleteCollection(t *testing.T) {
	ctx := context.Background()

	const tenant = "delcoll"
	createNamespace(t, tenant)

	orders := dynamicClient.Resource(ordersGVR).Namespace(tenant)
	for i := range 3 {
		name := fmt.Sprintf("order-delcoll-%d", i)
		if _, err := orders.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "store.example.com/v1alpha1",
			"kind":       "Order",
			"metadata":   map[string]any{"name": name},
			"spec":       map[string]any{"customer": "ada", "totalCents": int64(i)},
			"status":     map[string]any{"phase": "pending"},
		}}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}

	if err := orders.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil {
		t.Fatalf("deleting the collection: %v", err)
	}

	remaining, err := orders.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing after the collection delete: %v", err)
	}
	if got := len(remaining.Items); got != 0 {
		t.Fatalf("%d objects survived the collection delete", got)
	}

	// Another tenant's rows must be untouched.
	acme, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing the other tenant: %v", err)
	}
	if len(acme.Items) == 0 {
		t.Fatal("the collection delete crossed namespaces")
	}
}

// TestStatusSubresource checks that status and spec are owned separately, which
// is what lets a controller write status without racing a user's spec edits.
func TestStatusSubresource(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	const name = "order-e2e-status"
	t.Cleanup(func() {
		_ = orders.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	created, err := orders.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(1)},
		"status":     map[string]any{"phase": "pending"},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating the order: %v", err)
	}

	// A write to the main resource must not move status.
	spec := created.DeepCopy()
	if err := unstructured.SetNestedField(spec.Object, "cancelled", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	if err := unstructured.SetNestedField(spec.Object, "grace", "spec", "customer"); err != nil {
		t.Fatalf("setting spec.customer: %v", err)
	}
	updated, err := orders.Update(ctx, spec, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("updating the order: %v", err)
	}
	if phase, _, _ := unstructured.NestedString(updated.Object, "status", "phase"); phase == "cancelled" {
		t.Error("a write to the main resource changed status")
	}
	if customer, _, _ := unstructured.NestedString(updated.Object, "spec", "customer"); customer != "grace" {
		t.Errorf("spec.customer = %q, want %q", customer, "grace")
	}

	// A write to status must not move spec.
	statusWrite := updated.DeepCopy()
	if err := unstructured.SetNestedField(statusWrite.Object, "shipped", "status", "phase"); err != nil {
		t.Fatalf("setting status.phase: %v", err)
	}
	if err := unstructured.SetNestedField(statusWrite.Object, "someone-else", "spec", "customer"); err != nil {
		t.Fatalf("setting spec.customer: %v", err)
	}

	result, err := orders.UpdateStatus(ctx, statusWrite, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("updating status: %v", err)
	}
	if phase, _, _ := unstructured.NestedString(result.Object, "status", "phase"); phase != "shipped" {
		t.Errorf("status.phase = %q, want %q", phase, "shipped")
	}
	if customer, _, _ := unstructured.NestedString(result.Object, "spec", "customer"); customer != "grace" {
		t.Errorf("spec.customer = %q, want the stored %q: a status write changed spec", customer, "grace")
	}
}

func TestCreateWithGenerateName(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	created, err := orders.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"generateName": "order-gen-"},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(1)},
		"status":     map[string]any{"phase": "pending"},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating with generateName: %v", err)
	}
	t.Cleanup(func() {
		_ = orders.Delete(context.Background(), created.GetName(), metav1.DeleteOptions{})
	})

	if !strings.HasPrefix(created.GetName(), "order-gen-") || created.GetName() == "order-gen-" {
		t.Fatalf("generated name = %q, want a suffixed %q", created.GetName(), "order-gen-")
	}
}

// TestPaginationReportsRemainingItems checks the paging contract end to end:
// stable pages and a count of what is left.
func TestPaginationReportsRemainingItems(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(benchNamespace)

	first, err := orders.List(ctx, metav1.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("listing the first page: %v", err)
	}
	if got, want := len(first.Items), 100; got != want {
		t.Fatalf("first page held %d items, want %d", got, want)
	}
	if first.GetContinue() == "" {
		t.Fatal("first page carried no continue token")
	}

	remaining := first.GetRemainingItemCount()
	if remaining == nil {
		t.Fatal("the list reported no remainingItemCount")
	}
	if want := int64(rowCount - 100); *remaining != want {
		t.Errorf("remainingItemCount = %d, want %d", *remaining, want)
	}

	second, err := orders.List(ctx, metav1.ListOptions{Limit: 100, Continue: first.GetContinue()})
	if err != nil {
		t.Fatalf("listing the second page: %v", err)
	}
	if first.Items[0].GetName() == second.Items[0].GetName() {
		t.Error("the second page repeated the first")
	}
}

// createNamespace makes a namespace for a test that needs its own tenant.
func createNamespace(t *testing.T, name string) {
	t.Helper()

	namespaces := dynamicClient.Resource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"})
	_, err := namespaces.Create(context.Background(), &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata":   map[string]any{"name": name},
	}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating namespace %s: %v", name, err)
	}
}

// TestMySQLProjection covers the second supported driver. MySQL has no
// RETURNING, so this also exercises the write path that executes a statement
// and reads the row back, and the positional placeholder rewriter.
func TestMySQLProjection(t *testing.T) {
	ctx := context.Background()
	widgets := dynamicClient.Resource(widgetsGVR).Namespace(acmeNamespace)

	list, err := widgets.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing widgets: %v", err)
	}
	if got, want := len(list.Items), 2; got != want {
		t.Fatalf("listed %d widgets, want %d", got, want)
	}

	fetched, err := widgets.Get(ctx, "widget-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting widget-1: %v", err)
	}
	if colour, _, _ := unstructured.NestedString(fetched.Object, "spec", "colour"); colour != "red" {
		t.Errorf("spec.colour = %q, want %q", colour, "red")
	}
	if weight, _, _ := unstructured.NestedInt64(fetched.Object, "spec", "weightGrams"); weight != 120 {
		t.Errorf("spec.weightGrams = %d, want 120", weight)
	}

	const name = "widget-e2e"
	t.Cleanup(func() {
		_ = widgets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	created, err := widgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"colour": "green", "weightGrams": int64(55)},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating a widget: %v", err)
	}
	// The response comes from a read-back, since MySQL cannot return the row
	// it wrote.
	if colour, _, _ := unstructured.NestedString(created.Object, "spec", "colour"); colour != "green" {
		t.Errorf("created spec.colour = %q, want %q", colour, "green")
	}

	if _, err := widgets.Create(ctx, created, metav1.CreateOptions{}); !apierrors.IsAlreadyExists(err) {
		t.Errorf("duplicate create error = %v, want AlreadyExists", err)
	}

	if err := widgets.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the widget: %v", err)
	}
	if _, err := widgets.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("get after delete error = %v, want NotFound", err)
	}
}

// TestDryRunChangesNothing covers the contract every other Kubernetes resource
// honours: a dry run answers with what would have happened and writes nothing.
func TestDryRunChangesNothing(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	const name = "order-e2e-dryrun"

	created, err := orders.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(1)},
	}}, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		t.Fatalf("dry-run create: %v", err)
	}
	if created.GetName() != name {
		t.Errorf("dry-run create returned %q, want %q", created.GetName(), name)
	}

	if _, err := orders.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the dry-run create stored the object: %v", err)
	}

	// The same for delete: it must report the object without removing it.
	existing, err := orders.Get(ctx, "order-acme-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading a seeded order: %v", err)
	}
	if err := orders.Delete(ctx, "order-acme-1", metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		t.Fatalf("dry-run delete: %v", err)
	}
	if _, err := orders.Get(ctx, existing.GetName(), metav1.GetOptions{}); err != nil {
		t.Fatalf("the dry-run delete removed the object: %v", err)
	}
}

// TestStrictFieldValidationRejectsUnknownFields checks that a field the schema
// does not describe is reported rather than silently dropped.
func TestStrictFieldValidationRejectsUnknownFields(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": "order-e2e-strict"},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(1), "notAField": "surprise"},
	}}

	_, err := orders.Create(ctx, obj, metav1.CreateOptions{
		DryRun:          []string{metav1.DryRunAll},
		FieldValidation: metav1.FieldValidationStrict,
	})
	if err == nil {
		t.Fatal("an unknown field was accepted under strict validation")
	}
	if !strings.Contains(err.Error(), "notAField") {
		t.Errorf("error %q does not name the unknown field", err)
	}
}

// TestMultipleVersions covers serving one kind at two versions. There is no
// conversion involved: each version maps the same rows through its own mapping,
// so the shapes differ while the object is the same.
func TestMultipleVersions(t *testing.T) {
	ctx := context.Background()

	alpha, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).
		Get(ctx, "order-acme-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading v1alpha1: %v", err)
	}
	beta, err := dynamicClient.Resource(ordersBetaGVR).Namespace(acmeNamespace).
		Get(ctx, "order-acme-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading v1beta1: %v", err)
	}

	if got, want := beta.GetAPIVersion(), "store.example.com/v1beta1"; got != want {
		t.Errorf("apiVersion = %q, want %q", got, want)
	}
	if alpha.GetName() != beta.GetName() || alpha.GetUID() != beta.GetUID() {
		t.Errorf("the two versions disagree on identity: %s/%s versus %s/%s",
			alpha.GetName(), alpha.GetUID(), beta.GetName(), beta.GetUID())
	}

	// v1alpha1 puts the amount at spec.totalCents; v1beta1 nests it.
	flat, found, err := unstructured.NestedInt64(alpha.Object, "spec", "totalCents")
	if err != nil || !found {
		t.Fatalf("v1alpha1 spec.totalCents missing: %v", err)
	}
	nested, found, err := unstructured.NestedInt64(beta.Object, "spec", "amount", "cents")
	if err != nil || !found {
		t.Fatalf("v1beta1 spec.amount.cents missing: %v", err)
	}
	if flat != nested {
		t.Errorf("the versions report different amounts: %d and %d", flat, nested)
	}
	if _, found, _ := unstructured.NestedInt64(beta.Object, "spec", "totalCents"); found {
		t.Error("v1beta1 carries the v1alpha1 field as well")
	}

	// Each served version gets registered with the aggregation layer.
	if _, err := dynamicClient.Resource(apiServiceGVR).
		Get(ctx, "v1beta1.store.example.com", metav1.GetOptions{}); err != nil {
		t.Errorf("no APIService for the added version: %v", err)
	}

	resources, err := discoveryClient.ServerResourcesForGroupVersion("store.example.com/v1beta1")
	if err != nil {
		t.Fatalf("discovering v1beta1: %v", err)
	}
	var advertised bool
	for _, r := range resources.APIResources {
		if r.Name == "orders" {
			advertised = true
		}
	}
	if !advertised {
		t.Error("orders is not advertised in the v1beta1 discovery document")
	}
}

// TestBorrowedSchema covers schemaFrom: the shape comes from an existing CRD
// rather than being restated in the projection.
func TestBorrowedSchema(t *testing.T) {
	ctx := context.Background()
	borrowed := dynamicClient.Resource(borrowedOrdersGVR).Namespace(acmeNamespace)

	list, err := borrowed.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing borrowedorders: %v", err)
	}
	if len(list.Items) == 0 {
		t.Fatal("the projection with a borrowed schema served nothing")
	}

	// The borrowed schema is enforced: the CRD declares totalCents as an
	// integer, and nothing in the projection restates that.
	_, err = borrowed.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "BorrowedOrder",
		"metadata":   map[string]any{"name": "borrowed-invalid"},
		"spec":       map[string]any{"customer": "ada", "totalCents": "not-a-number"},
	}}, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err == nil {
		t.Fatal("an object violating the borrowed schema was accepted")
	}
	if !apierrors.IsInvalid(err) && !apierrors.IsBadRequest(err) && !apierrors.IsMethodNotSupported(err) {
		t.Fatalf("create error = %v, want a validation failure", err)
	}
}

// TestReadCache covers cacheTTL: a change made straight in the database is not
// visible until the entry expires, which is exactly the trade being made.
func TestReadCache(t *testing.T) {
	ctx := context.Background()
	cached := dynamicClient.Resource(cachedOrdersGVR).Namespace(acmeNamespace)

	before, err := cached.Get(ctx, "order-acme-2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("priming the cache: %v", err)
	}
	original, _, _ := unstructured.NestedString(before.Object, "spec", "customer")

	execSQL(t, "UPDATE orders SET customer = 'cache-probe' WHERE id = 'order-acme-2'")
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf("UPDATE orders SET customer = '%s' WHERE id = 'order-acme-2'", original))
	})

	stale, err := cached.Get(ctx, "order-acme-2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading through the cache: %v", err)
	}
	if customer, _, _ := unstructured.NestedString(stale.Object, "spec", "customer"); customer != original {
		t.Errorf("spec.customer = %q immediately after the change, want the cached %q", customer, original)
	}

	// The uncached projection over the same row sees it straight away.
	fresh, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).
		Get(ctx, "order-acme-2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading without the cache: %v", err)
	}
	if customer, _, _ := unstructured.NestedString(fresh.Object, "spec", "customer"); customer != "cache-probe" {
		t.Errorf("the uncached projection returned %q, want the new value", customer)
	}

	// And the cache catches up once the entry expires.
	err = wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		obj, err := cached.Get(ctx, "order-acme-2", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		customer, _, _ := unstructured.NestedString(obj.Object, "spec", "customer")
		return customer == "cache-probe", nil
	})
	if err != nil {
		t.Fatalf("the cache never expired: %v", err)
	}
}

// TestLoadShedding covers the concurrency limit: a projection allowed one query
// at a time sheds excess requests rather than queueing them behind a slow one.
//
// The requests are issued with retries disabled on purpose. client-go retries a
// 429 with backoff, so a client using the usual machinery sees the requests
// serialise and eventually succeed — which is the right behaviour for a client
// and the wrong lens for this test.
func TestLoadShedding(t *testing.T) {
	// Two namespaces, so the queries are genuinely different work. Identical
	// reads are collapsed into one query before the limiter ever sees them,
	// which is what TestIdenticalReadsAreNotShed covers.
	served, shed := hammerSlowOrders(t, []string{acmeNamespace, acmeNamespace, benchNamespace, benchNamespace})

	if served == 0 {
		t.Error("no request was served; the limit shed everything")
	}
	if shed == 0 {
		t.Error("no request was shed; two concurrent distinct queries fit under a limit of one")
	}
	t.Logf("%d served, %d shed", served, shed)
}

// TestIdenticalReadsAreNotShed states what coalescing changes about the limit:
// it bounds concurrent queries, and requests that ask for exactly the same
// thing are one query however many of them arrive.
func TestIdenticalReadsAreNotShed(t *testing.T) {
	namespaces := make([]string, 8)
	for i := range namespaces {
		namespaces[i] = acmeNamespace
	}

	served, shed := hammerSlowOrders(t, namespaces)
	if shed != 0 {
		t.Errorf("%d of %d identical concurrent reads were shed; they should have shared one query",
			shed, len(namespaces))
	}
	if served != len(namespaces) {
		t.Errorf("%d of %d identical concurrent reads were served", served, len(namespaces))
	}
}

// hammerSlowOrders lists the two-second projection once per namespace given, at
// the same time, and reports how many were served and how many shed.
//
// Retries are disabled on purpose. client-go retries a 429 with backoff, so a
// client using the usual machinery sees the requests serialise and eventually
// succeed — which is the right behaviour for a client and the wrong lens here.
func hammerSlowOrders(t *testing.T, namespaces []string) (served, shed int) {
	t.Helper()

	ctx := context.Background()
	type outcome struct {
		status int
		err    error
	}
	results := make(chan outcome, len(namespaces))

	for _, namespace := range namespaces {
		go func(namespace string) {
			var status int
			err := discoveryClient.RESTClient().Get().
				AbsPath("/apis/store.example.com/v1alpha1/namespaces/" + namespace + "/sloworders").
				MaxRetries(0).
				Do(ctx).
				StatusCode(&status).
				Error()
			results <- outcome{status: status, err: err}
		}(namespace)
	}

	for range namespaces {
		switch result := <-results; {
		case result.status == http.StatusTooManyRequests, apierrors.IsTooManyRequests(result.err):
			shed++
		case result.err == nil:
			served++
		default:
			t.Errorf("unexpected error: %v", result.err)
		}
	}
	return served, shed
}

// TestSecretMustOptIn covers the containment: pointing a projection at a Secret
// that has not opted in must fail, since the projection author chooses the SQL.
func TestSecretMustOptIn(t *testing.T) {
	ctx := context.Background()

	secrets := dynamicClient.Resource(schema.GroupVersionResource{Version: "v1", Resource: "secrets"}).
		Namespace("kube-crisp")
	if _, err := secrets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata":   map[string]any{"name": "unmarked-db"},
		"stringData": map[string]any{"dsn": "postgres://crisp:crisp@postgres.kube-crisp.svc:5432/store?sslmode=disable"},
	}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating the unmarked Secret: %v", err)
	}
	t.Cleanup(func() {
		_ = secrets.Delete(context.Background(), "unmarked-db", metav1.DeleteOptions{})
		_ = dynamicClient.Resource(crpGVR).Delete(context.Background(), "e2e-unmarked", metav1.DeleteOptions{})
	})

	projection := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "crisp.kubecrisp.io/v1alpha1",
		"kind":       "CustomResourceProjection",
		"metadata":   map[string]any{"name": "e2e-unmarked"},
		"spec": map[string]any{
			"dataSource": map[string]any{
				"driver":    "postgres",
				"secretRef": map[string]any{"name": "unmarked-db", "namespace": "kube-crisp"},
			},
			"resource": map[string]any{
				"group": "store.example.com", "version": "v1alpha1",
				"kind": "Unmarked", "plural": "unmarkeds", "scope": "Namespaced",
				"schema": map[string]any{"type": "object"},
			},
			"queries": map[string]any{
				"list": map[string]any{"sql": "SELECT id, tenant FROM orders WHERE tenant = :namespace"},
			},
			"mapping": map[string]any{"name": "id", "namespace": "tenant"},
		},
	}}
	// Refused where the mistake was made. The admission webhook resolves the
	// data source, so a Secret that has not opted in is caught at kubectl apply
	// rather than reported afterwards in a status the author has to know to go
	// and read.
	//
	// The status condition is still the backstop: the webhook fails open by
	// design, so a projection created while this server is down is rejected the
	// old way instead, at compile time and without ever being served.
	_, err := dynamicClient.Resource(crpGVR).Create(ctx, projection, metav1.CreateOptions{})
	if err == nil {
		t.Fatal("a projection over a Secret that has not opted in was accepted")
	}
	if !strings.Contains(err.Error(), "allow-projection") {
		t.Errorf("the refusal does not name the label that would fix it: %v", err)
	}

	// And nothing was stored, so there is no projection in the cluster that the
	// server then refuses to serve.
	if _, err := dynamicClient.Resource(crpGVR).Get(ctx, "e2e-unmarked", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("get after a refused create returned %v, want NotFound", err)
	}

	// The healthy projections keep serving: one bad projection is reported,
	// not fatal.
	if _, err := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace).
		List(ctx, metav1.ListOptions{}); err != nil {
		t.Errorf("a rejected projection disturbed the healthy ones: %v", err)
	}
}

// execSQL runs a statement straight against PostgreSQL, behind the server's
// back, which is how the cache test simulates another writer.
func execSQL(t *testing.T, statement string) {
	t.Helper()

	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath,
		"-n", "kube-crisp", "exec", "deploy/postgres", "--",
		"psql", "-U", "crisp", "-d", "store", "-v", "ON_ERROR_STOP=1", "-c", statement)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("running %q: %v\n%s", statement, err, output)
	}
}

// TestAdmissionPolicyApplies covers governance: a ValidatingAdmissionPolicy
// that matches a projected resource has to be enforced, or cluster policy
// simply does not reach the one API that writes to a database.
func TestAdmissionPolicyApplies(t *testing.T) {
	ctx := context.Background()
	widgets := dynamicClient.Resource(widgetsGVR).Namespace(acmeNamespace)

	forbidden := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "widget-forbidden"},
		"spec":       map[string]any{"colour": "forbidden", "weightGrams": int64(1)},
	}}
	t.Cleanup(func() {
		_ = widgets.Delete(context.Background(), "widget-forbidden", metav1.DeleteOptions{})
	})

	// The policy is admitted by the kube-apiserver but evaluated here, so give
	// the binding a moment to reach this server's informers.
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 90*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := widgets.Create(ctx, forbidden, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		if err == nil {
			return false, nil
		}
		if strings.Contains(err.Error(), "not allowed by cluster policy") {
			return true, nil
		}
		return false, fmt.Errorf("create was rejected for the wrong reason: %w", err)
	})
	if err != nil {
		t.Fatalf("the admission policy was not enforced: %v", err)
	}

	// A colour the policy permits still goes through.
	allowed := forbidden.DeepCopy()
	if err := unstructured.SetNestedField(allowed.Object, "green", "spec", "colour"); err != nil {
		t.Fatalf("setting spec.colour: %v", err)
	}
	if _, err := widgets.Create(ctx, allowed, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}); err != nil {
		t.Fatalf("a permitted object was rejected: %v", err)
	}
}

// TestNamespaceLifecycle covers the other half of admission: a projection must
// not write rows for a tenant the cluster does not have.
func TestNamespaceLifecycle(t *testing.T) {
	ctx := context.Background()
	widgets := dynamicClient.Resource(widgetsGVR).Namespace("no-such-namespace")

	_, err := widgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": "ghost-widget"},
		"spec":       map[string]any{"colour": "grey", "weightGrams": int64(1)},
	}}, metav1.CreateOptions{})
	if err == nil {
		_ = widgets.Delete(ctx, "ghost-widget", metav1.DeleteOptions{})
		t.Fatal("an object was written into a namespace that does not exist")
	}
	if !apierrors.IsNotFound(err) && !apierrors.IsForbidden(err) {
		t.Fatalf("create error = %v, want the namespace to be rejected", err)
	}
}

// TestDatabaseOutage is the chaos case. A database that goes away must produce
// 503 on reads rather than an opaque 500 or a disappearing API group, and the
// projection must recover on its own once the database returns.
//
// It runs last, and restores the database whatever happens, because everything
// else in the suite depends on it.
func TestDatabaseOutage(t *testing.T) {
	ctx := context.Background()
	orders := dynamicClient.Resource(ordersGVR).Namespace(acmeNamespace)

	if _, err := orders.List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		t.Fatalf("the projection was already unhealthy: %v", err)
	}

	t.Cleanup(func() {
		scaleDeployment(t, "postgres", 1)
		_ = waitForReads(context.Background(), orders, 3*time.Minute)
	})

	scaleDeployment(t, "postgres", 0)

	// Reads report the outage rather than succeeding. A database that refuses
	// connections gives 503; one that has gone away without closing its
	// sockets simply stops answering, which surfaces as a timeout. Either is a
	// retryable status, and neither is a 500.
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		_, err := orders.List(attempt, metav1.ListOptions{Limit: 1})
		lastErr = err
		return apierrors.IsServiceUnavailable(err) || apierrors.IsTimeout(err) ||
			goerrors.Is(err, context.DeadlineExceeded), nil
	})
	if err != nil {
		t.Fatalf("reads never reported the outage; last error was %v", lastErr)
	}

	// The API group is still registered, so clients see an outage rather than
	// a resource that never existed.
	if _, err := discoveryClient.ServerResourcesForGroupVersion("store.example.com/v1alpha1"); err != nil {
		t.Errorf("the API group disappeared during the outage: %v", err)
	}
	if _, err := dynamicClient.Resource(apiServiceGVR).
		Get(ctx, "v1alpha1.store.example.com", metav1.GetOptions{}); err != nil {
		t.Errorf("the APIService was withdrawn during the outage: %v", err)
	}

	scaleDeployment(t, "postgres", 1)

	if err := waitForReads(ctx, orders, 3*time.Minute); err != nil {
		t.Fatalf("the projection did not recover: %v", err)
	}

	// And writes work again, so the pool really reconnected rather than
	// serving from anything cached.
	const name = "order-e2e-recovered"
	t.Cleanup(func() {
		_ = orders.Delete(context.Background(), name, metav1.DeleteOptions{})
	})
	if _, err := orders.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Order",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"customer": "ada", "totalCents": int64(1)},
		"status":     map[string]any{"phase": "pending"},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("writing after recovery: %v", err)
	}
}

// waitForReads polls until the projection answers again.
func waitForReads(ctx context.Context, client dynamic.ResourceInterface, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := client.List(ctx, metav1.ListOptions{Limit: 1})
		return err == nil, nil
	})
}

// scaleDeployment takes the database away and brings it back.
func scaleDeployment(t *testing.T, name string, replicas int) {
	t.Helper()

	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath,
		"-n", "kube-crisp", "scale", "deployment", name,
		fmt.Sprintf("--replicas=%d", replicas))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("scaling %s to %d: %v\n%s", name, replicas, err, output)
	}
}

// TestSQLiteProjection covers the third driver, which has no server at all:
// the database is a file on the apiserver's own volume.
func TestSQLiteProjection(t *testing.T) {
	ctx := context.Background()
	items := dynamicClient.Resource(itemsGVR).Namespace(acmeNamespace)

	list, err := items.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing items: %v", err)
	}
	if got, want := len(list.Items), 50; got != want {
		t.Fatalf("listed %d items, want %d", got, want)
	}

	fetched, err := items.Get(ctx, "item-0001", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting item-0001: %v", err)
	}
	if quantity, _, _ := unstructured.NestedInt64(fetched.Object, "spec", "quantity"); quantity != 1 {
		t.Errorf("spec.quantity = %d, want 1", quantity)
	}

	// A declared selectable field, pushed into the query by column.
	filtered, err := items.List(ctx, metav1.ListOptions{FieldSelector: "spec.label=label-3"})
	if err != nil {
		t.Fatalf("listing with a field selector: %v", err)
	}
	if len(filtered.Items) == 0 || len(filtered.Items) >= len(list.Items) {
		t.Fatalf("the field selector matched %d of %d items", len(filtered.Items), len(list.Items))
	}
	for _, item := range filtered.Items {
		if label, _, _ := unstructured.NestedString(item.Object, "spec", "label"); label != "label-3" {
			t.Fatalf("the selector returned an item labelled %q", label)
		}
	}

	// Writes, including the optimistic concurrency the update statement carries.
	const name = "item-e2e"
	t.Cleanup(func() {
		_ = items.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	created, err := items.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Item",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"label": "fresh", "quantity": int64(7)},
	}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating an item: %v", err)
	}

	stale := created.DeepCopy()
	stale.SetResourceVersion("1")
	if _, err := items.Update(ctx, stale, metav1.UpdateOptions{}); !apierrors.IsConflict(err) {
		t.Errorf("update with a stale version = %v, want Conflict", err)
	}

	if err := items.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the item: %v", err)
	}
}

// TestSQLitePagination walks the whole collection by keyset, which is where a
// dialect difference in LIMIT would show up.
func TestSQLitePagination(t *testing.T) {
	ctx := context.Background()
	items := dynamicClient.Resource(itemsGVR).Namespace(acmeNamespace)

	seen := map[string]bool{}
	var pages int
	var continueToken string

	for {
		page, err := items.List(ctx, metav1.ListOptions{Limit: 20, Continue: continueToken})
		if err != nil {
			t.Fatalf("listing page %d: %v", pages+1, err)
		}
		pages++

		for _, item := range page.Items {
			if seen[item.GetName()] {
				t.Fatalf("%s appeared on two pages", item.GetName())
			}
			seen[item.GetName()] = true
		}

		if remaining := page.GetRemainingItemCount(); remaining != nil {
			if want := int64(50 - len(seen)); *remaining != want {
				t.Errorf("remainingItemCount = %d after %d items, want %d", *remaining, len(seen), want)
			}
		}

		continueToken = page.GetContinue()
		if continueToken == "" {
			break
		}
		if pages > 10 {
			t.Fatal("paging did not terminate")
		}
	}

	if len(seen) != 50 {
		t.Errorf("walked %d items over %d pages, want 50", len(seen), pages)
	}
	if pages < 3 {
		t.Errorf("50 items in pages of 20 took %d pages", pages)
	}
}

// TestMySQLWatchAndPagination covers the two things the MySQL projection did
// not exercise before, and both are where its dialect differs.
func TestMySQLWatchAndPagination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	widgets := dynamicClient.Resource(widgetsGVR).Namespace(acmeNamespace)

	first, err := widgets.List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("listing the first page: %v", err)
	}
	if got, want := len(first.Items), 1; got != want {
		t.Fatalf("first page held %d items, want %d", got, want)
	}
	if first.GetContinue() == "" {
		t.Fatal("MySQL paging produced no continue token")
	}
	if remaining := first.GetRemainingItemCount(); remaining == nil || *remaining < 1 {
		t.Errorf("remainingItemCount = %v, want at least 1", remaining)
	}

	second, err := widgets.List(ctx, metav1.ListOptions{Limit: 1, Continue: first.GetContinue()})
	if err != nil {
		t.Fatalf("listing the second page: %v", err)
	}
	if first.Items[0].GetName() == second.Items[0].GetName() {
		t.Error("the second page repeated the first")
	}

	// An informer over MySQL, which exercises the incremental watch query.
	events := make(chan event, 32)
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynamicClient, 0, acmeNamespace, nil)
	informer := factory.ForResource(widgetsGVR).Informer()
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { events <- event{"add", obj.(*unstructured.Unstructured).GetName()} },
		DeleteFunc: func(obj any) { events <- event{"delete", obj.(*unstructured.Unstructured).GetName()} },
	}); err != nil {
		t.Fatalf("registering the handler: %v", err)
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		t.Fatal("the informer never synced against MySQL")
	}
	drain(events)

	const name = "widget-watched"
	t.Cleanup(func() {
		_ = widgets.Delete(context.Background(), name, metav1.DeleteOptions{})
	})

	if _, err := widgets.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Widget",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"colour": "amber", "weightGrams": int64(3)},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating a widget: %v", err)
	}
	awaitEvent(t, events, "add", name)

	if err := widgets.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting the widget: %v", err)
	}
	awaitEvent(t, events, "delete", name)
}

// TestPartialObjectMetadata covers the metadata-only requests the garbage
// collector and many controllers make.
func TestPartialObjectMetadata(t *testing.T) {
	ctx := context.Background()

	raw, err := discoveryClient.RESTClient().Get().
		AbsPath("/apis/store.example.com/v1alpha1/namespaces/"+acmeNamespace+"/orders").
		SetHeader("Accept", "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io").
		Do(ctx).Raw()
	if err != nil {
		t.Fatalf("requesting metadata only: %v", err)
	}

	var list struct {
		Kind  string `json:"kind"`
		Items []struct {
			Metadata map[string]any `json:"metadata"`
			Spec     map[string]any `json:"spec"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}

	if list.Kind != "PartialObjectMetadataList" {
		t.Fatalf("kind = %q, want PartialObjectMetadataList", list.Kind)
	}
	if len(list.Items) == 0 {
		t.Fatal("the metadata-only list is empty")
	}
	for _, item := range list.Items {
		if item.Metadata["name"] == "" {
			t.Error("an item carries no name")
		}
		if len(item.Spec) != 0 {
			t.Errorf("a metadata-only response carried a spec: %v", item.Spec)
		}
		// The UID is what a controller resolves owner references against.
		if item.Metadata["uid"] == nil || item.Metadata["uid"] == "" {
			t.Error("an item carries no UID")
		}
	}
}

// TestScaleSubresource covers the /scale endpoint end to end, including the
// path that matters most: `kubectl scale` against a row in PostgreSQL.
func TestScaleSubresource(t *testing.T) {
	ctx := context.Background()
	scalable := dynamicClient.Resource(scalableOrdersGVR).Namespace(acmeNamespace)

	name := "order-scale-1"
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "ScalableOrder",
		"metadata":   map[string]any{"name": name, "namespace": acmeNamespace},
		"spec":       map[string]any{"customer": "ada", "replicas": int64(3)},
		"status":     map[string]any{"phase": "pending"},
	}}
	if _, err := scalable.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the object: %v", err)
	}
	t.Cleanup(func() { _ = scalable.Delete(context.Background(), name, metav1.DeleteOptions{}) })

	// Reading the subresource answers with an autoscaling/v1 Scale, which is
	// the only kind kubectl and the autoscaler understand.
	scale, err := scalable.Get(ctx, name, metav1.GetOptions{}, "scale")
	if err != nil {
		t.Fatalf("reading the scale subresource: %v", err)
	}
	if got, want := scale.GetAPIVersion(), "autoscaling/v1"; got != want {
		t.Errorf("scale apiVersion = %q, want %q", got, want)
	}
	if got, want := scale.GetKind(), "Scale"; got != want {
		t.Errorf("scale kind = %q, want %q", got, want)
	}
	if replicas, _, _ := unstructured.NestedInt64(scale.Object, "spec", "replicas"); replicas != 3 {
		t.Errorf("spec.replicas = %d, want 3", replicas)
	}
	if replicas, _, _ := unstructured.NestedInt64(scale.Object, "status", "replicas"); replicas != 3 {
		t.Errorf("status.replicas = %d, want 3", replicas)
	}

	// Writing it changes the row and nothing else about it.
	if err := unstructured.SetNestedField(scale.Object, int64(11), "spec", "replicas"); err != nil {
		t.Fatalf("preparing the scale: %v", err)
	}
	updated, err := scalable.Update(ctx, scale, metav1.UpdateOptions{}, "scale")
	if err != nil {
		t.Fatalf("writing the scale subresource: %v", err)
	}
	if replicas, _, _ := unstructured.NestedInt64(updated.Object, "spec", "replicas"); replicas != 11 {
		t.Errorf("spec.replicas after the write = %d, want 11", replicas)
	}

	after, err := scalable.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the object back: %v", err)
	}
	if replicas, _, _ := unstructured.NestedInt64(after.Object, "spec", "replicas"); replicas != 11 {
		t.Errorf("spec.replicas in the object = %d, want 11", replicas)
	}
	if customer, _, _ := unstructured.NestedString(after.Object, "spec", "customer"); customer != "ada" {
		t.Errorf("spec.customer = %q, want it untouched at %q", customer, "ada")
	}

	// The real client. kubectl discovers the subresource, builds a Scale, and
	// sends it; nothing here is hand-rolled.
	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath,
		"-n", acmeNamespace, "scale", "scalableorders/"+name, "--replicas=4")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("kubectl scale: %v\n%s", err, output)
	}

	final, err := scalable.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the object after kubectl scale: %v", err)
	}
	if replicas, _, _ := unstructured.NestedInt64(final.Object, "spec", "replicas"); replicas != 4 {
		t.Errorf("spec.replicas after kubectl scale = %d, want 4", replicas)
	}
}

// TestSchemaDefaultsOnWrite covers defaulting: a field the client leaves out
// arrives with the value the schema declares, and reaches the database.
func TestSchemaDefaultsOnWrite(t *testing.T) {
	ctx := context.Background()
	scalable := dynamicClient.Resource(scalableOrdersGVR).Namespace(acmeNamespace)

	name := "order-default-1"
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "ScalableOrder",
		"metadata":   map[string]any{"name": name, "namespace": acmeNamespace},
		"spec":       map[string]any{"customer": "grace"},
		"status":     map[string]any{"phase": "pending"},
	}}

	created, err := scalable.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating the object: %v", err)
	}
	t.Cleanup(func() { _ = scalable.Delete(context.Background(), name, metav1.DeleteOptions{}) })

	if replicas, _, _ := unstructured.NestedInt64(created.Object, "spec", "replicas"); replicas != 1 {
		t.Errorf("spec.replicas in the response = %d, want the schema default 1", replicas)
	}

	// The default has to have reached the row, not just the response.
	stored, err := scalable.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading the object back: %v", err)
	}
	if replicas, _, _ := unstructured.NestedInt64(stored.Object, "spec", "replicas"); replicas != 1 {
		t.Errorf("spec.replicas in the row = %d, want the schema default 1", replicas)
	}
}

// querySQL runs a query straight against PostgreSQL and returns its output,
// for checking what a write actually left in a table the API does not serve.
func querySQL(t *testing.T, statement string) string {
	t.Helper()
	return querySQLAs(t, "crisp", statement)
}

// querySQLAs runs a query as a particular role, which is what it takes to see
// what a row-level security policy does: it does not apply to a superuser.
func querySQLAs(t *testing.T, role, statement string) string {
	t.Helper()

	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath,
		"-n", "kube-crisp", "exec", "deploy/postgres", "--",
		"psql", "-U", role, "-d", "store", "-tAc", statement)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("running %q: %v\n%s", statement, err, output)
	}

	// psql prints a command tag for each statement, so a query preceded by a
	// SET arrives as "SET\n1". The answer is the last line.
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

func newAuditedOrder(name, customer string, total int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "AuditedOrder",
		"metadata":   map[string]any{"name": name, "namespace": acmeNamespace},
		"spec":       map[string]any{"customer": customer, "totalCents": total},
		"status":     map[string]any{"phase": "pending"},
	}}
}

// TestTransactionalWriteReachesBothTables covers a projected kind that spans
// more than one table: the write has to be all or nothing.
func TestTransactionalWriteReachesBothTables(t *testing.T) {
	ctx := context.Background()
	audited := dynamicClient.Resource(auditedOrdersGVR).Namespace(acmeNamespace)

	name := "order-tx-e2e"
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf("DELETE FROM orders WHERE id = '%s'", name))
		execSQL(t, fmt.Sprintf("DELETE FROM order_events WHERE id = '%s'", name))
	})

	created, err := audited.Create(ctx, newAuditedOrder(name, "ada", 500), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating: %v", err)
	}
	if got := created.GetGeneration(); got != 1 {
		t.Errorf("metadata.generation = %d on a fresh object, want 1", got)
	}

	if got := querySQL(t, fmt.Sprintf("SELECT count(*) FROM order_events WHERE id = '%s' AND event = 'created'", name)); got != "1" {
		t.Errorf("order_events holds %s created rows, want 1", got)
	}
}

// TestTransactionalWriteRollsBack is the property the transaction exists for:
// the first statement must not survive the second one failing.
func TestTransactionalWriteRollsBack(t *testing.T) {
	ctx := context.Background()
	audited := dynamicClient.Resource(auditedOrdersGVR).Namespace(acmeNamespace)

	// order-acme-1 is seeded, so the insert violates the primary key after the
	// audit row has already been written.
	_, err := audited.Create(ctx, newAuditedOrder("order-acme-1", "ada", 500), metav1.CreateOptions{})
	if !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create error = %v, want AlreadyExists", err)
	}

	if got := querySQL(t, "SELECT count(*) FROM order_events WHERE id = 'order-acme-1'"); got != "0" {
		t.Errorf("order_events holds %s rows after the transaction failed, want 0", got)
	}
}

// TestFinalizerLifecycle covers the whole flow a controller depends on: a
// delete is accepted, the object stays while something is holding it, and
// clearing the last finalizer is what actually removes the row.
func TestFinalizerLifecycle(t *testing.T) {
	ctx := context.Background()
	audited := dynamicClient.Resource(auditedOrdersGVR).Namespace(acmeNamespace)

	name := "order-lifecycle-e2e"
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf("DELETE FROM orders WHERE id = '%s'", name))
		execSQL(t, fmt.Sprintf("DELETE FROM order_events WHERE id = '%s'", name))
	})

	obj := newAuditedOrder(name, "ada", 500)
	obj.SetFinalizers([]string{"example.com/drain"})
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       "checkout",
		UID:        "6f1c2b7e-0f1a-4f5a-9b2e-1d3c4b5a6e7f",
	}})

	created, err := audited.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating: %v", err)
	}
	if got := created.GetFinalizers(); len(got) != 1 {
		t.Fatalf("finalizers = %v, want the one that was set", got)
	}
	if got := created.GetOwnerReferences(); len(got) != 1 || got[0].Name != "checkout" {
		t.Fatalf("ownerReferences = %v, want the Deployment it was given", got)
	}
	if created.GetGeneration() != 1 {
		t.Errorf("metadata.generation = %d on a fresh object, want 1", created.GetGeneration())
	}

	// A spec change advances the generation, which is what a controller
	// compares against its observedGeneration.
	if err := unstructured.SetNestedField(created.Object, int64(900), "spec", "totalCents"); err != nil {
		t.Fatalf("preparing the update: %v", err)
	}
	updated, err := audited.Update(ctx, created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("updating: %v", err)
	}
	if got := updated.GetGeneration(); got != 2 {
		t.Errorf("metadata.generation = %d after a spec change, want 2", got)
	}

	// The delete is accepted, and the object stays.
	if err := audited.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting: %v", err)
	}
	terminating, err := audited.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading an object held by a finalizer: %v", err)
	}
	if terminating.GetDeletionTimestamp() == nil {
		t.Fatal("the object carries no deletionTimestamp after being deleted")
	}
	if got := terminating.GetFinalizers(); len(got) != 1 {
		t.Errorf("finalizers = %v, want the delete to have left them alone", got)
	}

	// Adding another one now would hold open something already on its way out.
	greedy := terminating.DeepCopy()
	greedy.SetFinalizers([]string{"example.com/drain", "example.com/forever"})
	if _, err := audited.Update(ctx, greedy, metav1.UpdateOptions{}); !apierrors.IsForbidden(err) {
		t.Errorf("adding a finalizer to a terminating object returned %v, want Forbidden", err)
	}

	// Letting go is what removes the row.
	released := terminating.DeepCopy()
	released.SetFinalizers(nil)
	if _, err := audited.Update(ctx, released, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("clearing the finalizer: %v", err)
	}
	if _, err := audited.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("reading after the last finalizer was cleared returned %v, want NotFound", err)
	}
	if got := querySQL(t, fmt.Sprintf("SELECT count(*) FROM orders WHERE id = '%s'", name)); got != "0" {
		t.Errorf("the row survived (%s rows)", got)
	}
}

// TestDeleteWithoutFinalizersRemovesImmediately keeps the ordinary path
// ordinary: nothing holding the object means nothing to wait for.
func TestDeleteWithoutFinalizersRemovesImmediately(t *testing.T) {
	ctx := context.Background()
	audited := dynamicClient.Resource(auditedOrdersGVR).Namespace(acmeNamespace)

	name := "order-nofinalizer-e2e"
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf("DELETE FROM orders WHERE id = '%s'", name))
		execSQL(t, fmt.Sprintf("DELETE FROM order_events WHERE id = '%s'", name))
	})

	if _, err := audited.Create(ctx, newAuditedOrder(name, "grace", 100), metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating: %v", err)
	}
	if err := audited.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("deleting: %v", err)
	}
	if _, err := audited.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get after delete returned %v, want NotFound", err)
	}
}

// TestRowLevelSecurityEnforcesTenancy is the strongest containment claim in the
// project: the projection's queries do not filter by tenant at all, so if the
// rows are still separated it is the database doing it, from a setting this
// server put on the connection.
func TestRowLevelSecurityEnforcesTenancy(t *testing.T) {
	ctx := context.Background()
	secured := dynamicClient.Resource(securedOrdersGVR)

	// The query behind this list has no WHERE clause.
	acme, err := secured.Namespace(acmeNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing in %s: %v", acmeNamespace, err)
	}
	if len(acme.Items) != 2 {
		var names []string
		for i := range acme.Items {
			names = append(names, acme.Items[i].GetName())
		}
		t.Fatalf("listing in %s returned %v, want the two acme rows", acmeNamespace, names)
	}
	for i := range acme.Items {
		if got := acme.Items[i].GetNamespace(); got != acmeNamespace {
			t.Errorf("object %q belongs to %q; the policy let another tenant's row through",
				acme.Items[i].GetName(), got)
		}
	}

	// A row that exists, in a namespace that may not see it.
	if _, err := secured.Namespace(acmeNamespace).Get(ctx, "secured-globex-1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("reading another tenant's row returned %v, want NotFound", err)
	}

	// Nor from any other namespace.
	if _, err := secured.Namespace(benchNamespace).Get(ctx, "secured-globex-1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("reading it from a third namespace returned %v, want NotFound", err)
	}

	// The row is really there — the policy is hiding it, not the absence of
	// data. Asking the database directly needs the same setting, which is the
	// clearest demonstration of what is doing the work.
	if got := querySQLAs(t, "crisp_app", "SET app.tenant = 'globex'; SELECT count(*) FROM secured_orders"); got != "1" {
		t.Errorf("the database reports %s rows for globex, want 1", got)
	}
	if got := querySQLAs(t, "crisp_app", "SELECT count(*) FROM secured_orders"); got != "0" {
		t.Errorf("the database reports %s rows with no tenant set, want 0", got)
	}
	// The rows are all there for a role the policy does not constrain.
	if got := querySQL(t, "SELECT count(*) FROM secured_orders"); got != "3" {
		t.Errorf("the table holds %s rows, want 3", got)
	}
}

// TestCompositeIdentityOverSQL covers a table no single column can name.
func TestCompositeIdentityOverSQL(t *testing.T) {
	ctx := context.Background()
	shipments := dynamicClient.Resource(shipmentsGVR).Namespace(acmeNamespace)

	list, err := shipments.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}

	var names []string
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}
	want := []string{"eu-1042", "us-1042", "eu-2001"}
	if len(names) < len(want) {
		t.Fatalf("listed %v, want at least %v", names, want)
	}

	// The two rows sharing an order number are different objects, which is the
	// whole point of a composite identity.
	eu, err := shipments.Get(ctx, "eu-1042", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading eu-1042: %v", err)
	}
	us, err := shipments.Get(ctx, "us-1042", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading us-1042: %v", err)
	}
	euCarrier, _, _ := unstructured.NestedString(eu.Object, "spec", "carrier")
	usCarrier, _, _ := unstructured.NestedString(us.Object, "spec", "carrier")
	if euCarrier == usCarrier {
		t.Errorf("both halves of the composite key read as %q", euCarrier)
	}

	// A name that cannot have come from those columns names no row.
	if _, err := shipments.Get(ctx, "nodashes", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("get with an unsplittable name returned %v, want NotFound", err)
	}

	// Writes address the same identity.
	created := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "store.example.com/v1alpha1",
		"kind":       "Shipment",
		"metadata":   map[string]any{"name": "ap-3003", "namespace": acmeNamespace},
		"spec":       map[string]any{"carrier": "fedex"},
	}}
	if _, err := shipments.Create(ctx, created, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating ap-3003: %v", err)
	}
	t.Cleanup(func() { _ = shipments.Delete(context.Background(), "ap-3003", metav1.DeleteOptions{}) })

	back, err := shipments.Get(ctx, "ap-3003", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading ap-3003 back: %v", err)
	}
	if carrier, _, _ := unstructured.NestedString(back.Object, "spec", "carrier"); carrier != "fedex" {
		t.Errorf("spec.carrier = %q, want %q", carrier, "fedex")
	}
}

// TestKeysetPagingOnANonNameColumn walks a collection ordered by something
// other than its name, which is where paging on the name silently skipped rows.
func TestKeysetPagingOnANonNameColumn(t *testing.T) {
	ctx := context.Background()
	shipments := dynamicClient.Resource(shipmentsGVR).Namespace(acmeNamespace)

	all, err := shipments.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("listing: %v", err)
	}

	seen := map[string]bool{}
	var token string
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("paging did not finish")
		}

		page, err := shipments.List(ctx, metav1.ListOptions{Limit: 1, Continue: token})
		if err != nil {
			t.Fatalf("listing a page: %v", err)
		}
		for i := range page.Items {
			name := page.Items[i].GetName()
			if seen[name] {
				t.Errorf("object %q appeared on more than one page", name)
			}
			seen[name] = true
		}

		token = page.GetContinue()
		if token == "" {
			break
		}
	}

	if len(seen) != len(all.Items) {
		t.Errorf("paging saw %d objects, but the collection holds %d", len(seen), len(all.Items))
	}
}

// appliedOrdersGVR is the projection that maps metadata.managedFields onto a
// column, which is what gives server-side apply somewhere to keep ownership.
var appliedOrdersGVR = schema.GroupVersionResource{
	Group: "store.example.com", Version: "v1alpha1", Resource: "appliedorders",
}

// TestServerSideApplyDetectsConflicts is what mapping managedFields buys.
//
// Merging works from the schema alone, but detecting a conflict needs the
// record of who owns which field to survive between requests — and an object
// rebuilt from a row carries none unless a column holds it. Without that column
// two controllers applying the same field each silently overwrite the other and
// --force-conflicts never means anything.
func TestServerSideApplyDetectsConflicts(t *testing.T) {
	ctx := context.Background()
	client := dynamicClient.Resource(appliedOrdersGVR).Namespace(acmeNamespace)

	apply := func(manager, customer string, force bool) (*unstructured.Unstructured, error) {
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "store.example.com/v1alpha1",
			"kind":       "AppliedOrder",
			"metadata":   map[string]any{"name": "applied-1", "namespace": acmeNamespace},
			"spec":       map[string]any{"customer": customer},
		}}
		body, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("encoding the apply: %v", err)
		}
		return client.Patch(ctx, "applied-1", types.ApplyPatchType, body, metav1.PatchOptions{
			FieldManager: manager,
			Force:        &force,
		})
	}

	// The first manager takes ownership of spec.customer.
	first, err := apply("manager-one", "ada", false)
	if err != nil {
		t.Fatalf("the first apply failed: %v", err)
	}
	if len(first.GetManagedFields()) == 0 {
		t.Fatal("the applied object carries no managedFields, so ownership was not stored")
	}

	// It has to still be there on the next read, or the record did not survive
	// the round trip through the database.
	reread, err := client.Get(ctx, "applied-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("re-reading: %v", err)
	}
	// An object that arrives with no managedFields makes the field manager
	// invent a "before-first-apply" entry owning whatever the row already held,
	// so more than one entry can mention the field. The one that decides a
	// conflict is the Apply entry.
	var owned bool
	for _, entry := range reread.GetManagedFields() {
		if entry.Operation != metav1.ManagedFieldsOperationApply || entry.Manager != "manager-one" {
			continue
		}
		if entry.FieldsV1 != nil && strings.Contains(entry.FieldsV1.GetRawString(), "f:customer") {
			owned = true
		}
	}
	if !owned {
		t.Fatalf("manager-one does not own spec.customer after a re-read; entries: %+v",
			reread.GetManagedFields())
	}

	// A second manager writing the same field is the conflict this exists to
	// catch.
	if _, err := apply("manager-two", "grace", false); !apierrors.IsConflict(err) {
		t.Fatalf("the conflicting apply returned %v, want a conflict", err)
	}

	// And forcing it is what takes ownership over.
	forced, err := apply("manager-two", "grace", true)
	if err != nil {
		t.Fatalf("the forced apply failed: %v", err)
	}
	if got, _, _ := unstructured.NestedString(forced.Object, "spec", "customer"); got != "grace" {
		t.Errorf("spec.customer = %q after a forced apply, want grace", got)
	}

	// Restore, so the test can run again against the same cluster.
	if _, err := apply("manager-one", "ada", true); err != nil {
		t.Fatalf("restoring: %v", err)
	}
}

// TestListsCarryTheirOwnKind is the other half of the same bug, and the half a
// client is more likely to hit.
//
// Every projected kind is registered against one Go type, so the scheme's
// reverse lookup returned every kind in the group version and the encoder
// stamped responses with the first of them: a list of orders came back as a
// ScalableOrderList. kubectl never showed it, because it asks for a Table — but
// a typed client, or an informer decoding strictly, sees a kind it did not ask
// for.
func TestListsCarryTheirOwnKind(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		gvr  schema.GroupVersionResource
		kind string
	}{
		{ordersGVR, "OrderList"},
		{appliedOrdersGVR, "AppliedOrderList"},
	} {
		t.Run(tc.gvr.Resource, func(t *testing.T) {
			list, err := dynamicClient.Resource(tc.gvr).Namespace(acmeNamespace).
				List(ctx, metav1.ListOptions{Limit: 1})
			if err != nil {
				t.Fatalf("List() returned error: %v", err)
			}
			if got := list.GetKind(); got != tc.kind {
				t.Errorf("list kind = %q, want %q", got, tc.kind)
			}
			for _, item := range list.Items {
				if got, want := item.GetKind(), strings.TrimSuffix(tc.kind, "List"); got != want {
					t.Errorf("item kind = %q, want %q", got, want)
				}
			}
		})
	}
}

var (
	splitOrdersGVR      = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "splitorders"}
	taggedOrdersGVR     = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "taggedorders"}
	notifiedOrdersGVR   = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "notifiedorders"}
	polledOrdersGVR     = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "polledorders"}
	teamOrdersGVR       = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "teamorders"}
	tombstonedOrdersGVR = schema.GroupVersionResource{Group: "store.example.com", Version: "v1alpha1", Resource: "tombstonedorders"}
)

// TestReadReplicaServesReadsAndNotWrites.
//
// The two databases hold the same row with different values and nothing
// replicates between them, so which one answered is never in doubt — a cruder
// version of replication lag than any real replica has, and the point of it.
func TestReadReplicaServesReadsAndNotWrites(t *testing.T) {
	ctx := context.Background()
	client := dynamicClient.Resource(splitOrdersGVR).Namespace(acmeNamespace)

	// Wait for the replica to be in service before asserting that reads go to
	// it.
	//
	// A read that finds the replica unreachable is answered from the primary and
	// leaves the replica alone for a few seconds, so that an outage costs one
	// failed read per interval rather than one per request. Any earlier test
	// that took PostgreSQL down — TestDatabaseOutage does exactly that — can
	// therefore leave this projection reading from the primary, correctly, for
	// as long as that cooldown lasts. Asserting immediately made this pass or
	// fail on how long the tests in between happened to take.
	var got *unstructured.Unstructured
	if err := wait.PollUntilContextTimeout(ctx, time.Second, 60*time.Second, true,
		func(ctx context.Context) (bool, error) {
			read, err := client.Get(ctx, "split-1", metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			customer, _, _ := unstructured.NestedString(read.Object, "spec", "customer")
			if customer != "from-the-replica" {
				t.Logf("read %q; waiting for the replica to be back in service", customer)
				return false, nil
			}
			got = read
			return true, nil
		}); err != nil {
		t.Fatalf("reads never came from the replica: %v", err)
	}

	list, err := client.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	for _, item := range list.Items {
		if customer, _, _ := unstructured.NestedString(item.Object, "spec", "customer"); customer != "from-the-replica" {
			t.Errorf("List() read %q, want from-the-replica", customer)
		}
	}

	// The write lands on the primary, so the replica keeps its own value and
	// the next read still sees it. That is the trade a replica makes, stated
	// as a test rather than as a caveat.
	updated := got.DeepCopy()
	if err := unstructured.SetNestedField(updated.Object, "written-to-primary", "spec", "customer"); err != nil {
		t.Fatalf("building the update: %v", err)
	}
	if _, err := client.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("Update() returned error: %v", err)
	}

	after, err := client.Get(ctx, "split-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if customer, _, _ := unstructured.NestedString(after.Object, "spec", "customer"); customer != "from-the-replica" {
		t.Errorf("a read after the write returned %q; it should still be the replica's copy", customer)
	}

	primary := querySQL(t, "SELECT customer FROM replicated_orders WHERE id = 'split-1'")
	if !strings.Contains(primary, "written-to-primary") {
		t.Errorf("the primary holds %q after the write, want written-to-primary", strings.TrimSpace(primary))
	}

	// Put it back, so the test can run again against the same cluster.
	//
	// updated_at as well as customer, and that is the whole of it: updated_at is
	// the mapped resourceVersion, and the update above moved the primary's to a
	// timestamp while the replica's stayed at the seeded value. Restoring only
	// the customer left the two disagreeing about the version, so the next run
	// read '1' from the replica, sent it as the precondition, matched no row on
	// the primary, and failed with a conflict. The fixture has to go back
	// exactly as it was seeded, not merely look like it.
	execSQL(t, "UPDATE replicated_orders SET customer = 'from-the-primary', updated_at = '1' WHERE id = 'split-1'")
}

// TestLabelsComeOutOfAJSONColumn, with a promoted key overriding it.
func TestLabelsComeOutOfAJSONColumn(t *testing.T) {
	ctx := context.Background()
	client := dynamicClient.Resource(taggedOrdersGVR).Namespace(acmeNamespace)

	first, err := client.Get(ctx, "tagged-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	labels := first.GetLabels()
	if got := labels["team"]; got != "payments" {
		t.Errorf("labels[team] = %q, want payments; the JSON column was not read", got)
	}
	if got := labels["tier"]; got != "gold" {
		t.Errorf("labels[tier] = %q, want gold", got)
	}
	if got := labels["store.example.com/status"]; got != "shipped" {
		t.Errorf("labels[status] = %q, want shipped; the promoted column should supply it", got)
	}

	// A row whose JSON column is NULL still carries the promoted label.
	third, err := client.Get(ctx, "tagged-3", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := third.GetLabels()["store.example.com/status"]; got != "shipped" {
		t.Errorf("a row with no JSON labels lost its promoted one: %v", third.GetLabels())
	}
}

// TestSelectorsAreAnsweredByTheDatabase covers the push-down for both grammars:
// = and != for a field, and =, != and in for a label.
func TestSelectorsAreAnsweredByTheDatabase(t *testing.T) {
	ctx := context.Background()
	client := dynamicClient.Resource(taggedOrdersGVR).Namespace(acmeNamespace)

	names := func(opts metav1.ListOptions) []string {
		list, err := client.List(ctx, opts)
		if err != nil {
			t.Fatalf("List(%+v) returned error: %v", opts, err)
		}
		var out []string
		for _, item := range list.Items {
			out = append(out, item.GetName())
		}
		sort.Strings(out)
		return out
	}

	for _, tc := range []struct {
		name string
		opts metav1.ListOptions
		want []string
	}{
		{"field equality", metav1.ListOptions{FieldSelector: "spec.customer=ada"}, []string{"tagged-1"}},
		{"field inequality", metav1.ListOptions{FieldSelector: "spec.customer!=ada"}, []string{"tagged-2", "tagged-3"}},
		{"label equality", metav1.ListOptions{LabelSelector: "store.example.com/status=pending"}, []string{"tagged-2"}},
		{"label inequality", metav1.ListOptions{LabelSelector: "store.example.com/status!=pending"}, []string{"tagged-1", "tagged-3"}},
		{"label set membership", metav1.ListOptions{LabelSelector: "store.example.com/status in (pending,shipped)"},
			[]string{"tagged-1", "tagged-2", "tagged-3"}},
		{"a label only the JSON column carries", metav1.ListOptions{LabelSelector: "team=payments"}, []string{"tagged-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := names(tc.opts); !slices.Equal(got, tc.want) {
				t.Errorf("List() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRowsAreScopedToTheCallersGroups: authorization in Kubernetes is mostly by
// group, so a projection that scopes its rows the way RBAC scopes its verbs
// needs more than a username.
func TestRowsAreScopedToTheCallersGroups(t *testing.T) {
	ctx := context.Background()
	client := dynamicClient.Resource(teamOrdersGVR).Namespace(acmeNamespace)

	list, err := client.List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	var names []string
	for _, item := range list.Items {
		names = append(names, item.GetName())
	}
	// Every authenticated caller is in system:authenticated, so the row owned
	// by it is visible — and the one owned by a group nobody has is not.
	if !slices.Equal(names, []string{"team-authenticated"}) {
		t.Errorf("List() = %v, want just the row owned by a group the caller is in", names)
	}

	if _, err := client.Get(ctx, "team-nobody", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("Get() on a row owned by another group = %v, want NotFound", err)
	}
}

// TestWatchSeesADeletionWithoutAFullResync: the projection under test has
// fullResyncInterval: 0, so nothing re-reads the collection. Only the deletion
// query can tell a watcher the row is gone — which is the whole point, because
// that scan is what makes a large table expensive to watch.
func TestWatchSeesADeletionWithoutAFullResync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client := dynamicClient.Resource(tombstonedOrdersGVR).Namespace(acmeNamespace)

	// Its own table and its own row, so nothing else in the suite is disturbed.
	const name = "doomed-1"
	if _, err := client.Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("the fixture row is missing: %v", err)
	}
	t.Cleanup(func() {
		execSQL(t, fmt.Sprintf(
			"INSERT INTO tombstoned_orders (id, tenant, customer, updated_at) "+
				"VALUES ('%s', 'acme', 'ada', clock_timestamp()::text) "+
				"ON CONFLICT (id) DO NOTHING", name))
		execSQL(t, fmt.Sprintf("DELETE FROM order_tombstones WHERE id = '%s'", name))
	})

	watcher, err := client.Watch(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}
	defer watcher.Stop()

	// Drain the initial state so the deletion event is unambiguous.
	settled := time.After(5 * time.Second)
drain:
	for {
		select {
		case <-watcher.ResultChan():
		case <-settled:
			break drain
		}
	}

	if err := client.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	deadline := time.After(60 * time.Second)
	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				t.Fatal("the watch closed before the deletion arrived")
			}
			if event.Type != watch.Deleted {
				continue
			}
			if obj, ok := event.Object.(*unstructured.Unstructured); ok && obj.GetName() == name {
				return
			}
		case <-deadline:
			t.Fatal("no Deleted event arrived; with the full resync off, only the deletion query could report it")
		}
	}
}

// TestPollingLeaderIsReported: with several replicas exactly one should hold
// the lease, and a dashboard cannot tell a follower from a missing series
// unless every replica publishes the gauge.
func TestPollingLeaderIsReported(t *testing.T) {
	ctx := context.Background()

	leases := schema.GroupVersionResource{
		Group: "coordination.k8s.io", Version: "v1", Resource: "leases",
	}
	lease, err := dynamicClient.Resource(leases).Namespace("kube-crisp").
		Get(ctx, "kube-crisp-poller", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the polling Lease was not created: %v", err)
	}
	holder, found, _ := unstructured.NestedString(lease.Object, "spec", "holderIdentity")
	if !found || holder == "" {
		t.Error("the Lease has no holder; nothing is polling at the configured interval")
	}

	// The metrics themselves are asserted in unit tests. They cannot be scraped
	// from here: an APIService routes /apis/<group>/<version> and nothing else,
	// and a request through the Service proxy reaches the server without the
	// caller's identity, so /metrics answers it as system:anonymous.
	//
	// What only this test can show is that the lease exists at all, which means
	// the flag, the RBAC and the election are wired end to end.
}

// TestWatchIsWokenByTheDatabase covers spec.watch.notify end to end.
//
// Both projections poll every 60 seconds, and the change is made in SQL rather
// than through the API, so nothing kube-crisp does can find it early. One is
// subscribed to a channel a trigger sends on; the other is not. If the
// notification does not arrive, the only thing left is the timer, and the test
// runs out of patience long before it fires — which is the point. A short poll
// interval would make this pass whether or not the feature worked.
//
// This is the failure mode worth an e2e test: a subscription that silently
// stops delivering looks exactly like a table where nothing is changing, and
// every request keeps working while watches quietly slow to the interval they
// were meant to have escaped.
func TestWatchIsWokenByTheDatabase(t *testing.T) {
	ctx := context.Background()

	t.Cleanup(func() {
		execSQL(t, "UPDATE notified_orders SET customer = 'ada', updated_at = '1' WHERE id = 'notified-1'")
		execSQL(t, "UPDATE polled_orders SET customer = 'ada', updated_at = '1' WHERE id = 'polled-1'")
	})

	// How long to wait for an event that a notification should produce in
	// milliseconds. Well under the 60s poll interval, so a pass cannot come
	// from a tick.
	const patience = 20 * time.Second

	watchFor := func(gvr schema.GroupVersionResource) (watch.Interface, func()) {
		t.Helper()

		client := dynamicClient.Resource(gvr).Namespace(acmeNamespace)
		// From the current version, so only later changes arrive and the
		// initial contents are not mistaken for the event being waited on.
		list, err := client.List(ctx, metav1.ListOptions{})
		if err != nil {
			t.Fatalf("List(%s) returned error: %v", gvr.Resource, err)
		}

		w, err := client.Watch(ctx, metav1.ListOptions{ResourceVersion: list.GetResourceVersion()})
		if err != nil {
			t.Fatalf("Watch(%s) returned error: %v", gvr.Resource, err)
		}
		return w, w.Stop
	}

	awaitCustomer := func(w watch.Interface, want string, within time.Duration) bool {
		t.Helper()

		deadline := time.After(within)
		for {
			select {
			case event, open := <-w.ResultChan():
				if !open {
					return false
				}
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}
				if customer, _, _ := unstructured.NestedString(obj.Object, "spec", "customer"); customer == want {
					return true
				}
			case <-deadline:
				return false
			}
		}
	}

	notified, stopNotified := watchFor(notifiedOrdersGVR)
	defer stopNotified()
	polled, stopPolled := watchFor(polledOrdersGVR)
	defer stopPolled()

	// The watches have to be established before the change, or the poller has
	// nothing subscribed to wake. Both caches start polling on their first
	// watcher, and the subscription is made alongside.
	time.Sleep(2 * time.Second)

	// Written in SQL, so the API server learns of it only from the database.
	//
	// Timed from before the write rather than after: execSQL shells out to
	// kubectl exec, which is a second or so of its own, and by the time it
	// returns the notification has already been delivered. What comes out is an
	// upper bound that includes the kubectl round trip — good enough to say this
	// was not a poll, and not a measurement of the notification itself. That one
	// is in pkg/sql, against PostgreSQL directly, and is single-digit
	// milliseconds.
	start := time.Now()
	execSQL(t, "UPDATE notified_orders SET customer = 'woken', updated_at = '2' WHERE id = 'notified-1'")
	execSQL(t, "UPDATE polled_orders SET customer = 'woken', updated_at = '2' WHERE id = 'polled-1'")

	if !awaitCustomer(notified, "woken", patience) {
		t.Fatalf("the notified projection saw no event within %v, so the change was found by "+
			"neither the notification nor anything else; its poll interval is 60s", patience)
	}
	elapsed := time.Since(start)
	t.Logf("the notified watch had the change %v after the write was issued, "+
		"including the kubectl exec that made it", elapsed.Round(time.Millisecond))

	// The bound that means something: the poll interval is 60s, so anything
	// inside this window cannot have come from a tick.
	if elapsed > 15*time.Second {
		t.Errorf("the change took %v to arrive, which is close enough to the 60s poll "+
			"interval to be a tick rather than a notification", elapsed)
	}

	// And the control, which has only its timer: it should still be waiting.
	if arrived := awaitCustomer(polled, "woken", 5*time.Second); arrived {
		t.Error("the projection without notify saw the change too, so this test is not " +
			"measuring the notification — something else is finding changes early")
	} else {
		t.Log("the projection without notify is still waiting, as its 60s interval says it should")
	}
}

// TestNotifyIsRejectedWhereItCannotWork: the setting depends on the database
// rather than on the projection, so asking for it on a driver that cannot push
// has to fail the apply rather than produce a watch that quietly polls.
func TestNotifyIsRejectedWhereItCannotWork(t *testing.T) {
	ctx := context.Background()

	projection := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "crisp.kubecrisp.io/v1alpha1",
		"kind":       "CustomResourceProjection",
		"metadata":   map[string]any{"name": "notify-on-sqlite"},
		"spec": map[string]any{
			"dataSource": map[string]any{
				"driver":    "sqlite",
				"secretRef": map[string]any{"name": "sqlite-db", "namespace": "kube-crisp"},
			},
			"resource": map[string]any{
				"group": "store.example.com", "version": "v1alpha1",
				"kind": "Nope", "plural": "nopes", "scope": "Namespaced",
				"schema": map[string]any{"type": "object"},
			},
			"queries": map[string]any{
				"list": map[string]any{"sql": "SELECT id, tenant FROM items"},
			},
			"mapping": map[string]any{"name": "id", "namespace": "tenant"},
			"watch": map[string]any{
				"notify": map[string]any{"channel": "nope_changed"},
			},
		},
	}}

	_, err := dynamicClient.Resource(crpGVR).Create(ctx, projection, metav1.CreateOptions{})
	if err == nil {
		_ = dynamicClient.Resource(crpGVR).Delete(ctx, "notify-on-sqlite", metav1.DeleteOptions{})
		t.Fatal("a notify channel on a driver that cannot push notifications was accepted")
	}
	if !strings.Contains(err.Error(), "notification") {
		t.Errorf("error %q does not say why it was refused", err)
	}
}
