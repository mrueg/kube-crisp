//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// The identity the measured traffic runs as.
//
// kube-crisp is an aggregated API server, so every request it serves is
// authorized twice: once by the kube-apiserver before the request is proxied,
// and again by kube-crisp, which delegates the decision by POSTing a
// SubjectAccessReview back to the cluster. A native CRD is authorized once, in
// process.
//
// That second check is the one an admin never exercises: system:masters is in
// the always-allow list and is answered before the delegating authorizer is
// reached. Measuring as the admin therefore leaves out a per-request cost that
// only the projection pays — which does not merely make both columns optimistic,
// it biases the comparison towards the side being advertised.
var benchAsAdmin = os.Getenv("BENCH_AS_ADMIN") != ""

const benchSubject = "kube-crisp-bench"

// benchmarkConfig returns the credentials the measured traffic should use.
func benchmarkConfig(ctx context.Context, t *testing.T) *rest.Config {
	t.Helper()

	if benchAsAdmin {
		t.Log("measuring as the admin identity: the delegated authorization " +
			"kube-crisp pays per request is short-circuited for system:masters")
		return restConfig
	}

	fallback := func(what string, err error) *rest.Config {
		t.Logf("WARNING: %s (%v); measuring as the admin identity instead, which does not "+
			"exercise the delegated authorization a projected request pays for", what, err)
		return restConfig
	}

	cluster, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fallback("could not build a cluster client", err)
	}

	created := func(err error) error {
		if err == nil || apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	if err := created(func() error {
		_, err := cluster.CoreV1().ServiceAccounts(benchNamespace).Create(ctx,
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: benchSubject, Namespace: benchNamespace}},
			metav1.CreateOptions{})
		return err
	}()); err != nil {
		return fallback("could not create the benchmark service account", err)
	}

	// One role over both backends, so neither is measured with more or less
	// authorization work than the other.
	//
	// Every resource in those two groups rather than a list of the ones the
	// benchmarks touch today: the subject is still narrow — two API groups and
	// nothing else in the cluster — and the property that matters is that it is
	// not system:masters, so the delegated authorizer is actually reached.
	// Enumerating would only mean a benchmark added later fails on RBAC.
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: benchSubject},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{benchGVR.Group, ordersGVR.Group},
			Resources: []string{"*"},
			Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		}},
	}
	if _, err := cluster.RbacV1().ClusterRoles().Update(ctx, role, metav1.UpdateOptions{}); err != nil {
		if err := created(func() error {
			_, err := cluster.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{})
			return err
		}()); err != nil {
			return fallback("could not create the benchmark role", err)
		}
	}

	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: benchSubject},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: benchSubject},
		Subjects: []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: benchSubject, Namespace: benchNamespace},
		},
	}
	if err := created(func() error {
		_, err := cluster.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{})
		return err
	}()); err != nil {
		return fallback("could not bind the benchmark role", err)
	}

	hour := int64(3600)
	token, err := cluster.CoreV1().ServiceAccounts(benchNamespace).CreateToken(ctx, benchSubject,
		&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: &hour}},
		metav1.CreateOptions{})
	if err != nil {
		return fallback("could not mint a token for the benchmark subject", err)
	}

	cfg := rest.CopyConfig(restConfig)
	cfg.BearerToken = token.Status.Token
	cfg.BearerTokenFile = ""
	// The admin credentials would otherwise win over the token.
	cfg.CertFile, cfg.KeyFile = "", ""
	cfg.CertData, cfg.KeyData = nil, nil
	cfg.Username, cfg.Password = "", ""

	t.Logf("measuring as system:serviceaccount:%s:%s, so the delegated authorization "+
		"kube-crisp pays per request is included", benchNamespace, benchSubject)
	return cfg
}

// benchmarkClient is the client every comparison measures through, built once.
//
// It falls back to the admin identity if the subject cannot actually read what
// is about to be measured. A benchmark that fails on RBAC tells you nothing at
// all, while one that reports which identity it ran as is still a measurement —
// so the failure mode here is a louder number rather than no number.
func benchmarkClient(ctx context.Context, t *testing.T) dynamic.Interface {
	t.Helper()

	benchClientOnce.Do(func() {
		// Nothing in here calls t.Fatal. sync.Once marks itself done even when
		// its function exits through runtime.Goexit, which is what t.Fatal does
		// — so a failure here would leave every later caller holding a nil
		// client rather than a failing test.
		benchClient = dynamicClient

		client, err := dynamic.NewForConfig(benchmarkConfig(ctx, t))
		if err != nil {
			t.Logf("WARNING: could not build the benchmark client (%v); "+
				"falling back to the admin identity", err)
			return
		}

		if !benchAsAdmin {
			for _, gvr := range []schema.GroupVersionResource{benchGVR, ordersGVR} {
				if _, err := client.Resource(gvr).Namespace(benchNamespace).
					List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
					t.Logf("WARNING: the benchmark subject cannot read %s (%v); "+
						"falling back to the admin identity, which does not exercise the "+
						"delegated authorization a projected request pays for", gvr.Resource, err)
					return
				}
			}
		}
		benchClient = client
	})

	return benchClient
}

var (
	benchClientOnce sync.Once
	benchClient     dynamic.Interface
)

// TestAuthorizationCostComparison measures what the second authorization costs,
// which is the one asymmetry between the two backends that is structural rather
// than incidental.
//
// A native custom resource is authorized inside the kube-apiserver. A projected
// one is authorized there and then again by kube-crisp, which asks the cluster.
// The same read, as the same subject, against both.
func TestAuthorizationCostComparison(t *testing.T) {
	ctx := context.Background()

	seedBenchOrders(ctx, t)
	names := sampleNames(ctx, t, benchGVR, getIterations)

	asAdmin := dynamicClient
	asUser, err := dynamic.NewForConfig(benchmarkConfig(ctx, t))
	if err != nil {
		t.Fatalf("building the subject client: %v", err)
	}
	if benchAsAdmin {
		t.Skip("BENCH_AS_ADMIN is set, so there is no second identity to compare against")
	}

	get := func(client dynamic.Interface, gvr schema.GroupVersionResource) func(int) error {
		return func(i int) error {
			_, err := client.Resource(gvr).Namespace(benchNamespace).
				Get(ctx, names[i%len(names)], metav1.GetOptions{})
			return err
		}
	}

	before := subjectAccessReviews(ctx, t)

	results := []result{
		measure(t, "GET as cluster admin", "CRD (etcd)", getIterations, get(asAdmin, benchGVR)),
		measure(t, "GET as cluster admin", "projection (postgres)", getIterations, get(asAdmin, ordersGVR)),
		measure(t, "GET as an RBAC subject", "CRD (etcd)", getIterations, get(asUser, benchGVR)),
		measure(t, "GET as an RBAC subject", "projection (postgres)", getIterations, get(asUser, ordersGVR)),
	}
	report(t, results)

	reviews := subjectAccessReviews(ctx, t) - before
	t.Logf("the cluster served %d SubjectAccessReviews during this test", reviews)
	t.Log("Every projected request as a non-admin subject costs one, unless kube-crisp's " +
		"authorizer cache already holds the answer — and that cache keys on the object name, " +
		"so a get of a distinct object misses almost every time. A native CRD costs none: it " +
		"is authorized in the process that is already handling the request.")
}

// subjectAccessReviews reports how many the cluster has answered, which is what
// an aggregated API server asks it per request its cache has not seen.
func subjectAccessReviews(ctx context.Context, t *testing.T) int {
	t.Helper()

	cluster, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return 0
	}
	raw, err := cluster.Discovery().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		return 0
	}

	var total int
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "apiserver_request_total{") {
			continue
		}
		if !strings.Contains(line, `resource="subjectaccessreviews"`) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if count, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			total += int(count)
		}
	}
	return total
}

// TestUnrelatedTrafficComparison measures what the rest of the cluster feels
// while each backend is written to.
//
// This is the claim the whole project rests on — "nothing is copied into etcd" —
// and every other number here is a latency the claim is supposed to buy. So it
// is worth measuring rather than asserting.
//
// The probe is a write rather than a read: a read of an unchanging object comes
// from the kube-apiserver's watch cache and would measure almost nothing, while
// a ConfigMap goes to etcd, which is the thing a projection is meant to spare.
func TestUnrelatedTrafficComparison(t *testing.T) {
	ctx := context.Background()

	cluster, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("building a cluster client: %v", err)
	}
	client := benchmarkClient(ctx, t)

	// Seeded here rather than assumed. These tests read the native collection,
	// and it exists only because some other test created it — which holds when
	// the whole suite runs and not when a shard runs this one alone. Idempotent:
	// it skips when the cluster already holds the right number of objects.
	seedBenchOrders(ctx, t)

	// A baseline with nothing else running, so the two loaded rows have
	// something to be compared against rather than only each other.
	// Warmed first, and discarded. The first ConfigMap writes of a run pay for
	// connection setup and cold caches, and measuring them as the idle baseline
	// made the loaded rows look faster than doing nothing — which is how this
	// read on its first run.
	probeUnrelatedWrites(ctx, t, cluster, "warm-up", func() {
		time.Sleep(5 * time.Second)
	})

	// A fixed window rather than a fixed number of objects, so all three rows
	// hold the probe open for the same time and are counted from the same
	// number of samples. A fixed count made the loaded windows a third the
	// length of the idle one, which compared nine samples against fifty.
	window := time.Duration(envInt("UNRELATED_SECONDS", 20)) * time.Second

	idle := probeUnrelatedWrites(ctx, t, cluster, "nothing else running", func() {
		time.Sleep(window)
	})
	// Cleanup happens after the probe has stopped, not inside load(): deleting
	// eleven thousand objects takes longer than the window that created them,
	// and leaving it inside meant the loaded rows were counted over a window
	// three times the length of the idle one.
	var nativeCreated, projectedCreated []string

	native := probeUnrelatedWrites(ctx, t, cluster, "native objects being created", func() {
		nativeCreated = createForDuration(ctx, t, client, benchGVR, "unrelated-native", window)
	})
	removeObjects(t, client, benchGVR, nativeCreated)

	projected := probeUnrelatedWrites(ctx, t, cluster, "projected objects being created", func() {
		projectedCreated = createForDuration(ctx, t, client, ordersGVR, "unrelated-projected", window)
	})
	removeObjects(t, client, ordersGVR, projectedCreated)

	report(t, []result{idle, native, projected})

	t.Log("A ConfigMap write has nothing to do with either workload. What it costs while " +
		"one of them is running is what the rest of the cluster pays for that workload — " +
		"and the projected objects never reach etcd at all, so the gap between the two " +
		"loaded rows is what offloading them is worth here.")
	t.Log("Expect both loaded rows to beat the idle one on the median, which is not a " +
		"measurement error: etcd batches concurrent writes into one WAL fsync, so a write " +
		"arriving while eight others are in flight rides a commit it would otherwise have " +
		"paid for alone. The median therefore says more about batching than contention. " +
		"The tail is where contention shows, so p95 and p99 are the columns to read.")
}

// probeUnrelatedWrites writes and deletes a small ConfigMap every 200ms for as
// long as load runs, and reports what those writes cost.
func probeUnrelatedWrites(
	ctx context.Context,
	t *testing.T,
	cluster kubernetes.Interface,
	during string,
	load func(),
) result {
	t.Helper()

	scenario := "unrelated ConfigMap write"
	done := make(chan struct{})
	samples := make(chan []time.Duration, 1)

	go func() {
		var durations []time.Duration
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for i := 0; ; i++ {
			select {
			case <-done:
				samples <- durations
				return
			case <-ticker.C:
			}

			probe := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("unrelated-probe-%d", i),
					Namespace: benchNamespace,
				},
				Data: map[string]string{"probe": "1"},
			}

			start := time.Now()
			if _, err := cluster.CoreV1().ConfigMaps(benchNamespace).Create(ctx, probe, metav1.CreateOptions{}); err != nil {
				continue
			}
			durations = append(durations, time.Since(start))
			_ = cluster.CoreV1().ConfigMaps(benchNamespace).Delete(ctx, probe.Name, metav1.DeleteOptions{})
		}
	}()

	load()
	close(done)

	durations := <-samples
	if len(durations) == 0 {
		t.Fatal("the probe collected no samples")
	}
	t.Logf("probe: %d samples while %s", len(durations), during)
	return summarise(scenario, during, durations)
}

// unrelatedLoadWorkers is how many clients write into a backend while the probe
// runs. One writer is not load: it spends most of its time waiting, and what is
// being asked is what the cluster feels when a backend is actually busy.
var unrelatedLoadWorkers = envInt("UNRELATED_WORKERS", 8)

// createForDuration writes objects into a backend as fast as it can for a fixed
// window, then removes them.
func createForDuration(
	ctx context.Context,
	t *testing.T,
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	prefix string,
	window time.Duration,
) []string {
	t.Helper()

	apiVersion, kind := "bench.example.com/v1alpha1", "BenchOrder"
	if gvr != benchGVR {
		apiVersion, kind = "store.example.com/v1alpha1", "Order"
	}
	resource := client.Resource(gvr).Namespace(benchNamespace)

	var (
		mu      sync.Mutex
		created []string
		wg      sync.WaitGroup
	)
	deadline := time.Now().Add(window)

	for worker := range unrelatedLoadWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				name := fmt.Sprintf("%s-%d-%06d", prefix, worker, i)
				obj := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": apiVersion,
					"kind":       kind,
					"metadata":   map[string]any{"name": name, "namespace": benchNamespace},
					"spec": map[string]any{
						"customer":   fmt.Sprintf("probe-%d", i),
						"totalCents": int64(1000 + i),
					},
					"status": map[string]any{"phase": "pending"},
				}}
				if _, err := resource.Create(ctx, obj, metav1.CreateOptions{}); err != nil {
					// A failure here is load that did not happen, not a reason
					// to fail the measurement of what did.
					continue
				}
				mu.Lock()
				created = append(created, name)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	t.Logf("wrote %d objects to %s in %v", len(created), gvr.Resource, window)
	return created
}

// removeObjects takes the load back out.
//
// Every delete is checked and retried, and the whole thing fails loudly if any
// survive. Ignoring the errors left two thousand of eleven thousand objects
// behind, which the next benchmark to run — whose first act is to assert an
// exact object count — reported as its own failure. A test that cannot undo what
// it did has to say so where it happened.
func removeObjects(t *testing.T, client dynamic.Interface, gvr schema.GroupVersionResource, names []string) {
	t.Helper()

	ctx := context.Background()
	resource := client.Resource(gvr).Namespace(benchNamespace)

	remaining := names
	for attempt := 1; attempt <= 3 && len(remaining) > 0; attempt++ {
		var failed []string
		for _, name := range remaining {
			if err := resource.Delete(ctx, name, metav1.DeleteOptions{}); err != nil &&
				!apierrors.IsNotFound(err) {
				failed = append(failed, name)
			}
		}
		if len(failed) > 0 {
			t.Logf("%d of %d deletes from %s failed on attempt %d; retrying",
				len(failed), len(remaining), gvr.Resource, attempt)
		}
		remaining = failed
	}

	if len(remaining) > 0 {
		t.Fatalf("%d objects could not be removed from %s; the next benchmark to run "+
			"asserts an exact object count and would fail on this instead",
			len(remaining), gvr.Resource)
	}
	t.Logf("removed %d objects from %s", len(names), gvr.Resource)
}

// TestStorageAccounting is the other half of the trade: what the cluster's own
// storage never has to hold.
//
// The kube-apiserver reports how many objects of each resource it stores.
// Projected objects are answered from PostgreSQL at request time, so they should
// not appear in it at all — not a smaller number, no line.
func TestStorageAccounting(t *testing.T) {
	ctx := context.Background()

	seedBenchOrders(ctx, t)

	stored := storedObjects(ctx, t)

	nativeResource := benchGVR.Resource + "." + benchGVR.Group
	projectedResource := ordersGVR.Resource + "." + ordersGVR.Group

	native, nativeFound := stored[nativeResource]
	projected, projectedFound := stored[projectedResource]

	t.Logf("objects the cluster's own apiserver reports storing:")
	t.Logf("  %-40s %d", nativeResource, native)
	if projectedFound {
		t.Logf("  %-40s %d", projectedResource, projected)
	} else {
		t.Logf("  %-40s no line at all", projectedResource)
	}

	if !nativeFound || native < rowCount {
		t.Errorf("the native CRD reports %d stored objects, want at least %d — "+
			"without that this test proves nothing about the other row", native, rowCount)
	}
	if projectedFound && projected > 0 {
		t.Errorf("the projection reports %d stored objects; they are supposed to live "+
			"only in the database", projected)
	}

	countedProjected := countObjects(ctx, t, ordersGVR)
	t.Logf("the same projection answers for %d objects through the API", countedProjected)
	if countedProjected < rowCount {
		t.Errorf("the projection served %d objects, want %d", countedProjected, rowCount)
	}
}

// storedObjects reads apiserver_storage_objects, which is per resource and only
// has a line for a resource the apiserver actually stores.
func storedObjects(ctx context.Context, t *testing.T) map[string]int {
	t.Helper()

	cluster, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("building a cluster client: %v", err)
	}
	raw, err := cluster.Discovery().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	if err != nil {
		t.Fatalf("reading the apiserver's metrics: %v", err)
	}

	out := map[string]int{}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "apiserver_storage_objects{") {
			continue
		}
		open, close := strings.Index(line, `resource="`), 0
		if open < 0 {
			continue
		}
		open += len(`resource="`)
		if close = strings.Index(line[open:], `"`); close < 0 {
			continue
		}
		resource := line[open : open+close]

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if value, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			out[resource] = int(value)
		}
	}
	return out
}
