//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"text/tabwriter"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// Benchmark sizing. The defaults are what hack/e2e-up.sh seeds into PostgreSQL.
var (
	rowCount       = envInt("ROWS", 10000)
	getIterations  = envInt("GET_ITERATIONS", 200)
	listIterations = envInt("LIST_ITERATIONS", 10)
	seedWorkers    = envInt("SEED_WORKERS", 32)
	writeOps       = envInt("WRITE_OPS", 300)

	// Throughput sizing. Sixteen clients is enough to exceed the default
	// connection pool, which is the point: the numbers are only interesting
	// once requests have to wait for each other.
	throughputWorkers  = envInt("THROUGHPUT_WORKERS", 16)
	throughputSample   = envInt("THROUGHPUT_SAMPLE", 500)
	throughputDuration = time.Duration(envInt("THROUGHPUT_SECONDS", 5)) * time.Second

	// Generous: a request this slow is a failure worth reporting, not a slow
	// backend worth timing.
	throughputRequestTimeout = 60 * time.Second

	// warmupIterations are run and discarded before every measurement. Written
	// as a count rather than a single call because what needs warming is a
	// prepared statement cache and a connection pool, not one socket.
	warmupIterations = envInt("BENCH_WARMUP", 20)

	// benchRuns is how many times a measurement is repeated.
	//
	// One run is worth very little on a single-node cluster where the database,
	// etcd and both API servers compete for the same cores: consecutive runs of
	// the same code path here have differed by more than half. What is reported
	// is the median across runs and the spread around it, so a reader can see
	// how much of a ratio is signal.
	benchRuns = envInt("BENCH_RUNS", 3)
)

// unthrottledClient is the shared client without its rate limiter, so a
// saturation run measures the servers rather than client-go.
func unthrottledClient(t *testing.T) dynamic.Interface {
	t.Helper()

	cfg := rest.CopyConfig(restConfig)
	cfg.QPS = -1
	cfg.Burst = 0

	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("building an unthrottled client: %v", err)
	}
	return client
}

func envInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}

// TestPerformanceComparison measures the same read patterns against a native
// CRD served from etcd and against a PostgreSQL-backed projection holding the
// same number of objects.
func TestPerformanceComparison(t *testing.T) {
	ctx := context.Background()

	seedBenchOrders(ctx, t)
	measured := benchmarkClient(ctx, t)

	// Confirm both sides really hold the same number of objects before
	// comparing anything.
	native := countObjects(ctx, t, benchGVR)
	projected := countObjects(ctx, t, ordersGVR)
	if native != rowCount {
		t.Fatalf("native CRD holds %d objects, want %d", native, rowCount)
	}
	if projected != rowCount {
		t.Fatalf("projection holds %d objects, want %d", projected, rowCount)
	}
	t.Logf("both backends hold %d objects in namespace %q", rowCount, benchNamespace)

	names := sampleNames(ctx, t, benchGVR, getIterations)

	results := []result{
		measureRepeatedly(t, "GET single object", "CRD (etcd)", getIterations, func(i int) error {
			_, err := measured.Resource(benchGVR).Namespace(benchNamespace).
				Get(ctx, names[i%len(names)], metav1.GetOptions{})
			return err
		}),
		measureRepeatedly(t, "GET single object", "projection (postgres)", getIterations, func(i int) error {
			_, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
				Get(ctx, names[i%len(names)], metav1.GetOptions{})
			return err
		}),
		measureRepeatedly(t, fmt.Sprintf("LIST all %d", rowCount), "CRD (etcd)", listIterations, func(int) error {
			list, err := measured.Resource(benchGVR).Namespace(benchNamespace).List(ctx, metav1.ListOptions{})
			if err == nil && len(list.Items) != rowCount {
				return fmt.Errorf("listed %d objects, want %d", len(list.Items), rowCount)
			}
			return err
		}),
		measureRepeatedly(t, fmt.Sprintf("LIST all %d", rowCount), "projection (postgres)", listIterations, func(int) error {
			list, err := measured.Resource(ordersGVR).Namespace(benchNamespace).List(ctx, metav1.ListOptions{})
			if err == nil && len(list.Items) != rowCount {
				return fmt.Errorf("listed %d objects, want %d", len(list.Items), rowCount)
			}
			return err
		}),
		measureRepeatedly(t, "LIST first 500", "CRD (etcd)", listIterations, func(int) error {
			_, err := measured.Resource(benchGVR).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{Limit: 500})
			return err
		}),
		measureRepeatedly(t, "LIST first 500", "projection (postgres)", listIterations, func(int) error {
			_, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{Limit: 500})
			return err
		}),
		measureRepeatedly(t, fmt.Sprintf("LIST all %d, row scan versus json_agg", rowCount), "projection rows", listIterations, func(int) error {
			list, err := measured.Resource(ordersGVR).Namespace(benchNamespace).List(ctx, metav1.ListOptions{})
			if err == nil && len(list.Items) != rowCount {
				return fmt.Errorf("listed %d objects, want %d", len(list.Items), rowCount)
			}
			return err
		}),
		measureRepeatedly(t, fmt.Sprintf("LIST all %d, row scan versus json_agg", rowCount), "projection json_agg", listIterations, func(int) error {
			list, err := measured.Resource(jsonOrdersGVR).Namespace(benchNamespace).List(ctx, metav1.ListOptions{})
			if err == nil && len(list.Items) != rowCount {
				return fmt.Errorf("listed %d objects, want %d", len(list.Items), rowCount)
			}
			return err
		}),
	}

	report(t, results)
}

// result holds the latency distribution of one measured scenario.
type result struct {
	scenario string
	backend  string
	samples  int
	mean     time.Duration
	p50      time.Duration
	p95      time.Duration
	p99      time.Duration

	// runs, p50Low and p50High describe the spread when a measurement was
	// repeated. runs is zero for a single measurement, and the report leaves
	// the column out entirely when nothing was repeated.
	runs    int
	p50Low  time.Duration
	p50High time.Duration

	min time.Duration
	max time.Duration
}

func measure(t *testing.T, scenario, backend string, iterations int, call func(i int) error) result {
	t.Helper()

	// A warm-up pass rather than a single call. One call warms the connection;
	// it does not warm a prepared statement cache, a connection pool that has
	// yet to open its second connection, or the kube-apiserver's own caches —
	// and the first of those is on by default, so measuring before it fills
	// measures a configuration nobody runs.
	//
	// Discarded, not averaged in: a warm-up that lands in the samples is just a
	// slow first iteration with extra steps.
	for i := 0; i < warmupIterations && i < iterations; i++ {
		if err := call(i); err != nil {
			t.Fatalf("%s / %s: warm-up failed: %v", scenario, backend, err)
		}
	}

	durations := make([]time.Duration, 0, iterations)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := call(i); err != nil {
			t.Fatalf("%s / %s: iteration %d failed: %v", scenario, backend, i, err)
		}
		durations = append(durations, time.Since(start))
	}

	return summarise(scenario, backend, durations)
}

// measureRepeatedly runs a measurement benchRuns times and reports the median
// run, with the spread across runs recorded on it.
//
// The median rather than the pooled samples: pooling hides a run that was
// entirely slower, which on a shared machine is the failure mode that matters.
func measureRepeatedly(t *testing.T, scenario, backend string, iterations int, call func(i int) error) result {
	t.Helper()

	runs := make([]result, 0, benchRuns)
	for i := 0; i < benchRuns; i++ {
		runs = append(runs, measure(t, scenario, backend, iterations, call))
	}

	sort.Slice(runs, func(i, j int) bool { return runs[i].p50 < runs[j].p50 })
	median := runs[len(runs)/2]
	median.runs = len(runs)
	median.p50Low, median.p50High = runs[0].p50, runs[len(runs)-1].p50
	return median
}

// summarise turns a set of timings into the row the report prints.
func summarise(scenario, backend string, durations []time.Duration) result {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	var total time.Duration
	for _, d := range durations {
		total += d
	}

	return result{
		scenario: scenario,
		backend:  backend,
		samples:  len(durations),
		mean:     total / time.Duration(len(durations)),
		p50:      percentile(durations, 0.50),
		p95:      percentile(durations, 0.95),
		p99:      percentile(durations, 0.99),
		min:      durations[0],
		max:      durations[len(durations)-1],
	}
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	return sorted[idx]
}

// report prints a comparison table, pairing each scenario's two backends and
// showing the ratio between them.
func report(t *testing.T, results []result) {
	t.Helper()

	var out bytes.Buffer
	buf := tabwriter.NewWriter(&out, 0, 8, 2, ' ', 0)

	fmt.Fprintf(buf, "\nRead latency: native CRD versus SQL projection (%d objects each)\n\n", rowCount)
	var repeated bool
	for _, r := range results {
		if r.runs > 1 {
			repeated = true
			break
		}
	}

	if repeated {
		fmt.Fprintln(buf, "SCENARIO\tBACKEND\tRUNS\tSAMPLES\tMEAN\tP50\tP50 RANGE\tP95\tP99")
		for _, r := range results {
			spread := "-"
			if r.runs > 1 {
				spread = fmt.Sprintf("%s – %s", round(r.p50Low), round(r.p50High))
			}
			fmt.Fprintf(buf, "%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
				r.scenario, r.backend, max(r.runs, 1), r.samples,
				round(r.mean), round(r.p50), spread, round(r.p95), round(r.p99))
		}
	} else {
		fmt.Fprintln(buf, "SCENARIO\tBACKEND\tSAMPLES\tMEAN\tP50\tP95\tP99\tMIN\tMAX")
		for _, r := range results {
			fmt.Fprintf(buf, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
				r.scenario, r.backend, r.samples,
				round(r.mean), round(r.p50), round(r.p95), round(r.p99), round(r.min), round(r.max))
		}
	}

	// Every scenario is compared against whichever backend was measured first,
	// which is the CRD wherever one is in the table.
	pairs := map[string][]result{}
	var order []string
	for _, r := range results {
		if _, seen := pairs[r.scenario]; !seen {
			order = append(order, r.scenario)
		}
		pairs[r.scenario] = append(pairs[r.scenario], r)
	}

	var compared bool
	for _, scenario := range order {
		measured := pairs[scenario]
		if len(measured) < 2 {
			continue
		}
		if !compared {
			fmt.Fprintf(buf, "\nSCENARIO\tBACKEND\tVS %s (p50)\tVS %s (mean)\n",
				measured[0].backend, measured[0].backend)
			compared = true
		}
		for _, r := range measured[1:] {
			fmt.Fprintf(buf, "%s\t%s\t%s\t%s\n", scenario, r.backend,
				ratio(r.p50, measured[0].p50),
				ratio(r.mean, measured[0].mean))
		}
	}

	fmt.Fprintln(buf, "\nThe projection answers every read from PostgreSQL with no watch cache and no")
	fmt.Fprintln(buf, "pagination, while the CRD is served from the kube-apiserver's cache over etcd.")

	if err := buf.Flush(); err != nil {
		t.Fatalf("rendering the report: %v", err)
	}
	// One log call keeps the table intact; logging per line interleaves it.
	t.Log("\n" + out.String())
}

func ratio(projection, crd time.Duration) string {
	if crd == 0 {
		return "n/a"
	}
	r := float64(projection) / float64(crd)
	switch {
	case r >= 1:
		return fmt.Sprintf("%.2fx slower", r)
	default:
		return fmt.Sprintf("%.2fx faster", 1/r)
	}
}

func round(d time.Duration) time.Duration {
	switch {
	case d > time.Second:
		return d.Round(10 * time.Millisecond)
	case d > time.Millisecond:
		return d.Round(100 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}

// seedBenchOrders creates the native CRs the comparison needs, if they are not
// already there. Seeding is the slow part of the run, so it is skipped when the
// cluster already holds the right number of objects.
func seedBenchOrders(ctx context.Context, t *testing.T) {
	t.Helper()

	existing := countObjects(ctx, t, benchGVR)
	if existing >= rowCount {
		t.Logf("native CRD already holds %d objects; skipping seed", existing)
		return
	}

	t.Logf("seeding %d BenchOrder objects (%d already present)", rowCount-existing, existing)
	start := time.Now()

	work := make(chan int, seedWorkers)
	errs := make(chan error, rowCount)

	var wg sync.WaitGroup
	for w := 0; w < seedWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				obj := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "bench.example.com/v1alpha1",
					"kind":       "BenchOrder",
					"metadata": map[string]any{
						"name":      benchName(i),
						"namespace": benchNamespace,
					},
					"spec": map[string]any{
						"customer":   fmt.Sprintf("customer-%d", i%997),
						"totalCents": int64((i * 37) % 100000),
					},
					"status": map[string]any{"phase": phaseFor(i)},
				}}

				_, err := dynamicClient.Resource(benchGVR).Namespace(benchNamespace).
					Create(ctx, obj, metav1.CreateOptions{})
				if err != nil && !apierrors.IsAlreadyExists(err) {
					errs <- err
				}
			}
		}()
	}

	for i := 1; i <= rowCount; i++ {
		work <- i
	}
	close(work)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("seeding BenchOrders: %v", err)
	}
	t.Logf("seeded in %s", time.Since(start).Round(time.Second))
}

// benchName matches the identifiers hack/e2e-up.sh writes into PostgreSQL, so
// that a GET by name hits an existing object on both backends.
func benchName(i int) string { return fmt.Sprintf("order-%06d", i) }

func phaseFor(i int) string {
	if i%3 == 0 {
		return "shipped"
	}
	return "pending"
}

func countObjects(ctx context.Context, t *testing.T, gvr schema.GroupVersionResource) int {
	t.Helper()

	list, err := dynamicClient.Resource(gvr).Namespace(benchNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("counting %s: %v", gvr.Resource, err)
	}
	return len(list.Items)
}

func sampleNames(ctx context.Context, t *testing.T, gvr schema.GroupVersionResource, n int) []string {
	t.Helper()

	list, err := dynamicClient.Resource(gvr).Namespace(benchNamespace).List(ctx, metav1.ListOptions{Limit: int64(n)})
	if err != nil {
		t.Fatalf("sampling names: %v", err)
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}
	if len(names) == 0 {
		t.Fatal("no objects to sample")
	}
	return names
}

// TestWritePerformanceComparison measures create, update, and delete against
// both backends. It runs after the read comparison and leaves both backends
// holding exactly the objects they started with.
func TestWritePerformanceComparison(t *testing.T) {
	ctx := context.Background()

	measured := benchmarkClient(ctx, t)

	// Seeded here rather than assumed. These tests read the native collection,
	// and it exists only because some other test created it — which holds when
	// the whole suite runs and not when a shard runs this one alone. Idempotent:
	// it skips when the cluster already holds the right number of objects.
	seedBenchOrders(ctx, t)

	native := measured.Resource(benchGVR).Namespace(benchNamespace)
	projected := measured.Resource(ordersGVR).Namespace(benchNamespace)

	// Whatever happens, do not leave benchmark objects behind: the read
	// comparison asserts an exact object count.
	t.Cleanup(func() {
		cleanup := context.Background()
		for i := 0; i < writeOps; i++ {
			_ = native.Delete(cleanup, writeName(i), metav1.DeleteOptions{})
			_ = projected.Delete(cleanup, writeName(i), metav1.DeleteOptions{})
		}
	})

	results := []result{
		measureWrites(t, "CREATE", "CRD (etcd)", func(i int) error {
			_, err := native.Create(ctx, writeObject("bench.example.com/v1alpha1", "BenchOrder", i), metav1.CreateOptions{})
			return err
		}),
		measureWrites(t, "CREATE", "projection (postgres)", func(i int) error {
			_, err := projected.Create(ctx, writeObject("store.example.com/v1alpha1", "Order", i), metav1.CreateOptions{})
			return err
		}),
		measureWrites(t, "UPDATE", "CRD (etcd)", func(i int) error {
			return updateOne(ctx, native, writeName(i))
		}),
		measureWrites(t, "UPDATE", "projection (postgres)", func(i int) error {
			return updateOne(ctx, projected, writeName(i))
		}),
		measureWrites(t, "DELETE", "CRD (etcd)", func(i int) error {
			return native.Delete(ctx, writeName(i), metav1.DeleteOptions{})
		}),
		measureWrites(t, "DELETE", "projection (postgres)", func(i int) error {
			return projected.Delete(ctx, writeName(i), metav1.DeleteOptions{})
		}),
	}

	report(t, results)
}

// measureWrites runs one write operation per object. Unlike the read
// measurements it cannot warm up on the same input, because each call mutates
// state, so every iteration works on its own object.
func measureWrites(t *testing.T, scenario, backend string, call func(i int) error) result {
	t.Helper()

	durations := make([]time.Duration, 0, writeOps)
	for i := 0; i < writeOps; i++ {
		start := time.Now()
		if err := call(i); err != nil {
			t.Fatalf("%s / %s: operation %d failed: %v", scenario, backend, i, err)
		}
		durations = append(durations, time.Since(start))
	}

	return summarise(scenario, backend, durations)
}

func updateOne(ctx context.Context, client dynamic.ResourceInterface, name string) error {
	existing, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedField(existing.Object, int64(4242), "spec", "totalCents"); err != nil {
		return err
	}
	_, err = client.Update(ctx, existing, metav1.UpdateOptions{})
	return err
}

func writeName(i int) string { return fmt.Sprintf("order-write-%06d", i) }

func writeObject(apiVersion, kind string, i int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      writeName(i),
			"namespace": benchNamespace,
		},
		"spec": map[string]any{
			"customer":   fmt.Sprintf("writer-%d", i),
			"totalCents": int64(1000 + i),
		},
		"status": map[string]any{"phase": "pending"},
	}}
}

// TestWatchLatencyComparison measures how long a change takes to reach a
// watcher. It is the number that most distinguishes the two designs: etcd
// pushes, while a projection polls, so the projection's latency is bounded by
// its poll interval rather than by the write.
func TestWatchLatencyComparison(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const writes = 8

	measured := benchmarkClient(ctx, t)

	// Seeded here rather than assumed. These tests read the native collection,
	// and it exists only because some other test created it — which holds when
	// the whole suite runs and not when a shard runs this one alone. Idempotent:
	// it skips when the cluster already holds the right number of objects.
	seedBenchOrders(ctx, t)

	results := []result{
		measureWatchLatency(t, ctx, measured, "CRD (etcd)", benchGVR, writes, func(i int) *unstructured.Unstructured {
			return writeObject("bench.example.com/v1alpha1", "BenchOrder", 900000+i)
		}),
		measureWatchLatency(t, ctx, measured, "projection (postgres)", ordersGVR, writes, func(i int) *unstructured.Unstructured {
			obj := writeObject("store.example.com/v1alpha1", "Order", 900000+i)
			// This projection owns status separately, and the column is not
			// nullable.
			_ = unstructured.SetNestedField(obj.Object, "pending", "status", "phase")
			return obj
		}),
	}

	report(t, results)
}

// measureWatchLatency writes objects one at a time and times how long each
// takes to arrive at an informer.
func measureWatchLatency(
	t *testing.T,
	ctx context.Context,
	measured dynamic.Interface,
	backend string,
	gvr schema.GroupVersionResource,
	writes int,
	build func(i int) *unstructured.Unstructured,
) result {
	t.Helper()

	client := measured.Resource(gvr).Namespace(benchNamespace)

	// Large enough to hold the initial sync of a 10k collection: a dropped
	// event here would look like latency that never arrives.
	arrived := make(chan string, 32768)

	watchCtx, stop := context.WithCancel(ctx)
	defer stop()

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(measured, 0, benchNamespace, nil)
	informer := factory.ForResource(gvr).Informer()
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			select {
			case arrived <- obj.(*unstructured.Unstructured).GetName():
			case <-watchCtx.Done():
			}
		},
	}); err != nil {
		t.Fatalf("registering the handler: %v", err)
	}

	factory.Start(watchCtx.Done())
	if !cache.WaitForCacheSync(watchCtx.Done(), informer.HasSynced) {
		t.Fatalf("%s: the informer never synced", backend)
	}

	// The initial sync delivers everything already there, and keeps delivering
	// for a while after the store reports synced. Wait for it to go quiet
	// before timing anything, or the first write is measured against a queue.
	for {
		select {
		case <-arrived:
		case <-time.After(2 * time.Second):
		}
		if len(arrived) == 0 {
			break
		}
	}

	t.Cleanup(func() {
		for i := range writes {
			_ = client.Delete(context.Background(), writeName(900000+i), metav1.DeleteOptions{})
		}
	})

	durations := make([]time.Duration, 0, writes)
	for i := range writes {
		object := build(i)
		name := object.GetName()

		start := time.Now()
		if _, err := client.Create(ctx, object, metav1.CreateOptions{}); err != nil {
			t.Fatalf("%s: creating %s: %v", backend, name, err)
		}

		deadline := time.After(30 * time.Second)
		for {
			select {
			case seen := <-arrived:
				if seen != name {
					continue
				}
			case <-deadline:
				t.Fatalf("%s: %s never reached the informer", backend, name)
			}
			break
		}
		durations = append(durations, time.Since(start))
	}

	return summarise("WATCH propagation", backend, durations)
}

// TestPagedWalkComparison measures walking the whole collection in pages,
// which is how a client with a memory budget reads a large one.
func TestPagedWalkComparison(t *testing.T) {
	ctx := context.Background()

	measured := benchmarkClient(ctx, t)

	// Seeded here rather than assumed. These tests read the native collection,
	// and it exists only because some other test created it — which holds when
	// the whole suite runs and not when a shard runs this one alone. Idempotent:
	// it skips when the cluster already holds the right number of objects.
	seedBenchOrders(ctx, t)

	const pageSize = 500
	walk := func(gvr schema.GroupVersionResource) error {
		client := measured.Resource(gvr).Namespace(benchNamespace)

		var seen int
		var token string
		for {
			page, err := client.List(ctx, metav1.ListOptions{Limit: pageSize, Continue: token})
			if err != nil {
				return err
			}
			seen += len(page.Items)

			token = page.GetContinue()
			if token == "" {
				break
			}
		}
		if seen != rowCount {
			return fmt.Errorf("walked %d objects, want %d", seen, rowCount)
		}
		return nil
	}

	results := []result{
		measure(t, fmt.Sprintf("WALK %d in pages of %d", rowCount, pageSize), "CRD (etcd)", 3, func(int) error {
			return walk(benchGVR)
		}),
		measure(t, fmt.Sprintf("WALK %d in pages of %d", rowCount, pageSize), "projection (postgres)", 3, func(int) error {
			return walk(ordersGVR)
		}),
	}

	report(t, results)
}

// TestDriverComparison puts the three supported drivers side by side. The
// collections differ in size, which is the point: the number to compare is the
// single-object read, and the list is reported with its own size for context.
func TestDriverComparison(t *testing.T) {
	ctx := context.Background()

	measured := benchmarkClient(ctx, t)

	seedBenchOrders(ctx, t)

	// All four hold the same collection, so the comparison is between backends
	// rather than between datasets.
	backends := []struct {
		name string
		gvr  schema.GroupVersionResource
	}{
		{"CRD (etcd)", benchGVR},
		{"postgres", ordersGVR},
		{"mysql", mysqlOrdersGVR},
		{"sqlite", sqliteOrdersGVR},
	}

	names := sampleNames(ctx, t, benchGVR, getIterations)

	var results []result
	for _, backend := range backends {
		client := measured.Resource(backend.gvr).Namespace(benchNamespace)

		held := countObjects(ctx, t, backend.gvr)
		if held != rowCount {
			t.Fatalf("%s holds %d objects, want the %d the others hold; the comparison would be meaningless",
				backend.name, held, rowCount)
		}

		results = append(results,
			measure(t, "GET single object", backend.name, getIterations, func(i int) error {
				_, err := client.Get(ctx, names[i%len(names)], metav1.GetOptions{})
				return err
			}),
			measure(t, "LIST first 500", backend.name, listIterations, func(int) error {
				_, err := client.List(ctx, metav1.ListOptions{Limit: 500})
				return err
			}),
			measure(t, fmt.Sprintf("LIST all %d", rowCount), backend.name, listIterations, func(int) error {
				list, err := client.List(ctx, metav1.ListOptions{})
				if err == nil && len(list.Items) != rowCount {
					return fmt.Errorf("listed %d objects, want %d", len(list.Items), rowCount)
				}
				return err
			}),
		)
	}

	report(t, results)
}

// TestThroughputComparison measures what each backend sustains under
// concurrent load rather than one request at a time.
//
// Latency measured serially says how fast a single client is answered.
// Throughput says something else: how many clients the server can answer at
// once before queuing inside it starts to dominate. For a projection that is
// the more interesting number, because every read costs a database connection,
// and connections are the resource that runs out first.
func TestThroughputComparison(t *testing.T) {
	ctx := context.Background()

	seedBenchOrders(ctx, t)
	names := sampleNames(ctx, t, benchGVR, throughputSample)

	// The shared client is capped at 500 QPS, which is inside the range this
	// test is trying to measure: with it, the numbers describe client-go's
	// rate limiter rather than either server.
	client := unthrottledClient(t)

	get := func(gvr schema.GroupVersionResource) func(ctx context.Context, i int) error {
		return func(ctx context.Context, i int) error {
			_, err := client.Resource(gvr).Namespace(benchNamespace).
				Get(ctx, names[i%len(names)], metav1.GetOptions{})
			return err
		}
	}
	listPage := func(gvr schema.GroupVersionResource) func(ctx context.Context, i int) error {
		return func(ctx context.Context, _ int) error {
			_, err := client.Resource(gvr).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{Limit: 100})
			return err
		}
	}

	results := []throughput{
		saturate(t, "GET single object", "CRD (etcd)", get(benchGVR)),
		saturate(t, "GET single object", "projection (postgres)", get(ordersGVR)),
		saturate(t, "GET single object", "projection (cached)", get(cachedOrdersGVR)),
		saturate(t, "LIST first 100", "CRD (etcd)", listPage(benchGVR)),
		saturate(t, "LIST first 100", "projection (postgres)", listPage(ordersGVR)),
	}

	reportThroughput(t, results)
}

// throughput is what one saturation run produced.
type throughput struct {
	scenario string
	backend  string
	workers  int
	requests int
	shed     int
	elapsed  time.Duration
	perSec   float64
	mean     time.Duration
	p50      time.Duration
	p95      time.Duration
	p99      time.Duration
}

// saturate runs call from throughputWorkers goroutines for throughputDuration
// and reports the rate and the latency spread that rate was achieved at.
//
// A 429 is counted, not failed on: shedding load is the server working as
// designed, and a run that sheds is still a run worth reporting. Anything else
// fails the test.
func saturate(t *testing.T, scenario, backend string, call func(ctx context.Context, i int) error) throughput {
	t.Helper()

	// Warm up outside the measured window: the first request through a pool
	// pays for the connection everyone after it reuses.
	if err := call(testContext(t), 0); err != nil {
		t.Fatalf("%s / %s: warm-up failed: %v", scenario, backend, err)
	}

	var (
		mu        sync.Mutex
		durations []time.Duration
		shed      int
	)

	start := time.Now()
	window := start.Add(throughputDuration)

	var wg sync.WaitGroup
	for worker := 0; worker < throughputWorkers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			// Each worker walks its own slice of the name space, so the run
			// does not collapse onto one row that a cache would answer for.
			local := make([]time.Duration, 0, 256)
			localShed := 0

			// Merged on every exit path, not just the loop's: a worker that
			// gives up early still measured everything before that.
			defer func() {
				mu.Lock()
				durations = append(durations, local...)
				shed += localShed
				mu.Unlock()
			}()

			for i := worker; time.Now().Before(window); i += throughputWorkers {
				// Each request gets its own generous timeout rather than the
				// window's. Cancelling in-flight work at the bell would time
				// the harness shutting down instead of the server answering.
				reqCtx, cancel := context.WithTimeout(context.Background(), throughputRequestTimeout)
				began := time.Now()
				err := call(reqCtx, i)
				elapsed := time.Since(began)
				cancel()

				switch {
				case err == nil:
					local = append(local, elapsed)
				case apierrors.IsTooManyRequests(err):
					localShed++
				default:
					t.Errorf("%s / %s: request failed: %v", scenario, backend, err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if len(durations) == 0 {
		t.Fatalf("%s / %s: no request completed in %s", scenario, backend, throughputDuration)
	}

	summary := summarise(scenario, backend, durations)
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	return throughput{
		scenario: scenario,
		backend:  backend,
		workers:  throughputWorkers,
		requests: len(durations),
		shed:     shed,
		elapsed:  elapsed,
		perSec:   float64(len(durations)) / elapsed.Seconds(),
		mean:     summary.mean,
		p50:      summary.p50,
		p95:      summary.p95,
		p99:      percentile(durations, 0.99),
	}
}

// reportThroughput prints the rate table and the rate ratio per scenario.
func reportThroughput(t *testing.T, results []throughput) {
	t.Helper()

	var out bytes.Buffer
	buf := tabwriter.NewWriter(&out, 0, 8, 2, ' ', 0)

	fmt.Fprintf(buf, "\nThroughput under %d concurrent clients for %s (%d objects each)\n\n",
		throughputWorkers, throughputDuration, rowCount)
	fmt.Fprintln(buf, "SCENARIO\tBACKEND\tREQ/S\tREQUESTS\t429s\tMEAN\tP50\tP95\tP99")
	for _, r := range results {
		fmt.Fprintf(buf, "%s\t%s\t%.0f\t%d\t%d\t%s\t%s\t%s\t%s\n",
			r.scenario, r.backend, r.perSec, r.requests, r.shed,
			round(r.mean), round(r.p50), round(r.p95), round(r.p99))
	}

	byScenario := map[string][]throughput{}
	var order []string
	for _, r := range results {
		if _, seen := byScenario[r.scenario]; !seen {
			order = append(order, r.scenario)
		}
		byScenario[r.scenario] = append(byScenario[r.scenario], r)
	}

	fmt.Fprintln(buf, "\nSCENARIO\tBACKEND\tREQ/S VS FIRST")
	for _, scenario := range order {
		measured := byScenario[scenario]
		for _, r := range measured[1:] {
			fmt.Fprintf(buf, "%s\t%s\t%s\n", scenario, r.backend, rate(r.perSec, measured[0].perSec))
		}
	}

	fmt.Fprintln(buf, "\nThe projection's ceiling is its connection pool and any per-projection query")
	fmt.Fprintln(buf, "limit; requests beyond it queue, and a projection with maxConcurrentQueries set")
	fmt.Fprintln(buf, "sheds them as 429 instead.")

	if err := buf.Flush(); err != nil {
		t.Fatalf("rendering the report: %v", err)
	}
	t.Log("\n" + out.String())
}

// rate compares two request rates the way ratio compares two latencies.
func rate(measured, baseline float64) string {
	if baseline == 0 {
		return "n/a"
	}
	r := measured / baseline
	if r >= 1 {
		return fmt.Sprintf("%.2fx more", r)
	}
	return fmt.Sprintf("%.2fx fewer", 1/r)
}

// testContext returns a context bounded by the test, for calls made outside a
// measured window.
func testContext(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return c
}

// scalingSizes are the collection sizes the scaling comparison reads, capped at
// what the fixture actually holds.
func scalingSizes() []int {
	var sizes []int
	for _, n := range []int{100, 1000, 10000} {
		if n <= rowCount {
			sizes = append(sizes, n)
		}
	}
	if len(sizes) == 0 || sizes[len(sizes)-1] != rowCount {
		sizes = append(sizes, rowCount)
	}
	return sizes
}

// TestScalingComparison answers the question the single-size table cannot: how
// the two backends behave as a collection grows.
//
// A ratio measured at one size says which is faster there and nothing about
// where the lines cross. Reading the same collection at several sizes shows the
// shape — and the shape is the part that travels to a cluster with different
// data, where the absolute numbers do not.
func TestScalingComparison(t *testing.T) {
	ctx := context.Background()

	seedBenchOrders(ctx, t)
	measured := benchmarkClient(ctx, t)

	var results []result
	for _, size := range scalingSizes() {
		scenario := fmt.Sprintf("LIST %d objects", size)

		results = append(results,
			measure(t, scenario, "CRD (etcd)", listIterations, func(int) error {
				list, err := measured.Resource(benchGVR).Namespace(benchNamespace).
					List(ctx, metav1.ListOptions{Limit: int64(size)})
				if err == nil && len(list.Items) != size {
					return fmt.Errorf("listed %d objects, want %d", len(list.Items), size)
				}
				return err
			}),
			measure(t, scenario, "projection (postgres)", listIterations, func(int) error {
				list, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
					List(ctx, metav1.ListOptions{Limit: int64(size)})
				if err == nil && len(list.Items) != size {
					return fmt.Errorf("listed %d objects, want %d", len(list.Items), size)
				}
				return err
			}),
		)
	}

	report(t, results)

	// The per-object cost is what says whether a backend scales linearly or
	// worse, and it is not visible in the totals.
	var out bytes.Buffer
	buf := tabwriter.NewWriter(&out, 0, 8, 2, ' ', 0)
	fmt.Fprintf(buf, "\nPer-object cost as the collection grows\n\n")
	fmt.Fprintln(buf, "OBJECTS\tCRD (etcd)\tPROJECTION\tRATIO")

	perObject := map[string]map[int]time.Duration{}
	for _, r := range results {
		var size int
		if _, err := fmt.Sscanf(r.scenario, "LIST %d objects", &size); err != nil || size == 0 {
			continue
		}
		if perObject[r.backend] == nil {
			perObject[r.backend] = map[int]time.Duration{}
		}
		perObject[r.backend][size] = r.p50 / time.Duration(size)
	}
	for _, size := range scalingSizes() {
		crd := perObject["CRD (etcd)"][size]
		projection := perObject["projection (postgres)"][size]
		fmt.Fprintf(buf, "%d\t%s\t%s\t%s\n", size, round(crd), round(projection), ratio(projection, crd))
	}
	fmt.Fprintln(buf, "\nA per-object cost that stays flat as the collection grows is linear scaling.")
	fmt.Fprintln(buf, "One that rises is paying something per collection rather than per object.")

	if err := buf.Flush(); err != nil {
		t.Fatalf("rendering the report: %v", err)
	}
	t.Log("\n" + out.String())
}

// TestSelectorPushdownComparison measures what a selector costs when the
// database answers it against what it costs when kube-crisp does.
//
// A field selector is honoured either way — the collection is filtered again
// after mapping — so the difference is invisible from the outside and shows up
// only as latency. Which makes it exactly the kind of thing that quietly stops
// working: a projection whose list statement drops the parameter still returns
// the right objects, just by reading all of them first.
func TestSelectorPushdownComparison(t *testing.T) {
	ctx := context.Background()

	seedBenchOrders(ctx, t)
	measured := benchmarkClient(ctx, t)
	names := sampleNames(ctx, t, benchGVR, getIterations)

	results := []result{
		// The baseline: what reading the whole collection costs, which is what
		// a selector the database cannot see falls back to.
		measure(t, "one object by name", "LIST everything", listIterations, func(int) error {
			list, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{})
			if err == nil && len(list.Items) != rowCount {
				return fmt.Errorf("listed %d objects, want %d", len(list.Items), rowCount)
			}
			return err
		}),
		// The same answer, with the name bound into the statement.
		measure(t, "one object by name", "field selector, pushed down", getIterations, func(i int) error {
			list, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + names[i%len(names)]})
			if err == nil && len(list.Items) != 1 {
				return fmt.Errorf("selector matched %d objects, want 1", len(list.Items))
			}
			return err
		}),
		// And a get, which is the same round trip through a statement written
		// for one row. A pushed-down selector should land near it.
		measure(t, "one object by name", "GET", getIterations, func(i int) error {
			_, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
				Get(ctx, names[i%len(names)], metav1.GetOptions{})
			return err
		}),
	}

	// A label selector on a mapped column. The fixture makes one row in three
	// "shipped", so a selector the database answers reads a third of the table
	// while one it cannot see reads all of it and discards two thirds.
	results = append(results,
		measure(t, "a third of the collection by label", "LIST everything", listIterations, func(int) error {
			list, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{})
			if err == nil && len(list.Items) != rowCount {
				return fmt.Errorf("listed %d objects, want %d", len(list.Items), rowCount)
			}
			return err
		}),
		measure(t, "a third of the collection by label", "label selector, pushed down", listIterations, func(int) error {
			list, err := measured.Resource(ordersGVR).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{LabelSelector: "store.example.com/status=shipped"})
			if err == nil && len(list.Items) == 0 {
				return fmt.Errorf("the selector matched nothing")
			}
			return err
		}),
	)

	report(t, results)

	t.Log("A pushed-down selector lands near the GET: both are one statement written for the " +
		"rows asked for. One that is not pushed down costs a full read whatever it selects.")
}

// TestPagingDepthComparison measures whether a page costs the same wherever it
// falls in the collection.
//
// This is the property keyset paging has and offset paging does not: resuming
// after a value is an index seek, while skipping N rows is work proportional to
// N, so a client walking a large collection pays more for every page than for
// the one before. The projection pages by key, so its last page should cost
// what its first did — and a change that quietly turned that into an offset
// would show up here and nowhere else.
func TestPagingDepthComparison(t *testing.T) {
	ctx := context.Background()

	seedBenchOrders(ctx, t)
	measured := benchmarkClient(ctx, t)

	const pageSize = 500
	depth := rowCount / pageSize
	if depth < 2 {
		t.Skipf("%d objects at %d per page is not deep enough to measure", rowCount, pageSize)
	}

	// The continue token for the page `pages` deep, walked once up front so the
	// measurement itself is one request.
	tokenAt := func(gvr schema.GroupVersionResource, pages int) string {
		var token string
		for i := 0; i < pages; i++ {
			list, err := measured.Resource(gvr).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{Limit: pageSize, Continue: token})
			if err != nil {
				t.Fatalf("walking to page %d of %s: %v", i, gvr.Resource, err)
			}
			if token = list.GetContinue(); token == "" {
				t.Fatalf("%s ran out of pages at %d, want at least %d", gvr.Resource, i, pages)
			}
		}
		return token
	}

	deepProjection := tokenAt(ordersGVR, depth-1)
	deepCRD := tokenAt(benchGVR, depth-1)

	page := func(gvr schema.GroupVersionResource, token string) func(int) error {
		return func(int) error {
			list, err := measured.Resource(gvr).Namespace(benchNamespace).
				List(ctx, metav1.ListOptions{Limit: pageSize, Continue: token})
			if err == nil && len(list.Items) == 0 {
				return fmt.Errorf("page is empty")
			}
			return err
		}
	}

	first := fmt.Sprintf("page 1 of %d, %d per page", depth, pageSize)
	last := fmt.Sprintf("page %d of %d, %d per page", depth, depth, pageSize)

	report(t, []result{
		measure(t, first, "CRD (etcd)", listIterations, page(benchGVR, "")),
		measure(t, first, "projection (postgres)", listIterations, page(ordersGVR, "")),
		measure(t, last, "CRD (etcd)", listIterations, page(benchGVR, deepCRD)),
		measure(t, last, "projection (postgres)", listIterations, page(ordersGVR, deepProjection)),
	})

	t.Log("A last page that costs what the first did is keyset paging working: the query " +
		"resumes after a value rather than skipping rows to reach it.")
}
