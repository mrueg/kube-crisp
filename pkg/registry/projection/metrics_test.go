package projection

import (
	goerrors "errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/component-base/metrics/testutil"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// TestQueryMetricsRecordOutcomes checks that reads are measured and that an
// ordinary 404 is reported as not_found rather than as an error, since the
// difference is what separates a healthy projection from a broken one.
func TestQueryMetricsRecordOutcomes(t *testing.T) {
	crispmetrics.QueryDuration.Reset()
	crispmetrics.QueryRows.Reset()

	store := newTestREST(t)
	ctx := namespacedContext("acme")

	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if _, err := store.Get(ctx, "order-9999", &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Fatalf("Get() error = %v, want NotFound", err)
	}
	if _, err := store.List(ctx, &metainternalversion.ListOptions{}); err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	const metric = "kube_crisp_query_duration_seconds"

	// Gathered by name from the registry the apiserver serves, which is the
	// same path a Prometheus scrape takes.
	testutil.AssertHistogramTotalCount(t, metric,
		map[string]string{"verb": "get", "result": crispmetrics.ResultSuccess}, 1)
	testutil.AssertHistogramTotalCount(t, metric,
		map[string]string{"verb": "get", "result": crispmetrics.ResultNotFound}, 1)
	testutil.AssertHistogramTotalCount(t, metric,
		map[string]string{"verb": "list", "result": crispmetrics.ResultSuccess}, 1)

	for _, tc := range []struct{ verb, result string }{
		{"get", crispmetrics.ResultSuccess},
		{"get", crispmetrics.ResultNotFound},
		{"list", crispmetrics.ResultSuccess},
	} {
		count, err := testutil.GetHistogramMetricValue(
			crispmetrics.QueryDuration.WithLabelValues("orders", "orders.store.example.com", tc.verb, tc.result))
		if err != nil {
			t.Fatalf("reading %s{verb=%s,result=%s}: %v", metric, tc.verb, tc.result, err)
		}
		if count == 0 {
			t.Errorf("%s{verb=%s,result=%s} was never observed", metric, tc.verb, tc.result)
		}
	}

	// A failed read must not be counted as rows returned: only the two
	// successful reads land in the rows histogram.
	testutil.AssertHistogramTotalCount(t, "kube_crisp_query_rows",
		map[string]string{"verb": "get"}, 1)
	testutil.AssertHistogramTotalCount(t, "kube_crisp_query_rows",
		map[string]string{"verb": "list"}, 1)
}

// TestWatchMetricsTrackWatchers checks that the watcher gauge rises and falls,
// which is what explains database load with no API request behind it.
func TestWatchMetricsTrackWatchers(t *testing.T) {
	crispmetrics.Watchers.Reset()

	store := newStorage(t, watchableSpec()).(*WritableREST)
	const resource = "orders.store.example.com"

	w, err := store.Watch(namespacedContext("acme"), &metainternalversion.ListOptions{})
	if err != nil {
		t.Fatalf("Watch() returned error: %v", err)
	}

	value, err := testutil.GetGaugeMetricValue(crispmetrics.Watchers.WithLabelValues(resource))
	if err != nil {
		t.Fatalf("reading the watcher gauge: %v", err)
	}
	if value != 1 {
		t.Errorf("watcher gauge = %v while one watcher is connected, want 1", value)
	}

	w.Stop()

	value, err = testutil.GetGaugeMetricValue(crispmetrics.Watchers.WithLabelValues(resource))
	if err != nil {
		t.Fatalf("reading the watcher gauge: %v", err)
	}
	if value != 0 {
		t.Errorf("watcher gauge = %v after the watcher stopped, want 0", value)
	}
}

// TestGetWithoutAGetQueryReportsRowsRead is a regression test. Every write and
// single read used to record exactly one row, so kube_crisp_query_rows said
// nothing about any verb but list — least of all about the case that matters,
// where a projection with no get query filters the whole collection and the
// metric is the only thing that would show it.
func TestGetWithoutAGetQueryReportsRowsRead(t *testing.T) {
	spec := testSpec()
	spec.Queries.Get = nil // forces the list-and-filter fallback

	store := newStorage(t, spec).(*REST)
	ctx := namespacedContext("acme")

	if _, _, err := store.getObject(ctx, "order-1001", shared); err != nil {
		t.Fatalf("getObject() returned error: %v", err)
	}

	_, rows, err := store.getObject(ctx, "order-1001", fresh)
	if err != nil {
		t.Fatalf("getObject() returned error: %v", err)
	}
	if rows < 2 {
		t.Errorf("a get served by filtering the collection reported %d rows; "+
			"the fixture holds more than one, so the cost is not being recorded", rows)
	}
}

// TestGetWithAGetQueryReportsOneRow: the ordinary path really does read one.
func TestGetWithAGetQueryReportsOneRow(t *testing.T) {
	store := newTestREST(t)

	_, rows, err := store.getObject(namespacedContext("acme"), "order-1001", fresh)
	if err != nil {
		t.Fatalf("getObject() returned error: %v", err)
	}
	if rows != 1 {
		t.Errorf("getObject() read %d rows, want 1", rows)
	}
}

// TestCachedGetReportsNoRows: a cache hit is not a database round trip, and
// this metric counts round trips.
func TestCachedGetReportsNoRows(t *testing.T) {
	spec := testSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Hour}

	store := newStorage(t, spec).(*REST)
	ctx := namespacedContext("acme")

	if _, _, err := store.getObject(ctx, "order-1001", shared); err != nil {
		t.Fatalf("priming the cache: %v", err)
	}
	_, rows, err := store.getObject(ctx, "order-1001", shared)
	if err != nil {
		t.Fatalf("getObject() returned error: %v", err)
	}
	if rows != 0 {
		t.Errorf("a cache hit reported %d rows read, want 0", rows)
	}
}

// TestReadsAreCountedByTheDatabaseThatAnsweredThem: a projection with a replica
// is trading freshness for load, and this is what says whether the trade is
// actually happening.
func TestReadsAreCountedByTheDatabaseThatAnsweredThem(t *testing.T) {
	crispmetrics.QueriesRouted.Reset()

	store, _, _ := twoDatabases(t, writableSpec())
	ctx := namespacedContext("acme")

	if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	if got := routed(t, crispmetrics.RoleReplica); got < 1 {
		t.Errorf("reads routed to the replica = %v, want at least 1", got)
	}

	// A write's precondition cannot go to a replica, so it counts as primary.
	before := routed(t, crispmetrics.RolePrimary)
	if _, err := store.read(ctx, "order-1001", fresh); err != nil {
		t.Fatalf("reading the write base: %v", err)
	}
	if got := routed(t, crispmetrics.RolePrimary); got <= before {
		t.Errorf("reads routed to the primary = %v, want more than %v", got, before)
	}
}

// TestReadsAreCountedAsPrimaryWithoutAReplica keeps the ordinary case legible:
// a projection with one data source reports all of its reads against it.
func TestReadsAreCountedAsPrimaryWithoutAReplica(t *testing.T) {
	crispmetrics.QueriesRouted.Reset()

	store := newWritableREST(t)
	if _, err := store.Get(namespacedContext("acme"), "order-1001", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	if got := routed(t, crispmetrics.RolePrimary); got < 1 {
		t.Errorf("reads routed to the primary = %v, want at least 1", got)
	}
	if got := routed(t, crispmetrics.RoleReplica); got != 0 {
		t.Errorf("reads routed to a replica = %v with none configured, want 0", got)
	}
}

// routed reads the counter for one role.
func routed(t *testing.T, role string) float64 {
	t.Helper()

	value, err := testutil.GetCounterMetricValue(
		crispmetrics.QueriesRouted.WithLabelValues("orders", "orders.store.example.com", role))
	if err != nil {
		t.Fatalf("reading the routing counter: %v", err)
	}
	return value
}

// TestCoalescedReadsAreLabelledLikeEverythingElse guards the label rather than
// the behaviour.
//
// Every series this package publishes is keyed by plural.group, and this one
// was keyed by the projection's name instead — so a dashboard joining coalesced
// reads against query duration or rows silently matched nothing.
func TestCoalescedReadsAreLabelledLikeEverythingElse(t *testing.T) {
	crispmetrics.QueriesCoalesced.Reset()

	store := newTestREST(t)
	ctx := namespacedContext("acme")

	// Two identical reads racing each other, so at least one joins the other.
	// Whether it does is timing; that it is labelled correctly if it does is not.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = store.List(ctx, &metainternalversion.ListOptions{})
		}()
	}
	wg.Wait()

	const resource = "orders.store.example.com"
	if _, err := testutil.GetCounterMetricValue(
		crispmetrics.QueriesCoalesced.WithLabelValues("orders", resource)); err != nil {
		t.Fatalf("reading the coalesced counter at resource=%q: %v", resource, err)
	}

	// The label that used to be published: the projection's name where every
	// other metric carries plural.group.
	stale, err := testutil.GetCounterMetricValue(
		crispmetrics.QueriesCoalesced.WithLabelValues("orders", "orders"))
	if err != nil {
		t.Fatalf("reading the coalesced counter at resource=\"orders\": %v", err)
	}
	if stale != 0 {
		t.Errorf("coalesced reads were counted at resource=\"orders\", which no other metric uses")
	}
}

// TestResultLabelSeparatesTheKindsOfFailure covers the distinction the label
// exists to make.
//
// Collapsed into one "error" value, a rate that had gone up said only that
// something was wrong — not whether to look at the database, at the
// projection's SQL, or at the load. These are the four answers, and they point
// at different people.
func TestResultLabelSeparatesTheKindsOfFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a healthy read", nil, crispmetrics.ResultSuccess},
		{"an ordinary 404", errors.NewNotFound(schema.GroupResource{Resource: "orders"}, "order-1"), crispmetrics.ResultNotFound},
		{"a query that outran its timeout", errors.NewTimeoutError("no answer", 0), crispmetrics.ResultTimeout},
		{"a database that is down", errors.NewServiceUnavailable("unreachable"), crispmetrics.ResultUnavailable},
		{"a request shed at the limit", errors.NewTooManyRequests("at the limit", 1), crispmetrics.ResultShed},
		{"a write the schema rejected", errors.NewBadRequest("no such field"), crispmetrics.ResultInvalid},
		{"a write that lost a race", errors.NewConflict(schema.GroupResource{Resource: "orders"}, "order-1", nil), crispmetrics.ResultConflict},
		{"SQL the database refused", errors.NewInternalError(goerrors.New(`syntax error at or near "SELCT"`)), crispmetrics.ResultError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resultFor(tc.err); got != tc.want {
				t.Errorf("resultFor(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}

	// The four failure values have to be distinct, or the split does not
	// actually separate anything.
	seen := map[string]bool{}
	for _, result := range []string{
		crispmetrics.ResultTimeout, crispmetrics.ResultUnavailable,
		crispmetrics.ResultShed, crispmetrics.ResultInvalid,
		crispmetrics.ResultConflict, crispmetrics.ResultError,
	} {
		if seen[result] {
			t.Errorf("%q is used for more than one kind of failure", result)
		}
		seen[result] = true
	}
}
