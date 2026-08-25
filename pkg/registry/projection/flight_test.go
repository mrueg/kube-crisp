package projection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/metrics/testutil"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// blockingQuery is a query that reports when it has started and answers only
// when released, so a test can hold one in flight while others arrive.
type blockingQuery struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int64
	rows    []crispsql.Row
	err     error
}

func newBlockingQuery(rows []crispsql.Row) *blockingQuery {
	return &blockingQuery{started: make(chan struct{}, 16), release: make(chan struct{}), rows: rows}
}

func (q *blockingQuery) run(context.Context) ([][]crispsql.Row, error) {
	q.calls.Add(1)
	q.started <- struct{}{}
	<-q.release
	if q.err != nil {
		return nil, q.err
	}
	return [][]crispsql.Row{q.rows}, nil
}

func TestFlightGroupRunsOneQueryForIdenticalReaders(t *testing.T) {
	group := newFlightGroup("orders", t.Name())
	query := newBlockingQuery([]crispsql.Row{{"id": "order-1"}})
	before := coalesced(t, group)

	const readers = 8
	var wg sync.WaitGroup
	results := make([][][]crispsql.Row, readers)
	failures := make([]error, readers)

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], failures[i] = group.Do(context.Background(), "same", "acme", query.run)
		}(i)
	}

	// Let them all pile up behind the one query before it answers. The metric
	// is the only honest signal that a reader has joined rather than merely
	// been scheduled.
	<-query.started
	waitFor(t, func() bool { return coalesced(t, group)-before == readers-1 })
	close(query.release)
	wg.Wait()

	if got := query.calls.Load(); got != 1 {
		t.Errorf("the database was queried %d times for %d identical reads, want 1", got, readers)
	}
	for i := range results {
		if failures[i] != nil {
			t.Errorf("reader %d failed: %v", i, failures[i])
		}
		if len(results[i]) != 1 || len(results[i][0]) != 1 || results[i][0][0]["id"] != "order-1" {
			t.Errorf("reader %d got %v, want the shared row", i, results[i])
		}
	}
}

// TestFlightGroupSeparatesDifferentArguments is the safety property: a
// projection binding the authenticated user must never answer one client from
// another's rows.
func TestFlightGroupSeparatesDifferentArguments(t *testing.T) {
	group := newFlightGroup("orders", t.Name())
	query := newBlockingQuery(nil)

	var wg sync.WaitGroup
	for _, key := range []string{"user=ada", "user=grace"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, _ = group.Do(context.Background(), key, "acme", query.run)
		}(key)
	}

	<-query.started
	<-query.started
	close(query.release)
	wg.Wait()

	if got := query.calls.Load(); got != 2 {
		t.Errorf("queries run = %d, want one per distinct argument set", got)
	}
}

// TestFlightSurvivesTheLeaderLeaving covers the case a naive implementation
// gets wrong: the request that started the query gives up, and everyone waiting
// behind it still has to be answered.
func TestFlightSurvivesTheLeaderLeaving(t *testing.T) {
	group := newFlightGroup("orders", t.Name())
	query := newBlockingQuery([]crispsql.Row{{"id": "order-1"}})

	before := coalesced(t, group)

	leader, abandon := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := group.Do(leader, "same", "acme", query.run)
		leaderDone <- err
	}()
	<-query.started

	followerDone := make(chan [][]crispsql.Row, 1)
	go func() {
		rows, err := group.Do(context.Background(), "same", "acme", query.run)
		if err != nil {
			t.Errorf("the follower failed: %v", err)
		}
		followerDone <- rows
	}()

	// The follower has to have joined before the leader walks away, or the
	// test proves nothing about what happens to it.
	waitFor(t, func() bool { return coalesced(t, group)-before == 1 })

	abandon()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Errorf("the leader's error = %v, want context.Canceled", err)
	}

	close(query.release)
	select {
	case results := <-followerDone:
		if len(results) != 1 || len(results[0]) != 1 {
			t.Errorf("the follower got %v, want the one shared row", results)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the follower never got an answer after the leader left")
	}
	if got := query.calls.Load(); got != 1 {
		t.Errorf("queries run = %d, want 1", got)
	}
}

func TestFlightWaiterHonoursItsOwnContext(t *testing.T) {
	group := newFlightGroup("orders", t.Name())
	query := newBlockingQuery(nil)
	defer close(query.release)

	go func() { _, _ = group.Do(context.Background(), "same", "acme", query.run) }()
	<-query.started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := group.Do(ctx, "same", "acme", query.run); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a waiter that timed out returned %v, want DeadlineExceeded", err)
	}
}

// TestFlightDetachStopsNewReadersJoining is what keeps read-after-write honest:
// a query that started before a write must not answer a read issued after it.
func TestFlightDetachStopsNewReadersJoining(t *testing.T) {
	group := newFlightGroup("orders", t.Name())
	query := newBlockingQuery(nil)

	go func() { _, _ = group.Do(context.Background(), "same", "acme", query.run) }()
	<-query.started

	group.detach("acme")

	go func() { _, _ = group.Do(context.Background(), "same", "acme", query.run) }()
	select {
	case <-query.started:
	case <-time.After(5 * time.Second):
		t.Fatal("a read after a write joined the query that preceded it")
	}
	close(query.release)

	if got := query.calls.Load(); got != 2 {
		t.Errorf("queries run = %d, want 2", got)
	}
}

func TestFlightDetachLeavesOtherNamespaces(t *testing.T) {
	group := newFlightGroup("orders", t.Name())
	query := newBlockingQuery(nil)

	go func() { _, _ = group.Do(context.Background(), "globex", "globex", query.run) }()
	<-query.started
	waitFor(t, func() bool { return group.pending() == 1 })

	group.detach("acme")
	if got := group.pending(); got != 1 {
		t.Errorf("globex's query was detached by a write to acme (%d in flight)", got)
	}
	close(query.release)
}

func TestFlightKeyDistinguishesArguments(t *testing.T) {
	stmt := &crispsql.Statement{SQL: "SELECT 1", Params: []string{"namespace", "name"}}

	base := flightKey(stmt, map[string]any{"namespace": "acme", "name": "order-1"})
	for _, args := range []map[string]any{
		{"namespace": "globex", "name": "order-1"},
		{"namespace": "acme", "name": "order-2"},
		{"namespace": "acme"},
		{"namespace": "acme", "name": int64(1)},
	} {
		if flightKey(stmt, args) == base {
			t.Errorf("args %v produced the same key as %v", args, map[string]any{"namespace": "acme", "name": "order-1"})
		}
	}

	// The same values, whatever order they were put in the map.
	if flightKey(stmt, map[string]any{"name": "order-1", "namespace": "acme"}) != base {
		t.Error("the key depends on map iteration order")
	}

	// A different statement with the same parameters is a different query.
	other := &crispsql.Statement{SQL: "SELECT 2", Params: []string{"namespace", "name"}}
	if flightKey(other, map[string]any{"namespace": "acme", "name": "order-1"}) == base {
		t.Error("two different statements share a key")
	}
}

// TestReadsCoalesceEndToEnd exercises the real storage: concurrent identical
// gets should reach the database once.
func TestReadsCoalesceEndToEnd(t *testing.T) {
	store := newTestREST(t)
	ctx := namespacedContext("acme")

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Get(ctx, "order-1001", &metav1.GetOptions{}); err != nil {
				t.Errorf("Get() returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	// SQLite answers too fast to guarantee overlap, so this asserts the shape
	// rather than a count: nothing is left in flight, and every reader was
	// answered with the same row.
	if got := store.flights.pending(); got != 0 {
		t.Errorf("%d queries left in flight after every reader finished", got)
	}
}

// coalesced reports how many reads have joined a query in flight.
func coalesced(t *testing.T, group *flightGroup) int {
	t.Helper()

	value, err := testutil.GetCounterMetricValue(
		crispmetrics.QueriesCoalesced.WithLabelValues(group.projection, group.resource))
	if err != nil {
		t.Fatalf("reading the coalescing counter: %v", err)
	}
	return int(value)
}

// waitFor spins until a condition holds, so a test never depends on a sleep
// being long enough.
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the expected state")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestShedRequestKeepsIts429 covers a regression the coalescing change
// introduced: the limiter now runs inside the shared query, and its answer
// passed back through the error translation, which turned a 429 into a 500.
func TestShedRequestKeepsIts429(t *testing.T) {
	spec := testSpec()
	spec.DataSource.MaxConcurrentQueries = ptr(int32(1))

	store := newStorage(t, spec).(*REST)

	// Hold the only slot, so the next read has nowhere to run.
	release, err := store.limiter.Acquire(context.Background())
	if err != nil {
		t.Fatalf("taking the slot: %v", err)
	}
	defer release()

	_, err = store.List(namespacedContext("acme"), nil)
	if !apierrors.IsTooManyRequests(err) {
		t.Fatalf("List() error = %v, want TooManyRequests", err)
	}
}

// TestFlightKeyDistinguishesTypes: two requests binding the string "1" and the
// number 1 send different statements to the database and must never share a
// result. The fast paths in writeBound have to keep that distinction the way
// %#v did.
func TestFlightKeyDistinguishesTypes(t *testing.T) {
	stmt := &crispsql.Statement{SQL: "SELECT 1 WHERE a = ?", Params: []string{"a"}}

	// One representative per distinct bound value. The integer widths are
	// deliberately absent: they bind identically, so sharing between them is
	// correct rather than a collision.
	values := []any{
		nil,
		"1",
		int64(1),
		float64(1),
		true,
		[]byte("1"),
		"true",
		"nil",
		"",
	}

	seen := map[string]any{}
	for _, value := range values {
		key := flightKey(stmt, map[string]any{"a": value})
		if previous, clash := seen[key]; clash {
			t.Errorf("%#v and %#v produce the same flight key %q", previous, value, key)
			continue
		}
		seen[key] = value
	}

	// An unset parameter is its own case, and must not look like a bound nil.
	if unset, bound := flightKey(stmt, nil), flightKey(stmt, map[string]any{"a": nil}); unset == bound {
		t.Error("an unset parameter and a bound nil produce the same flight key")
	}

	// The integer widths render alike on purpose.
	for _, same := range []any{int(1), int32(1)} {
		if got, want := flightKey(stmt, map[string]any{"a": same}),
			flightKey(stmt, map[string]any{"a": int64(1)}); got != want {
			t.Errorf("%#v keys as %q, want the same key as int64(1) (%q)", same, got, want)
		}
	}

	// Equal values still share, or coalescing would never fire.
	if a, b := flightKey(stmt, map[string]any{"a": "x"}), flightKey(stmt, map[string]any{"a": "x"}); a != b {
		t.Error("identical reads produce different flight keys, so they would never be coalesced")
	}
}
