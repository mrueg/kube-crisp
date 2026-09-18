package projection

import (
	"context"
	"strings"
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
)

// oversizedPagedSpec is a keyset-paged projection whose list query is allowed
// fewer rows than the table holds — the shape a large table is meant to be
// read in, one page at a time, and the shape the priming poll cannot read at
// all.
func oversizedPagedSpec(maxRows int32) crispv1alpha1.CustomResourceProjectionSpec {
	spec := testSpec()
	spec.Queries.List = crispv1alpha1.Query{
		SQL: `SELECT id, tenant, customer, status, total_cents, line_items, updated_at, seq
		      FROM orders
		      WHERE (:namespace IS NULL OR tenant = :namespace) AND (:after IS NULL OR seq > :after)
		      ORDER BY seq
		      LIMIT :limit`,
		KeysetColumn: "seq",
		MaxRows:      ptr(maxRows),
	}
	return spec
}

// TestAFailedPrimeIsNotRetriedByEveryList is a regression test.
//
// Every list on an unprimed cache ran the priming poll, and on a table larger
// than maxRows that poll was refused every time — so every page of a keyset
// walk first paid for a read of maxRows rows that was always going to fail,
// and the pages behind it queued for the same read. A failed prime is now
// waited out: the lists in between answer from their own rows, and the prime
// is tried again once the wait has passed.
func TestAFailedPrimeIsNotRetriedByEveryList(t *testing.T) {
	const rows = 25
	store := newPagedStorage(t, oversizedPagedSpec(10), rows)
	if store.watch == nil {
		t.Fatal("the projection has no watch cache; this test is about priming one")
	}

	// The full reads the cache asks for, which is what the prime costs.
	var fullReads int
	list := store.watch.list
	store.watch.list = func(ctx context.Context) ([]unstructured.Unstructured, error) {
		fullReads++
		return list(ctx)
	}

	ctx := namespacedContext("acme")
	page := func(token string) *unstructured.UnstructuredList {
		t.Helper()
		result, err := store.List(ctx, &metainternalversion.ListOptions{Limit: 5, Continue: token})
		if err != nil {
			t.Fatalf("List() returned error: %v", err)
		}
		return result.(*unstructured.UnstructuredList)
	}

	first := page("")
	if fullReads != 1 {
		t.Fatalf("the first list ran %d full reads, want the one priming attempt", fullReads)
	}
	if first.GetResourceVersion() == "" {
		t.Error("a list whose prime failed reported no resourceVersion; the newest version among its rows is the fallback")
	}

	second := page(first.GetContinue())
	if fullReads != 1 {
		t.Errorf("the second page ran the priming poll again (%d full reads); a prime that just "+
			"failed on a table larger than maxRows fails the same way on the next page", fullReads)
	}
	if second.GetResourceVersion() == "" {
		t.Error("a list inside the backoff reported no resourceVersion")
	}

	store.watch.mu.Lock()
	deferred := store.watch.primeDeferredLocked()
	// The wait has passed.
	store.watch.nextPrime = time.Now().Add(-time.Second)
	store.watch.mu.Unlock()
	if !deferred {
		t.Fatal("the failed prime was not deferred; the second page was spared a read for some other reason")
	}

	page(second.GetContinue())
	if fullReads != 2 {
		t.Errorf("once the backoff had passed the list ran %d full reads, want a second priming attempt", fullReads)
	}

	// A watch has no rows to fall back to. It still primes, and is still told
	// why it cannot be served, so the operator sees the maxRows it has to raise.
	_, err := store.Watch(ctx, &metainternalversion.ListOptions{})
	if err == nil {
		t.Fatal("Watch() succeeded though the poll cannot read the table")
	}
	if !strings.Contains(err.Error(), "maxRows") {
		t.Errorf("Watch() returned %v, want the maxRows refusal the poll produced", err)
	}
	if fullReads != 3 {
		t.Errorf("the watch ran %d full reads in total, want one of its own regardless of the lists' backoff", fullReads)
	}
}

// TestTheBackoffGrowsAndIsBounded: a table that will never fit is retried
// less and less often, but never less often than the cap — rows may be deleted,
// or the table may simply be smaller next time.
func TestTheBackoffGrowsAndIsBounded(t *testing.T) {
	cache := newWatchCache(time.Second, "orders", nil, nil)

	cache.mu.Lock()
	defer cache.mu.Unlock()

	var waits []time.Duration
	for i := 0; i < 12; i++ {
		cache.deferPrimeLocked()
		waits = append(waits, cache.primeBackoff)
	}
	if waits[0] != time.Second {
		t.Errorf("the first wait is %v, want the poll interval", waits[0])
	}
	for i := 1; i < len(waits); i++ {
		if waits[i] < waits[i-1] {
			t.Errorf("the wait shrank from %v to %v", waits[i-1], waits[i])
		}
	}
	if last := waits[len(waits)-1]; last != maxPrimeBackoff {
		t.Errorf("after repeated failures the wait is %v, want the cap of %v", last, maxPrimeBackoff)
	}
	if !cache.primeDeferredLocked() {
		t.Error("a prime that just failed is not being waited out")
	}
}

// TestAListDoesNotWaitForAnotherListsPrime covers the other half of the cost:
// a first list's priming poll is a full read, and every list arriving while it
// runs used to queue behind it. Such a list can answer from its own rows, so it
// does.
func TestAListDoesNotWaitForAnotherListsPrime(t *testing.T) {
	querying := make(chan struct{})
	release := make(chan struct{})

	cache := newWatchCache(time.Hour, "orders", nil, func(context.Context) ([]unstructured.Unstructured, error) {
		close(querying)
		<-release
		return []unstructured.Unstructured{cachedItem("acme", "order-1", "7")}, nil
	})
	t.Cleanup(cache.Close)

	primed := make(chan string, 1)
	go func() { primed <- cache.versionFor(context.Background()) }()
	<-querying

	answered := make(chan string, 1)
	go func() { answered <- cache.versionFor(context.Background()) }()
	select {
	case version := <-answered:
		if version != "" {
			t.Errorf("a list arriving during the prime was stamped %q; it has to answer from its own rows", version)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("versionFor() blocked behind another list's priming poll")
	}

	close(release)
	if version := <-primed; version != "7" {
		t.Errorf("the list that primed the cache was stamped %q, want 7", version)
	}
	if version := cache.versionFor(context.Background()); version != "7" {
		t.Errorf("a list after the prime was stamped %q, want the cache's 7", version)
	}
}
