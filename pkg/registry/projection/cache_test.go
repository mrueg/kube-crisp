package projection

import (
	"fmt"
	"testing"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/component-base/metrics/testutil"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// seedCache fills a cache with one object and one list per namespace, plus a
// cluster-wide list that spans them.
func seedCache(t *testing.T) *readCache {
	t.Helper()

	c := newReadCache(time.Minute, "orders.store.example.com")
	c.putObject("acme/order-1", "acme", &unstructured.Unstructured{Object: map[string]any{}})
	c.putList("acme#", "acme", &unstructured.UnstructuredList{})
	c.putObject("globex/order-9", "globex", &unstructured.Unstructured{Object: map[string]any{}})
	c.putList("globex#", "globex", &unstructured.UnstructuredList{})
	c.putList("#", "", &unstructured.UnstructuredList{})
	return c
}

func TestCacheInvalidateKeepsOtherNamespaces(t *testing.T) {
	c := seedCache(t)

	c.invalidate("acme")

	for _, key := range []string{"acme/order-1", "acme#", "#"} {
		if _, ok := c.lookup(key); ok {
			t.Errorf("entry %q survived a write to its namespace", key)
		}
	}
	for _, key := range []string{"globex/order-9", "globex#"} {
		if _, ok := c.lookup(key); !ok {
			t.Errorf("entry %q was dropped by a write to another namespace", key)
		}
	}
	if got, want := c.Len(), 2; got != want {
		t.Errorf("cache holds %d entries, want %d", got, want)
	}
}

func TestCacheInvalidateWithoutNamespaceDropsEverything(t *testing.T) {
	c := seedCache(t)

	// A cluster-scoped write, or a collection delete across all namespaces, has
	// no one namespace to blame.
	c.invalidate("")

	if got := c.Len(); got != 0 {
		t.Errorf("cache holds %d entries after an unscoped invalidation, want 0", got)
	}
}

// A poll reads every namespace in one query, so what it observed changing is a
// set rather than the single namespace a write knows about. The set is dropped
// in one pass over the cache, and the tenants outside it keep their entries.
func TestCacheInvalidateNamespacesDropsTheWholeSetAndNothingElse(t *testing.T) {
	c := seedCache(t)
	c.putList("initech#", "initech", &unstructured.UnstructuredList{})

	c.invalidateNamespaces([]string{"acme", "globex", "acme"})

	for _, key := range []string{"acme/order-1", "acme#", "globex/order-9", "globex#", "#"} {
		if _, ok := c.lookup(key); ok {
			t.Errorf("entry %q survived a poll that saw its namespace change", key)
		}
	}
	if _, ok := c.lookup("initech#"); !ok {
		t.Error("initech's list was dropped by a change in another tenant")
	}
}

// An unscoped change anywhere in the set subsumes every scoped one: it names no
// namespace to narrow to, so narrowing would mean keeping an entry that may
// well be stale.
func TestCacheInvalidateNamespacesWithAnUnscopedChangeDropsEverything(t *testing.T) {
	c := seedCache(t)

	c.invalidateNamespaces([]string{"acme", ""})

	if got := c.Len(); got != 0 {
		t.Errorf("cache holds %d entries after an unscoped change, want 0", got)
	}
}

// A poll that observed nothing has learned that the entries are still good.
// Dropping them anyway would leave cacheTTL meaning almost nothing.
func TestCacheInvalidateNamespacesWithAnEmptySetKeepsEverything(t *testing.T) {
	c := seedCache(t)

	c.invalidateNamespaces(nil)

	if got, want := c.Len(), 5; got != want {
		t.Errorf("cache holds %d entries after invalidating nothing, want %d", got, want)
	}
}

func TestCacheNilReceiverIsInert(t *testing.T) {
	var c *readCache

	c.putObject("k", "acme", &unstructured.Unstructured{})
	c.putList("k", "acme", &unstructured.UnstructuredList{})
	c.invalidate("acme")

	if _, ok := c.getObject("k"); ok {
		t.Error("a nil cache answered a read")
	}
	if got := c.Len(); got != 0 {
		t.Errorf("a nil cache reports %d entries, want 0", got)
	}
}

// cachedFor counts the entries the cache holds for one namespace. The total is
// not a proxy for that: a write's own read back caches the object it answers
// with, in the namespace it has just invalidated.
func cachedFor(c *readCache, namespace string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries := 0
	for _, entry := range c.entries {
		if entry.namespace == namespace {
			entries++
		}
	}
	return entries
}

// TestCachedReadsSurviveAWriteElsewhere is the point of scoping: a write in one
// namespace must not make every other namespace pay for a fresh query.
func TestCachedReadsSurviveAWriteElsewhere(t *testing.T) {
	spec := writableSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}

	store := newStorage(t, spec).(*WritableREST)

	acme := namespacedContext("acme")
	globex := namespacedContext("globex")

	if _, err := store.List(globex, nil); err != nil {
		t.Fatalf("List() in globex returned error: %v", err)
	}
	if _, err := store.Get(globex, "order-1003", &metav1.GetOptions{}); err != nil {
		t.Fatalf("Get() in globex returned error: %v", err)
	}
	cached := cachedFor(store.cache, "globex")
	if cached == 0 {
		t.Fatal("reads in globex cached nothing")
	}

	if _, err := store.Create(acme, newOrder("order-2100", "ada", 10), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("Create() in acme returned error: %v", err)
	}

	if got := cachedFor(store.cache, "globex"); got != cached {
		t.Errorf("cache holds %d entries for globex after a write to acme, want the %d cached before", got, cached)
	}

	// The namespace that was written to does lose its entries.
	if _, err := store.List(acme, nil); err != nil {
		t.Fatalf("List() in acme returned error: %v", err)
	}
	before := cachedFor(store.cache, "acme")
	if _, err := store.Create(acme, newOrder("order-2101", "ada", 10), nil, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("second Create() in acme returned error: %v", err)
	}
	if got := cachedFor(store.cache, "acme"); got >= before {
		t.Errorf("cache holds %d entries for acme after a write to acme, want fewer than %d", got, before)
	}
}

// TestCachedListViewIsIndependentOfTheEntry covers the compromise the cache
// makes: readers share the items, so everything a reader is allowed to write
// to — the collection's own metadata and the slice holding the items — has to
// be its own.
func TestCachedListViewIsIndependentOfTheEntry(t *testing.T) {
	c := newReadCache(time.Minute, "orders.store.example.com")

	stored := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{
		{Object: map[string]any{"metadata": map[string]any{"name": "order-1"}}},
		{Object: map[string]any{"metadata": map[string]any{"name": "order-2"}}},
	}}
	stored.SetResourceVersion("7")

	first := c.putList("acme#", "acme", stored)
	first.SetResourceVersion("999")
	first.SetContinue("token")
	first.Items = first.Items[:1]

	second, ok := c.getList("acme#")
	if !ok {
		t.Fatal("the collection was not cached")
	}
	if got := second.GetResourceVersion(); got != "7" {
		t.Errorf("resourceVersion = %q after another reader stamped its own view, want %q", got, "7")
	}
	if got := second.GetContinue(); got != "" {
		t.Errorf("continue = %q, want it unset", got)
	}
	if got := len(second.Items); got != 2 {
		t.Errorf("the cached collection holds %d items after a reader truncated its view, want 2", got)
	}
}

// TestCachedListSharesItems states the contract plainly, so that a future
// change that starts mutating a returned object fails here rather than in
// production: the items are the same objects, and are read-only.
func TestCachedListSharesItems(t *testing.T) {
	c := newReadCache(time.Minute, "orders.store.example.com")

	stored := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{
		{Object: map[string]any{"metadata": map[string]any{"name": "order-1"}}},
	}}
	_ = c.putList("acme#", "acme", stored)

	first, _ := c.getList("acme#")
	second, _ := c.getList("acme#")

	// Same map, not a copy of it: this is what makes a cache hit cheap.
	if &first.Items[0].Object == &second.Items[0].Object {
		t.Fatal("two views share a slice element address; they must have their own slices")
	}
	firstMeta, _, _ := unstructured.NestedMap(first.Items[0].Object, "metadata")
	if firstMeta == nil {
		t.Fatal("the view lost the object's metadata")
	}
	if first.Items[0].GetName() != second.Items[0].GetName() {
		t.Error("two views of one cached collection disagree about an item")
	}
}

// TestCacheEvictsOldestRatherThanEverything is a regression test. A full cache
// used to be dropped wholesale, so one client paging through a large collection
// — every page a distinct key, because the continue token is part of it — threw
// away every other client's entries on a regular cadence.
func TestCacheEvictsOldestRatherThanEverything(t *testing.T) {
	c := newReadCache(time.Hour, "orders")

	// The entry that has to survive: stored last, so it expires last.
	for i := 0; i < maxCacheEntries; i++ {
		c.putObject(fmt.Sprintf("filler/%d", i), "acme", &unstructured.Unstructured{Object: map[string]any{}})
	}
	c.putObject("kept", "acme", &unstructured.Unstructured{Object: map[string]any{}})

	if got := c.Len(); got > maxCacheEntries {
		t.Errorf("cache holds %d entries, which is past the bound of %d", got, maxCacheEntries)
	}
	if got, want := c.Len(), maxCacheEntries-evictionBatch; got < want {
		t.Errorf("cache holds %d entries; a full cache should drop a batch, not everything (want at least %d)", got, want)
	}
	if _, ok := c.lookup("kept"); !ok {
		t.Error("the most recently stored entry was evicted")
	}
}

// TestCacheEvictsExpiredEntriesFirst: an entry that is already past its TTL is
// answering nothing, so it goes before one that is still valid.
func TestCacheEvictsExpiredEntriesFirst(t *testing.T) {
	c := newReadCache(time.Hour, "orders")

	// Half the cache, already expired.
	for i := 0; i < maxCacheEntries/2; i++ {
		c.store(fmt.Sprintf("stale/%d", i), cacheEntry{
			object:    &unstructured.Unstructured{Object: map[string]any{}},
			namespace: "acme",
			expires:   time.Now().Add(-time.Minute),
		})
	}
	for i := 0; i < maxCacheEntries/2; i++ {
		c.putObject(fmt.Sprintf("live/%d", i), "acme", &unstructured.Unstructured{Object: map[string]any{}})
	}

	// One more insert tips it over the bound and triggers eviction.
	c.putObject("trigger", "acme", &unstructured.Unstructured{Object: map[string]any{}})

	for i := 0; i < maxCacheEntries/2; i++ {
		if _, ok := c.lookup(fmt.Sprintf("live/%d", i)); !ok {
			t.Fatalf("live entry %d was evicted while expired entries remained", i)
		}
	}
}

// TestCachedListCarriesTheSameResourceVersion covers what a cache hit has to
// say about itself.
//
// A list is what a client resumes a watch from, so a cached page answering
// with no resourceVersion leaves an informer with nothing to resume from and
// it replays the whole collection on every resync — the opposite of what
// caching it was for. The version also has to be the one the read that filled
// the cache saw, since stamping a cached page with the current version would
// date stale rows to now.
func TestCachedListCarriesTheSameResourceVersion(t *testing.T) {
	spec := writableSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}

	store := newStorage(t, spec).(*WritableREST)
	acme := namespacedContext("acme")

	first, err := store.List(acme, nil)
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	fresh, ok := first.(*unstructured.UnstructuredList)
	if !ok {
		t.Fatalf("List() returned %T", first)
	}
	if fresh.GetResourceVersion() == "" {
		t.Fatal("a fresh list carries no resourceVersion")
	}

	before := store.cache.Len()
	second, err := store.List(acme, nil)
	if err != nil {
		t.Fatalf("second List() returned error: %v", err)
	}
	if got := store.cache.Len(); got != before {
		t.Fatalf("the second list added %d cache entries, so it was not a hit", got-before)
	}

	cached, ok := second.(*unstructured.UnstructuredList)
	if !ok {
		t.Fatalf("List() returned %T", second)
	}
	if got, want := cached.GetResourceVersion(), fresh.GetResourceVersion(); got != want {
		t.Errorf("a cache hit reports resourceVersion %q, want %q as the read that filled it", got, want)
	}
}

// TestCachedListIsStillOfferedToAClientAskingForItsOwnVersion: a client that
// says "not older than X" can be answered from the cache when the cached page
// is at X. Without a resourceVersion on the entry that check could never pass,
// so every such request would miss.
func TestCachedListIsStillOfferedToAClientAskingForItsOwnVersion(t *testing.T) {
	spec := writableSpec()
	spec.CacheTTL = &metav1.Duration{Duration: time.Minute}

	store := newStorage(t, spec).(*WritableREST)
	acme := namespacedContext("acme")

	first, err := store.List(acme, nil)
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}
	version := first.(*unstructured.UnstructuredList).GetResourceVersion()

	before := store.cache.Len()
	options := &metainternalversion.ListOptions{
		ResourceVersion:      version,
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
	}
	if _, err := store.List(acme, options); err != nil {
		t.Fatalf("List() with a resourceVersion returned error: %v", err)
	}
	if got := store.cache.Len(); got != before {
		t.Errorf("a list at the cached version added %d entries, so it missed the cache", got-before)
	}
}

// TestCacheReportsItsSizeAndWhyEntriesGoAway covers the numbers that turn a hit
// rate into a diagnosis.
//
// A low hit rate says the cache is not paying for itself; it does not say why.
// Entries expiring is the cache working as configured, entries dropped because
// it was full means the key space is bigger than the cache, and entries dropped
// by a write mean the projection changes faster than the TTL it was given —
// three different problems with three different answers.
func TestCacheReportsItsSizeAndWhyEntriesGoAway(t *testing.T) {
	crispmetrics.CacheEvictions.Reset()
	crispmetrics.CacheEntries.Reset()

	const resource = "orders.store.example.com"
	entries := func() float64 {
		value, err := testutil.GetGaugeMetricValue(crispmetrics.CacheEntries.WithLabelValues(resource))
		if err != nil {
			t.Fatalf("reading the size gauge: %v", err)
		}
		return value
	}
	evictions := func(reason string) float64 {
		value, err := testutil.GetCounterMetricValue(
			crispmetrics.CacheEvictions.WithLabelValues(resource, reason))
		if err != nil {
			t.Fatalf("reading the %s counter: %v", reason, err)
		}
		return value
	}

	c := newReadCache(time.Minute, resource)
	c.putObject("acme/order-1", "acme", &unstructured.Unstructured{Object: map[string]any{}})
	c.putList("acme#", "acme", &unstructured.UnstructuredList{})

	if got := entries(); got != 2 {
		t.Errorf("the cache reports %v entries, want 2", got)
	}

	// A write to the namespace drops what it could have changed.
	c.invalidate("acme")
	if got := evictions(crispmetrics.CacheEvictionInvalidated); got != 2 {
		t.Errorf("%v entries were reported as invalidated, want 2", got)
	}
	if got := entries(); got != 0 {
		t.Errorf("the cache reports %v entries after invalidation, want 0", got)
	}

	// An entry read back after its TTL is an expiry, not a miss against an
	// empty cache — the difference is what says the TTL is too short.
	short := newReadCache(time.Millisecond, resource)
	short.putObject("acme/order-2", "acme", &unstructured.Unstructured{Object: map[string]any{}})
	time.Sleep(5 * time.Millisecond)
	if _, ok := short.getObject("acme/order-2"); ok {
		t.Fatal("an entry past its TTL was served")
	}
	if got := evictions(crispmetrics.CacheEvictionExpired); got != 1 {
		t.Errorf("%v entries were reported as expired, want 1", got)
	}
}

// TestCacheReportsPressureSeparatelyFromExpiry: entries dropped while still
// live are the ones that say the cache is too small — a client paging through a
// large collection is the usual cause, since every continue token is a key of
// its own.
func TestCacheReportsPressureSeparatelyFromExpiry(t *testing.T) {
	crispmetrics.CacheEvictions.Reset()

	const resource = "orders.store.example.com"
	c := newReadCache(time.Hour, resource)

	// Long TTL, so nothing here expires: every eviction is pressure.
	for i := range maxCacheEntries + 1 {
		c.putList(fmt.Sprintf("page-%d", i), "acme", &unstructured.UnstructuredList{})
	}

	full, err := testutil.GetCounterMetricValue(
		crispmetrics.CacheEvictions.WithLabelValues(resource, crispmetrics.CacheEvictionFull))
	if err != nil {
		t.Fatalf("reading the pressure counter: %v", err)
	}
	if full == 0 {
		t.Error("a cache that filled up reported no entries dropped under pressure")
	}

	expired, err := testutil.GetCounterMetricValue(
		crispmetrics.CacheEvictions.WithLabelValues(resource, crispmetrics.CacheEvictionExpired))
	if err != nil {
		t.Fatalf("reading the expiry counter: %v", err)
	}
	if expired != 0 {
		t.Errorf("%v entries were reported as expired, but nothing had reached its TTL", expired)
	}
}
