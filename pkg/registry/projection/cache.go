package projection

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// maxCacheEntries bounds the read cache. A projection with a high-cardinality
// key space — a client paging, whose continue token is part of every key —
// should not be able to grow it without limit.
const maxCacheEntries = 1024

// evictionBatch is how many entries a full cache drops at once, as a fraction
// of its size. Dropping a batch rather than a single entry keeps the eviction
// scan from running on every insert once the cache is full.
const evictionBatch = maxCacheEntries / 4

// readCache holds recent read results for cacheTTL.
//
// It is deliberately simple: entries expire on time, any write to the
// projection drops the whole set, and it never serves anything it is not sure
// about. Caching a projection trades freshness for load, which is why it is off
// unless a projection asks for it.
type readCache struct {
	ttl      time.Duration
	resource string

	mu      sync.Mutex
	entries map[string]cacheEntry
}

type cacheEntry struct {
	object *unstructured.Unstructured
	list   *unstructured.UnstructuredList

	// namespace is what the entry describes. A cluster-wide read has none and
	// covers every namespace, so it is invalidated by any write.
	namespace string
	expires   time.Time
}

func newReadCache(ttl time.Duration, resource string) *readCache {
	if ttl <= 0 {
		return nil
	}
	return &readCache{ttl: ttl, resource: resource, entries: map[string]cacheEntry{}}
}

// getObject returns a cached single object.
func (c *readCache) getObject(key string) (*unstructured.Unstructured, bool) {
	entry, ok := c.lookup(key)
	if !ok || entry.object == nil {
		return nil, false
	}
	return entry.object.DeepCopy(), true
}

// getList returns a cached collection as a view over it.
//
// The items are shared rather than copied. Deep-copying a large collection
// costs as much as the query it is standing in for, which would make the cache
// pointless at exactly the size it matters most.
func (c *readCache) getList(key string) (*unstructured.UnstructuredList, bool) {
	entry, ok := c.lookup(key)
	if !ok || entry.list == nil {
		return nil, false
	}
	return viewOf(entry.list), true
}

// viewOf returns a collection a caller may hold and stamp without disturbing
// the cached original.
//
// The list's own metadata is copied in full — nested, because a resourceVersion
// or a continue token is written inside it, not beside it. The items are not
// copied: they are the bulk of the collection, and every reader only
// serializes them.
//
// This is the cache's one rule: an object that has been given to it is
// immutable from then on. It is the same contract the kube-apiserver's watch
// cache keeps, for the same reason — copying a large collection costs as much
// as the query the cache exists to avoid.
func viewOf(list *unstructured.UnstructuredList) *unstructured.UnstructuredList {
	return &unstructured.UnstructuredList{
		Object: runtime.DeepCopyJSON(list.Object),
		Items:  append(make([]unstructured.Unstructured, 0, len(list.Items)), list.Items...),
	}
}

func (c *readCache) lookup(key string) (cacheEntry, bool) {
	if c == nil {
		return cacheEntry{}, false
	}

	c.mu.Lock()
	entry, ok := c.entries[key]
	if ok && time.Now().After(entry.expires) {
		delete(c.entries, key)
		c.evictedLocked(crispmetrics.CacheEvictionExpired, 1)
		ok = false
	}
	c.mu.Unlock()

	result := "miss"
	if ok {
		result = "hit"
	}
	crispmetrics.CacheReads.WithLabelValues(c.resource, result).Inc()
	return entry, ok
}

// putObject stores a single object. Every method tolerates a nil receiver,
// because a projection without cacheTTL has no cache and the read paths should
// not have to ask.
func (c *readCache) putObject(key, namespace string, obj *unstructured.Unstructured) {
	if c == nil {
		return
	}
	c.store(key, cacheEntry{
		object:    obj.DeepCopy(),
		namespace: namespace,
		expires:   time.Now().Add(c.ttl),
	})
}

// putList stores a collection and returns what to answer the current request
// with.
//
// The cache takes ownership of the collection: nothing may modify it
// afterwards, including the caller that built it, which is handed a view over
// it instead. Without a cache configured the collection is simply passed back,
// since there is nothing to protect it from.
func (c *readCache) putList(key, namespace string, list *unstructured.UnstructuredList) *unstructured.UnstructuredList {
	if c == nil {
		return list
	}
	c.store(key, cacheEntry{
		list:      list,
		namespace: namespace,
		expires:   time.Now().Add(c.ttl),
	})
	return viewOf(list)
}

func (c *readCache) store(key string, entry cacheEntry) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= maxCacheEntries {
		c.evictLocked()
	}
	c.entries[key] = entry
	c.sizeLocked()
}

// evictedLocked records entries that went away and republishes the size.
func (c *readCache) evictedLocked(reason string, count int) {
	if count > 0 {
		crispmetrics.CacheEvictions.WithLabelValues(c.resource, reason).Add(float64(count))
	}
	c.sizeLocked()
}

// sizeLocked publishes how much the cache is holding.
func (c *readCache) sizeLocked() {
	crispmetrics.CacheEntries.WithLabelValues(c.resource).Set(float64(len(c.entries)))
}

// evictLocked makes room in a full cache.
//
// Expired entries go first, since they are not answering anything; if that is
// not enough, the entries closest to expiring follow. Every entry has the same
// TTL, so that is the same as evicting the oldest — which for a cache whose
// whole contract is "no older than the TTL" is the right order: an entry does
// not become fresher for being read again.
//
// The alternative it replaces was dropping the entire cache, which let one
// client paging through a large collection throw away every other client's
// entries on a regular cadence.
func (c *readCache) evictLocked() {
	now := time.Now()

	var expired int
	for key, entry := range c.entries {
		if now.After(entry.expires) {
			delete(c.entries, key)
			expired++
		}
	}
	c.evictedLocked(crispmetrics.CacheEvictionExpired, expired)

	if len(c.entries) < maxCacheEntries {
		return
	}

	type aged struct {
		key     string
		expires time.Time
	}
	entries := make([]aged, 0, len(c.entries))
	for key, entry := range c.entries {
		entries = append(entries, aged{key: key, expires: entry.expires})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].expires.Before(entries[j].expires) })

	drop := min(evictionBatch, len(entries))
	for _, entry := range entries[:drop] {
		delete(c.entries, entry.key)
	}
	// Counted apart from the expired ones above: these were still live, and
	// dropping them means the cache is smaller than the key space it is being
	// asked to cover rather than simply doing its job.
	c.evictedLocked(crispmetrics.CacheEvictionFull, drop)
}

// invalidate drops what a write to one namespace could have changed: that
// namespace's entries, and any cluster-wide read, which by definition included
// the row.
//
// Entries for other namespaces are left alone. A write to one tenant is not a
// reason to make every other tenant pay for a fresh read.
//
// An empty namespace means the write was not scoped to one — a cluster-scoped
// kind, or a collection delete across all namespaces — and everything goes.
func (c *readCache) invalidate(namespace string) {
	c.invalidateNamespaces([]string{namespace})
}

// invalidateNamespaces drops what changes across several namespaces could have
// made stale, in one pass over the cache.
//
// It exists for the poller. A write knows the single namespace it touched, but
// a poll reads every namespace of a projection in one query and comes back with
// a batch of rows that may span any number of them. Calling invalidate once per
// namespace would walk the whole cache once per changed tenant, on every poll
// that saw anything — so the set is collected first and the cache is walked
// once.
//
// An empty string anywhere in the set means everything goes, for the same
// reason it does for a write: it names no namespace to narrow to, and a cached
// entry that might cover the change has to be dropped rather than guessed
// about. An empty set means there was nothing to drop and the cache stands.
func (c *readCache) invalidateNamespaces(namespaces []string) {
	if c == nil || len(namespaces) == 0 {
		return
	}

	// Nil rather than empty once an unscoped change is in the set: it is the
	// difference between "these tenants" and "all of them", and the second
	// subsumes every version of the first.
	scoped := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		if namespace == "" {
			scoped = nil
			break
		}
		scoped[namespace] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if scoped == nil {
		dropped := len(c.entries)
		c.entries = map[string]cacheEntry{}
		c.evictedLocked(crispmetrics.CacheEvictionInvalidated, dropped)
		return
	}

	var dropped int
	for key, entry := range c.entries {
		// A cluster-wide read has no namespace of its own and spans them all,
		// so it included the changed row by definition.
		if _, hit := scoped[entry.namespace]; hit || entry.namespace == "" {
			delete(c.entries, key)
			dropped++
		}
	}
	c.evictedLocked(crispmetrics.CacheEvictionInvalidated, dropped)
}

// Len reports how many entries are held, for tests and diagnostics.
func (c *readCache) Len() int {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// objectKey identifies a single-object read.
func objectKey(namespace, name string) string {
	return "get/" + namespace + "/" + name
}

// listKey identifies a collection read, including everything that changes what
// the collection contains.
func listKey(namespace string, options *metainternalversion.ListOptions) string {
	parts := []string{"list", namespace}
	if options != nil {
		if options.LabelSelector != nil {
			parts = append(parts, "labels="+options.LabelSelector.String())
		}
		if options.FieldSelector != nil {
			parts = append(parts, "fields="+options.FieldSelector.String())
		}
		if options.Limit > 0 {
			parts = append(parts, "limit="+strconv.FormatInt(options.Limit, 10))
		}
		if options.Continue != "" {
			parts = append(parts, "continue="+options.Continue)
		}
	}
	return strings.Join(parts, "/")
}
