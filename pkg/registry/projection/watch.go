package projection

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/klog/v2"

	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
)

// DefaultPollInterval is how often a watched projection re-runs its query.
const DefaultPollInterval = 5 * time.Second

// DefaultFullResyncInterval is how often a projection polling incrementally
// re-reads everything, which is what detects deletions.
const DefaultFullResyncInterval = time.Minute

// DefaultBookmarkInterval is how often an idle watch is told the current
// resourceVersion, so a client that reconnects resumes from a recent point.
const DefaultBookmarkInterval = time.Minute

// eventBuffer is the handover between the cache and one watcher's connection.
// It is small because the queue that matters is the watcher's own backlog.
const eventBuffer = 16

// maxPendingEvents bounds a watcher's backlog. It has to be larger than any
// collection a client might be sent on connect: the initial replay is queued in
// one go, and a client-go informer will not report itself synced until it has
// received all of it. Beyond this the watcher is disconnected and relists.
const maxPendingEvents = 50000

// maxCachePendingEvents bounds the backlog of every watcher on one projection
// together.
//
// The per-watcher bound says nothing about how many watchers there are, and the
// replay is the whole collection: fifty informers reconnecting at once — a
// rolling restart of their controllers — each queue a copy of it. On a
// ten-thousand-object projection that is half a million objects held at once,
// and nothing was counting.
//
// Roughly twenty watchers replaying a ten-thousand-object collection. Past it a
// watcher is refused rather than admitted, because a client that is told to
// relist comes back, and one that is admitted into an out-of-memory kill takes
// every other watcher with it.
const maxCachePendingEvents = 200000

// DefaultHistorySize is how many recent changes are kept so that a watch can
// resume instead of relisting.
const DefaultHistorySize = 1000

// watchCache turns a projection into a stream of watch events by polling its
// list query and diffing consecutive snapshots.
//
// One cache covers every namespace of a projection: it polls with a NULL
// namespace so a single query answers all watchers, and each watcher filters
// the events it cares about. Polling runs only while watchers are connected.
type watchCache struct {
	list     func(ctx context.Context) ([]unstructured.Unstructured, error)
	interval time.Duration

	// followerInterval is what this cache polls at while some other replica
	// holds the lease. It only applies with leader election on.
	followerInterval time.Duration

	// incremental reads only what changed at or after a resourceVersion. When
	// set, most polls become a small read instead of a full scan; passing an
	// empty version makes it read everything, which is how the periodic full
	// resync runs.
	incremental func(ctx context.Context, since string) ([]unstructured.Unstructured, error)

	// deleted reads the identities removed at or after a resourceVersion. With
	// it an incremental poll sees deletions too, and the full resync — which
	// exists only to notice them — becomes optional.
	deleted func(ctx context.Context, since string) ([]cacheIdentity, error)

	fullResyncInterval time.Duration
	lastFullResync     time.Time
	highestVersion     string

	// bookmarkInterval bounds how stale a watcher's idea of the current
	// version becomes while nothing changes. Zero disables bookmarks.
	bookmarkInterval time.Duration
	lastBookmark     time.Time

	// lightweight drops the objects from the cache and keeps only each row's
	// key and mapped resourceVersion.
	//
	// The cache held whole objects for two reasons. The diff needs to know
	// whether a row changed, which a mapped resourceVersion answers on its own.
	// And a Deleted event needs the object, which the cache was the only place
	// to find — a row that is gone has nothing left to describe it. A tombstone
	// carrying the mapped columns removes the second reason, and then the
	// collection does not have to be held in memory at all.
	//
	// Set by the caller only when both hold, because getting it wrong means
	// deletions that name a row and describe nothing.
	lightweight bool

	// history is a ring of recent changes, oldest first, so a client that
	// reconnects can be handed what it missed rather than being told to start
	// over. Beyond its span there is nothing to replay, and 410 is the only
	// honest answer.
	history     []recordedEvent
	historySize int

	// resource labels this cache's metrics.
	resource string

	// matchFields applies the projection's field selectors to an object.
	matchFields func(*unstructured.Unstructured, fields.Selector) bool

	// group drives this cache's polling together with the other versions of
	// the same projection.
	group *pollGroup

	// polled says whether this cache has ever completed a poll, and primeMu
	// serialises the first one. Until it has, the cache knows nothing about the
	// collection and its version means nothing — see versionFor.
	polled  bool
	primeMu sync.Mutex

	// pollMu serialises polls against each other. It is held for a whole poll,
	// including the query, while mu is taken only to plan one and to apply its
	// result — so a database round trip never blocks a List, a new watcher, or
	// a departing one.
	pollMu sync.Mutex

	// pending counts the events queued across every watcher of this cache.
	// Atomic rather than under mu, because it is read and written on the
	// delivery path, which deliberately holds no cache lock.
	pending atomic.Int64

	mu       sync.Mutex
	items    map[string]*unstructured.Unstructured
	version  int64
	watchers map[int64]*cacheWatcher
	nextID   int64
	lastPoll time.Time
}

func newWatchCache(interval time.Duration, resource string, group *pollGroup, list func(ctx context.Context) ([]unstructured.Unstructured, error)) *watchCache {
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	if group == nil {
		// A cache with nobody to share with still needs a timer, and has
		// nothing to be notified through.
		group = newPollGroup(nil, resource)
	}
	return &watchCache{
		group:              group,
		list:               list,
		interval:           interval,
		followerInterval:   DefaultFollowerPollInterval,
		fullResyncInterval: DefaultFullResyncInterval,
		bookmarkInterval:   DefaultBookmarkInterval,
		historySize:        DefaultHistorySize,
		resource:           resource,
		items:              map[string]*unstructured.Unstructured{},
		watchers:           map[int64]*cacheWatcher{},
		version:            1,
	}
}

// versionFor is what a List stamps: the cache's version, priming the cache
// first if it has never polled.
//
// The priming matters. Polling starts with the first watcher, so before that
// the cache has read nothing, and currentVersionLocked falls back to the
// counter — which for a projection whose versions come from a column is not a
// version at all, it is the number 1. A client that listed and then watched was
// therefore refused with 410 every time, on every projection with a mapped
// resourceVersion, after every restart: "too old resource version: 1 (10000)".
// That is an informer's first sync, so it happened to everyone, always.
//
// One query, once per projection per process, on the first list that has no
// watcher behind it. If it fails the caller gets "" and falls back to the rows
// it just read, which is a worse answer than the cache's but a better one than
// a number the watch will refuse.
func (c *watchCache) versionFor(ctx context.Context) string {
	c.mu.Lock()
	primed := c.polled
	version := c.currentVersionLocked()
	c.mu.Unlock()
	if primed {
		return version
	}

	// Re-checked under primeMu so that concurrent first lists cost one poll
	// rather than one each. Separate from pollMu, which poll takes itself.
	c.primeMu.Lock()
	defer c.primeMu.Unlock()

	c.mu.Lock()
	primed = c.polled
	c.mu.Unlock()
	if !primed {
		if err := c.poll(ctx); err != nil {
			klog.V(2).InfoS("could not prime the watch cache to stamp a list",
				"resource", c.resource, "err", err)
			return ""
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentVersionLocked()
}

// ResourceVersion reports the version of the most recent snapshot. Lists stamp
// this so that a watch started from a list's version continues from the same
// point instead of replaying the collection.
func (c *watchCache) ResourceVersion() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentVersionLocked()
}

// currentVersionLocked prefers the highest version seen in the data over the
// cache's own counter.
//
// That is what lets more than one replica serve the same projection: every
// replica reads the same rows, so a version derived from them means the same
// thing everywhere, while a per-replica counter does not. A projection with no
// mapped resourceVersion falls back to the counter and is therefore
// single-replica.
func (c *watchCache) currentVersionLocked() string {
	if c.highestVersion != "" {
		return c.highestVersion
	}
	return strconv.FormatInt(c.version, 10)
}

// Watch registers a watcher, starting the poller if this is the first one.
//
// A watcher that asks for the current version receives only later changes.
// Anything else, including an empty version, receives the current contents as
// Added events first, which is what a client-go informer needs in order to
// build a complete store.
func (c *watchCache) Watch(ctx context.Context, namespace string, selector labels.Selector, fieldSelector fields.Selector, resourceVersion string, sendInitialEvents, allowBookmarks bool, gvk schema.GroupVersionKind) (watch.Interface, error) {
	// Populating an empty cache runs a query, and that must not happen holding
	// mu: every List stamps its resourceVersion from this cache, so a read
	// arriving during the first watcher's poll would wait for the database.
	c.mu.Lock()
	empty := len(c.watchers) == 0
	c.mu.Unlock()

	if empty {
		if err := c.poll(ctx); err != nil {
			return nil, err
		}
	}

	// A client resuming from beyond the in-memory window may still be
	// answerable from the database. Attempted before the lock, because reading
	// it is a round trip and the cache lock is held by every List.
	//
	// The check here is advisory and the authoritative one happens below: if
	// the ring turns out to cover the version after all, memory wins.
	var (
		fromDatabase   []recordedEvent
		databaseReplay bool
	)
	if !sendInitialEvents && resourceVersion != "" && resourceVersion != "0" &&
		!c.canReplayFromMemory(resourceVersion) {
		fromDatabase, databaseReplay = c.replayFromDatabase(ctx, resourceVersion, gvk)
	}

	c.mu.Lock()

	first := len(c.watchers) == 0

	c.nextID++
	w := newCacheWatcher(c.nextID, c)
	w.namespace = namespace
	w.selector = selector
	w.fields = fieldSelector
	w.bookmarks = allowBookmarks
	w.gvk = gvk
	w.matchFields = c.matchFields

	version := c.currentVersionLocked()

	// What a client may resume from, following the same contract as any other
	// Kubernetes resource:
	//
	//   - unset, "0", or a WatchList request: the current contents as Added
	//     events, then changes.
	//   - the version it was last told about: changes only.
	//   - any other version: refused with 410, because a projection keeps no
	//     history and cannot replay from an arbitrary point. The client
	//     relists, rather than silently receiving a collection it did not ask
	//     for and believing it resumed.
	replay := sendInitialEvents || resourceVersion == "" || resourceVersion == "0"

	var missed []recordedEvent
	if !replay {
		var resumable bool
		missed, resumable = c.replayable(resourceVersion)
		if !resumable && databaseReplay {
			missed, resumable = fromDatabase, true
		}
		if !resumable {
			c.nextID--
			c.mu.Unlock()
			w.terminate()
			return nil, apierrors.NewResourceExpired(fmt.Sprintf(
				"too old resource version: %s (%s)", resourceVersion, version))
		}
	}

	// Only the pointers, and only while the lock is held. The objects a cache
	// holds are immutable from the moment they enter it, so copying them can
	// happen afterwards — and it has to, because copying a ten-thousand-object
	// collection under this lock made every List on the projection wait for it.
	// Every List stamps its resourceVersion from here.
	var (
		snapshot     []*unstructured.Unstructured
		readSnapshot bool
	)
	if replay {
		// A lightweight cache holds no objects to hand out, so its initial
		// state is read from the database instead — after this watcher is
		// registered, and after this lock is released. Both matter, and for
		// different reasons: a query here would block every List on the
		// projection for its duration, and a read taken before registration
		// would miss anything a poll delivered in between.
		readSnapshot = c.lightweight
		if !readSnapshot {
			snapshot = c.sortedItemsLocked()
		}
	}

	// Registered before the replay is built, so a poll that lands in between
	// delivers to this watcher rather than past it. Its events describe changes
	// after the snapshot, and prepend puts the snapshot in front of them.
	c.watchers[w.id] = w
	crispmetrics.Watchers.WithLabelValues(c.resource).Set(float64(len(c.watchers)))
	c.mu.Unlock()

	// After the unlock: the poller drops a cache with no watchers, so the group
	// could not be joined any earlier than this watcher being in the map.
	if first {
		c.group.add(c)
	}

	if readSnapshot {
		rows, err := c.list(ctx)
		if err != nil {
			// Registered already, so it has to be taken back out — and
			// stop is what also leaves the poll group when this was the
			// only watcher.
			c.stop(w.id)
			w.terminate()
			return nil, err
		}
		snapshot = make([]*unstructured.Unstructured, 0, len(rows))
		for i := range rows {
			snapshot = append(snapshot, &rows[i])
		}
		// Key order, matching what the cache would have handed out.
		slices.SortFunc(snapshot, func(a, b *unstructured.Unstructured) int {
			return strings.Compare(cacheKey(a), cacheKey(b))
		})
	}

	initial := make([]watch.Event, 0, len(snapshot)+len(missed)+1)

	// The replay is filtered exactly like the live stream: one poll covers
	// every namespace, but a watcher only ever sees its own.
	for _, item := range snapshot {
		if !w.matches(item) {
			continue
		}
		initial = append(initial, watch.Event{Type: watch.Added, Object: item.DeepCopy()})
	}

	// Whatever the client missed while it was away, in the order it happened.
	for _, recorded := range missed {
		if !w.matches(recorded.event.Object) {
			continue
		}
		initial = append(initial, watch.Event{
			Type: recorded.event.Type, Object: recorded.event.Object.DeepCopyObject(),
		})
	}

	// A client using WatchList streams the collection over the watch itself and
	// waits for this bookmark to know the initial set is complete. Without it a
	// client-go informer never finishes syncing.
	//
	// Both conditions, because that is how the endpoint defines a WatchList —
	// and it only wires up the hook that consumes this bookmark when both are
	// set. Sending it to a watcher that asked for neither made the handler call
	// a nil function and take the connection down with it, which is what an
	// ordinary `kubectl get --watch` used to get.
	if sendInitialEvents && allowBookmarks {
		bookmark := &unstructured.Unstructured{Object: map[string]any{}}
		bookmark.SetGroupVersionKind(gvk)
		bookmark.SetResourceVersion(version)
		bookmark.SetAnnotations(map[string]string{metav1.InitialEventsAnnotationKey: "true"})
		initial = append(initial, watch.Event{Type: watch.Bookmark, Object: bookmark})
	}

	if !w.prepend(initial) {
		// More than this watcher may hold, or more than the projection has room
		// for across all of them. Saying so beats delivering part of a
		// collection silently.
		c.stop(w.id)
		w.terminate()
		return nil, apierrors.NewInternalError(fmt.Errorf(
			"the collection is too large to send over a watch; list it instead"))
	}
	return w, nil
}

// stop removes a watcher and stops polling once the last one leaves.
func (c *watchCache) stop(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.watchers, id)
	crispmetrics.Watchers.WithLabelValues(c.resource).Set(float64(len(c.watchers)))

	if len(c.watchers) == 0 {
		c.group.remove(c)
	}
}

// Close tears the cache down regardless of connected watchers.
func (c *watchCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, w := range c.watchers {
		w.terminate()
		delete(c.watchers, id)
	}
	crispmetrics.Watchers.WithLabelValues(c.resource).Set(0)
	c.group.remove(c)
}

func (c *watchCache) refresh(ctx context.Context) error {
	c.mu.Lock()
	if len(c.watchers) == 0 {
		// The last watcher left between the tick and this call.
		c.group.remove(c)
		c.mu.Unlock()
		return nil
	}
	c.lastPoll = time.Now()
	c.mu.Unlock()

	return c.poll(ctx)
}

// due reports whether this cache wants the tick it is being offered. The group
// ticks at its fastest member's interval, so a slower cache skips most of them.
// Half a tick of tolerance keeps a cache whose interval is a multiple of the
// group's from skipping every other poll to timer drift.
func (c *watchCache) due(tick time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lastPoll.IsZero() {
		return true
	}
	return time.Since(c.lastPoll) >= c.effectiveInterval()-tick/2
}

// effectiveInterval is how often this cache actually wants to poll.
//
// With leader election off, or on this replica while it holds the lease, that
// is the projection's own interval. On a replica that does not hold it, the
// slower follower interval: its watchers then lag by that much rather than
// stopping, which is the trade leader election makes here. A follower interval
// shorter than the configured one would be no reduction at all, so it is only
// ever used when it is genuinely slower.
//
// It takes no lock, deliberately. Both intervals are set while the cache is
// being built and never change afterwards, and leadership is an atomic read —
// so there is nothing here to protect. That matters because the poll group asks
// every member for this while holding its own lock, and taking the cache's here
// would invert the order every other path uses: Watch and stop both take the
// cache's lock and then the group's.
func (c *watchCache) effectiveInterval() time.Duration {
	if leading() || c.followerInterval <= c.interval {
		return c.interval
	}
	return c.followerInterval
}

// poll re-runs the projection's query and broadcasts the differences.
//
// The query runs with mu released. Every List stamps its resourceVersion from
// this cache, and Watch and stop take the same lock, so holding it across a
// database round trip — a full resync over a large table, on a slow link —
// would make all of them wait on the poll. pollMu serialises polls against each
// other instead, which is all the ordering a diff of consecutive snapshots
// needs.
func (c *watchCache) poll(ctx context.Context) error {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()

	start := time.Now()
	defer func() {
		crispmetrics.WatchPollDuration.WithLabelValues(c.resource).Observe(time.Since(start).Seconds())
	}()

	// A full read is needed to notice deletions, because a row that is gone
	// simply stops being returned. Incremental polling therefore covers the
	// common case and the full read runs on its own, slower, schedule.
	c.mu.Lock()
	// Captured before the poll advances it, so the diff below can tell what an
	// incremental read would have been able to return.
	watermark := c.highestVersion
	// The first poll is always full: an incremental read has no version to
	// start from, and the cache has to know what is already there.
	full := c.incremental == nil ||
		c.highestVersion == "" ||
		(c.fullResyncInterval > 0 && time.Since(c.lastFullResync) >= c.fullResyncInterval)
	since := c.highestVersion
	timeout := c.interval + DefaultPollInterval
	c.mu.Unlock()

	mode := "incremental"
	if full {
		mode = "full"
	}
	crispmetrics.WatchPolls.WithLabelValues(c.resource, mode).Inc()

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		items []unstructured.Unstructured
		err   error
	)
	switch {
	case c.incremental == nil:
		items, err = c.list(queryCtx)
	case full:
		items, err = c.incremental(queryCtx, "")
	default:
		items, err = c.incremental(queryCtx, since)
	}
	if err != nil {
		return err
	}

	// A forward-reading poll cannot see a row that is gone, so the deletions
	// are asked for separately. A full read already knows what is missing and
	// does not need to ask.
	var removed []cacheIdentity
	if !full && c.deleted != nil {
		if removed, err = c.deleted(queryCtx, since); err != nil {
			return err
		}
	}

	c.mu.Lock()
	events, targets := c.applyLocked(items, full, watermark, removed)
	c.polled = true
	c.mu.Unlock()

	// Deliberately outside mu. Every watcher is handed its own copy of every
	// event, so a projection with many watchers does real work here — work that
	// would otherwise block every List stamping its resourceVersion from this
	// cache. pollMu still holds, so events cannot interleave with another poll.
	c.broadcast(events, targets)
	return nil
}

// applyLocked folds one poll's rows into the cache and broadcasts what changed.
//
// full says whether items describe the whole collection or only the rows that
// moved; watermark is the highest version seen before the poll started, which
// is what tells a resync it found a change an incremental read should have.
func (c *watchCache) applyLocked(
	items []unstructured.Unstructured,
	full bool,
	watermark string,
	removed []cacheIdentity,
) ([]watch.Event, []*cacheWatcher) {
	// The high-water mark is what both incremental polling and the reported
	// resourceVersion are built on, so it is tracked whichever query ran. The
	// incremental query returns rows in version order, but a list query need
	// not, so the maximum is taken rather than the last row.
	for i := range items {
		if version := items[i].GetResourceVersion(); movesForward(c.highestVersion, version) {
			c.highestVersion = version
		}
	}
	if full && c.incremental != nil {
		c.lastFullResync = time.Now()
	}

	var events []watch.Event
	next := c.items

	if full {
		next = make(map[string]*unstructured.Unstructured, len(items))
		for i := range items {
			item := items[i]
			next[cacheKey(&item)] = c.retain(&item)
		}

		// A resync that finds a change at or below the incremental high-water
		// mark has found something a forward-reading poll can never return.
		// The static check rejects the usual cause; this catches the rest.
		var missed int
		for key, item := range next {
			previous, existed := c.items[key]
			switch {
			case !existed:
				events = append(events, watch.Event{Type: watch.Added, Object: item})
			case changed(previous, item):
				events = append(events, watch.Event{Type: watch.Modified, Object: item})
			default:
				continue
			}
			if c.incremental != nil && watermark != "" && !movesForward(watermark, item.GetResourceVersion()) {
				missed++
			}
		}
		if missed > 0 {
			crispmetrics.WatchMissedEvents.WithLabelValues(c.resource).Add(float64(missed))
			klog.InfoS("a full resync found changes an incremental poll should have seen; "+
				"the mapped resourceVersion column is not moving forward on every write",
				"resource", c.resource, "changes", missed)
		}
		for key, item := range c.items {
			if _, still := next[key]; !still {
				events = append(events, watch.Event{Type: watch.Deleted, Object: item})
			}
		}
	} else {
		// Only the rows that moved came back, so the rest of the cache stands.
		for i := range items {
			item := items[i]
			key := cacheKey(&item)
			previous, existed := next[key]
			switch {
			case !existed:
				events = append(events, watch.Event{Type: watch.Added, Object: &item})
			case changed(previous, &item):
				events = append(events, watch.Event{Type: watch.Modified, Object: &item})
			default:
				continue
			}
			next[key] = c.retain(&item)
		}

		// Whatever the deletion query reported, in the same poll: the event
		// carries the object as the cache last saw it, since a row that is gone
		// has nothing left to describe it.
		for _, identity := range removed {
			key := identity.key()
			previous, existed := next[key]

			// A tombstone describing an older incarnation than the row in hand
			// is stale: the name was deleted and created again, and what exists
			// now is not what was removed. Applying it would emit a Deleted for
			// the live row and drop it from the cache — and an incremental poll
			// will not return it again, because its version is no longer past
			// :since, so it stays invisible to every watcher while sitting in
			// the table.
			//
			// Only decidable when the tombstone records the row's version,
			// which is one more reason for it to carry the mapped columns.
			if existed && identity.object != nil &&
				movesForward(identity.object.GetResourceVersion(), previous.GetResourceVersion()) {
				continue
			}

			// The tombstone's own row when the cache is holding a trimmed
			// entry: it is a whole object, so a field selector over a mapped
			// column still matches, which a trimmed one could not answer.
			if c.lightweight && identity.object != nil {
				previous, existed = identity.object, true
			}

			if !existed {
				// Not in the cache. With a tombstone that describes the row
				// this is still a deletion worth reporting — a row created and
				// removed between two polls, which a cache-only path drops
				// silently.
				if identity.object != nil {
					events = append(events, watch.Event{Type: watch.Deleted, Object: identity.object})
				}
				continue
			}
			events = append(events, watch.Event{Type: watch.Deleted, Object: previous})
			delete(next, key)
		}
	}

	c.items = next

	c.emitBookmarksLocked()
	if len(events) == 0 {
		return nil, nil
	}

	previous := c.currentVersionLocked()
	c.version++
	version := c.currentVersionLocked()

	// Stamping every object with the snapshot version lets a client resume from
	// the version its last event carried. Done before the events are recorded
	// or broadcast, because from here on the objects are shared and immutable.
	//
	// Onto a copy, because the object in the event is the same one the cache is
	// keeping. Stamping in place put a version on the cached object that the
	// database never gave it, and the next poll compared a freshly mapped row
	// carrying no version against a cached one carrying this counter — which
	// differ, always. Every object on a projection that maps no
	// resourceVersion was reported as modified on every poll, forever, and the
	// equalValues comparison that exists for exactly that case was never
	// reached after the first one.
	//
	// Only for objects with no version of their own, so a projection that maps
	// one copies nothing.
	for i, event := range events {
		obj, ok := event.Object.(*unstructured.Unstructured)
		if !ok || obj.GetResourceVersion() != "" {
			continue
		}
		stamped := obj.DeepCopy()
		stamped.SetResourceVersion(version)
		events[i].Object = stamped
	}

	c.record(previous, version, events)

	for _, event := range events {
		crispmetrics.WatchEvents.WithLabelValues(c.resource, string(event.Type)).Inc()
	}

	// Snapshotted here, at the same moment c.items took the new state. A
	// watcher that registers after this point replayed that state already and
	// must not also be sent it as an event; one that registered before is in
	// this list and gets it exactly once.
	targets := make([]*cacheWatcher, 0, len(c.watchers))
	for _, w := range c.watchers {
		targets = append(targets, w)
	}
	return events, targets
}

// emitBookmarksLocked tells watchers the current version when nothing has
// changed for a while. A client that reconnects then resumes from here instead
// of replaying everything it already has.
func (c *watchCache) emitBookmarksLocked() {
	if c.bookmarkInterval <= 0 || len(c.watchers) == 0 {
		return
	}
	if !c.lastBookmark.IsZero() && time.Since(c.lastBookmark) < c.bookmarkInterval {
		return
	}
	c.lastBookmark = time.Now()

	version := c.currentVersionLocked()
	for id, w := range c.watchers {
		if !w.bookmarks {
			continue
		}

		bookmark := &unstructured.Unstructured{Object: map[string]any{}}
		bookmark.SetGroupVersionKind(w.gvk)
		bookmark.SetResourceVersion(version)

		if !w.enqueue(watch.Event{Type: watch.Bookmark, Object: bookmark}) {
			w.terminate()
			delete(c.watchers, id)
		}
	}
	crispmetrics.Watchers.WithLabelValues(c.resource).Set(float64(len(c.watchers)))
}

// broadcast delivers events to the watchers that want them.
//
// Each watcher is handed its own copy: an event's object is shared with the
// cache and with every other watcher, while the response path downstream is
// free to stamp and transform what it is given. This runs without mu, so the
// copying does not block anything reading the cache.
//
// The copy is linear in watchers and is the largest cost a poll has — a hundred
// watchers seeing a hundred changed objects is around 25ms and 24MB. It is kept
// anyway. The projected serializer was measured and does not mutate what it
// encodes, in either direction, so the encoder alone would not need it; but the
// encoder is not the only thing downstream of ResultChan, and the rest of the
// watch path — the WatchList transformer, table conversion, the event wrapper —
// has not been shown the same way. Sharing one object between watchers is only
// safe if every one of them is read-only, and being wrong is one client seeing
// another's object change under it, with nothing raised anywhere. That is not a
// trade worth 25ms, so the copy stays until the whole path has been established
// rather than sampled.
func (c *watchCache) broadcast(events []watch.Event, targets []*cacheWatcher) {
	var behind []*cacheWatcher

	for _, event := range events {
		for _, w := range targets {
			if !w.matches(event.Object) {
				continue
			}
			if !w.enqueue(watch.Event{Type: event.Type, Object: event.Object.DeepCopyObject()}) {
				// The watcher is too far behind; drop it so the client relists.
				klog.V(2).InfoS("disconnecting a watcher that fell behind")
				w.terminate()
				behind = append(behind, w)
			}
		}
	}

	if len(behind) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range behind {
		delete(c.watchers, w.id)
	}
	crispmetrics.Watchers.WithLabelValues(c.resource).Set(float64(len(c.watchers)))
}

// keyedItem pairs an entry with the key it is stored under, so an ordering can
// be decided without rebuilding that key from the object.
type keyedItem struct {
	key  string
	item *unstructured.Unstructured
}

// sortedItemsLocked returns the cache in key order.
//
// The keys come out of the map rather than being recomputed from each object.
// cacheKey is two lookups into an unstructured object and a concatenation, and
// calling it inside the comparator ran that O(n log n) times — on the path that
// replays a whole collection to every arriving watcher, which is the one place
// a projection hands over everything it holds at once.
func (c *watchCache) sortedItemsLocked() []*unstructured.Unstructured {
	keyed := make([]keyedItem, 0, len(c.items))
	for key, item := range c.items {
		keyed = append(keyed, keyedItem{key: key, item: item})
	}
	slices.SortFunc(keyed, func(a, b keyedItem) int { return strings.Compare(a.key, b.key) })

	items := make([]*unstructured.Unstructured, 0, len(keyed))
	for _, entry := range keyed {
		items = append(items, entry.item)
	}
	return items
}

// movesForward reports whether version is strictly newer than watermark.
// Versions are compared as numbers when both are numeric, since a column like
// a microsecond timestamp orders numerically but not lexically. This only feeds
// a diagnostic counter, so an unfamiliar format degrades to a string compare
// rather than affecting what is served.
func movesForward(watermark, version string) bool {
	if version == "" {
		return false
	}
	if watermark == "" {
		return true
	}

	previous, previousErr := strconv.ParseInt(watermark, 10, 64)
	current, currentErr := strconv.ParseInt(version, 10, 64)
	if previousErr == nil && currentErr == nil {
		return current > previous
	}
	return version > watermark
}

// recordedEvent is one change, the version the cache was at before it, and the
// version it reached by applying it. Both ends are needed to answer whether a
// client sitting at some version can still be caught up.
type recordedEvent struct {
	from    string
	version string
	event   watch.Event
}

// record appends to the history ring, dropping the oldest when it is full.
func (c *watchCache) record(from, version string, events []watch.Event) {
	if c.historySize <= 0 {
		return
	}

	for _, event := range events {
		c.history = append(c.history, recordedEvent{from: from, version: version, event: event})
	}
	if overflow := len(c.history) - c.historySize; overflow > 0 {
		c.history = append(c.history[:0], c.history[overflow:]...)
	}
}

// replayable reports the events recorded after a version, and whether that
// version is still within the history at all.
//
// A version at or after the newest recorded change is resumable with nothing to
// replay. A version older than the oldest recorded change is not resumable:
// something happened that the cache no longer remembers, and pretending
// otherwise would leave the client silently missing it.
func (c *watchCache) replayable(version string) ([]recordedEvent, bool) {
	current := c.currentVersionLocked()
	if version == current {
		return nil, true
	}

	// A version the cache has never reached refers to a state it cannot
	// describe. Relisting is the only way for such a client to be correct.
	if movesForward(current, version) {
		return nil, false
	}
	if len(c.history) == 0 {
		return nil, false
	}

	// Older than the ring's earliest starting point: something happened that
	// is no longer remembered.
	if movesForward(version, c.history[0].from) {
		return nil, false
	}

	for i, recorded := range c.history {
		if movesForward(version, recorded.version) {
			// A copy, not a window onto the ring. The caller reads this after
			// releasing the lock, and record compacts the ring in place — so a
			// poll landing in between overwrote the very events the caller was
			// still holding. The client was handed events it had already seen
			// and silently lost the one it was resuming from, with no 410 and
			// nothing logged.
			return slices.Clone(c.history[i:]), true
		}
	}
	return nil, true
}

func cacheKey(obj *unstructured.Unstructured) string {
	return obj.GetNamespace() + "/" + obj.GetName()
}

// cacheIdentity is what a deleted row leaves behind: enough to find the object
// in the cache, and nothing more.
type cacheIdentity struct {
	namespace string
	name      string

	// object is the row as the tombstone describes it, when the tombstone
	// carries enough columns to map one.
	//
	// A tombstone that records only the identity leaves the cache as the only
	// place a deleted object exists, which is what kept whole objects in the
	// cache and made a replayed deletion carry a bare name. One that records
	// the mapped columns describes the row itself, so the deletion can be
	// answered from the table rather than from memory.
	object *unstructured.Unstructured
}

func (i cacheIdentity) key() string { return i.namespace + "/" + i.name }

// changed reports whether an object differs from its previous snapshot. The
// mapped resourceVersion is used when the projection provides one, since
// comparing a single column is far cheaper than comparing whole objects.
func changed(previous, next *unstructured.Unstructured) bool {
	if rv := next.GetResourceVersion(); rv != "" || previous.GetResourceVersion() != "" {
		return previous.GetResourceVersion() != rv
	}
	return !equalValues(previous.Object, next.Object)
}

// equalValues compares two decoded JSON values.
//
// reflect.DeepEqual answers the same question, and answers it through reflection
// at every node of every object. This runs once per object on every full resync
// of a projection that maps no resourceVersion — which is exactly the projection
// with nothing cheaper to compare, so it is the case worth not doing slowly.
//
// An unstructured object holds only what the JSON converter produces, so those
// types are compared directly and anything unexpected falls back to reflection
// rather than being declared unequal.
func equalValues(a, b any) bool {
	switch left := a.(type) {
	case nil:
		return b == nil
	case string:
		right, ok := b.(string)
		return ok && left == right
	case bool:
		right, ok := b.(bool)
		return ok && left == right
	case int64:
		right, ok := b.(int64)
		return ok && left == right
	case float64:
		right, ok := b.(float64)
		return ok && left == right

	case map[string]any:
		right, ok := b.(map[string]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for key, value := range left {
			// The lengths match, so a key missing on the right means a key on
			// the right that is missing on the left: still a difference.
			other, present := right[key]
			if !present || !equalValues(value, other) {
				return false
			}
		}
		return true

	case []any:
		right, ok := b.([]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for i := range left {
			if !equalValues(left[i], right[i]) {
				return false
			}
		}
		return true

	default:
		return reflect.DeepEqual(a, b)
	}
}

// cacheWatcher is one client's view of the event stream.
//
// Events are queued rather than written straight to the connection, and a
// goroutine drains the queue. Writing directly would mean either blocking the
// cache on the slowest client, or dropping events when a buffer filled — and a
// dropped event is invisible to the client, which is the worst of the three.
type cacheWatcher struct {
	id        int64
	cache     *watchCache
	namespace string
	selector  labels.Selector
	fields    fields.Selector

	// bookmarks records whether this client accepts bookmark events, and gvk
	// is what they are stamped with.
	bookmarks bool
	gvk       schema.GroupVersionKind

	// matchFields is the projection's field matcher, since which fields are
	// selectable is a property of the projection.
	matchFields func(*unstructured.Unstructured, fields.Selector) bool

	mu      sync.Mutex
	cond    *sync.Cond
	pending []watch.Event
	closed  bool

	result chan watch.Event
	done   chan struct{}
}

// newCacheWatcher returns a watcher and starts draining its queue.
func newCacheWatcher(id int64, cache *watchCache) *cacheWatcher {
	w := &cacheWatcher{
		id:     id,
		cache:  cache,
		result: make(chan watch.Event, eventBuffer),
		done:   make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	go w.run()
	return w
}

// run delivers queued events in order until the watcher is closed.
func (w *cacheWatcher) run() {
	defer close(w.result)

	for {
		w.mu.Lock()
		for len(w.pending) == 0 && !w.closed {
			w.cond.Wait()
		}
		if w.closed {
			w.mu.Unlock()
			return
		}
		event := w.pending[0]
		// Cleared before the reslice so a delivered event's object is not kept
		// alive by the part of the array the queue has moved past, and the
		// array itself is released once the queue drains. An initial replay can
		// queue tens of thousands of objects, and reslicing alone would pin
		// that allocation for as long as the watcher lives.
		w.pending[0] = watch.Event{}
		w.pending = w.pending[1:]
		if len(w.pending) == 0 {
			w.pending = nil
		}
		w.countPendingLocked(-1)
		w.mu.Unlock()

		select {
		case w.result <- event:
		case <-w.done:
			return
		}
	}
}

// ResultChan implements watch.Interface.
func (w *cacheWatcher) ResultChan() <-chan watch.Event { return w.result }

// Stop implements watch.Interface.
func (w *cacheWatcher) Stop() {
	w.cache.stop(w.id)
	w.terminate()
}

// terminate closes the watcher. The draining goroutine closes the channel, so
// this is safe to call more than once and from either side.
func (w *cacheWatcher) terminate() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}
	w.closed = true

	// Whatever this watcher was still holding stops counting against the
	// projection's backlog, or a client that disconnects mid-replay would leave
	// its share reserved forever.
	if len(w.pending) > 0 {
		w.countPendingLocked(-len(w.pending))
		w.pending = nil
	}

	close(w.done)
	w.cond.Signal()
}

// enqueue queues an event, reporting false when the watcher is closed or has
// fallen too far behind to be caught up.
func (w *cacheWatcher) enqueue(event watch.Event) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return false
	}
	if len(w.pending) >= maxPendingEvents {
		return false
	}
	// And the projection's whole backlog, not only this watcher's: the
	// per-watcher bound says nothing about how many watchers there are.
	if w.cache != nil && w.cache.pending.Load() >= maxCachePendingEvents {
		return false
	}

	w.pending = append(w.pending, event)
	w.countPendingLocked(1)
	w.cond.Signal()
	return true
}

// prepend puts a set of events in front of whatever is already queued, and
// reports false when there is no room for them.
//
// This is how an initial replay is delivered. The watcher is registered before
// the replay is built, so a poll that lands in between has already queued its
// events here — and those describe changes after the snapshot was taken, so the
// snapshot belongs in front of them. The client then sees the collection and
// then what happened to it, in that order.
func (w *cacheWatcher) prepend(events []watch.Event) bool {
	if len(events) == 0 {
		return true
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return false
	}
	if len(w.pending)+len(events) > maxPendingEvents {
		return false
	}
	if w.cache != nil && w.cache.pending.Load()+int64(len(events)) > maxCachePendingEvents {
		return false
	}

	w.pending = append(events, w.pending...)
	w.countPendingLocked(len(events))
	w.cond.Signal()
	return true
}

// countPendingLocked keeps the cache's total in step with this watcher's queue.
func (w *cacheWatcher) countPendingLocked(delta int) {
	if w.cache != nil {
		w.cache.pending.Add(int64(delta))
	}
}

// matches reports whether an event is relevant to this watcher.
func (w *cacheWatcher) matches(object any) bool {
	obj, ok := object.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	if w.namespace != "" && obj.GetNamespace() != w.namespace {
		return false
	}
	if w.selector != nil && !w.selector.Empty() && !w.selector.Matches(labels.Set(obj.GetLabels())) {
		return false
	}
	if w.matchFields == nil {
		return true
	}
	return w.matchFields(obj, w.fields)
}

// Watch streams changes to a projected resource.
//
// Events come from polling the list query, so a projection is watchable
// without any change-data-capture support in the database, at the cost of
// events lagging by up to the poll interval.
func (r *REST) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	if r.watch == nil {
		return nil, apierrors.NewMethodNotSupported(r.groupResource(), "watch")
	}

	namespace := namespaceFrom(ctx, r.NamespaceScoped())

	var (
		selector          labels.Selector
		fieldSelector     fields.Selector
		resourceVersion   string
		sendInitialEvents bool
		allowBookmarks    bool
	)
	if options != nil {
		selector = options.LabelSelector
		fieldSelector = options.FieldSelector
		resourceVersion = options.ResourceVersion
		sendInitialEvents = options.SendInitialEvents != nil && *options.SendInitialEvents
		allowBookmarks = options.AllowWatchBookmarks

		if err := r.validateFieldSelector(fieldSelector); err != nil {
			return nil, err
		}
	}

	w, err := r.watch.Watch(ctx, namespace, selector, fieldSelector, resourceVersion, sendInitialEvents, allowBookmarks, r.gvk)
	if err != nil {
		// Anything that already carries a status is already an answer, and a
		// better one than this could invent. The poll behind a watch goes
		// through queryError, which has classified an unreachable database as
		// 503 and a shed request as 429 — both with the Retry-After a client
		// backs off on. Wrapping those in a 500 threw that away, so during an
		// outage a LIST correctly said "retry in a second" while a WATCH said
		// "internal error" and client-go retried it flat out.
		if isStatusError(err) {
			return nil, err
		}
		return nil, apierrors.NewInternalError(fmt.Errorf("starting watch on %s: %w", r.resource.Plural, err))
	}
	return w, nil
}

// minDatabaseReplay is how many events are always worth replaying, however
// small the collection. Below this the comparison with the collection size is
// noise: re-reading a handful of rows is not cheaper than being handed them.
const minDatabaseReplay = 100

// canReplayFromMemory reports whether the history ring covers a version.
//
// Advisory, and taken without committing to anything: the authoritative check
// happens again under the lock that registers the watcher. This one only
// decides whether it is worth asking the database, which cannot be done while
// holding the cache lock.
func (c *watchCache) canReplayFromMemory(version string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.replayable(version)
	return ok
}

// replayFromDatabase answers a watcher resuming from beyond the in-memory
// window by reading what changed out of the database.
//
// The history ring is small, lives in memory, dies with the process, and is
// different on every replica — so a client that reconnects to a restarted
// server, or to a different one, is told 410 and relists the whole collection.
// For a projection that records versions and keeps tombstones, the database
// already holds everything needed to answer properly: the incremental query
// reads forward from a version, and the deletion query says what went away.
//
// Both are required. Without the deletion query a replay would report every
// change and no removal, so a client would keep objects that are gone —
// silently, and for as long as it stayed connected. Refusing with 410 is worse
// service and better behaviour, so that is what a projection without tombstones
// still gets.
func (c *watchCache) replayFromDatabase(
	ctx context.Context,
	since string,
	gvk schema.GroupVersionKind,
) ([]recordedEvent, bool) {
	if c.incremental == nil || c.deleted == nil || since == "" {
		return nil, false
	}

	changed, err := c.incremental(ctx, since)
	if err != nil {
		klog.V(2).InfoS("could not read watch history from the database; the client will be asked to relist",
			"resource", c.resource, "since", since, "err", err)
		return nil, false
	}
	removed, err := c.deleted(ctx, since)
	if err != nil {
		klog.V(2).InfoS("could not read deletions from the database; the client will be asked to relist",
			"resource", c.resource, "since", since, "err", err)
		return nil, false
	}

	// Past a point, replaying costs more than starting over. The collection is
	// the natural bound: a replay bigger than the thing being replayed into is
	// one the client should have relisted instead.
	//
	// With a floor under it, because the ratio alone gives the wrong answer for
	// small collections — three events against a two-row table is not a reason
	// to make a client re-read the table. The floor is what a relist would have
	// to beat before the comparison means anything.
	c.mu.Lock()
	collection := len(c.items)
	c.mu.Unlock()
	if len(changed)+len(removed) > max(collection, minDatabaseReplay) {
		klog.V(2).InfoS("a database replay would be larger than the collection; asking the client to relist",
			"resource", c.resource, "changes", len(changed)+len(removed), "objects", collection)
		return nil, false
	}

	events := make([]recordedEvent, 0, len(changed)+len(removed))

	// Modified rather than Added, because that is what the database actually
	// said: the row changed at or after this version. Whether the client had
	// already seen it is not something a row can answer.
	//
	// This is the right event to send anyway. client-go turns an update for an
	// object its store does not hold into an add, so an informer resuming this
	// way ends up in the same state either way; a client reading raw events
	// sees a modification for something it did not know about, which is the
	// honest description of what the database told us.
	for i := range changed {
		item := changed[i]
		events = append(events, recordedEvent{
			from:    since,
			version: item.GetResourceVersion(),
			event:   watch.Event{Type: watch.Modified, Object: &item},
		})
	}

	// A deleted row has nothing left to describe it, and the tombstone carries
	// only the identity — so the event carries the identity. That is what a
	// client needs to drop it, and it is all that exists.
	for _, identity := range removed {
		events = append(events, recordedEvent{
			from:    since,
			version: since,
			event:   watch.Event{Type: watch.Deleted, Object: c.tombstone(identity, gvk)},
		})
	}

	crispmetrics.WatchDatabaseReplays.WithLabelValues(c.resource).Inc()
	klog.V(2).InfoS("replayed watch history from the database",
		"resource", c.resource, "since", since, "changes", len(changed), "deletions", len(removed))

	return events, true
}

// tombstone builds the object a Deleted event carries for a row the cache no
// longer holds.
//
// The cached object when there is one, since it describes what was actually
// removed. Otherwise identity alone: the row is gone from the table and the
// tombstone records only which row it was.
func (c *watchCache) tombstone(
	identity cacheIdentity,
	gvk schema.GroupVersionKind,
) *unstructured.Unstructured {
	// The tombstone's own row first when the cache keeps only keys and
	// versions, since what it holds cannot describe anything.
	if c.lightweight && identity.object != nil {
		return identity.object
	}

	// Otherwise the cached object: the row as this server last saw it, which is
	// what a Deleted event is defined to carry.
	c.mu.Lock()
	cached, ok := c.items[identity.key()]
	c.mu.Unlock()
	if ok {
		return cached
	}

	// Then the tombstone's own columns, for a replay the cache never held —
	// resuming against a restarted server, or a replica this client has not
	// spoken to. This is the case that used to produce a bare name.
	if identity.object != nil {
		return identity.object
	}

	// Identity alone, which is all a tombstone recording only the identity
	// columns can give. The kind is not optional even here: an event carrying
	// an object without one cannot be encoded, and the watch response ends
	// mid-stream with no Error event and nothing logged on either side.
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(identity.name)
	if identity.namespace != "" {
		obj.SetNamespace(identity.namespace)
	}
	return obj
}

// retain is what the cache keeps for a row.
//
// The whole object normally, because a Deleted event has to carry the row as
// this server last saw it and the cache is the only place that exists once the
// row is gone. A lightweight cache keeps the identity and the version instead:
// the diff needs the version and nothing else, and the deletion is described by
// a tombstone that carries the mapped columns.
//
// The saving is the point. A watched projection otherwise holds its whole
// collection in memory and needs maxRows above the row count, which is the
// ceiling on how large a table can be watched at all.
func (c *watchCache) retain(item *unstructured.Unstructured) *unstructured.Unstructured {
	if !c.lightweight {
		return item
	}

	// A row with no version of its own is kept whole.
	//
	// The diff compares versions when there are versions and falls back to
	// comparing the objects when there are not. Trimming a row that has no
	// version leaves that fallback comparing a trimmed entry against a full
	// row, which never matches — so an unchanged row is reported as modified on
	// every poll, for as long as the projection is watched.
	//
	// The column is mapped or this cache would not be lightweight, so this is a
	// NULL in a particular row rather than the ordinary case, and the saving
	// still applies to every row that has one.
	if item.GetResourceVersion() == "" {
		return item
	}

	trimmed := &unstructured.Unstructured{Object: map[string]any{}}

	// The kind travels with the object. A watch event carrying one without it
	// cannot be serialised, and the stream dies mid-response with nothing said
	// on either side — which is how this was found.
	trimmed.SetGroupVersionKind(item.GroupVersionKind())
	trimmed.SetName(item.GetName())
	trimmed.SetNamespace(item.GetNamespace())
	trimmed.SetResourceVersion(item.GetResourceVersion())

	// Labels, because a watcher with a label selector filters deletions on
	// them. Dropping them would have such a watcher silently miss removals it
	// asked to see.
	if labels := item.GetLabels(); len(labels) > 0 {
		trimmed.SetLabels(labels)
	}

	return trimmed
}
