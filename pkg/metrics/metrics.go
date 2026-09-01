// Package metrics defines the Prometheus metrics kube-crisp publishes.
//
// They are registered with the legacy registry that the generic apiserver
// already serves on /metrics, so a projection's query behaviour shows up next
// to the standard apiserver request metrics rather than on a second endpoint.
package metrics

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// Result values for the result label.
//
// Split by what the client was told, because the answers point at different
// people. A timeout and an unreachable database are the database's; a statement
// the database rejected is the projection author's; a shed request is this
// server's own concurrency limit. Collapsed into one "error" they were
// indistinguishable, so the only signal an operator had was a rate that had
// gone up — with no way to tell whether to look at the database, the projection,
// or the load.
//
// not_found stays separate from all of them: a projection serving ordinary 404s
// is a healthy projection.
const (
	ResultSuccess  = "success"
	ResultNotFound = "not_found"

	// ResultTimeout is a query that outran its own timeout.
	ResultTimeout = "timeout"
	// ResultUnavailable is a database that could not be reached at all.
	ResultUnavailable = "unavailable"
	// ResultShed is a request refused at maxConcurrentQueries before it ran.
	ResultShed = "shed"
	// ResultInvalid is a request the projection or its schema rejected.
	ResultInvalid = "invalid"
	// ResultConflict is a write that lost to a concurrent one.
	ResultConflict = "conflict"
	// ResultContended is a read the database rolled back rather than serialise
	// against a concurrent transaction. Separate from unavailable, which is a
	// database that could not be reached: this one answered, and answered that
	// it was too busy to be consistent. Conflating them would page somebody
	// about an outage every time a table got hot.
	ResultContended = "contended"

	// ResultError is everything else, which in practice is a statement the
	// database refused — the projection's own SQL being wrong.
	ResultError = "error"
)

// Connection pool states, mirroring sql.DBStats.
const (
	ConnectionsOpen   = "open"
	ConnectionsInUse  = "in_use"
	ConnectionsIdle   = "idle"
	ConnectionsWaited = "wait_count"
)

// Outcomes of one admission review.
const (
	// AdmissionAllowed is a projection the webhook had no objection to.
	AdmissionAllowed = "allowed"
	// AdmissionDenied is one it refused, which is the webhook working.
	AdmissionDenied = "denied"
	// AdmissionError is a request it could not answer at all — malformed, or
	// not an admission review. Distinct from denied, because a request the
	// webhook could not read says nothing about the projection in it.
	AdmissionError = "error"
)

// Reasons a pooled connection was closed rather than returned to the pool.
const (
	ClosedMaxIdle     = "max_idle"
	ClosedMaxIdleTime = "max_idle_time"
	ClosedMaxLifetime = "max_lifetime"
)

// Reasons a watch poll did not complete.
const (
	// PollShed is a poll rejected at the projection's own concurrency limit.
	// It is worth separating: the fix is a higher maxConcurrentQueries or less
	// load, not a database investigation.
	PollShed = "shed"
	// PollFailed is anything else — most often a database that is unreachable.
	PollFailed = "failed"
)

// Which database answered a read.
const (
	// RolePrimary is the data source a projection writes to.
	RolePrimary = "primary"
	// RoleReplica is the read replica, when a projection names one.
	RoleReplica = "replica"
)

// Why a read cache entry went away. The distinction is the whole diagnosis: an
// entry that expired did its job, one dropped under pressure means the cache is
// too small for the key space, and one dropped by a write means the projection
// is written to faster than the TTL it was given.
const (
	CacheEvictionExpired     = "expired"
	CacheEvictionFull        = "full"
	CacheEvictionInvalidated = "invalidated"
)

// Projection states.
const (
	ProjectionServing = "serving"
	ProjectionFailed  = "failed"
	// ProjectionStale is a projection that failed to compile but is still
	// serving what it compiled to last time. It is neither of the other two:
	// requests are answered, and the spec answering them is not the spec in
	// the cluster.
	ProjectionStale = "stale"
	// ProjectionUnrouted is a projection that compiled and installed here but
	// that the aggregation layer is not sending requests to. It looks healthy
	// from inside this process and answers nothing from outside it, which is
	// why it needs a count of its own rather than being folded into failed.
	ProjectionUnrouted = "unrouted"
)

var (
	// QueryDuration measures the database round trip, not the API request:
	// comparing the two shows how much of a request kube-crisp itself costs.
	QueryDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "duration_seconds",
			Help:           "Time spent executing SQL and mapping the result, by projection and verb.",
			Buckets:        metrics.ExponentialBuckets(0.0005, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource", "verb", "result"},
	)

	// QueryRows records how many rows a read produced, which is what usually
	// explains a slow list.
	QueryRows = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "rows",
			Help:           "Rows returned by a projection's query.",
			Buckets:        []float64{1, 10, 100, 500, 1000, 5000, 10000, 50000},
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource", "verb"},
	)

	// QueriesShed counts requests rejected because a projection was already
	// running as many queries as it is allowed to.
	QueriesShed = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "shed_total",
			Help:           "Requests rejected because the projection was at its concurrency limit.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// QueriesCoalesced counts reads answered by a query another request had
	// already started. It is how much duplicate load concurrency was creating.
	QueriesCoalesced = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "coalesced_total",
			Help:           "Reads that joined a query already in flight instead of issuing their own.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// RowsUnmappable counts rows a projection could not turn into an object —
	// a name that is not a valid object name, a NULL where the mapping needs a
	// value, a column the query did not return.
	//
	// They are skipped rather than failing the whole collection, so this is the
	// only place a shrinking result set shows up as a number rather than as a
	// client quietly seeing fewer objects than the table holds.
	RowsUnmappable = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "rows_unmappable_total",
			Help:           "Rows skipped because they could not be mapped onto an object.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// RowsOutOfNamespace counts rows a namespaced read was not served because
	// the query returned them from a different namespace.
	//
	// The filter belongs in the query, and a projection whose query has it
	// never increments this. One that does not is handing every caller rows
	// from namespaces they were never granted, which is exactly what
	// mapping.namespace exists to prevent — so this is not a tuning signal, it
	// is a projection to go and fix.
	RowsOutOfNamespace = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "rows_out_of_namespace_total",
			Help:           "Rows withheld from a namespaced read because the query returned them from another namespace.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// QueriesRouted counts reads by the database that answered them.
	//
	// A projection with a read replica is trading freshness for load, and this
	// is what says whether the trade is happening: reads on the primary are
	// the ones that could not go to the replica — a write's precondition — plus
	// anything that ran before the replica was configured.
	QueriesRouted = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "routed_total",
			Help:           "Reads by the data source that answered them.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource", "role"},
	)

	// ReplicaFallbacks counts reads that went to the primary because the read
	// replica could not be reached.
	//
	// Separate from QueriesRouted rather than a third value of its role label,
	// so that label keeps counting each read once, against the data source it
	// was routed to. Anything above zero means the replica is not taking the
	// load it was configured to take, which is otherwise only visible as
	// role="replica" quietly falling to nothing.
	ReplicaFallbacks = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "query",
			Name:           "replica_fallback_total",
			Help:           "Reads answered by the primary because the read replica was unreachable.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// PollLeader reports whether this replica holds the lease that decides who
	// polls at the configured interval.
	//
	// With several replicas exactly one should be 1. Two would mean a split
	// brain and twice the poll load; none for longer than a lease duration
	// means every replica is on the follower interval and every watch is
	// lagging, which is otherwise indistinguishable from a quiet database.
	PollLeader = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "poll",
			Name:           "leader",
			Help:           "1 when this replica holds the polling lease, 0 when it does not.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"lease"},
	)

	// CacheReads counts read-cache lookups, which is how a projection's
	// cacheTTL is judged: a low hit rate is just added staleness.
	CacheReads = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "cache",
			Name:           "reads_total",
			Help:           "Read cache lookups, by projected resource and result.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "result"},
	)

	// CacheEntries reports how many results a projection's read cache holds.
	//
	// The cache is bounded, so this sitting at the bound is the difference
	// between a cache that is working and one that is evicting entries a
	// request was about to ask for.
	CacheEntries = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "cache",
			Name:           "entries",
			Help:           "Results currently held in a projection's read cache.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// CacheEvictions counts entries dropped before they were read again.
	//
	// A hit rate says whether the cache is paying for itself; this says why when
	// it is not. Entries expiring is the cache working as configured. Entries
	// dropped because it was full means the key space is larger than the cache —
	// a client paging through a large collection is the usual cause, since every
	// continue token is a key of its own. Entries dropped by a write mean the
	// projection changes faster than its cacheTTL, and the TTL is buying
	// staleness without buying reads.
	CacheEvictions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "cache",
			Name:           "evictions_total",
			Help:           "Read cache entries dropped, by projected resource and reason.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "reason"},
	)

	// Projections reports how many projections are being served and how many
	// were rejected, so a bad projection is visible without reading logs.
	Projections = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Name:           "projections",
			Help:           "Number of CustomResourceProjections by state.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"state"},
	)

	// Watchers counts connected watch clients. Polling only runs while this is
	// above zero, so it also explains query load that has no request behind it.
	Watchers = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "watchers",
			Help:           "Currently connected watchers, by projected resource.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// WatchEvents counts the events polling produced.
	WatchEvents = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "events_total",
			Help:           "Watch events emitted, by projected resource and event type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "type"},
	)

	// WatchPolls counts polls by mode, which is what shows whether a
	// projection is really polling incrementally or falling back to full
	// reads.
	WatchPolls = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "polls_total",
			Help:           "Watch polls, by projected resource and mode (full or incremental).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "mode"},
	)

	// WatchMissedEvents counts changes a full resync found that an incremental
	// poll should already have seen. Anything above zero means the mapped
	// resourceVersion column is not moving forward on every write, so watchers
	// are relying on the resync to notice changes.
	WatchMissedEvents = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "missed_events_total",
			Help:           "Changes found only by a full resync, which indicates a resourceVersion that is not monotonic.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// WatchNotifications counts change notifications the database pushed.
	//
	// It is what says whether a projection configured for them is actually
	// getting them: a subscription that silently stopped delivering looks
	// exactly like a table where nothing is changing, and the poll timer
	// underneath means requests keep working while watches quietly slow to the
	// interval they were meant to have escaped.
	WatchNotifications = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "notifications_total",
			Help:           "Change notifications received from the database, by projected resource.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// ProjectionsUnguardedUpdate marks projections whose update statement
	// cannot enforce the resourceVersion a client sent with it.
	//
	// The check happens twice: this server compares the version it read against
	// the one the client asserted, and the statement can repeat that comparison
	// in SQL by binding :resourceVersion. Only the second one is atomic. Without
	// it there is a window between the read and the write in which another
	// writer commits, and because a projection's update statement rewrites every
	// mapped column from the copy it read, the later write silently reverts the
	// earlier one — both having been answered 200.
	//
	// Measured against the e2e fixture: six of six rounds of two concurrent
	// patches lost a write, one round losing both.
	ProjectionsUnguardedUpdate = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "projections",
			Name:           "unguarded_update",
			Help:           "1 for a projection whose update statement does not bind :resourceVersion, so concurrent writes can be lost.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// ProjectionsUnversioned marks projections that map no resourceVersion
	// while this server has peers.
	//
	// A gauge rather than a log line alone, because the failure it describes is
	// silent and intermittent: two replicas hand the same client versions that
	// mean different things, so a watch resumed against the other one either
	// replays what the client already has or skips what it does not. It looks
	// like a client bug from every angle except this one.
	ProjectionsUnversioned = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "projections",
			Name:           "unversioned",
			Help:           "1 for a projection that maps no resourceVersion on a server running with peers.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// ProjectionsCacheUnshared marks projections that cache reads while this
	// server has peers.
	//
	// spec.cacheTTL invalidates the entries a write could have changed, in the
	// replica that served the write. The cache is in process and nothing
	// connects the replicas, so a read routed elsewhere can be answered from an
	// entry that predates the write for as long as the TTL. The client cannot
	// tell which replica it reached, and the read is not wrong in any way it
	// could detect — it is old, which looks exactly like not having written yet.
	//
	// The same shape as ProjectionsUnversioned and reported the same way: a
	// hazard that only exists with peers, silent when it bites, and a decision
	// rather than a fault, since a single replica has none of this.
	ProjectionsCacheUnshared = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "projections",
			Name:           "cache_unshared",
			Help:           "1 for a projection caching reads on a server running with peers.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource"},
	)

	// WatchDatabaseReplays counts watchers answered from the database rather
	// than from the in-memory history ring.
	//
	// The alternative for those clients is 410 and a full relist, so a rate
	// here is work that used to be a stampede: every reconnecting informer
	// reading the whole collection at once, which is what a rolling restart
	// used to produce.
	WatchDatabaseReplays = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "database_replays_total",
			Help:           "Watchers resumed by reading history from the database instead of being asked to relist.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// WatchListenerConnected is 1 while a notification subscription is holding a
	// connection and listening on it, and 0 while it is not.
	//
	// This is what makes the gap named above observable. WatchNotifications is
	// a counter, so a subscription that died and cannot reconnect leaves it
	// flat — which is indistinguishable from a table nobody is writing to.
	// Requests keep working either way, because the poll timer is still
	// running underneath, so nothing else reports it: the projection simply
	// goes back to the latency it was configured to escape.
	//
	// Alert on this rather than on the counter. "No notifications for ten
	// minutes" is a normal Sunday; "the listener has been down for ten minutes"
	// never is.
	WatchListenerConnected = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "listener_connected",
			Help:           "1 while a change-notification subscription is connected, by data source and channel.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"datasource", "channel"},
	)

	// WatchListenerReconnects counts how often a subscription had to be
	// re-established.
	//
	// A database restart shows up here as one reconnect and is unremarkable. A
	// number that keeps climbing is a subscription that cannot stay up, which
	// is worth knowing even though the gauge is 1 whenever anybody looks.
	WatchListenerReconnects = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "listener_reconnects_total",
			Help:           "Change-notification subscriptions re-established after dropping, by data source and channel.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"datasource", "channel"},
	)

	// WatchPollErrors counts polls that did not complete. A watch that stops
	// advancing looks exactly like a projection where nothing is changing, so
	// without this a database that has gone away — or a projection shedding its
	// own polls at maxConcurrentQueries — is invisible to everything except the
	// log.
	WatchPollErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "poll_errors_total",
			Help:           "Watch polls that failed, by reason.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "reason"},
	)

	// WatchPollDuration measures one poll: the list query plus the diff against
	// the previous snapshot.
	WatchPollDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "watch",
			Name:           "poll_duration_seconds",
			Help:           "Time to re-list a watched projection and diff it against the previous snapshot.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 14),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource"},
	)

	// DataSourceConnections mirrors sql.DBStats so pool exhaustion is visible
	// before it shows up as request latency.
	DataSourceConnections = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "datasource",
			Name:           "connections",
			Help:           "Connection pool state, by data source.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"datasource", "state"},
	)

	// AdmissionReviews counts admission requests the projection webhook
	// answered, by outcome.
	//
	// Nothing measured this path, and it is one that can fail silently: the
	// webhook's policy is Ignore, so a configuration the kube-apiserver cannot
	// call is a webhook that is skipped rather than one that errors. This
	// series going flat at zero is what that looks like, and it is the only
	// thing that would have shown it.
	AdmissionReviews = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "admission",
			Name:           "reviews_total",
			Help:           "Admission reviews answered by the projection webhook, by result.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// AdmissionDuration is how long answering one took.
	//
	// The check reaches the database to ask whether it could run the
	// projection's statements, so this is a database round trip inside an
	// admission request — and an admission request the cluster gives ten
	// seconds before it gives up on.
	AdmissionDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "admission",
			Name:           "duration_seconds",
			Help:           "Time spent answering an admission review.",
			Buckets:        metrics.ExponentialBuckets(0.005, 2, 10),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// ProjectionState reports the state of each projection by name.
	//
	// Projections counts them by state, which answers "is anything wrong" and
	// not "what". An alert on it can say one projection failed and cannot say
	// which, so the next step is always to go and look — and the controller
	// knew the name all along, since it keeps the same list for the
	// projections-degraded health check.
	//
	// One series per projection per state, with exactly one of them at 1.
	ProjectionState = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "",
			Name:           "projection_state",
			Help:           "Current state of each projection: 1 for the state it is in, 0 for the others.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"projection", "resource", "state"},
	)

	// DataSourceConnectionsClosed counts connections the pool discarded instead
	// of keeping, by reason.
	//
	// max_idle climbing with request volume is the signature of a pool whose
	// MaxIdleConns is below the concurrency it actually serves: every query
	// past the idle limit hands its connection back to be closed, and the next
	// one dials again. The other pool gauges cannot show this — open, in_use
	// and idle all look reasonable throughout — but a TLS handshake per query
	// is most of the cost of a fast query.
	DataSourceConnectionsClosed = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "datasource",
			Name:           "connections_closed_total",
			Help:           "Connections closed by the pool rather than reused, by data source and reason.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"datasource", "reason"},
	)

	// DataSourceWaitSeconds is the cumulative time requests have spent waiting
	// for a connection from a pool. Paired with the wait count it gives the
	// average wait, which is the number that says whether a pool is the
	// bottleneck rather than merely busy.
	DataSourceWaitSeconds = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "datasource",
			Name:           "wait_seconds_total",
			Help:           "Cumulative time spent waiting for a connection from a pool.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"datasource"},
	)

	// PreparedStatements reports the size of each pool's statement cache.
	PreparedStatements = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kube_crisp",
			Subsystem:      "datasource",
			Name:           "prepared_statements",
			Help:           "Statements currently held in the prepared statement cache, by data source.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"datasource"},
	)
)

func init() {
	legacyregistry.MustRegister(
		QueryDuration,
		QueryRows,
		Projections,
		CacheReads,
		CacheEntries,
		CacheEvictions,
		QueriesShed,
		QueriesCoalesced,
		QueriesRouted,
		ReplicaFallbacks,
		PollLeader,
		RowsUnmappable,
		RowsOutOfNamespace,
		Watchers,
		WatchEvents,
		WatchPolls,
		WatchNotifications,
		ProjectionsUnguardedUpdate,
		ProjectionsUnversioned,
		ProjectionsCacheUnshared,
		WatchDatabaseReplays,
		WatchListenerConnected,
		WatchListenerReconnects,
		WatchMissedEvents,
		WatchPollDuration,
		WatchPollErrors,
		AdmissionReviews,
		AdmissionDuration,
		ProjectionState,
		DataSourceConnections,
		DataSourceConnectionsClosed,
		DataSourceWaitSeconds,
		PreparedStatements,
	)
}
