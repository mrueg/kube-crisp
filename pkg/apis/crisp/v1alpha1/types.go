package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=crp;crps
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Resource",type=string,JSONPath=`.spec.resource.plural`
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.resource.group`
// +kubebuilder:printcolumn:name="Driver",type=string,JSONPath=`.spec.dataSource.driver`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// CustomResourceProjection declares a read-only custom resource kind whose
// objects are constructed on demand from queries against a SQL database.
//
// The projection is served by kube-crisp-apiserver through the Kubernetes
// aggregation layer, so no objects are ever written to etcd: every GET and
// LIST is answered by executing the configured SQL against the data source.
type CustomResourceProjection struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CustomResourceProjectionSpec   `json:"spec"`
	Status CustomResourceProjectionStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// CustomResourceProjectionList is a list of CustomResourceProjection objects.
type CustomResourceProjectionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []CustomResourceProjection `json:"items"`
}

// CustomResourceProjectionSpec describes the data source, the shape of the
// projected resource, the queries that populate it, and how result columns
// map onto object fields.
//
// Every rule below restates one the apiserver already enforces when it compiles
// a projection. Stating them here as well is what moves the rejection from a
// status condition nobody is watching to the kubectl apply that caused it.
//
// +kubebuilder:validation:XValidation:rule="self.resource.scope != 'Namespaced' || has(self.mapping.namespace)",message="mapping.namespace is required for a Namespaced projection: something has to say which column holds the namespace"
// +kubebuilder:validation:XValidation:rule="self.resource.scope != 'Cluster' || !has(self.mapping.namespace)",message="mapping.namespace must be empty for a Cluster-scoped projection"
// +kubebuilder:validation:XValidation:rule="!has(self.mapping.finalizers) || has(self.queries.markDeleted)",message="mapping.finalizers needs queries.markDeleted: something has to write the deletion timestamp instead of removing the row"
// +kubebuilder:validation:XValidation:rule="!has(self.mapping.finalizers) || has(self.queries.update)",message="mapping.finalizers needs queries.update: a finalizer is cleared by writing the object"
// +kubebuilder:validation:XValidation:rule="!has(self.watch) || !has(self.watch.query) || has(self.mapping.resourceVersion)",message="watch.query needs mapping.resourceVersion, which is the value it pages through"
// +kubebuilder:validation:XValidation:rule="!has(self.watch) || !has(self.watch.deletedQuery) || has(self.watch.query)",message="watch.deletedQuery needs watch.query, since a full poll already sees deletions"
// +kubebuilder:validation:XValidation:rule="!has(self.watch) || !has(self.watch.notify) || self.dataSource.driver == 'postgres'",message="watch.notify needs a driver that can push a change notification, which among the built-in drivers is postgres"
// disabled is omitempty, so false is pruned and the key is simply absent — which
// is why this asks whether it is there before asking what it says.
// +kubebuilder:validation:XValidation:rule="!has(self.watch) || !has(self.watch.notify) || !has(self.watch.disabled) || !self.watch.disabled",message="watch.notify wakes a watch, and watch.disabled turns watching off"
type CustomResourceProjectionSpec struct {
	// DataSource identifies the SQL database backing this projection.
	DataSource DataSource `json:"dataSource"`

	// Resource describes the API surface of the projected kind.
	Resource ProjectedResource `json:"resource"`

	// Queries are the statements used to answer read requests.
	Queries Queries `json:"queries"`

	// Mapping describes how a result row becomes an API object.
	Mapping Mapping `json:"mapping"`

	// CacheTTL, if set, caches query results for this duration. Omit for
	// fully read-through behaviour.
	// +optional
	CacheTTL *metav1.Duration `json:"cacheTTL,omitempty"`

	// Watch configures how WATCH requests are served.
	// +optional
	Watch *WatchSpec `json:"watch,omitempty"`
}

// WatchSpec configures watch support for a projection.
//
// SQL databases have no general change feed, so watches are served by polling
// the list query and turning the differences between snapshots into events.
// Polling starts when the first watcher arrives and stops when the last one
// leaves, so a projection nobody watches costs nothing.
type WatchSpec struct {
	// HistorySize is how many recent changes are kept so that a client can
	// resume a watch instead of relisting. Defaults to 1000; zero keeps none,
	// in which case any resume from a version other than the current one is
	// refused with 410.
	// +optional
	HistorySize *int32 `json:"historySize,omitempty"`

	// PollInterval is how often the list query runs while at least one watcher
	// is connected. Defaults to 5s. Events can therefore lag a change in the
	// database by up to this interval.
	// +optional
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// Query polls incrementally. It receives :since, holding the highest
	// resourceVersion seen so far, and must return only rows at or after it,
	// ordered by the mapped resourceVersion column ascending:
	//
	//   SELECT ... FROM orders
	//   WHERE (:since::text IS NULL OR updated_at > :since)
	//   ORDER BY updated_at ASC
	//
	// This turns each poll into a small read instead of a full scan, which is
	// what makes watching a large table affordable. Deletions cannot be seen
	// this way, so the full list query still runs every FullResyncInterval.
	//
	// Requires mapping.resourceVersion. Without this query every poll lists
	// the whole collection.
	// +optional
	Query *Query `json:"query,omitempty"`

	// FollowerPollInterval is how often a replica that does not hold the
	// leader lease re-runs the poll. Defaults to 1m, and only applies with
	// --enable-leader-election.
	//
	// It is a slower interval rather than no polling at all: a watcher is
	// served from the cache of whichever replica it connected to, so a follower
	// that stopped polling would leave its watchers seeing nothing, silently.
	// Set it below PollInterval and it is ignored — that would be no reduction.
	// +optional
	FollowerPollInterval *metav1.Duration `json:"followerPollInterval,omitempty"`

	// Notify subscribes to a database channel so that a change wakes the poll
	// instead of the poll waiting to find it.
	//
	// This is the difference between a watch that lags by PollInterval and one
	// that lags by a round trip. It is additive rather than a replacement: a
	// notification says only that something changed, the poll is still what
	// reads what it was, and PollInterval still runs underneath — so a
	// notification that is never delivered costs latency rather than events.
	//
	// PostgreSQL only. Something has to send the notifications, which is
	// usually a trigger:
	//
	//   CREATE FUNCTION orders_changed() RETURNS trigger AS $$
	//   BEGIN
	//       PERFORM pg_notify('orders_changed', '');
	//       RETURN NULL;
	//   END;
	//   $$ LANGUAGE plpgsql;
	//
	//   CREATE TRIGGER orders_notify
	//       AFTER INSERT OR UPDATE OR DELETE ON orders
	//       FOR EACH STATEMENT EXECUTE FUNCTION orders_changed();
	//
	// FOR EACH STATEMENT rather than FOR EACH ROW: the payload is not read, so
	// one notification per statement says everything a thousand would.
	//
	// The subscription holds a connection of its own, outside the pool, for as
	// long as a watcher is connected. LISTEN occupies a session, so taking a
	// pooled connection would be one the projection could never run a query on
	// — and on a small pool that is every connection it has. So a watched
	// projection with Notify costs one connection more than
	// DataSource.MaxOpenConns, which bounds the connections doing query work.
	// +optional
	Notify *NotifySpec `json:"notify,omitempty"`

	// DeletedQuery reads the rows removed since a resourceVersion, so that a
	// deletion can be seen without re-reading the whole collection.
	//
	// An incremental poll reads forward, and a row that is gone stops being
	// returned — which is indistinguishable from a row that did not change. The
	// full resync exists only to close that gap. Give the projection a way to
	// ask what was deleted and the gap closes without the scan:
	//
	//   SELECT id, tenant FROM order_tombstones
	//   WHERE (:since::text IS NULL OR deleted_at > :since)
	//   ORDER BY deleted_at ASC
	//
	// It has to return the mapping's identity columns — whatever builds the
	// name, plus the namespace column for a namespaced kind. A tombstone table
	// holding only those is enough.
	//
	// Returning the mapped columns as well is worth it. A tombstone that
	// describes the row it removed means a Deleted event can be answered from
	// the table rather than from memory — which is what a watcher resuming
	// against a restarted server gets instead of a bare name, and what lets the
	// watch cache keep only keys and versions instead of the whole collection.
	//
	// This is also what makes FullResyncInterval: 0 safe. Requires Query.
	// +optional
	DeletedQuery *Query `json:"deletedQuery,omitempty"`

	// FullResyncInterval is how often the full list query runs while polling
	// incrementally, which is what detects deletions. Defaults to 1m.
	//
	// "0s" disables it, which is only sound with DeletedQuery: without one,
	// nothing would ever notice a row disappearing. It is a duration, so the
	// zero has to be written as a duration too — a bare 0 is rejected.
	// +optional
	FullResyncInterval *metav1.Duration `json:"fullResyncInterval,omitempty"`

	// BookmarkInterval is how often an otherwise idle watch receives a
	// bookmark carrying the current resourceVersion, so a client that
	// reconnects resumes from a recent point instead of replaying. Defaults to
	// 1m; zero disables periodic bookmarks.
	// +optional
	BookmarkInterval *metav1.Duration `json:"bookmarkInterval,omitempty"`

	// Disabled turns watch support off. The verb is still advertised, but
	// WATCH requests are rejected, which is useful when the list query is too
	// expensive to run on a timer.
	// +optional
	Disabled bool `json:"disabled,omitempty"`
}

// NotifySpec names the channel a projection is woken by.
type NotifySpec struct {
	// Channel is the name a trigger sends to with pg_notify. It is an
	// identifier rather than a bind parameter, so it is restricted to what one
	// can safely be.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_]*$`
	Channel string `json:"channel"`
}

// DataSource describes how to reach the backing database. The DSN itself is
// always read from a Secret so that credentials never appear in this object.
//
// +kubebuilder:validation:XValidation:rule="!has(self.statementTimeout) || !self.statementTimeout || self.driver == 'postgres' || self.driver == 'cockroach'",message="statementTimeout is supported on the postgres and cockroach drivers only: MySQL bounds read-only SELECTs rather than every statement, and SQLite has no equivalent"
type DataSource struct {
	// Driver selects the database/sql driver. One of: postgres, cockroach,
	// mysql, sqlite.
	// +kubebuilder:validation:Enum=postgres;cockroach;mysql;sqlite
	Driver string `json:"driver"`

	// SecretRef points at a Secret holding the connection string.
	SecretRef SecretReference `json:"secretRef"`

	// DSNKey is the key within the Secret holding the connection string.
	// +kubebuilder:default=dsn
	// +optional
	DSNKey string `json:"dsnKey,omitempty"`

	// ReadSecretRef points at a Secret holding the connection string of a read
	// replica. Reads go there; writes always go to the primary.
	//
	// A projection's scarce resource is the primary's connections, and reads
	// are almost all of what a projected kind does — lists, gets, and a watch
	// poll on a timer. Sending those to a replica takes that load off the
	// database that has to serve the writes.
	//
	// What it costs is replication lag: a client that writes and then reads can
	// see the row as it was before its own write, and no amount of cache
	// invalidation here can fix that. Two things are deliberately kept on the
	// primary because they cannot tolerate it — writes, and the read a write is
	// based on, since the resourceVersion check and the untouched half of a
	// merged object are decided on the row as the primary has it.
	//
	// Empty means no replica, and everything uses the primary.
	// +optional
	ReadSecretRef *SecretReference `json:"readSecretRef,omitempty"`

	// ReadDSNKey is the key within ReadSecretRef holding the replica's
	// connection string. Defaults to DSNKey.
	// +optional
	ReadDSNKey string `json:"readDsnKey,omitempty"`

	// MaxOpenConns bounds the connection pool. Defaults to 10.
	// +optional
	MaxOpenConns *int32 `json:"maxOpenConns,omitempty"`

	// MaxIdleConns bounds idle connections. Defaults to MaxOpenConns, so that
	// a pool does not close connections it is about to need again: traffic to
	// an API server arrives in waves, and a lower idle limit makes every wave
	// re-dial and re-authenticate whatever the last one left behind. Lower it
	// to hold fewer connections against the database between waves.
	// +optional
	MaxIdleConns *int32 `json:"maxIdleConns,omitempty"`

	// ConnMaxLifetime bounds connection reuse. Defaults to 30m.
	// +optional
	ConnMaxLifetime *metav1.Duration `json:"connMaxLifetime,omitempty"`

	// ConnMaxIdleTime closes a connection that has been idle this long, before
	// its lifetime is up.
	//
	// It is what keeps a pool from holding connections a database, a proxy, or
	// a firewall has already decided to drop: those are not closed, they simply
	// stop answering, and the request that finds one pays a timeout for it.
	// Zero leaves connections idle until ConnMaxLifetime.
	ConnMaxIdleTime *metav1.Duration `json:"connMaxIdleTime,omitempty"`

	// PreparedStatements caches a prepared statement per query on each pooled
	// connection, so repeated reads and writes skip parsing and planning.
	// Defaults to true. Turn it off for connection poolers that cannot route
	// prepared statements, such as PgBouncer in transaction mode.
	// +optional
	PreparedStatements *bool `json:"preparedStatements,omitempty"`

	// StatementTimeout asks the database to abort a statement that outruns its
	// query's timeout.
	//
	// Without it a timeout only stops this server waiting: the context is
	// cancelled, the client is answered, and PostgreSQL carries on producing a
	// result nobody will read — so a query with a bad plan keeps burning the
	// database's CPU, and everything queued behind it starts further behind.
	//
	// It costs a transaction per query, because that is the only scope a
	// setting can be confined to rather than left on a pooled connection every
	// projection reaching this database shares. A transaction does not use the
	// prepared statement cache either, so this trades some read latency for a
	// bound on the worst case. Off by default for that reason.
	//
	// PostgreSQL only. MySQL's max_execution_time covers read-only SELECTs
	// rather than every statement, and SQLite has no equivalent, so asking for
	// it on either is refused rather than silently doing nothing.
	//
	// A single fixed bound for every query on a data source needs none of this:
	// put "options=-c statement_timeout=5000" in the connection string instead.
	// +optional
	StatementTimeout *bool `json:"statementTimeout,omitempty"`

	// KeepAliveInterval pings idle connections on this interval so that the
	// pool holds warm connections instead of paying connection setup on the
	// first request after an idle period. Defaults to 30s; zero disables it.
	// +optional
	KeepAliveInterval *metav1.Duration `json:"keepAliveInterval,omitempty"`

	// SessionVariables are set on the connection before every query this
	// projection runs, from the request that caused it.
	//
	// This is how a database enforces the tenancy boundary itself. With
	// PostgreSQL row-level security, a policy reading
	// current_setting('app.tenant') decides which rows exist for the query, so
	// a mistake in a projection's WHERE clause cannot hand one tenant another's
	// rows — the database never offers them.
	//
	// Setting any of these moves every query into a transaction, since that is
	// the only way a setting can be scoped to one request rather than left on a
	// pooled connection for whoever gets it next. PostgreSQL and MySQL only;
	// SQLite has no session state and a projection asking for it is rejected.
	// +optional
	SessionVariables []SessionVariable `json:"sessionVariables,omitempty"`

	// MaxConcurrentQueries caps how many queries this projection runs at once.
	// Requests beyond the cap wait briefly and are then rejected with 429, so a
	// slow query sheds load instead of occupying the pool until every client
	// times out. Defaults to MaxOpenConns; zero means unlimited.
	//
	// The limit is per projection rather than per pool, because projections
	// reaching the same database share a pool: a limit on the pool would be
	// whatever the first projection to open it asked for. Connections to the
	// database are bounded separately, by MaxOpenConns and by the server's
	// --max-open-conns-per-datasource.
	// +optional
	MaxConcurrentQueries *int32 `json:"maxConcurrentQueries,omitempty"`
}

// SecretReference names the Secret holding a data source's connection string.
//
// It is deliberately not corev1.SecretReference, where both fields are
// optional: a projection whose Secret has no namespace has nowhere to read
// from, and the resolver refuses one at runtime. Requiring them here is the
// same rule, said early enough for the API to enforce it.
type SecretReference struct {
	// Name of the Secret.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace the Secret lives in. It has to be one --datasource-namespaces
	// allows.
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
}

// ResourceScope mirrors apiextensions scope semantics for projected kinds.
// +kubebuilder:validation:Enum=Namespaced;Cluster
type ResourceScope string

const (
	// NamespaceScoped projections expose objects inside namespaces.
	NamespaceScoped ResourceScope = "Namespaced"
	// ClusterScoped projections expose objects at cluster scope.
	ClusterScoped ResourceScope = "Cluster"
)

// ProjectedResource describes the API identity and schema of the projected kind.
// +kubebuilder:validation:XValidation:rule="has(self.schema) != has(self.schemaFrom)",message="exactly one of schema or schemaFrom is required"
type ProjectedResource struct {
	// Group is the API group to serve the projected kind in. An APIService
	// must exist delegating this group to kube-crisp-apiserver.
	Group string `json:"group"`

	// Version is the API version of the projected kind.
	Version string `json:"version"`

	// Kind is the projected kind, e.g. "Order".
	Kind string `json:"kind"`

	// ListKind defaults to Kind + "List".
	// +optional
	ListKind string `json:"listKind,omitempty"`

	// Plural is the lowercase plural resource name, e.g. "orders".
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9]*$`
	Plural string `json:"plural"`

	// Singular defaults to the lowercase Kind.
	// +optional
	Singular string `json:"singular,omitempty"`

	// ShortNames are optional kubectl aliases.
	// +optional
	ShortNames []string `json:"shortNames,omitempty"`

	// Categories are optional kubectl categories, e.g. "all".
	// +optional
	Categories []string `json:"categories,omitempty"`

	// Scope determines whether projected objects are namespaced.
	Scope ResourceScope `json:"scope"`

	// Schema is the OpenAPI v3 schema published for the projected kind. It is
	// used for discovery, kubectl explain, and response validation.
	//
	// Exactly one of Schema or SchemaFrom must be set.
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Schema *apiextensionsv1.JSONSchemaProps `json:"schema,omitempty"`

	// SchemaFrom borrows the schema from an existing CustomResourceDefinition
	// rather than restating it here. The CRD is read for its schema only; it is
	// not served by the kube-apiserver for this group.
	// +optional
	SchemaFrom *CRDReference `json:"schemaFrom,omitempty"`

	// AdditionalPrinterColumns customises kubectl table output.
	// +optional
	AdditionalPrinterColumns []apiextensionsv1.CustomResourceColumnDefinition `json:"additionalPrinterColumns,omitempty"`

	// SelectableFields declares fields a client may filter on beyond
	// metadata.name and metadata.namespace, mirroring the CustomResourceDefinition
	// feature of the same name.
	// +optional
	SelectableFields []SelectableField `json:"selectableFields,omitempty"`

	// Conversion says what a client may assume when a kind is served at more
	// than one version. There is no conversion webhook here and no stored
	// version: every version reads the same rows and maps them its own way, so
	// what matters is whether a write through one version preserves what
	// another shows. Defaults to RoundTrip.
	// +optional
	// +kubebuilder:default=RoundTrip
	Conversion ConversionPolicy `json:"conversion,omitempty"`

	// Subresources enables the subresources of the projected kind.
	// +optional
	Subresources *ProjectedSubresources `json:"subresources,omitempty"`

	// Versions declares further versions of the kind, alongside the one named
	// by Version.
	//
	// There is no conversion between them and none is needed: every read goes
	// back to the database, so each version maps the same rows through its own
	// schema and mapping. A column added to one version is simply absent from
	// the other.
	// +optional
	Versions []ProjectedVersion `json:"versions,omitempty"`
}

// ConversionPolicy says what is promised across versions of a kind.
// +kubebuilder:validation:Enum=RoundTrip;None
type ConversionPolicy string

const (
	// ConversionRoundTrip requires every served version to map the same
	// columns, so a write through one version cannot silently drop a value
	// another version displays. This is the default, and it is checked when
	// the projection is compiled rather than discovered by a client.
	ConversionRoundTrip ConversionPolicy = "RoundTrip"

	// ConversionNone allows the versions to map different columns. Nothing
	// translates between them: a client that writes one version and reads
	// another may find fields it did not set and lose fields it did. Choose it
	// deliberately, for a version that deliberately exposes less.
	ConversionNone ConversionPolicy = "None"
)

// ProjectedVersion is one served version of a projected kind.
// +kubebuilder:validation:XValidation:rule="has(self.schema) != has(self.schemaFrom)",message="exactly one of schema or schemaFrom is required"
type ProjectedVersion struct {
	// Name is the version, for example "v1beta1".
	Name string `json:"name"`

	// Served turns the version off without removing its definition. Defaults
	// to true.
	// +optional
	Served *bool `json:"served,omitempty"`

	// Schema is the OpenAPI v3 schema for this version. Exactly one of Schema
	// or SchemaFrom must be set.
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Schema *apiextensionsv1.JSONSchemaProps `json:"schema,omitempty"`

	// SchemaFrom borrows this version's schema from a CustomResourceDefinition.
	// +optional
	SchemaFrom *CRDReference `json:"schemaFrom,omitempty"`

	// Mapping overrides how result columns map onto objects of this version.
	// Defaults to the projection's mapping, which is what makes an added
	// version cheap when only the schema differs.
	// +optional
	Mapping *Mapping `json:"mapping,omitempty"`

	// AdditionalPrinterColumns customises kubectl output for this version.
	// +optional
	AdditionalPrinterColumns []apiextensionsv1.CustomResourceColumnDefinition `json:"additionalPrinterColumns,omitempty"`
}

// SelectableField makes one field available to field selectors.
type SelectableField struct {
	// JSONPath is the field, written as a simple dotted path with a leading
	// dot, such as ".spec.customer". Clients then select on "spec.customer".
	JSONPath string `json:"jsonPath"`

	// Column names the result column holding the same value. When set, the
	// requested value is bound under that name so the query can filter in the
	// database:
	//
	//   WHERE (:customer IS NULL OR customer = :customer)
	//
	// The result is filtered again after mapping either way, so a query that
	// ignores the parameter is still correct, only slower.
	// +optional
	Column string `json:"column,omitempty"`
}

// ProjectedSubresources lists the subresources a projected kind serves.
type ProjectedSubresources struct {
	// Status serves the kind's status separately, at <resource>/status. As with
	// a CustomResourceDefinition, enabling it splits the two: a write to the
	// main resource cannot change status, and a write to status cannot change
	// anything else.
	// +optional
	Status *ProjectedStatusSubresource `json:"status,omitempty"`

	// Scale serves <resource>/scale, so `kubectl scale` and the horizontal pod
	// autoscaler can drive a projected object the way they drive a Deployment.
	// +optional
	Scale *ProjectedScaleSubresource `json:"scale,omitempty"`
}

// ProjectedScaleSubresource says where the replica counts live in the projected
// object, mirroring the CustomResourceDefinition subresource of the same name.
type ProjectedScaleSubresource struct {
	// SpecReplicasPath is the dotted path to the desired count, for example
	// ".spec.replicas". It must be mapped from a column, since scaling writes
	// to it.
	SpecReplicasPath string `json:"specReplicasPath"`

	// StatusReplicasPath is the dotted path to the observed count.
	StatusReplicasPath string `json:"statusReplicasPath"`

	// LabelSelectorPath is the dotted path to the selector an autoscaler uses
	// to find the pods behind this object. Optional, and only meaningful when
	// something is actually scaled by it.
	// +optional
	LabelSelectorPath string `json:"labelSelectorPath,omitempty"`
}

// ProjectedStatusSubresource enables the status subresource. It has no fields
// of its own, matching how a CustomResourceDefinition declares it.
type ProjectedStatusSubresource struct{}

// CRDReference names an existing CustomResourceDefinition to borrow a schema from.
type CRDReference struct {
	// Name is the CRD name, e.g. "orders.acme.example.com".
	Name string `json:"name"`

	// Version selects which CRD version's schema to use. Defaults to the
	// storage version.
	// +optional
	Version string `json:"version,omitempty"`
}

// Queries holds the statements backing each read verb.
// +kubebuilder:validation:XValidation:rule="has(self.list.sql)",message="queries.list.sql is required: a list is one statement, so it cannot be written as a transaction"
type Queries struct {
	// List answers LIST requests. It may reference the :namespace, :limit,
	// :offset, and :labelSelector bind parameters.
	List Query `json:"list"`

	// Get answers GET requests for a single object. It must reference :name,
	// and :namespace for namespaced projections. If omitted, GET is served by
	// filtering the List query, which is correct but less efficient.
	// +optional
	Get *Query `json:"get,omitempty"`

	// Create answers POST requests. Defining any write query makes the
	// projection writable; verbs whose queries are absent are rejected with
	// 405 Method Not Allowed.
	// +optional
	Create *Query `json:"create,omitempty"`

	// Update answers PUT and PATCH requests.
	// +optional
	Update *Query `json:"update,omitempty"`

	// Delete answers DELETE requests.
	// +optional
	Delete *Query `json:"delete,omitempty"`

	// MarkDeleted writes metadata.deletionTimestamp instead of removing the
	// row, for a projection with finalizers: an object that still has one is
	// marked as terminating and stays, and Delete runs only once the last
	// finalizer is cleared.
	// +optional
	MarkDeleted *Query `json:"markDeleted,omitempty"`

	// DeleteCollection answers DELETE requests against the whole collection in
	// a single statement. Without it, a collection delete removes the matching
	// objects one at a time.
	// +optional
	DeleteCollection *Query `json:"deleteCollection,omitempty"`

	// UpdateStatus answers writes to the status subresource. Defaults to the
	// Update query, which is usually what you want when status lives in the
	// same row.
	// +optional
	UpdateStatus *Query `json:"updateStatus,omitempty"`

	// Count returns the total number of objects a list would match, as a
	// single row with a single column. It populates the remainingItemCount a
	// paged list reports, and is only run when a client pages.
	// +optional
	Count *Query `json:"count,omitempty"`
}

// Query is a single SQL statement plus its execution constraints.
// +kubebuilder:validation:XValidation:rule="has(self.sql) != (has(self.statements) && size(self.statements) > 0)",message="set either sql or statements, not both, and not neither"
type Query struct {
	// SQL is the statement to execute. Bind parameters are written as :name
	// and are always passed as driver placeholders, never interpolated, so
	// user-supplied values cannot alter the statement.
	//
	// Exactly one of sql or statements is required.
	// +optional
	SQL string `json:"sql,omitempty"`

	// Statements runs several statements as one transaction, in order, on one
	// connection. Either all of them take effect or none do.
	//
	// This is how a projected kind spans more than one table: a create that
	// inserts an order and its line items has to be atomic, or a failure
	// halfway leaves a row the API would then serve as a complete object.
	//
	// Only the last statement may return rows, since only its result can be
	// the object the client is answered with; the rest are executed for their
	// effect. Every statement binds whichever :name parameters it declares.
	//
	// Reads cannot use this: they are shared between requests and served from
	// a cache, neither of which a transaction survives. Only create, update,
	// updateStatus, delete, and deleteCollection accept it.
	// +optional
	Statements []string `json:"statements,omitempty"`

	// Parameters declares additional bind parameters beyond the built-ins,
	// along with where their values come from.
	// +optional
	Parameters []QueryParameter `json:"parameters,omitempty"`

	// Timeout bounds execution. Defaults to 10s.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// MaxRows caps rows read from a single result set. Defaults to 5000.
	// +optional
	MaxRows *int32 `json:"maxRows,omitempty"`

	// MaxBytes caps the size of the values a result set carries. Defaults to
	// 64Mi. Written as a quantity, so "512Ki" and "1Gi" both work.
	//
	// MaxRows does not bound memory on its own. One row can carry a megabyte
	// of JSON or text, so a modest row count can still be gigabytes — and a
	// query using resultFormat: JSONArray returns the whole collection as a
	// single row, where MaxRows never applies at all. The server is shared by
	// every projection, so a read that cannot be bounded is a read that can
	// take the others with it.
	//
	// Exceeding it fails the read rather than truncating it, exactly as
	// MaxRows does: a collection that silently omits rows is one a client
	// cannot tell from a collection that genuinely has fewer.
	// +optional
	MaxBytes *resource.Quantity `json:"maxBytes,omitempty"`

	// KeysetColumn names the column whose value the :after parameter carries
	// between pages, for a list query that pages by key.
	//
	// It must be the column the query orders by. A continue token holds the
	// last row's value of this column, and the next page asks for rows beyond
	// it; if the ordering is on a different column the pages silently skip and
	// repeat rows. Defaults to the column mapping.name reads, which is the
	// right answer for the usual "ORDER BY id" list.
	// +optional
	KeysetColumn string `json:"keysetColumn,omitempty"`

	// ResultFormat describes the shape of the result set. Defaults to Rows.
	//
	// JSONArray expects the statement to return a single row holding a single
	// JSON array column, as produced by PostgreSQL's json_agg. The database
	// then does the row-to-document assembly and the server decodes one value
	// instead of scanning every column of every row, which is measurably
	// cheaper for large lists.
	// +optional
	ResultFormat ResultFormat `json:"resultFormat,omitempty"`
}

// ResultFormat describes how a query returns its results.
// +kubebuilder:validation:Enum=Rows;JSONArray
type ResultFormat string

const (
	// ResultFormatRows is one API object per result row.
	ResultFormatRows ResultFormat = "Rows"
	// ResultFormatJSONArray is a single row holding a JSON array of objects.
	ResultFormatJSONArray ResultFormat = "JSONArray"
)

// SessionVariable is one setting applied to the connection a query runs on.
// A session variable is resolved before the query runs, so only the sources that
// have a value at that point are meaningful. Field reads the submitted object,
// which a read has none of, and LabelSelector is a list's filter rather than an
// identity. Both resolve to the empty string — and an empty setting is the
// dangerous kind of wrong, because a row-level security policy comparing against
// it does not fail, it matches nothing, or written the other way round,
// everything.
//
// +kubebuilder:validation:XValidation:rule="self.from != 'Field' && self.from != 'LabelSelector'",message="a session variable cannot be sourced from Field or LabelSelector: neither has a value at the time the connection is prepared"
type SessionVariable struct {
	// Name is the setting to apply, for example "app.tenant". PostgreSQL takes
	// it as it is written; MySQL has no namespaced settings, so dots become
	// underscores and it arrives as the user variable @app_tenant.
	//
	// The name cannot be a bind parameter in any supported driver, so it goes
	// into the statement text and has to be beyond suspicion.
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`
	Name string `json:"name"`

	// From says where the value comes from. Value, RequestNamespace,
	// RequestName, and RequestUser are meaningful here; the rest are not,
	// because there is no object or selector at the time the connection is
	// prepared.
	From ParameterSource `json:"from"`

	// Value supplies a constant when From is Value.
	// +optional
	Value string `json:"value,omitempty"`
}

// ParameterSource enumerates where a bind parameter's value comes from.
// +kubebuilder:validation:Enum=Value;RequestNamespace;RequestName;RequestUser;RequestUserUID;RequestUserGroups;RequestUserExtra;LabelSelector;Field
type ParameterSource string

const (
	// ParameterSourceValue supplies a constant.
	ParameterSourceValue ParameterSource = "Value"
	// ParameterSourceRequestNamespace supplies the requested namespace.
	ParameterSourceRequestNamespace ParameterSource = "RequestNamespace"
	// ParameterSourceRequestName supplies the requested object name.
	ParameterSourceRequestName ParameterSource = "RequestName"
	// ParameterSourceRequestUser supplies the authenticated username, which
	// allows a projection to scope rows to the caller.
	ParameterSourceRequestUser ParameterSource = "RequestUser"
	// ParameterSourceRequestUserUID supplies the authenticated user's UID.
	//
	// It is the half of an identity that does not change: a username can be
	// reassigned to a different person, a UID cannot, so a policy that has to
	// survive that keys on this instead.
	ParameterSourceRequestUserUID ParameterSource = "RequestUserUID"
	// ParameterSourceRequestUserGroups supplies the caller's groups, as a JSON
	// array of strings.
	//
	// Authorization in Kubernetes is mostly by group rather than by user, so a
	// projection that wants its rows scoped the way RBAC scopes its verbs wants
	// this one.
	ParameterSourceRequestUserGroups ParameterSource = "RequestUserGroups"
	// ParameterSourceRequestUserExtra supplies the authenticator's extra
	// attributes, as a JSON object of string arrays. What is in it depends on
	// the authenticator; an OIDC one can carry claims worth filtering on.
	ParameterSourceRequestUserExtra ParameterSource = "RequestUserExtra"
	// ParameterSourceLabelSelector supplies the serialized label selector.
	ParameterSourceLabelSelector ParameterSource = "LabelSelector"
	// ParameterSourceField supplies a value read out of the submitted object,
	// at the path named by the parameter's Path. Only meaningful for writes.
	ParameterSourceField ParameterSource = "Field"
)

// QueryParameter binds a named SQL parameter to a request-derived value.
type QueryParameter struct {
	// Name is the bind parameter name, without the leading colon.
	Name string `json:"name"`

	// From selects the source of the value.
	From ParameterSource `json:"from"`

	// Value is the literal value when From is "Value".
	// +optional
	Value string `json:"value,omitempty"`

	// Path is the dotted path read from the submitted object when From is
	// "Field", for example "spec.customer".
	// +optional
	Path string `json:"path,omitempty"`

	// Type coerces the value read from the object before binding it. Defaults
	// to string.
	// +optional
	Type FieldType `json:"type,omitempty"`
}

// FieldType is the JSON type a column is coerced to when mapped.
// +kubebuilder:validation:Enum=string;integer;number;boolean;timestamp;json
type FieldType string

const (
	// FieldTypeString coerces to a JSON string.
	FieldTypeString FieldType = "string"
	// FieldTypeInteger coerces to a JSON integer.
	FieldTypeInteger FieldType = "integer"
	// FieldTypeNumber coerces to a JSON number.
	FieldTypeNumber FieldType = "number"
	// FieldTypeBoolean coerces to a JSON boolean.
	FieldTypeBoolean FieldType = "boolean"
	// FieldTypeTimestamp coerces to an RFC3339 string.
	FieldTypeTimestamp FieldType = "timestamp"
	// FieldTypeJSON parses the column as embedded JSON, for jsonb columns.
	FieldTypeJSON FieldType = "json"
)

// Mapping describes how one result row becomes one API object.
// +kubebuilder:validation:XValidation:rule="has(self.name) != (has(self.nameColumns) && size(self.nameColumns) > 0)",message="set either name or nameColumns, not both, and not neither"
// +kubebuilder:validation:XValidation:rule="!has(self.finalizers) || has(self.deletionTimestamp)",message="mapping.finalizers needs mapping.deletionTimestamp: an object with a finalizer is marked as terminating rather than removed"
type Mapping struct {
	// Name selects the column providing metadata.name. The value must be a
	// valid Kubernetes object name.
	//
	// Exactly one of name or nameColumns is required.
	// +optional
	Name string `json:"name,omitempty"`

	// NameColumns builds metadata.name out of several columns, for a table
	// whose identity is composite: {region, order_no} becomes "eu-1042".
	//
	// The name is the only handle the API has on a row, so it has to be
	// reversible. Requests are answered by splitting the name back into its
	// parts and binding each one under its own column name, so a query reads
	// WHERE region = :region AND order_no = :order_no. A value containing the
	// separator is rejected rather than guessed at, since it would make two
	// different rows produce the same name.
	// +optional
	NameColumns []string `json:"nameColumns,omitempty"`

	// NameSeparator joins the parts of a composite name. Defaults to "-", and
	// must be legal in an object name.
	// +optional
	NameSeparator string `json:"nameSeparator,omitempty"`

	// Namespace selects the column providing metadata.namespace. Required for
	// namespaced projections.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// UID selects a column providing a stable metadata.uid. Recommended so
	// clients can detect identity changes.
	// +optional
	UID string `json:"uid,omitempty"`

	// ResourceVersion selects a column providing metadata.resourceVersion,
	// typically an updated_at timestamp or a row version counter.
	//
	// Watch relies on this column: it is how a poll tells a changed row from an
	// unchanged one, so every write must move it. Projections that leave it
	// unset are still watchable, but each poll then compares whole objects.
	// +optional
	ResourceVersion string `json:"resourceVersion,omitempty"`

	// CreationTimestamp selects a column providing metadata.creationTimestamp.
	// +optional
	CreationTimestamp string `json:"creationTimestamp,omitempty"`

	// DeletionTimestamp selects a column providing metadata.deletionTimestamp,
	// for projections that delete softly: the row stays, marked as going away.
	// A NULL means the object is not being deleted.
	//
	// Clients read it the way they read it anywhere else in Kubernetes — an
	// object with it set is terminating — and a delete of an object that
	// already carries one is answered with the object rather than run again.
	// +optional
	DeletionTimestamp string `json:"deletionTimestamp,omitempty"`

	// Generation selects a column providing metadata.generation, the counter a
	// controller compares against status.observedGeneration to tell whether it
	// has caught up with the spec it is looking at.
	//
	// The database owns it, exactly as it owns resourceVersion: a write never
	// carries a client's value back to the row. For the comparison to mean
	// anything the column has to advance when the spec changes and stay put
	// when only status does.
	// +optional
	Generation string `json:"generation,omitempty"`

	// Finalizers selects a column holding metadata.finalizers as a JSON array
	// of strings, for a projection whose objects have work to finish before
	// they can go away.
	//
	// It changes what a delete does, exactly as it does for a custom resource:
	// an object with finalizers is marked as terminating rather than removed,
	// and the row is deleted only once the last finalizer is cleared. That
	// needs both mapping.deletionTimestamp, to carry the mark, and
	// queries.markDeleted, to write it.
	// +optional
	Finalizers string `json:"finalizers,omitempty"`

	// OwnerReferences selects a column holding metadata.ownerReferences as a
	// JSON array, so a projected object can belong to something else.
	//
	// Serving them is what lets the cluster's garbage collector reach these
	// objects: it deletes a child whose owner is gone, through the same API as
	// any other client. Nothing is enforced here — an owner reference is a
	// claim the projection stores and returns.
	// +optional
	OwnerReferences string `json:"ownerReferences,omitempty"`

	// ManagedFields is the column holding metadata.managedFields, as a JSON
	// array.
	//
	// Server-side apply merges correctly without it — the schema is what tells
	// field management how lists and maps combine — but it cannot detect a
	// conflict, because an object rebuilt from a row carries no record of who
	// owns which field. Map a column and that record survives, so two
	// controllers applying the same field are told about each other and
	// --force-conflicts starts to mean something.
	//
	// The column has to be wide: field management writes an entry per manager
	// per subresource, each holding a set of field paths.
	ManagedFields string `json:"managedFields,omitempty"`

	// LabelsFrom is a column holding the whole label map as a JSON object.
	//
	// Labels maps one key to one column, which is right when the labels are a
	// fixed, known set — a status, a tier. It cannot describe a table whose
	// labels vary per row, and that is what this is for: one jsonb or TEXT
	// column holding {"team":"payments"} becomes the object's labels.
	//
	// The two combine. Anything Labels names is read from its own column and
	// wins, so a key can be promoted out of the JSON without moving the rest.
	// +optional
	LabelsFrom string `json:"labelsFrom,omitempty"`

	// Labels maps label keys to columns.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// AnnotationsFrom is a column holding the whole annotation map as a JSON
	// object, the counterpart of LabelsFrom.
	// +optional
	AnnotationsFrom string `json:"annotationsFrom,omitempty"`

	// Annotations maps annotation keys to columns.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// Fields maps result columns onto dotted paths within the object, for
	// example {column: total_cents, path: spec.totalCents, type: integer}.
	// +optional
	Fields []FieldMapping `json:"fields,omitempty"`

	// OnUnmappableRow decides what happens to a row that cannot be turned into
	// an object — a NULL where the name comes from, a column whose type does
	// not fit the schema, a name that is not a valid object name.
	//
	// Skip is the default and leaves the row out: one bad row does not stop a
	// client seeing the rest of the table, or stop every watcher on the
	// projection seeing any change at all. It is counted in
	// kube_crisp_query_rows_unmappable_total and a read carries a warning
	// saying how many were left out.
	//
	// Fail is for a projection where a partial answer is worse than no answer.
	// A collection that silently omits rows is one a client cannot tell from a
	// collection that genuinely has fewer, so anything reconciling towards it
	// will delete what it cannot see. Choose Fail when that is the greater
	// risk, and accept that one bad row takes the whole projection out.
	// +kubebuilder:validation:Enum=Skip;Fail
	// +optional
	OnUnmappableRow UnmappableRowPolicy `json:"onUnmappableRow,omitempty"`
}

// UnmappableRowPolicy decides what a row that cannot become an object does to
// the read that found it.
type UnmappableRowPolicy string

const (
	// UnmappableRowSkip leaves the row out of the result. The default.
	UnmappableRowSkip UnmappableRowPolicy = "Skip"
	// UnmappableRowFail makes the whole read fail.
	UnmappableRowFail UnmappableRowPolicy = "Fail"
)

// FieldMapping places one result column at one path in the projected object.
type FieldMapping struct {
	// Column is the result column name.
	Column string `json:"column"`

	// Path is the dotted destination path, e.g. "spec.owner" or
	// "status.phase". Leading "metadata." paths are rejected; use the
	// dedicated Mapping fields instead.
	Path string `json:"path"`

	// Type coerces the column value. Defaults to string.
	// +optional
	Type FieldType `json:"type,omitempty"`

	// OmitEmpty drops the field when the column is NULL rather than emitting
	// an explicit null.
	// +optional
	OmitEmpty bool `json:"omitEmpty,omitempty"`
}

// CustomResourceProjectionStatus reports whether the projection is being served.
type CustomResourceProjectionStatus struct {
	// ObservedGeneration is the spec generation most recently reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions describe the projection's serving state. Known types are
	// "Ready", "Registered", "DataSourceConnected", and "SchemaResolved".
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ServedPaths lists the API paths currently served for this projection.
	// +optional
	ServedPaths []string `json:"servedPaths,omitempty"`

	// RequiredSchema is what this projection reads from its database, gathered
	// from the queries and the mapping into one place.
	//
	// kube-crisp does not create tables, and a projection whose table is
	// missing reports CompilationFailed with the database's own message. This
	// is the other half of that: what the table would have to contain, without
	// anybody reading the SQL to work it out. It is meant to be handed to
	// whatever manages the schema — a migration tool, or a person writing DDL.
	// +optional
	RequiredSchema *RequiredSchema `json:"requiredSchema,omitempty"`
}

// RequiredSchema describes what a projection reads from its database.
//
// A description, never an instruction: nothing in kube-crisp acts on this, and
// it is derived from the projection alone rather than from the database, so it
// says what the projection asks for and not what is there.
type RequiredSchema struct {
	// Tables names the tables the projection's statements refer to.
	//
	// Read out of the statement text, so a name inside a string literal or a
	// comment is not one. Deliberately incomplete in two ways worth knowing: a
	// common table expression is listed alongside the tables it reads, and a
	// table reached only through a view or through dynamic SQL is not listed at
	// all, because nothing in the statement says so.
	// +optional
	Tables []string `json:"tables,omitempty"`

	// Columns names the columns the mapping reads out of query results, with
	// the type each value is coerced to.
	//
	// These are result columns rather than table columns. A projection whose
	// list query computes one — SELECT total_cents * 2 AS doubled — needs
	// "doubled" in its results and no such column in any table.
	// +optional
	Columns []RequiredColumn `json:"columns,omitempty"`
}

// RequiredColumn is one column a projection reads.
type RequiredColumn struct {
	// Name is the result column name.
	Name string `json:"name"`

	// Type is what the value is coerced to. Columns carrying identity and
	// metadata are read as strings; only mapping.fields can ask for anything
	// else.
	// +optional
	Type FieldType `json:"type,omitempty"`

	// UsedFor says what the column provides, so a reader can tell a column the
	// projection cannot work without from one that fills in a field.
	// +optional
	UsedFor string `json:"usedFor,omitempty"`
}

// Condition types reported in CustomResourceProjectionStatus.
const (
	// ConditionReady is true when the projection is serving requests.
	ConditionReady = "Ready"
	// ConditionDataSourceConnected is true when the database is reachable.
	ConditionDataSourceConnected = "DataSourceConnected"
	// ConditionSchemaResolved is true when the projected schema is known.
	ConditionSchemaResolved = "SchemaResolved"
	// ConditionRegistered is true when the aggregation layer is routing the
	// projected group version to this server.
	//
	// Compiling a projection and installing its handlers is only half of
	// serving it: until an APIService exists and the aggregator reports it
	// available, a request for the projected resource never reaches this
	// process. Reported separately from Ready because the two fail for
	// unrelated reasons — a projection can be perfectly valid and unreachable
	// because the Service has no endpoints, or because the CA bundle is stale.
	ConditionRegistered = "Registered"
)
