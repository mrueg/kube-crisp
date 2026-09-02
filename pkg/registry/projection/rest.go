// Package projection implements the REST storage that answers requests for a
// projected kind by running SQL.
package projection

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresource/tableconvertor"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/cel/common"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispmetrics "github.com/mrueg/kube-crisp/pkg/metrics"
	"github.com/mrueg/kube-crisp/pkg/projection"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// REST serves reads for one projected kind.
type REST struct {
	rest.TableConvertor

	// projection and label identify this storage in metrics.
	projection string
	label      string

	resource crispv1alpha1.ProjectedResource

	// pool is the primary: every write goes there, and so does every read a
	// write is based on. readPool is the replica, when the projection names
	// one, and answers the reads that can tolerate replication lag.
	pool     *crispsql.Pool
	readPool *crispsql.Pool
	mapper   *projection.Mapper

	// failUnmappable makes a row that cannot become an object fail the read
	// instead of being left out. See mapping.onUnmappableRow.
	failUnmappable bool

	// replicaDownUntil is when the replica may be tried again, as Unix nanos,
	// after a read found it unreachable. Zero means it is in use.
	replicaDownUntil atomic.Int64

	list             *compiledQuery
	get              *compiledQuery
	create           *compiledQuery
	update           *compiledQuery
	delete           *compiledQuery
	deleteCollection *compiledQuery
	updateStatus     *compiledQuery
	count            *compiledQuery
	watchQuery       *compiledQuery
	deletedQuery     *compiledQuery

	// statusSubresource splits status from the rest of the object, exactly as
	// enabling it on a CustomResourceDefinition does.
	statusSubresource bool

	watch *watchCache

	// cache serves reads for spec.cacheTTL when a projection asks for it, and
	// is nil otherwise.
	cache *readCache

	// flights collapse identical concurrent reads onto one round trip.
	flights *flightGroup

	// limiter bounds this projection's in-flight queries.
	limiter *crispsql.Limiter

	// finalizers is the column holding metadata.finalizers, and markDeleted the
	// statement that marks an object terminating instead of removing it. An
	// object with a finalizer is never deleted outright.
	finalizers  string
	markDeleted *compiledQuery

	// sessionVariables are set on the connection before every query, from the
	// request that caused it.
	sessionVariables []crispv1alpha1.SessionVariable

	// keysetColumn is the column a paged list resumes after. Empty means the
	// column mapping.name reads, which is what "ORDER BY id" pages on.
	keysetColumn string

	// selectable maps a field selector key onto where the value lives in the
	// object and, when the projection says so, the column holding it.
	selectable map[string]crispv1alpha1.SelectableField

	// labelColumns maps a label key onto the column holding it, so a selector
	// on that label can be answered in the database rather than after mapping.
	labelColumns map[string]string

	// validator enforces the projection's declared schema on writes. It is nil
	// only when no schema could be resolved at all, since a schema borrowed
	// with schemaFrom is read from the CustomResourceDefinition before the
	// projection is compiled.
	validator apiservervalidation.SchemaValidator

	// structural and celValidator carry the x-kubernetes-validations rules the
	// schema declares, which the OpenAPI validator alone does not evaluate.
	structural   *structuralschema.Structural
	celValidator *cel.Validator

	gvk     schema.GroupVersionKind
	listGVK schema.GroupVersionKind
}

// WritableREST adds the write verbs. A projection gets this implementation
// when it defines at least one write query; the verbs a projection did not
// define are rejected with 405 rather than silently doing nothing.
type WritableREST struct {
	*REST
}

// compiledQuery pairs a prepared statement with the parameters declared for it.
type compiledQuery struct {
	// statement produces the query's result. With a prelude it is the last one
	// to run, and the whole sequence is one transaction.
	statement  *crispsql.Statement
	prelude    []*crispsql.Statement
	parameters []crispv1alpha1.QueryParameter

	// returnsRows records whether the statement answers with the row it wrote.
	// It decides how a write is executed, and it is what makes "no rows" mean
	// "nothing matched" rather than "nothing was selected".
	returnsRows bool
}

// declares reports whether any of the query's statements binds the named
// parameter. A precondition bound in the first statement of a transaction
// counts just as much as one bound in the last.
func (c *compiledQuery) declares(name string) bool {
	if c == nil {
		return false
	}
	for _, stmt := range c.all() {
		for _, p := range stmt.Params {
			if p == name {
				return true
			}
		}
	}
	return false
}

// all returns every statement the query runs, in order.
func (c *compiledQuery) all() []*crispsql.Statement {
	// Nil-safe like declares above, because most of a projection's queries are
	// undeclared and callers iterate over the whole set.
	if c == nil {
		return nil
	}
	if len(c.prelude) == 0 {
		return []*crispsql.Statement{c.statement}
	}
	return append(append(make([]*crispsql.Statement, 0, len(c.prelude)+1), c.prelude...), c.statement)
}

// transactional reports whether this query runs as more than one statement.
func (c *compiledQuery) transactional() bool {
	return c != nil && len(c.prelude) > 0
}

// Compile-time assertions that the implementations behind the storage types in
// verbs.go are the shapes the endpoint installer expects. What a given
// projection advertises is decided there, by which of these get composed in.
var (
	_ rest.Storage                  = &REST{}
	_ rest.Scoper                   = &REST{}
	_ rest.Getter                   = &REST{}
	_ rest.Lister                   = &REST{}
	_ rest.SingularNameProvider     = &REST{}
	_ rest.GroupVersionKindProvider = &REST{}
	_ rest.Watcher                  = &REST{}

	_ rest.Creater           = &WritableREST{}
	_ rest.CollectionDeleter = &WritableREST{}
	_ rest.Updater           = &WritableREST{}
	_ rest.Patcher           = &WritableREST{}
	_ rest.GracefulDeleter   = &WritableREST{}
)

// Storages holds the storage for a projected kind and its subresources.
type Storages struct {
	// Resource serves the kind itself.
	Resource rest.Storage

	// Status serves <resource>/status, and is nil unless the projection
	// enables the subresource.
	Status rest.Storage

	// Scale serves <resource>/scale, and is nil unless the projection enables
	// the subresource.
	Scale rest.Storage

	// read and writable are the implementations behind Resource, which is one
	// of the composed types in verbs.go and deliberately exposes only the verbs
	// this projection declares. Anything that needs to call a method regardless
	// of what is advertised — the subresource storages, and the tests — goes
	// through these.
	read     *REST
	writable *WritableREST
}

// ResourceLabel is how a projected resource is named in metrics: plural.group.
//
// Every series this package publishes is keyed by it, so it is worth having one
// definition rather than several spellings that agree by habit.
func ResourceLabel(res crispv1alpha1.ProjectedResource) string {
	return res.Plural + "." + res.Group
}

// New compiles a projection into REST storage bound to a connection pool. The
// main storage is writable when the projection defines any write query.
//
// shared carries what every version of the projection has in common — the
// in-flight reads and the poll timer — so a kind served at several versions
// still reads its table once. Passing nil gives this storage its own.
func New(
	name string,
	spec crispv1alpha1.CustomResourceProjectionSpec,
	pool *crispsql.Pool,
	readPool *crispsql.Pool,
	shared *Shared,
) (*Storages, error) {
	if shared == nil {
		shared = NewShared(name, ResourceLabel(spec.Resource), nil)
	}

	mapper, err := projection.NewMapper(spec.Resource, spec.Mapping)
	if err != nil {
		return nil, err
	}

	tableConvertor, err := newTableConvertor(spec.Resource)
	if err != nil {
		return nil, err
	}

	validator, structural, err := newSchemaValidator(spec.Resource)
	if err != nil {
		return nil, err
	}

	r := &REST{
		TableConvertor: tableConvertor,
		validator:      validator,
		structural:     structural,
		projection:     name,
		label:          ResourceLabel(spec.Resource),
		resource:       spec.Resource,
		pool:           pool,
		readPool:       readPool,
		mapper:         mapper,
		failUnmappable: spec.Mapping.OnUnmappableRow == crispv1alpha1.UnmappableRowFail,
		gvk:            schema.GroupVersionKind{Group: spec.Resource.Group, Version: spec.Resource.Version, Kind: spec.Resource.Kind},
		listGVK:        schema.GroupVersionKind{Group: spec.Resource.Group, Version: spec.Resource.Version, Kind: listKind(spec.Resource)},
	}

	if r.list, err = compile(pool, &spec.Queries.List, "list"); err != nil {
		return nil, err
	}
	if r.get, err = compile(pool, spec.Queries.Get, "get"); err != nil {
		return nil, err
	}
	if r.create, err = compile(pool, spec.Queries.Create, "create"); err != nil {
		return nil, err
	}
	if r.update, err = compile(pool, spec.Queries.Update, "update"); err != nil {
		return nil, err
	}
	if r.delete, err = compile(pool, spec.Queries.Delete, "delete"); err != nil {
		return nil, err
	}
	if r.deleteCollection, err = compile(pool, spec.Queries.DeleteCollection, "deleteCollection"); err != nil {
		return nil, err
	}
	if r.markDeleted, err = compile(pool, spec.Queries.MarkDeleted, "markDeleted"); err != nil {
		return nil, err
	}
	if r.updateStatus, err = compile(pool, spec.Queries.UpdateStatus, "updateStatus"); err != nil {
		return nil, err
	}
	if r.count, err = compile(pool, spec.Queries.Count, "count"); err != nil {
		return nil, err
	}
	if spec.Watch != nil {
		if r.watchQuery, err = compile(pool, spec.Watch.Query, "watch"); err != nil {
			return nil, err
		}
		if r.deletedQuery, err = compile(pool, spec.Watch.DeletedQuery, "deletedQuery"); err != nil {
			return nil, err
		}
	}

	// Both settings belong to this projection, not to the pool, which is shared
	// by everything reaching the same database and whose defaults are whichever
	// projection opened it first. Stamped here rather than inside compile so
	// that every statement gets them, prelude statements included.
	prepared := spec.DataSource.PreparedStatements == nil || *spec.DataSource.PreparedStatements
	enforce := pool.EnforceTimeoutOn(spec.DataSource.StatementTimeout != nil && *spec.DataSource.StatementTimeout)
	for _, query := range []*compiledQuery{
		r.list, r.get, r.create, r.update, r.delete, r.deleteCollection,
		r.markDeleted, r.updateStatus, r.count, r.watchQuery, r.deletedQuery,
	} {
		for _, statement := range query.all() {
			statement.Prepared = prepared
			statement.EnforceTimeout = enforce
		}
	}

	if spec.CacheTTL != nil {
		r.cache = newReadCache(spec.CacheTTL.Duration, r.label)
	}
	r.flights = shared.flights

	// Defaults to the pool size: more in-flight queries than connections only
	// builds a queue inside database/sql.
	concurrency := crispsql.DefaultMaxOpenConns
	if spec.DataSource.MaxOpenConns != nil {
		concurrency = int(*spec.DataSource.MaxOpenConns)
	}
	if spec.DataSource.MaxConcurrentQueries != nil {
		concurrency = int(*spec.DataSource.MaxConcurrentQueries)
	}
	r.limiter = crispsql.NewLimiter(concurrency)

	// Finalizers only mean something if an object can be marked as going away
	// and left there. Without both halves a delete would remove the row while
	// its finalizers were still asking for time, which is the one outcome the
	// feature exists to prevent.
	r.finalizers = spec.Mapping.Finalizers
	if r.finalizers != "" {
		if spec.Mapping.DeletionTimestamp == "" {
			return nil, fmt.Errorf("mapping.finalizers needs mapping.deletionTimestamp: an object with a finalizer is marked as terminating rather than removed")
		}
		if r.markDeleted == nil {
			return nil, fmt.Errorf("mapping.finalizers needs queries.markDeleted: something has to write the deletion timestamp")
		}
		if r.update == nil {
			return nil, fmt.Errorf("mapping.finalizers needs queries.update: a finalizer is cleared by writing the object")
		}
	}

	if len(spec.DataSource.SessionVariables) > 0 {
		if !crispsql.SupportsSessionVariables(spec.DataSource.Driver) {
			return nil, fmt.Errorf("dataSource.sessionVariables: driver %q has no session variables", spec.DataSource.Driver)
		}
		for _, variable := range spec.DataSource.SessionVariables {
			if err := crispsql.ValidateSessionVariableName(variable.Name); err != nil {
				return nil, fmt.Errorf("dataSource.sessionVariables: %w", err)
			}
			switch variable.From {
			case crispv1alpha1.ParameterSourceValue,
				crispv1alpha1.ParameterSourceRequestNamespace,
				crispv1alpha1.ParameterSourceRequestName,
				crispv1alpha1.ParameterSourceRequestUser:
			default:
				return nil, fmt.Errorf("dataSource.sessionVariables[%s]: %q is not a source a session variable can use",
					variable.Name, variable.From)
			}
		}
		r.sessionVariables = spec.DataSource.SessionVariables

		// Watch polls on a timer, not on behalf of anyone: there is no user and
		// no namespace to set. A policy keyed on either would show the poller
		// nothing, and the cache would read that as every row having been
		// deleted. Refusing to serve watch is the only honest answer.
		if watchEnabled(spec) && !onlyConstantSessions(spec.DataSource.SessionVariables) {
			return nil, fmt.Errorf(
				"dataSource.sessionVariables that depend on the request cannot be combined with watch; set watch.disabled: true")
		}
	}

	// A list that pages by key resumes on this column's value. Defaulting to
	// the name column keeps the common "ORDER BY id" projection working
	// without saying anything, while a projection ordering by anything else
	// has to name it — the alternative is pages that silently skip rows.
	r.keysetColumn = spec.Queries.List.KeysetColumn
	if r.keysetColumn == "" {
		r.keysetColumn = spec.Mapping.Name
	}
	if r.keysetColumn == "" && r.list.declares("after") {
		// A composite identity has no single column to page on, and guessing
		// one is how pages come to skip rows.
		return nil, fmt.Errorf("queries.list: keysetColumn is required when the identity is composed of several columns")
	}

	r.selectable = map[string]crispv1alpha1.SelectableField{}
	for _, field := range spec.Resource.SelectableFields {
		key := strings.TrimPrefix(field.JSONPath, ".")
		if key == "" {
			return nil, fmt.Errorf("selectableFields: jsonPath is required")
		}
		r.selectable[key] = field
	}

	// A label with a column of its own can be filtered in the database.
	r.labelColumns = map[string]string{}
	for key, column := range spec.Mapping.Labels {
		if column != "" {
			r.labelColumns[key] = column
		}
	}

	if structural != nil {
		// Compiled once: parsing and type-checking the rules on every write
		// would dominate the cost of a small object.
		r.celValidator = cel.NewValidator(structural, true, celconfig.PerCallLimit)
	}

	r.statusSubresource = spec.Resource.Subresources != nil && spec.Resource.Subresources.Status != nil
	if r.updateStatus == nil {
		// Status usually lives in the same row, so the update statement serves
		// both unless the projection says otherwise.
		r.updateStatus = r.update
	}

	// Watch is served by polling the list query, so it costs nothing until a
	// client actually watches. Projections can opt out when that query is too
	// expensive to run on a timer.
	//
	// A projection that scopes rows to the caller cannot be watched at all, for
	// the same reason session variables that depend on the request cannot: see
	// callerScopedQueries.
	if scoped := r.callerScopedQueries(); watchEnabled(spec) && len(scoped) > 0 {
		return nil, fmt.Errorf(
			"queries that scope rows to the caller cannot be combined with watch (%s); set watch.disabled: true",
			strings.Join(scoped, "; "))
	}
	if watchEnabled(spec) {
		interval := DefaultPollInterval
		if spec.Watch != nil && spec.Watch.PollInterval != nil {
			interval = spec.Watch.PollInterval.Duration
		}
		r.watch = newWatchCache(interval, r.label, shared.polls, r.listAllNamespaces)
		r.watch.matchFields = r.matchesFields

		// With a watch query the poller reads only what changed, which is what
		// makes watching a large table affordable.
		if r.watchQuery != nil {
			r.watch.incremental = r.pollSince
			if r.deletedQuery != nil {
				r.watch.deleted = r.deletedSince

				// Keep only keys and versions, so a watched projection no
				// longer holds its whole collection in memory and no longer
				// needs maxRows above the row count.
				//
				// Both conditions are load-bearing. The diff compares the
				// mapped resourceVersion, which is the only thing kept; without
				// one it would have to compare whole objects it no longer has.
				// And a Deleted event has to carry the row, which the tombstone
				// describes — the cache being the only place a deleted object
				// existed is what made it hold them in the first place.
				//
				// The initial state of a new watcher is then read rather than
				// remembered, which is the trade: memory for a query per
				// watcher that asks for it.
				r.watch.lightweight = spec.Mapping.ResourceVersion != ""
			}
			if spec.Watch.FullResyncInterval != nil {
				r.watch.fullResyncInterval = spec.Watch.FullResyncInterval.Duration
			}
		}
		if spec.Watch != nil && spec.Watch.FollowerPollInterval != nil {
			r.watch.followerInterval = spec.Watch.FollowerPollInterval.Duration
		}
		if spec.Watch != nil && spec.Watch.BookmarkInterval != nil {
			r.watch.bookmarkInterval = spec.Watch.BookmarkInterval.Duration
		}
		if spec.Watch != nil && spec.Watch.HistorySize != nil {
			r.watch.historySize = int(*spec.Watch.HistorySize)
		}
	}

	if r.create == nil && r.update == nil && r.delete == nil && r.deleteCollection == nil {
		return &Storages{Resource: newProjectionStorage(r, nil), read: r}, nil
	}

	writable := &WritableREST{REST: r}
	writable.reportUnguardedUpdate(spec)
	writable.reportSharedColumns()

	storages := &Storages{Resource: newProjectionStorage(r, writable), read: r, writable: writable}
	if r.statusSubresource {
		storages.Status = &StatusREST{writable: writable}
	}
	if sub := spec.Resource.Subresources; sub != nil && sub.Scale != nil {
		if sub.Scale.SpecReplicasPath == "" {
			return nil, fmt.Errorf("subresources.scale: specReplicasPath is required")
		}
		storages.Scale = &ScaleREST{writable: writable, spec: *sub.Scale}
	}
	return storages, nil
}

// reportUnguardedUpdate says so when a projection's update statement cannot
// enforce the resourceVersion a client asserts with it.
//
// The precondition is checked twice, and only one of them is atomic. This
// server reads the row and compares versions in Go, which leaves a window
// before the write; the statement closes it by binding :resourceVersion, so the
// database decides. Without that bind another writer can commit inside the
// window, and because an update statement rewrites every mapped column from the
// copy this server read, the second write silently reverts the first — both
// clients having been told 200.
//
// Reported rather than refused: a projection that never sees concurrent writes
// to one object works perfectly well without it, and a single-writer controller
// is a normal way to use this. What is not acceptable is that it fails silently,
// which is what this is for. Only for projections that map a resourceVersion at
// all, since without one there is no precondition to enforce and clients cannot
// assert it.
// reportSharedColumns says so when a projection maps one column both as a
// label or annotation and as a field.
//
// Reading it twice is a reasonable thing to want: select on it as a label, show
// it as a field. Writing is where they part company, because only one of them
// can reach the column and the field is the one that does. Nothing is refused —
// the read pattern is legitimate and common — but the author is told that half
// of what they mapped is read-only, rather than finding out when kubectl says
// "labeled" and the row has not moved.
func (w *WritableREST) reportSharedColumns() {
	shared := w.mapper.SharedColumns()
	if len(shared) == 0 {
		return
	}
	klog.InfoS("projection maps a column both as metadata and as a field; writes take the field and the label or annotation is read-only",
		"projection", w.projection, "resource", w.label, "columns", shared,
		"fix", "drop one of the two mappings, or expect to change the field rather than the label")
}

func (w *WritableREST) reportUnguardedUpdate(spec crispv1alpha1.CustomResourceProjectionSpec) {
	if spec.Mapping.ResourceVersion == "" {
		return
	}

	// Both writing statements, because a controller owning status and one
	// owning spec are the pair most likely to be writing at the same time.
	var unguarded []string
	for _, q := range []struct {
		name  string
		query *compiledQuery
	}{
		{"update", w.update},
		{"updateStatus", w.updateStatus},
	} {
		if q.query != nil && !q.query.declares("resourceVersion") {
			unguarded = append(unguarded, "queries."+q.name)
		}
	}
	if len(unguarded) == 0 {
		return
	}

	crispmetrics.ProjectionsUnguardedUpdate.WithLabelValues(w.projection, w.label).Set(1)
	klog.InfoS("projection maps a resourceVersion but a writing statement does not bind it; "+
		"two writers to one object can each be told 200 while one silently reverts the other",
		"projection", w.projection, "resource", w.label, "statements", unguarded,
		"fix", "add AND resource_version_column = :resourceVersion to each, so the database "+
			"enforces the precondition and a lost race answers 409")
}

// pollSince reads the rows that changed at or after a resourceVersion, across
// every namespace. An empty version reads everything, which is how the periodic
// full resync runs.
func (r *REST) pollSince(ctx context.Context, since string) ([]unstructured.Unstructured, error) {
	args := r.builtinArgs(ctx, "")
	args["since"] = nullIfEmpty(since)
	// Bound for the same reason a list is: MySQL accepts no expression after
	// LIMIT, and a NULL there is a syntax error rather than "no limit".
	args["limit"] = int64(r.watchQuery.statement.MaxRows) + 1
	if err := r.applyParameters(ctx, args, r.watchQuery.parameters, "", "", nil); err != nil {
		return nil, err
	}

	start := time.Now()
	// Shared: every version of this projection polls the same statement on the
	// same tick, so one query answers all of them.
	rows, err := r.query(ctx, r.watchQuery.statement, args, "", r.session(ctx, "", ""), shared)
	r.observe("watch", start, len(rows), err)
	if err != nil {
		return nil, fmt.Errorf("polling %s: %w", r.resource.Plural, err)
	}

	// As in a list, a row that cannot be mapped is skipped rather than failing
	// the poll — one bad row would otherwise stop every watcher on the
	// projection from seeing any change at all. There is no request behind a
	// poll to warn, so the metric and the log are what report it.
	items := make([]unstructured.Unstructured, 0, len(rows))
	var skipped int
	for i, row := range rows {
		obj, err := r.mapper.Row(row)
		if err != nil {
			if r.failUnmappable {
				crispmetrics.RowsUnmappable.WithLabelValues(r.projection, r.label).Inc()
				return nil, fmt.Errorf("row %d could not be mapped onto a %s and "+
					"mapping.onUnmappableRow is Fail: %w", i, r.resource.Kind, err)
			}
			skipped++
			klog.V(4).InfoS("skipping a row a poll could not map",
				"resource", r.label, "row", i, "err", err)
			continue
		}
		items = append(items, *obj)
	}
	if skipped > 0 {
		crispmetrics.RowsUnmappable.WithLabelValues(r.projection, r.label).Add(float64(skipped))
	}
	return items, nil
}

// deletedSince reads the rows removed at or after a resourceVersion, across
// every namespace.
//
// The identity is what this needs; the rest of the row is taken too when the
// tombstone carries it.
//
// A tombstone recording only the identity leaves the cache as the only place a
// deleted object exists, so a client resuming against a restarted server — or
// against a replica it has not spoken to — is told a name and nothing else. A
// tombstone that records the mapped columns describes the row itself, and then
// the deletion can be answered from the table.
//
// Best effort, and deliberately: a tombstone table holding only the identity
// columns is still the documented minimum, and mapping one is expected to fail.
func (r *REST) deletedSince(ctx context.Context, since string) ([]cacheIdentity, error) {
	args := r.builtinArgs(ctx, "")
	args["since"] = nullIfEmpty(since)
	args["limit"] = int64(r.deletedQuery.statement.MaxRows) + 1
	if err := r.applyParameters(ctx, args, r.deletedQuery.parameters, "", "", nil); err != nil {
		return nil, err
	}

	start := time.Now()
	rows, err := r.query(ctx, r.deletedQuery.statement, args, "", r.session(ctx, "", ""), shared)
	r.observe("watchDeleted", start, len(rows), err)
	if err != nil {
		return nil, fmt.Errorf("reading deletions from %s: %w", r.resource.Plural, err)
	}

	out := make([]cacheIdentity, 0, len(rows))
	for i, row := range rows {
		name, err := r.mapper.NameFrom(row)
		if err != nil {
			// As elsewhere, a row that cannot be identified is skipped rather
			// than stopping every watcher on the projection.
			crispmetrics.RowsUnmappable.WithLabelValues(r.projection, r.label).Inc()
			klog.V(4).InfoS("skipping a deletion row that could not be identified",
				"resource", r.label, "row", i, "err", err)
			continue
		}

		identity := cacheIdentity{name: name}

		// Not counted as unmappable when this fails: a tombstone holding only
		// the identity columns is doing exactly what it is meant to.
		if obj, err := r.mapper.Row(row); err == nil {
			identity.object = obj
		}

		if r.NamespaceScoped() {
			namespace, err := r.mapper.NamespaceFrom(row)
			if err != nil {
				crispmetrics.RowsUnmappable.WithLabelValues(r.projection, r.label).Inc()
				klog.V(4).InfoS("skipping a deletion row with no namespace",
					"resource", r.label, "row", i, "err", err)
				continue
			}
			identity.namespace = namespace
		}
		out = append(out, identity)
	}
	return out, nil
}

// listAllNamespaces feeds the watch cache. It passes a NULL namespace so a
// single query covers every namespace; a projection whose list query cannot
// handle that returns nothing and is effectively unwatchable.
func (r *REST) listAllNamespaces(ctx context.Context) ([]unstructured.Unstructured, error) {
	list, err := r.listObjects(ctx, "", nil, shared)
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// newSchemaValidator compiles the projection's declared schema into a validator
// so that writes are checked against the shape the projection publishes, rather
// than being passed through to the database unexamined.
func newSchemaValidator(res crispv1alpha1.ProjectedResource) (apiservervalidation.SchemaValidator, *structuralschema.Structural, error) {
	if res.Schema == nil {
		return nil, nil, nil
	}

	internal := &apiextensions.JSONSchemaProps{}
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(res.Schema, internal, nil); err != nil {
		return nil, nil, fmt.Errorf("converting the projected schema: %w", err)
	}

	validator, _, err := apiservervalidation.NewSchemaValidator(internal)
	if err != nil {
		return nil, nil, fmt.Errorf("compiling the projected schema: %w", err)
	}

	// A structural schema is what CEL rules and field management are defined
	// against. A schema that is not structural still validates; it just cannot
	// carry rules.
	structural, err := structuralschema.NewStructural(internal)
	if err != nil {
		klog.V(2).InfoS("the projected schema is not structural, so validation rules are not evaluated",
			"resource", res.Plural+"."+res.Group, "err", err)
		return validator, nil, nil
	}
	return validator, structural, nil
}

// newTableConvertor renders the columns the projection declares, so kubectl
// prints them instead of only NAME. Without any declared columns the default
// convertor is used, which prints name and age.
func newTableConvertor(res crispv1alpha1.ProjectedResource) (rest.TableConvertor, error) {
	if len(res.AdditionalPrinterColumns) == 0 {
		return rest.NewDefaultTableConvertor(schema.GroupResource{Group: res.Group, Resource: res.Plural}), nil
	}

	convertor, err := tableconvertor.New(res.AdditionalPrinterColumns)
	if err != nil {
		return nil, fmt.Errorf("building printer columns: %w", err)
	}
	return convertor, nil
}

func compile(pool *crispsql.Pool, query *crispv1alpha1.Query, verb string) (*compiledQuery, error) {
	if query == nil {
		return nil, nil
	}

	sources, err := statementsOf(query, verb)
	if err != nil {
		return nil, err
	}

	compiled := make([]*crispsql.Statement, 0, len(sources))
	for i, source := range sources {
		statement, err := pool.Prepare(source, durationOf(query.Timeout), maxRowsOf(query.MaxRows))
		if err != nil {
			return nil, fmt.Errorf("compiling %s query: %w", verb, err)
		}
		if declared := maxBytesOf(query.MaxBytes); declared > 0 {
			statement.MaxBytes = declared
		}
		if last := i == len(sources)-1; last {
			if query.ResultFormat == crispv1alpha1.ResultFormatJSONArray {
				statement.Format = crispsql.FormatJSONArray
			}
			statement.ReturnsRows = crispsql.HasReturning(source, pool.Driver())
		} else if crispsql.HasReturning(source, pool.Driver()) {
			// Its rows would be discarded, which is never what was meant.
			return nil, fmt.Errorf("%s query: only the last statement may return rows, but statement %d does", verb, i+1)
		}
		compiled = append(compiled, statement)
	}

	last := compiled[len(compiled)-1]
	return &compiledQuery{
		statement:   last,
		prelude:     compiled[:len(compiled)-1],
		parameters:  query.Parameters,
		returnsRows: last.ReturnsRows,
	}, nil
}

// statementsOf returns the statements a query runs, in order, rejecting the
// combinations that cannot mean anything.
func statementsOf(query *crispv1alpha1.Query, verb string) ([]string, error) {
	switch {
	case query.SQL != "" && len(query.Statements) > 0:
		return nil, fmt.Errorf("%s query: set either sql or statements, not both", verb)
	case query.SQL == "" && len(query.Statements) == 0:
		return nil, fmt.Errorf("%s query: sql or statements is required", verb)
	case len(query.Statements) > 0 && !transactionalVerbs.Has(verb):
		return nil, fmt.Errorf("%s query: statements is only supported for writes (%s)",
			verb, strings.Join(sets.List(transactionalVerbs), ", "))
	case query.SQL != "":
		return []string{query.SQL}, nil
	default:
		return query.Statements, nil
	}
}

// transactionalVerbs are the queries that may run as a transaction. Reads are
// excluded on purpose: they are shared between concurrent requests and may be
// answered from a cache, so a transaction around one would be a promise the
// projection cannot keep.
var transactionalVerbs = sets.New("create", "update", "updateStatus", "delete", "deleteCollection", "markDeleted")

// New returns an empty object of the projected kind.
func (r *REST) New() runtime.Object {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.gvk)
	return obj
}

// NewList returns an empty list of the projected kind.
func (r *REST) NewList() runtime.Object {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(r.listGVK)
	return list
}

// Destroy releases resources owned by this storage. Connection pools are owned
// by the pool cache and shared between projections, so only the watch cache is
// torn down here.
func (r *REST) Destroy() {
	if r.watch != nil {
		r.watch.Close()
	}
}

// NamespaceScoped reports whether the projected kind lives in namespaces.
func (r *REST) NamespaceScoped() bool {
	return r.resource.Scope == crispv1alpha1.NamespaceScoped
}

// ShortNames advertises the projection's kubectl abbreviations.
//
// The endpoint installer reads these off the storage through
// rest.ShortNamesProvider and nowhere else, so a resource that does not
// implement it advertises none however many the spec declares — which is what
// happened here: shortNames and categories reached the synthetic CRD used for
// OpenAPI and stopped there, so "kubectl get ord" answered "the server doesn't
// have a resource type" for a projection that declared it.
func (r *REST) ShortNames() []string { return r.resource.ShortNames }

// Categories advertises the kubectl categories the projection belongs to, so a
// projection declaring "all" appears in kubectl get all. Read through
// rest.CategoriesProvider, the same way.
func (r *REST) Categories() []string { return r.resource.Categories }

// GetSingularName returns the singular resource name for kubectl.
func (r *REST) GetSingularName() string {
	if r.resource.Singular != "" {
		return r.resource.Singular
	}
	return strings.ToLower(r.resource.Kind)
}

// GroupVersionKind reports the kind served, which the endpoint installer uses
// to set apiVersion and kind on responses.
func (r *REST) GroupVersionKind(schema.GroupVersion) schema.GroupVersionKind {
	return r.gvk
}

// replicaRetryAfter is how long the read replica is left alone once a read has
// found it unreachable.
//
// Without a pause every read pays the replica's failure before falling back,
// which makes a replica that is down slower than no replica at all. With one,
// the cost of an outage is one failed read per interval.
const replicaRetryAfter = 10 * time.Second

// readMode says whether a read may be answered by something other than a
// query made for it.
type readMode bool

const (
	// shared lets a read come from the cache, or join a query already running.
	shared readMode = false

	// fresh insists on a round trip. Writes read this way: an optimistic
	// concurrency check against a cached resourceVersion is a check against
	// something that may already have moved, and a status merge onto a cached
	// object writes back a spec that may already have been replaced.
	fresh readMode = true
)

// Get answers a single-object read.
//
// A resourceVersion on the request is a floor, not a selector: it says the
// client has already seen that version and must not be handed anything older.
// Nothing here can reconstruct a past version of a row, so the only thing that
// can be too old is a cached copy — and that is what the version turns away.
// List has said this about a cached page since caching arrived; a client
// reading one object had the same request ignored.
func (r *REST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	notOlderThan := ""
	if options != nil {
		notOlderThan = options.ResourceVersion
	}
	return r.read(ctx, name, shared, notOlderThan)
}

// read answers a single-object read in the given mode, and records it.
//
// notOlderThan is the version a cached copy has to have reached to be usable,
// and is empty for the reads that go to the database whatever it says.
func (r *REST) read(ctx context.Context, name string, mode readMode, notOlderThan string) (runtime.Object, error) {
	ctx, done := r.startQuery(ctx, "get")
	obj, rows, err := r.getObject(ctx, name, mode, notOlderThan)
	done(rows, err)
	return obj, err
}

// getObject answers a single-object read, reporting how many rows it read to
// do it. That is not always one: a projection with no get query filters the
// list instead, and the whole collection is what it cost.
func (r *REST) getObject(ctx context.Context, name string, mode readMode, notOlderThan string) (runtime.Object, int, error) {
	namespace := namespaceFrom(ctx, r.NamespaceScoped())
	session := r.session(ctx, namespace, name)

	// The get query's parameters when there is one; otherwise the list's, since
	// a projection with no get query filters the list instead and it is those
	// bindings that scoped the row.
	callerQuery := r.list
	if r.get != nil {
		callerQuery = r.get
	}
	key := objectKey(namespace, name) + sessionKey(session) + r.callerKey(ctx, callerQuery)
	if mode == shared {
		// A cached object may be older than the version the client insisted
		// on, and then it is not an answer to the request that was made. The
		// same check List makes on a cached page, for the same reason.
		if cached, ok := r.cache.getObject(key); ok && freshEnoughVersion(cached.GetResourceVersion(), notOlderThan) {
			// Answered without touching the database; the cache has its own
			// metric and this one counts round trips.
			return cached, 0, nil
		}
	}

	// Without a dedicated get query, fall back to filtering the list results.
	// This is correct but reads more rows than necessary, so a get query is
	// recommended for any projection over a large table.
	if r.get == nil {
		list, err := r.listObjects(ctx, namespace, nil, mode)
		if err != nil {
			return nil, 0, err
		}
		read := len(list.Items)
		for i := range list.Items {
			if list.Items[i].GetName() == name {
				return &list.Items[i], read, nil
			}
		}
		return nil, read, errors.NewNotFound(r.groupResource(), name)
	}

	args := r.builtinArgs(ctx, namespace)
	args["name"] = name

	// A composite identity is bound one column at a time, so the query can ask
	// for the row the name refers to. A name that does not split into those
	// columns names no row at all.
	identity, err := r.mapper.SplitName(name)
	if err != nil {
		return nil, 0, errors.NewNotFound(r.groupResource(), name)
	}
	for column, value := range identity {
		args[column] = value
	}

	if err := r.applyParameters(ctx, args, r.get.parameters, namespace, name, nil); err != nil {
		return nil, 0, errors.NewInternalError(err)
	}

	rows, err := r.query(ctx, r.get.statement, args, namespace, session, mode)
	if err != nil {
		return nil, 0, r.queryError(err, "getting")
	}
	if len(rows) == 0 {
		return nil, 0, errors.NewNotFound(r.groupResource(), name)
	}
	if len(rows) > 1 {
		return nil, len(rows), errors.NewInternalError(fmt.Errorf("get query for %s/%s returned %d rows; it must return at most one",
			namespace, name, len(rows)))
	}

	obj, err := r.mapper.Row(rows[0])
	if err != nil {
		return nil, len(rows), errors.NewInternalError(fmt.Errorf("mapping row: %w", err))
	}

	// The row the query found, from another namespace. Not found is what a
	// namespaced read is entitled to hear about it: see listWith, where the
	// same thing is done to a collection.
	if namespace != "" && obj.GetNamespace() != namespace {
		crispmetrics.RowsOutOfNamespace.WithLabelValues(r.projection, r.label).Inc()
		klog.InfoS("refused a row the query returned from another namespace; the get query does not "+
			"filter on the column mapped to metadata.namespace",
			"resource", r.label, "namespace", namespace, "found", obj.GetNamespace(),
			"fix", "add the namespace to the query's WHERE clause, as \"tenant = :namespace\" or the equivalent")
		return nil, len(rows), errors.NewNotFound(r.groupResource(), name)
	}

	if mode == shared {
		r.cache.putObject(key, namespace, obj)
	}
	return obj, len(rows), nil
}

// List answers a collection read.
func (r *REST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	namespace := namespaceFrom(ctx, r.NamespaceScoped())

	if err := r.checkResourceVersionMatch(options); err != nil {
		return nil, err
	}

	// A cached page may be older than the version the client insists on.
	session := r.session(ctx, namespace, "")
	key := listKey(namespace, options) + sessionKey(session) + r.callerKey(ctx, r.list)
	if cached, ok := r.cache.getList(key); ok && r.freshEnough(cached, options) {
		return cached, nil
	}

	ctx, done := r.startQuery(ctx, "list")
	list, err := r.listWith(ctx, namespace, options, shared, session)
	if err != nil {
		done(0, err)
		return nil, err
	}
	done(len(list.Items), nil)

	// Stamp the watch cache's version so a client that lists and then watches
	// resumes from this point instead of replaying the collection. Only List
	// does this: the cache calls listObjects itself while holding its own lock.
	//
	// Before the collection is cached, not after, and for two reasons. A hit
	// would otherwise answer with no resourceVersion at all, leaving a client
	// nothing to resume a watch from — so an informer would replay the whole
	// collection on every resync, which is the opposite of what caching it was
	// for. And the version belongs to the moment the rows were read: stamping a
	// cached page with the version it is handed out at would date stale data to
	// now, which is a stronger claim than the cache can make.
	// The watch cache's version when there is one, because that is the point a
	// watch can resume from. Otherwise — watch disabled, or the cache could not
	// be primed — the newest version among the rows just read.
	version := ""
	if r.watch != nil {
		version = r.watch.versionFor(ctx)
	}
	if version == "" {
		version = highestVersion(list.Items)
	}
	list.SetResourceVersion(version)

	// The cache takes the collection; what comes back is a view over it.
	return r.cache.putList(key, namespace, list), nil
}

// highestVersion is the newest resourceVersion among the rows in a response, or
// "" if none of them carry one.
//
// It is what a projection with watch.disabled reports as its list version.
// There is no poll loop for these, so there is no watermark to quote, and until
// now they answered with no list resourceVersion at all — which the API
// contract does not allow ListMeta to omit, and which leaves a client with
// nothing to pass back on a subsequent read.
//
// The high-water mark of the returned rows is the honest answer to that. It is
// a version this server has actually observed for this collection, drawn from
// the same mapped column watch would have used, so a client passing it back
// gets data at least as new. Two things it deliberately is not: it is not a
// resumable watch point — these projections do not advertise watch at all, so
// nothing can try (see verbs.go) — and it is not the collection maximum when
// the response was narrowed by a selector or a page limit, only the maximum of
// what was returned. Understating it is the safe direction; a client is told
// the data is no newer than it is.
//
// Empty when the projection maps no resourceVersion, because then there is
// genuinely no version anywhere to report.
func highestVersion(items []unstructured.Unstructured) string {
	var highest string
	for i := range items {
		if version := items[i].GetResourceVersion(); movesForward(highest, version) {
			highest = version
		}
	}
	return highest
}

// pageSize is how many rows to read to serve a page of limit rows.
//
// One more than the page, because that extra row is what reveals whether
// another page exists — without it the last page and a full one look alike and
// a client is told it has seen everything one page early.
//
// Asking for one more is not always possible. limit is whatever the client
// sent, ListOptions bounds it at nothing, and MaxInt64 + 1 wraps to a negative
// bind value: PostgreSQL refuses a negative LIMIT outright and SQLite reads it
// as no limit at all, so an absurd page size answered with a 500 or with the
// whole collection rather than with the page that was asked for. A limit that
// large already covers every row the statement may return, so there is nothing
// beyond it to look for and the bound is simply the largest one there is.
func pageSize(limit int64) int64 {
	if limit == math.MaxInt64 {
		return math.MaxInt64
	}
	return limit + 1
}

func (r *REST) listObjects(ctx context.Context, namespace string, options *metainternalversion.ListOptions, mode readMode) (*unstructured.UnstructuredList, error) {
	return r.listWith(ctx, namespace, options, mode, r.session(ctx, namespace, ""))
}

// listWith is listObjects with the request's session variables already
// resolved, which the callers that also need them for a cache key have in hand.
func (r *REST) listWith(
	ctx context.Context,
	namespace string,
	options *metainternalversion.ListOptions,
	mode readMode,
	session []crispsql.SessionVariable,
) (*unstructured.UnstructuredList, error) {
	args := r.builtinArgs(ctx, namespace)

	// Paging is only offered when the projection's query can actually skip
	// rows, either by key or by offset. Applying a limit without one would
	// silently truncate the collection and tell the client it had seen
	// everything.
	keyset := r.list.declares("after")
	pageable := keyset || r.list.declares("offset")
	paging := pageable && options != nil && options.Limit > 0

	// A continue token with no limit is a request in its own right, not a
	// malformed page: ValidateListOptions accepts it — only pairing continue
	// with resourceVersionMatch is rejected — and the etcd store answers it by
	// resuming where the token points and returning everything that is left.
	// Treating it as an unpaged list instead would quietly serve page one over
	// again, so a client that dropped the limit on its follow-up request would
	// see every item it already had a second time and never learn it had.
	resuming := pageable && options != nil && options.Limit <= 0 && options.Continue != ""

	var (
		limit    int64
		token    continueToken
		consumed int64
	)
	switch {
	case paging || resuming:
		var err error
		if token, err = decodeContinue(options.Continue); err != nil {
			return nil, errors.NewBadRequest(err.Error())
		}
		consumed = token.Consumed

		if paging {
			limit = options.Limit
			args["limit"] = pageSize(limit)
		} else {
			// Resuming without a limit asks for the whole remainder in one
			// answer, so the only bound left is the one an unpaged list uses.
			args["limit"] = int64(r.list.statement.MaxRows) + 1
		}

		if keyset {
			// Keyset paging resumes after the last row of the previous page,
			// so rows inserted meanwhile cannot shift the window.
			args["after"] = token.After
		} else {
			args["offset"] = token.Offset
		}
	default:
		// Bound rather than left NULL: MySQL and SQLite accept no expression
		// after LIMIT, so "LIMIT :limit" has to be usable without paging. One
		// past maxRows keeps the overflow check meaningful, so an oversized
		// collection still errors instead of being silently truncated.
		args["limit"] = int64(r.list.statement.MaxRows) + 1
	}

	if options != nil && options.LabelSelector != nil {
		args["labelSelector"] = options.LabelSelector.String()
	}
	if options != nil {
		if err := r.validateFieldSelector(options.FieldSelector); err != nil {
			return nil, err
		}
		r.bindSelectableFields(args, options.FieldSelector)
		r.bindIdentitySelector(args, options.FieldSelector, namespace)
		r.bindLabelSelector(args, options.LabelSelector)
	} else {
		// Bound even with no options at all, so the same statement serves a
		// watch poll and a plain list.
		r.bindSelectableFields(args, nil)
		r.bindLabelSelector(args, nil)
	}
	if err := r.applyParameters(ctx, args, r.list.parameters, namespace, "", nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	// A paged list reads its count in the same breath, so the two describe one
	// moment: rows inserted between them would otherwise make the client's
	// remainingItemCount disagree with the page it is holding.
	statements := []*crispsql.Statement{r.list.statement}
	counted := paging && r.count != nil
	if counted {
		statements = append(statements, r.count.statement)
	}

	results, err := r.queryAll(ctx, statements, args, namespace, session, mode)
	if err != nil {
		return nil, r.queryError(err, "listing")
	}
	rows := results[0]
	var countRows []crispsql.Row
	if counted {
		countRows = results[1]
	}

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(r.listGVK)

	// Only a limited request can have a next page: resuming without one
	// returned everything that was left, so there is no token to hand back.
	more := false
	if paging && int64(len(rows)) > limit {
		rows = rows[:limit]
		more = true
	}
	list.Items = make([]unstructured.Unstructured, 0, len(rows))

	// The keyset resumes after the last row that was read, not the last one
	// that survived filtering, or a filtered-out row would be read again on
	// every subsequent page. Only a limited request can hand back a token, so
	// only a limited request needs the key at all.
	var lastKey any
	needKey := keyset && paging

	// A row that cannot be mapped is skipped rather than failing the whole
	// read: one row whose name is not a valid object name would otherwise make
	// the entire collection unreadable, with no way to see the rest of it. The
	// client is told what it did not get, in a warning and in a metric.
	var (
		skipped   int
		firstSkip error
	)

	// Rows the query returned from somewhere else. A namespaced read must only
	// ever answer with rows in that namespace: it is what makes ordinary
	// namespace RBAC apply to database rows, which is the reason
	// mapping.namespace exists at all. The filter belongs in the query, but a
	// query that forgets it — a rename, a refactor, a clause copied from a
	// cluster-wide read — would otherwise hand every caller the whole table
	// with nothing anywhere saying so.
	var foreign int

	for i, row := range rows {
		obj, mapErr := r.mapper.Row(row)

		// The keyset resumes after the last row that was read, including one
		// that could not be mapped — otherwise every later page reads it again.
		//
		// Not read at all when the request cannot produce a token. The key is
		// allowed to fail — a NULL one cannot anchor a page, and pretending
		// otherwise would build a token that skips or repeats rows — so a row
		// with no key has to take the read down with it while paging. Deriving
		// it anyway and discarding it made that failure reach requests that
		// were never going to page: one row with a NULL keyset column turned
		// every unpaged list into a 500, and listAllNamespaces goes through
		// here too, so the watch cache never primed and the projection could
		// not be watched at all.
		if needKey {
			key, err := r.keysetValue(row, obj)
			if err != nil {
				return nil, errors.NewInternalError(err)
			}
			lastKey = key
		}

		if mapErr != nil {
			if r.failUnmappable {
				crispmetrics.RowsUnmappable.WithLabelValues(r.projection, r.label).Inc()
				return nil, errors.NewInternalError(fmt.Errorf(
					"row %d could not be mapped onto a %s and mapping.onUnmappableRow is Fail: %w",
					i, r.resource.Kind, mapErr))
			}
			skipped++
			if firstSkip == nil {
				firstSkip = fmt.Errorf("row %d: %w", i, mapErr)
			}
			continue
		}

		// Label selection happens here unless the projection pushed the
		// selector down into SQL via a labelSelector parameter.
		if options != nil && options.LabelSelector != nil &&
			!options.LabelSelector.Matches(labels.Set(obj.GetLabels())) {
			continue
		}
		if options != nil && !r.matchesFields(obj, options.FieldSelector) {
			continue
		}
		if namespace != "" && obj.GetNamespace() != namespace {
			foreign++
			continue
		}
		list.Items = append(list.Items, *obj)
	}

	if foreign > 0 {
		crispmetrics.RowsOutOfNamespace.WithLabelValues(r.projection, r.label).Add(float64(foreign))
		klog.InfoS("dropped rows the query returned from another namespace; the list query does not "+
			"filter on the column mapped to metadata.namespace",
			"resource", r.label, "namespace", namespace, "rows", foreign,
			"fix", "add the namespace to the query's WHERE clause, as \"tenant = :namespace\" or the equivalent")
		warning.AddWarning(ctx, "", fmt.Sprintf(
			"%d row(s) the query returned were outside namespace %q and were not served; "+
				"the list query does not filter on the column mapped to metadata.namespace",
			foreign, namespace))
	}

	if skipped > 0 {
		crispmetrics.RowsUnmappable.WithLabelValues(r.projection, r.label).Add(float64(skipped))
		klog.V(2).InfoS("skipped rows that could not be mapped onto objects",
			"resource", r.label, "rows", skipped, "first", firstSkip)
		warning.AddWarning(ctx, "", fmt.Sprintf(
			"%d row(s) were skipped because they could not be mapped onto %s objects: %v",
			skipped, r.resource.Kind, firstSkip))
	}

	if more {
		next := continueToken{Consumed: consumed + int64(len(rows))}
		if keyset {
			next.After = lastKey
		} else {
			next.Offset = token.Offset + limit
		}
		list.SetContinue(encodeContinue(next))

		// remainingItemCount is advisory, so a count that could not be read
		// leaves it unset rather than failing the list.
		if remaining, ok := r.remainingItems(countRows, next.Consumed); ok {
			list.SetRemainingItemCount(&remaining)
		}
	}
	return list, nil
}

// watchEnabled reports whether the projection serves watch.
//
// Shared with anything generating RBAC for a projection, which has to grant
// exactly the verbs discovery advertises and so has to agree with this.
func watchEnabled(spec crispv1alpha1.CustomResourceProjectionSpec) bool {
	return projection.WatchEnabled(spec)
}

// onlyConstantSessions reports whether every session variable has a value that
// does not depend on the request, which is what makes it safe to set on a poll.
func onlyConstantSessions(variables []crispv1alpha1.SessionVariable) bool {
	for _, variable := range variables {
		if variable.From != crispv1alpha1.ParameterSourceValue {
			return false
		}
	}
	return true
}

// session resolves this request's session variables.
//
// They are part of what a query means: two requests that differ only in the
// tenant they set are different queries, and the values are folded into the
// cache and coalescing keys for exactly that reason.
func (r *REST) session(ctx context.Context, namespace, name string) []crispsql.SessionVariable {
	if len(r.sessionVariables) == 0 {
		return nil
	}

	caller := callerArgs(ctx)
	text := func(key string) string {
		if value, ok := caller[key].(string); ok {
			return value
		}
		return ""
	}

	out := make([]crispsql.SessionVariable, 0, len(r.sessionVariables))
	for _, variable := range r.sessionVariables {
		value := ""
		switch variable.From {
		case crispv1alpha1.ParameterSourceValue:
			value = variable.Value
		case crispv1alpha1.ParameterSourceRequestNamespace:
			value = namespace
		case crispv1alpha1.ParameterSourceRequestName:
			value = name
		case crispv1alpha1.ParameterSourceRequestUser:
			value = text("user")
		case crispv1alpha1.ParameterSourceRequestUserUID:
			value = text("userUID")
		case crispv1alpha1.ParameterSourceRequestUserGroups:
			value = text("userGroups")
		case crispv1alpha1.ParameterSourceRequestUserExtra:
			value = text("userExtra")
		}
		out = append(out, crispsql.SessionVariable{Name: variable.Name, Value: value})
	}
	return out
}

// sessionKey renders the resolved session as part of a cache or flight key.
// Nothing may be shared across a difference in it.
// callerKey is the part of a read-cache key that depends on who is asking.
//
// A projection may scope rows by binding the authenticated identity into its
// query — customer = :caller — rather than through row-level security. The rest
// of the key covers the request: namespace, selectors, limit, continue, and the
// session variables. None of that distinguishes two callers whose requests are
// otherwise identical, so without this the second is served the first's rows
// and their own binding never reaches the database.
//
// flightKey already makes this distinction for in-flight coalescing, and its
// comment names the same case. This is the cache's half of it.
//
// Empty unless a query actually declares a caller-derived parameter, so a
// projection that does not scope by identity pays nothing and keys exactly as
// before.
func (r *REST) callerKey(ctx context.Context, query *compiledQuery) string {
	bindings := query.callerBindings()
	if len(bindings) == 0 {
		return ""
	}

	caller := callerArgs(ctx)
	var b strings.Builder
	for _, binding := range bindings {
		b.WriteByte(0)
		b.WriteString(binding.name)
		b.WriteByte('=')
		writeBound(&b, caller[binding.source])
	}
	return b.String()
}

// callerScopedQueries names the reads whose rows depend on who is asking.
//
// The cache behind a watch is filled by a single query and shared by every
// watcher. There is no request behind a poll, so the query runs with whatever
// context the first watcher brought — and every watcher after that is served
// the rows it returned. For a projection scoping rows to the caller that is a
// cross-tenant leak, and a lasting one: the stream keeps delivering another
// caller's rows for as long as it is open. The read cache keys on the caller
// bindings to prevent exactly this; a watch has nothing to key, because there
// is only one poller.
//
// Session variables that depend on the request are already refused alongside
// watch a few lines above, and this is the same rule for the other way of
// scoping rows. Refused rather than served wrong, and refused at load time
// rather than per request, so it is the projection that fails and not a client.
func (r *REST) callerScopedQueries() []string {
	var reasons []string

	for _, feeding := range []struct {
		name  string
		query *compiledQuery
	}{
		{"list", r.list},
		{"watch.query", r.watchQuery},
		{"watch.deletedQuery", r.deletedQuery},
	} {
		bindings := feeding.query.callerBindings()
		if len(bindings) == 0 {
			continue
		}
		bound := make([]string, 0, len(bindings))
		for _, binding := range bindings {
			bound = append(bound, ":"+binding.name)
		}
		sort.Strings(bound)
		reasons = append(reasons, fmt.Sprintf("the %s query binds %s", feeding.name, strings.Join(bound, ", ")))
	}

	return reasons
}

// callerBinding is one caller-derived value a query binds: the name it is bound
// under, and which part of the caller it comes from.
type callerBinding struct {
	name   string
	source string
}

// builtinCallerArgs are bound on every query whether or not it declares
// anything, which is what makes them the documented way to scope rows to the
// caller — "always available", per the reference.
var builtinCallerArgs = map[string]bool{
	"user": true, "userUID": true, "userGroups": true, "userExtra": true,
}

// callerBindings names every caller-derived value this query actually binds.
//
// Both ways of binding one count, and the second is the reason this exists.
// Declaring a parameter with from: RequestUser is one way; writing :user
// straight into the statement is the other, and it is the one the reference and
// the shipped example use. Keying the read cache off the declared parameters
// alone therefore gave every caller the same key for the second form, and the
// second caller was served the first one's rows for the whole cacheTTL.
//
// Derived from what will be bound rather than from what was declared, which is
// the same rule flightKey already follows for in-flight coalescing.
func (c *compiledQuery) callerBindings() []*callerBinding {
	if c == nil {
		return nil
	}

	seen := map[string]bool{}
	var out []*callerBinding
	add := func(name, source string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		out = append(out, &callerBinding{name: name, source: source})
	}

	for _, p := range c.parameters {
		switch p.From {
		case crispv1alpha1.ParameterSourceRequestUser:
			add(p.Name, "user")
		case crispv1alpha1.ParameterSourceRequestUserUID:
			add(p.Name, "userUID")
		case crispv1alpha1.ParameterSourceRequestUserGroups:
			add(p.Name, "userGroups")
		case crispv1alpha1.ParameterSourceRequestUserExtra:
			add(p.Name, "userExtra")
		}
	}
	for _, stmt := range c.all() {
		for _, name := range stmt.Params {
			if builtinCallerArgs[name] {
				add(name, name)
			}
		}
	}

	// Stable, because this builds a cache key.
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func sessionKey(session []crispsql.SessionVariable) string {
	if len(session) == 0 {
		return ""
	}

	var b strings.Builder
	for _, variable := range session {
		b.WriteByte(0)
		b.WriteString(variable.Name)
		b.WriteByte('=')
		b.WriteString(variable.Value)
	}
	return b.String()
}

// keysetValue reads the value the next page resumes after.
//
// It comes from the row rather than the object, because the column a query
// orders by need not appear in the projected object at all — an internal
// sequence number is a good key and a poor field.
func (r *REST) keysetValue(row crispsql.Row, obj *unstructured.Unstructured) (any, error) {
	if r.keysetColumn == "" {
		if obj == nil {
			// The default key is the name, and the name comes from the mapping
			// that just failed. Paging past this row would mean guessing where
			// the next page starts, so the read fails instead.
			return nil, fmt.Errorf(
				"a row could not be mapped and the list pages on its name; set queries.list.keysetColumn to page on a column instead")
		}
		return obj.GetName(), nil
	}

	value, ok := row[r.keysetColumn]
	if !ok {
		return nil, fmt.Errorf("keysetColumn %q is not returned by the list query", r.keysetColumn)
	}
	if value == nil {
		return nil, fmt.Errorf("keysetColumn %q is NULL in a row; a paging key has to have a value", r.keysetColumn)
	}

	// MySQL hands back text columns as bytes, which would travel through the
	// continue token as base64 and come back as something the database has
	// never seen.
	if raw, ok := value.([]byte); ok {
		return string(raw), nil
	}
	return value, nil
}

// checkResourceVersionMatch answers what a projection can honestly promise
// about the freshness of a read.
//
// Every read goes to the database, so "not older than" is always satisfied. An
// exact past version cannot be reconstructed from the current state of a table,
// so asking for one is refused rather than answered with something else.
func (r *REST) checkResourceVersionMatch(options *metainternalversion.ListOptions) error {
	if options == nil || options.ResourceVersionMatch == "" {
		return nil
	}

	switch options.ResourceVersionMatch {
	case metav1.ResourceVersionMatchNotOlderThan:
		return nil
	case metav1.ResourceVersionMatchExact:
		if options.ResourceVersion == "" || options.ResourceVersion == "0" {
			return errors.NewBadRequest("resourceVersionMatch=Exact requires a resourceVersion")
		}
		return errors.NewResourceExpired(fmt.Sprintf(
			"resourceVersionMatch=Exact is not supported: %s is projected from a database and can only be read as it is now",
			r.groupResource().String()))
	default:
		return errors.NewBadRequest(fmt.Sprintf("unsupported resourceVersionMatch %q", options.ResourceVersionMatch))
	}
}

// freshEnough reports whether a cached page still satisfies the version the
// client asked not to read behind.
func (r *REST) freshEnough(list *unstructured.UnstructuredList, options *metainternalversion.ListOptions) bool {
	if options == nil {
		return true
	}
	return freshEnoughVersion(list.GetResourceVersion(), options.ResourceVersion)
}

// freshEnoughVersion reports whether something read at have still answers a
// client that asked not to read behind want.
//
// An empty version, and the zero every client-go cache read sends, mean the
// client is not asserting anything and anything may answer it. Anything else
// is a floor: only a copy that has reached it will do, and a projection that
// maps no resourceVersion has nothing to show and re-reads instead.
//
// Shared by the object and the collection so the two cannot drift: a client
// that gets one answer from Get and another from List over the same rows has
// no way to tell which of them to believe.
func freshEnoughVersion(have, want string) bool {
	if want == "" || want == "0" {
		return true
	}
	if have == want {
		return true
	}
	return movesForward(want, have)
}

// remainingItems reports how many objects a client still has to page through,
// from the count read alongside the page.
func (r *REST) remainingItems(rows []crispsql.Row, consumed int64) (int64, bool) {
	if len(rows) != 1 {
		if r.count != nil {
			klog.V(3).InfoS("the count query did not return a single row", "resource", r.label, "rows", len(rows))
		}
		return 0, false
	}

	for _, value := range rows[0] {
		total, err := toInt64(value)
		if err != nil {
			return 0, false
		}
		if remaining := total - consumed; remaining > 0 {
			return remaining, true
		}
		return 0, true
	}
	return 0, false
}

// toInt64 reads the single value a count query returns, whatever numeric type
// the driver chose for it.
func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("cannot read %T as a count", value)
	}
}

// Create inserts an object.
func (w *WritableREST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	if w.create == nil {
		return nil, errors.NewMethodNotSupported(w.groupResource(), "create")
	}

	incoming, err := w.prepare(ctx, obj)
	if err != nil {
		return nil, err
	}
	if incoming.GetName() == "" && incoming.GetGenerateName() == "" {
		return nil, errors.NewBadRequest("metadata.name or metadata.generateName is required")
	}
	if incoming.GetName() == "" && len(w.mapper.NameColumns()) > 1 {
		// A generated name would have to split back into the identity columns,
		// and a random suffix does not.
		return nil, errors.NewBadRequest(
			"metadata.generateName is not supported for a projection whose identity is composed of several columns; supply metadata.name")
	}
	if err := w.pruneUnknownFields(ctx, incoming, fieldValidationOf(options)); err != nil {
		return nil, err
	}
	w.applyDefaults(incoming)
	if err := w.validateAgainstSchema(ctx, incoming, nil); err != nil {
		return nil, err
	}
	if createValidation != nil {
		if err := createValidation(ctx, incoming.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	// A dry run answers with what would have been stored, without storing it.
	if isDryRun(options.DryRun) {
		if incoming.GetName() == "" {
			incoming.SetName(names.SimpleNameGenerator.GenerateName(incoming.GetGenerateName()))
		}
		return incoming, nil
	}

	ctx, done := w.startQuery(ctx, "create")
	result, rows, err := w.insert(ctx, incoming)
	done(int(rows), err)
	return result, err
}

// generateNameAttempts matches how many times the kube-apiserver retries a
// generated name before giving up.
const generateNameAttempts = 8

// insert performs the create, generating a name and retrying on collision when
// the client asked for generateName rather than a fixed name.
func (w *WritableREST) insert(ctx context.Context, incoming *unstructured.Unstructured) (runtime.Object, int64, error) {
	if err := w.checkOwnerReferences(incoming); err != nil {
		return nil, 0, err
	}

	if incoming.GetName() != "" {
		return w.write(ctx, w.create, incoming, "create")
	}

	base := incoming.GetGenerateName()
	var rows int64
	for attempt := 0; attempt < generateNameAttempts; attempt++ {
		candidate := incoming.DeepCopy()
		candidate.SetName(names.SimpleNameGenerator.GenerateName(base))

		result, written, err := w.write(ctx, w.create, candidate, "create")
		// Every attempt is a round trip, so a name that collided several times
		// still accounts for the rows those statements touched.
		rows += written
		if err == nil {
			return result, rows, nil
		}
		if !errors.IsAlreadyExists(err) {
			return nil, rows, err
		}
	}

	return nil, rows, errors.NewServerTimeout(w.groupResource(), "create", 0)
}

// Update replaces an object, and is also the path a PATCH takes.
func (w *WritableREST) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	if w.update == nil {
		return nil, false, errors.NewMethodNotSupported(w.groupResource(), "update")
	}

	// With the status subresource enabled, a write here cannot change status.
	var merge func(incoming, existing *unstructured.Unstructured)
	if w.statusSubresource {
		merge = specOnly
	}

	result, created, err := w.applyUpdate(ctx, name, objInfo, updateValidation, options, w.update, merge)
	if !forceAllowCreate || w.create == nil || !errors.IsNotFound(err) {
		return result, created, err
	}
	return w.createOnUpdate(ctx, name, objInfo, createValidation, options)
}

// createOnUpdate is how server-side apply creates an object that is not there
// yet: it asks for it through Update rather than by posting to the collection.
//
// kubectl apply --server-side is how most objects are written now, so a
// projection that declares a create statement answering it with 404 makes the
// ordinary way of writing one fail. A projection that declares no create
// statement still refuses, as it does everywhere else.
//
// Only reached for the resource itself. Status and scale write to a row that
// has to exist already, and neither goes through here.
func (w *WritableREST) createOnUpdate(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	options *metav1.UpdateOptions,
) (runtime.Object, bool, error) {
	// Against an empty object of the projected kind, which is what the
	// kube-apiserver's own store passes when the object does not exist.
	updated, err := objInfo.UpdatedObject(ctx, w.New())
	if err != nil {
		return nil, false, err
	}

	incoming, ok := updated.(*unstructured.Unstructured)
	if !ok {
		return nil, false, errors.NewInternalError(fmt.Errorf("expected an unstructured object, got %T", updated))
	}
	incoming = incoming.DeepCopy()
	// The name comes from the request path, and a patch need not repeat it.
	if incoming.GetName() == "" {
		incoming.SetName(name)
	}

	create := &metav1.CreateOptions{}
	if options != nil {
		create.DryRun = options.DryRun
		create.FieldManager = options.FieldManager
		create.FieldValidation = options.FieldValidation
	}

	result, err := w.Create(ctx, incoming, createValidation, create)
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}

// applyUpdate is the shared path for writes to the resource and to its status.
// merge, when set, decides which parts of the submitted object are allowed to
// take effect.
func (w *WritableREST) applyUpdate(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	updateValidation rest.ValidateObjectUpdateFunc,
	options *metav1.UpdateOptions,
	query *compiledQuery,
	merge func(incoming, existing *unstructured.Unstructured),
) (runtime.Object, bool, error) {
	// Projections have no create-on-update path: the row either exists in the
	// database or it does not.
	//
	// Read fresh rather than through the caches: this object is what the
	// client's resourceVersion is checked against and what an unmodified half
	// of the object is merged onto, so a stale copy turns optimistic
	// concurrency into a check against something that has already moved.
	existing, err := w.read(ctx, name, fresh, "")
	if err != nil {
		return nil, false, err
	}

	current, ok := existing.(*unstructured.Unstructured)
	if !ok {
		return nil, false, errors.NewInternalError(fmt.Errorf("expected an unstructured object, got %T", existing))
	}

	updated, err := objInfo.UpdatedObject(ctx, existing)
	if err != nil {
		return nil, false, err
	}

	incoming, err := w.prepare(ctx, updated)
	if err != nil {
		return nil, false, err
	}
	if incoming.GetName() != name {
		return nil, false, errors.NewBadRequest(fmt.Sprintf("metadata.name %q does not match the request name %q", incoming.GetName(), name))
	}

	// Optimistic concurrency. An empty resourceVersion means the client is not
	// asserting anything, which matches how the kube-apiserver treats writes
	// without a precondition.
	requested := incoming.GetResourceVersion()
	if requested != "" && requested != current.GetResourceVersion() {
		return nil, false, errors.NewConflict(w.groupResource(), name, fmt.Errorf("%s", registry.OptimisticLockErrorMsg))
	}

	// metadata.uid is immutable here as it is on any other object.
	// ownerReferences and the garbage collector resolve against it, so a row
	// whose uid can be rewritten by whoever updates it takes its children with
	// it — and mapping.uid is recommended precisely so that clients can detect
	// identity changing, which this would let them fake.
	//
	// A client that sends none is asserting nothing and gets the uid that is
	// already there. Most patches send none.
	if uid := incoming.GetUID(); uid == "" {
		incoming.SetUID(current.GetUID())
	} else if uid != current.GetUID() {
		return nil, false, errors.NewInvalid(w.gvk.GroupKind(), name, field.ErrorList{
			field.Invalid(field.NewPath("metadata", "uid"), uid, "field is immutable"),
		})
	}

	if merge != nil {
		merge(incoming, current)
	}

	// The precondition the statement binds is the version this server actually
	// read, not the one the client asserted. That holds for the merged writes
	// too: statusOnly rebuilds the object from the stored one, so whatever
	// version came in has already been replaced by this point either way.
	//
	// They agree whenever the client asserted anything — the comparison above
	// has just required it. What differs is the case where it asserted nothing,
	// which is most patches: kubectl's merge patch sends no resourceVersion, so
	// binding the client's value left :resourceVersion NULL and the guard in
	// the statement passed unconditionally. The read-then-write window stayed
	// open and concurrent patches silently reverted one another.
	//
	// Binding what was read closes it for every writer, whether or not they
	// asked for it. A projection whose statement does not bind
	// :resourceVersion is unaffected, and reportUnguardedUpdate says so.
	incoming.SetResourceVersion(current.GetResourceVersion())

	if err := w.pruneUnknownFields(ctx, incoming, updateFieldValidation(options)); err != nil {
		return nil, false, err
	}
	w.applyDefaults(incoming)
	if err := w.validateAgainstSchema(ctx, incoming, current); err != nil {
		return nil, false, err
	}
	if updateValidation != nil {
		if err := updateValidation(ctx, incoming.DeepCopyObject(), existing.DeepCopyObject()); err != nil {
			return nil, false, err
		}
	}

	if options != nil && isDryRun(options.DryRun) {
		return incoming, false, nil
	}

	if err := w.checkFinalizers(current, incoming); err != nil {
		return nil, false, err
	}
	if err := w.checkOwnerReferences(incoming); err != nil {
		return nil, false, err
	}

	ctx, done := w.startQuery(ctx, "update")
	result, rows, err := w.write(ctx, query, incoming, "update")
	done(int(rows), err)
	if err != nil {
		return nil, false, err
	}

	// Clearing the last finalizer on a terminating object is what finally
	// removes it.
	if w.releasedByUpdate(current, incoming) {
		written, ok := result.(*unstructured.Unstructured)
		if !ok {
			written = incoming
		}
		final, err := w.finishDeletion(ctx, written)
		if err != nil {
			return nil, false, err
		}
		return final, false, nil
	}
	return result, false, nil
}

// checkOwnerReferences applies the rules the kube-apiserver applies to any
// object, because the garbage collector acts on what is stored here.
//
// A reference it cannot resolve, or two controllers disagreeing about who owns
// an object, is how things get deleted by surprise. Refusing the write is the
// only point at which that is cheap to prevent.
func (w *WritableREST) checkOwnerReferences(obj *unstructured.Unstructured) error {
	if w.mapper.OwnerReferenceColumn() == "" {
		return nil
	}

	owners := obj.GetOwnerReferences()
	if len(owners) == 0 {
		return nil
	}

	path := field.NewPath("metadata", "ownerReferences")
	var (
		errs       field.ErrorList
		controller string
		seen       = sets.New[string]()
	)
	for i, owner := range owners {
		at := path.Index(i)
		switch {
		case owner.APIVersion == "":
			errs = append(errs, field.Required(at.Child("apiVersion"), ""))
		case owner.Kind == "":
			errs = append(errs, field.Required(at.Child("kind"), ""))
		case owner.Name == "":
			errs = append(errs, field.Required(at.Child("name"), ""))
		case owner.UID == "":
			// The collector matches an owner by uid, not by name: without one
			// it cannot tell the owner from a later object of the same name.
			errs = append(errs, field.Required(at.Child("uid"), ""))
		}

		if _, err := schema.ParseGroupVersion(owner.APIVersion); err != nil {
			errs = append(errs, field.Invalid(at.Child("apiVersion"), owner.APIVersion, err.Error()))
		}

		key := owner.APIVersion + "/" + owner.Kind + "/" + string(owner.UID)
		if seen.Has(key) {
			errs = append(errs, field.Duplicate(at, owner.UID))
		}
		seen.Insert(key)

		if owner.Controller != nil && *owner.Controller {
			if controller != "" {
				errs = append(errs, field.Invalid(at.Child("controller"), owner.Name,
					fmt.Sprintf("only one reference may be the controller, and %s already is", controller)))
			}
			controller = owner.Name
		}
	}

	if len(errs) > 0 {
		return errors.NewInvalid(w.gvk.GroupKind(), obj.GetName(), errs)
	}
	return nil
}

// checkFinalizers rejects the one update Kubernetes does not allow: adding a
// finalizer to an object that is already going away. Anything else would let a
// client hold an object open forever after its deletion was accepted.
func (w *WritableREST) checkFinalizers(current, incoming *unstructured.Unstructured) error {
	if w.finalizers == "" || current.GetDeletionTimestamp().IsZero() {
		return nil
	}

	existing := sets.New(current.GetFinalizers()...)
	for _, finalizer := range incoming.GetFinalizers() {
		if !existing.Has(finalizer) {
			return errors.NewForbidden(w.groupResource(), current.GetName(), fmt.Errorf(
				"metadata.finalizers: cannot add %q to an object that is being deleted", finalizer))
		}
	}
	return nil
}

// Delete removes an object.
func (w *WritableREST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	return w.deleteObject(ctx, name, nil, deleteValidation, options)
}

// deleteObject removes an object, deciding on known when the caller already
// holds a fresh copy of it and reading one otherwise.
func (w *WritableREST) deleteObject(
	ctx context.Context,
	name string,
	known runtime.Object,
	deleteValidation rest.ValidateObjectFunc,
	options *metav1.DeleteOptions,
) (runtime.Object, bool, error) {
	if w.delete == nil {
		return nil, false, errors.NewMethodNotSupported(w.groupResource(), "delete")
	}

	// Read first so the response can carry the object that was removed, which
	// is what clients expect from a delete. Fresh, because the delete
	// preconditions and the terminating-object checks below are decided on it.
	existing := known
	if existing == nil {
		var err error
		if existing, err = w.read(ctx, name, fresh, ""); err != nil {
			return nil, false, err
		}
	}
	if err := checkDeletePreconditions(existing, options, w.groupResource(), name); err != nil {
		return nil, false, err
	}
	held, err := w.propagationFinalizer(options)
	if err != nil {
		return nil, false, err
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, existing.DeepCopyObject()); err != nil {
			return nil, false, err
		}
	}

	if options != nil && isDryRun(options.DryRun) {
		return existing, true, nil
	}

	namespace := namespaceFrom(ctx, w.NamespaceScoped())
	args := w.builtinArgs(ctx, namespace)
	args["name"] = name

	obj, ok := existing.(*unstructured.Unstructured)
	if !ok {
		return nil, false, errors.NewInternalError(fmt.Errorf("expected an unstructured object, got %T", existing))
	}

	// An object already carrying a deletionTimestamp is on its way out, and
	// deleting it again is not an error: the client is told what it asked for
	// has happened. Running the statement a second time would restamp the
	// column and reset how long the object has been terminating.
	//
	// Unless nothing is holding it any more. A terminating object whose last
	// finalizer has gone is one this call should finish off, which is how the
	// update that cleared it reaches the row.
	if !obj.GetDeletionTimestamp().IsZero() && (w.finalizers == "" || len(obj.GetFinalizers()) > 0) {
		return existing, false, nil
	}

	// A propagation policy the garbage collector acts on is expressed as a
	// finalizer, exactly as the kube-apiserver expresses it: the object is
	// marked terminating and holds until the collector has dealt with its
	// dependents and cleared the finalizer.
	//
	// Two statements, because they write different things. markDeleted stamps
	// the deletion timestamp and nothing else — a projection's own finalizer
	// flow expects the finalizer to already be on the row — so the update
	// statement, which writes every mapped column, is what puts it there. It
	// runs first: adding a finalizer to an object that is already terminating
	// is the one update Kubernetes does not allow.
	if held != "" && !hasFinalizer(obj, held) {
		holding := obj.DeepCopy()
		holding.SetFinalizers(append(holding.GetFinalizers(), held))

		written, _, err := w.write(ctx, w.update, holding, "update")
		if err != nil {
			return nil, false, err
		}
		if updated, ok := written.(*unstructured.Unstructured); ok {
			holding = updated
		}

		marked, err := w.markTerminating(ctx, holding)
		if err != nil {
			return nil, false, err
		}
		return marked, false, nil
	}

	// A finalizer is a request for time. The object is marked as terminating
	// and stays, and whoever put the finalizer there removes it when its work
	// is done — at which point the update path deletes the row.
	if len(w.finalizers) > 0 && len(obj.GetFinalizers()) > 0 {
		marked, err := w.markTerminating(ctx, obj)
		if err != nil {
			return nil, false, err
		}
		return marked, false, nil
	}

	params, err := w.mapper.Params(obj)
	if err != nil {
		return nil, false, errors.NewInternalError(err)
	}
	for k, v := range params {
		args[k] = v
	}
	if err := w.applyParameters(ctx, args, w.delete.parameters, namespace, name, obj); err != nil {
		return nil, false, errors.NewInternalError(err)
	}

	// Under the projection's concurrency limit, like every other statement. A
	// delete that skipped it was a way past the only bound on how much work one
	// projection can have in flight — and kubectl delete --all is the request
	// most likely to find it.
	release, err := w.acquire(ctx)
	if err != nil {
		return nil, false, err
	}

	ctx, done := w.startQuery(ctx, "delete")
	_, affected, err := w.run(ctx, w.delete, args, w.session(ctx, namespace, name))
	release()
	w.flights.detach(namespace)
	w.cache.invalidate(namespace)
	w.auditWrite(ctx, "delete", w.delete, affected)
	// A driver that cannot report a count gives -1, which is not a row count.
	removed := affected
	if removed < 0 {
		removed = 0
	}
	done(int(removed), err)
	if err != nil {
		// Translated like any other write. Wrapping it in an internal error
		// made every delete failure a 500: a row something else references
		// answered "internal error" rather than 409, an unreachable database
		// answered 500 rather than 503 with a Retry-After, and a shed request
		// lost the 429 it had already been given.
		return nil, false, translateWriteError(err, w.groupResource(), name, "delete")
	}
	if affected == 0 {
		return nil, false, errors.NewNotFound(w.groupResource(), name)
	}

	return existing, true, nil
}

// propagationFinalizer reports the finalizer a delete's propagation policy asks
// this object to be held by, or empty when the policy needs none.
//
// Kubernetes expresses Foreground and Orphan as finalizers on the object: the
// storage marks it terminating and the garbage collector deals with the
// dependents before clearing the finalizer. Background — the default — asks
// nothing of storage, since the collector cleans up afterwards.
//
// A projection that maps no finalizer column cannot hold an object, so a policy
// that needs one is refused rather than quietly downgraded to Background. That
// is the difference between "this projection cannot do that" and a client being
// told its dependents will be handled when they will not.
func (w *WritableREST) propagationFinalizer(options *metav1.DeleteOptions) (string, error) {
	policy := propagationPolicyOf(options)

	var finalizer string
	switch policy {
	case metav1.DeletePropagationForeground:
		finalizer = metav1.FinalizerDeleteDependents
	case metav1.DeletePropagationOrphan:
		finalizer = metav1.FinalizerOrphanDependents
	default:
		// Background, or unset. Nothing to hold the object for.
		return "", nil
	}

	// mapping.finalizers is what makes the object holdable at all, and New
	// already refuses that mapping without queries.update and
	// queries.markDeleted — so having it means having everywhere this needs to
	// write.
	if w.finalizers == "" {
		return "", errors.NewBadRequest(fmt.Sprintf(
			"propagationPolicy=%s holds the object with a finalizer until the garbage collector has "+
				"dealt with its dependents, and %s maps no finalizer column to record one in; "+
				"set mapping.finalizers, or delete with propagationPolicy=Background",
			policy, w.groupResource().String()))
	}
	return finalizer, nil
}

// propagationPolicyOf reads the policy a delete asked for, honouring the
// deprecated boolean that predates it.
func propagationPolicyOf(options *metav1.DeleteOptions) metav1.DeletionPropagation {
	if options == nil {
		return metav1.DeletePropagationBackground
	}
	if options.PropagationPolicy != nil {
		return *options.PropagationPolicy
	}
	// orphanDependents is what clients used before the policy existed, and the
	// kube-apiserver still reads it. Deprecated for twenty-odd releases and
	// still sent, so ignoring it would orphan nothing for a client that
	// believes it asked to — which is the failure this whole function exists to
	// stop.
	if options.OrphanDependents != nil { //nolint:staticcheck // SA1019: still honoured by the kube-apiserver, so still honoured here
		if *options.OrphanDependents { //nolint:staticcheck // SA1019: as above
			return metav1.DeletePropagationOrphan
		}
		return metav1.DeletePropagationBackground
	}
	return metav1.DeletePropagationBackground
}

func hasFinalizer(obj *unstructured.Unstructured, name string) bool {
	for _, existing := range obj.GetFinalizers() {
		if existing == name {
			return true
		}
	}
	return false
}

// checkDeletePreconditions honours the UID and resourceVersion preconditions a
// client can attach to a delete, so "delete only if unchanged" works.
func checkDeletePreconditions(existing runtime.Object, options *metav1.DeleteOptions, gr schema.GroupResource, name string) error {
	if options == nil || options.Preconditions == nil {
		return nil
	}

	obj, ok := existing.(*unstructured.Unstructured)
	if !ok {
		return errors.NewInternalError(fmt.Errorf("expected an unstructured object, got %T", existing))
	}

	if uid := options.Preconditions.UID; uid != nil && *uid != obj.GetUID() {
		return errors.NewConflict(gr, name,
			fmt.Errorf("the UID in the precondition (%s) does not match the UID in record (%s)", *uid, obj.GetUID()))
	}
	if rv := options.Preconditions.ResourceVersion; rv != nil && *rv != obj.GetResourceVersion() {
		return errors.NewConflict(gr, name,
			fmt.Errorf("the resourceVersion in the precondition (%s) does not match the resourceVersion in record (%s)", *rv, obj.GetResourceVersion()))
	}
	return nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// validateAgainstSchema rejects an object that does not match the projection's
// declared schema, so a malformed write fails at the API rather than landing in
// the database as an unreadable row.
func (w *WritableREST) validateAgainstSchema(ctx context.Context, obj, old *unstructured.Unstructured) error {
	if w.validator == nil {
		return nil
	}

	// Ratcheting: on an update, a part of the object that did not change is
	// not re-judged. A schema that grew stricter after a row was written
	// should not make every unrelated edit to that row impossible.
	var (
		options    []apiservervalidation.ValidationOption
		celOptions []cel.Option
	)
	if old != nil && w.structural != nil {
		correlated := common.NewCorrelatedObject(obj.Object, old.Object, &model.Structural{Structural: w.structural})
		options = append(options, apiservervalidation.WithRatcheting(correlated))
		celOptions = append(celOptions, cel.WithRatcheting(correlated))
	}

	var errs field.ErrorList
	if old != nil {
		// The update path is the only one that can ratchet: it is the only one
		// with a previous object to compare against.
		errs = apiservervalidation.ValidateCustomResourceUpdate(nil, obj.Object, old.Object, w.validator, options...)
	} else {
		errs = apiservervalidation.ValidateCustomResource(nil, obj.Object, w.validator)
	}
	if len(errs) > 0 {
		return errors.NewInvalid(w.gvk.GroupKind(), obj.GetName(), errs)
	}

	if w.celValidator == nil {
		return nil
	}

	// Transition rules compare against the stored object, so the old value is
	// passed on updates and omitted on create.
	var oldObject any
	if old != nil {
		oldObject = old.Object
	}
	if errs, _ := w.celValidator.Validate(
		ctx, nil, w.structural, obj.Object, oldObject, celconfig.RuntimeCELCostBudget, celOptions...,
	); len(errs) > 0 {
		return errors.NewInvalid(w.gvk.GroupKind(), obj.GetName(), errs)
	}
	return nil
}

// applyDefaults fills in what the schema says a field should hold when the
// client left it out.
//
// Only writes are defaulted. Reads are answered from the database as it is:
// a projection whose whole premise is that the rows are the truth should not
// invent values that are not in them.
func (w *WritableREST) applyDefaults(obj *unstructured.Unstructured) {
	if w.structural == nil {
		return
	}
	defaulting.Default(obj.Object, w.structural)
}

// DeleteCollection removes every object the request's selectors match.
//
// A projection can supply a single statement for this; without one, or when
// the request was narrowed by something the statement cannot see — a selector,
// or a page limit — the matching objects are deleted one at a time so the
// result is always exactly what was selected.
func (w *WritableREST) DeleteCollection(
	ctx context.Context,
	deleteValidation rest.ValidateObjectFunc,
	options *metav1.DeleteOptions,
	listOptions *metainternalversion.ListOptions,
) (runtime.Object, error) {
	if w.delete == nil && w.deleteCollection == nil {
		return nil, errors.NewMethodNotSupported(w.groupResource(), "deletecollection")
	}

	namespace := namespaceFrom(ctx, w.NamespaceScoped())

	// The objects are read first so the response can report what was removed,
	// and so validation sees each one.
	list, err := w.listObjects(ctx, namespace, listOptions, fresh)
	if err != nil {
		return nil, err
	}

	if deleteValidation != nil {
		for i := range list.Items {
			if err := deleteValidation(ctx, list.Items[i].DeepCopyObject()); err != nil {
				return nil, err
			}
		}
	}

	if options != nil && isDryRun(options.DryRun) {
		return list, nil
	}

	if w.canDeleteInBulk(listOptions) {
		if err := w.deleteInBulk(ctx, namespace, listOptions, len(list.Items)); err != nil {
			return nil, err
		}
		return list, nil
	}

	if w.delete == nil {
		return nil, errors.NewMethodNotSupported(w.groupResource(), "deletecollection")
	}

	// One object at a time, but not one after another. This path runs when a
	// single statement cannot express the request — a selector it cannot see,
	// or finalizers it cannot tell apart — and a collection of any size would
	// otherwise take the sum of every round trip. The projection's own query
	// limit still decides how many are in flight, so this cannot put more load
	// on the database than any other request is allowed to.
	if err := w.deleteEach(ctx, list.Items, options); err != nil {
		return nil, err
	}
	return list, nil
}

// deleteInBulk removes the whole collection with the projection's single
// statement. reported is what the response will say went, which is what the
// query metrics record for the request; the statement's own affected count is
// what the audit annotation carries.
func (w *WritableREST) deleteInBulk(
	ctx context.Context,
	namespace string,
	listOptions *metainternalversion.ListOptions,
	reported int,
) error {
	args := w.builtinArgs(ctx, namespace)
	if listOptions != nil && listOptions.LabelSelector != nil {
		args["labelSelector"] = listOptions.LabelSelector.String()
	}
	if err := w.applyParameters(ctx, args, w.deleteCollection.parameters, namespace, "", nil); err != nil {
		return errors.NewInternalError(err)
	}

	// Under the projection's concurrency limit, like every other statement.
	// This is one statement but rarely a small one: a collection delete is
	// often the heaviest write the projection ever sends, and it was the only
	// write that could start while the projection was already at its limit.
	// The per-object fallback below has always been bounded, so `delete --all`
	// respected the limit or ignored it depending on which path it took.
	release, err := w.acquire(ctx)
	if err != nil {
		return err
	}

	ctx, done := w.startQuery(ctx, "deletecollection")
	_, affected, err := w.run(ctx, w.deleteCollection, args, w.session(ctx, namespace, ""))

	// In the order every other write uses. Detaching before invalidating is
	// what stops a reader arriving between the two from joining a query that
	// started before this delete and then storing its pre-delete rows in the
	// cache — rows that would then outlive the request by the whole cacheTTL.
	release()
	w.flights.detach(namespace)
	w.cache.invalidate(namespace)

	w.auditWrite(ctx, "deletecollection", w.deleteCollection, affected)
	done(reported, err)
	if err != nil {
		// Translated like any other write, and for the reason the single
		// delete gives: wrapping it made every failure a 500, so a collection
		// something else references answered "internal error" rather than 409,
		// an unreachable database answered 500 rather than 503 with a
		// Retry-After, and a shed request lost the 429 it had been given.
		// There is no one object to name, so the error names the verb.
		return translateWriteError(err, w.groupResource(), "", "deletecollection")
	}
	return nil
}

// deleteEach removes objects concurrently and reports the first real failure.
//
// The collection was just read, and read fresh, so each delete is handed the
// copy it was listed with rather than reading the same row again — which is the
// difference between one round trip per object and two.
//
// Not always, though. A finalizer that appeared since the list has to be seen,
// or the row is removed where it should have been marked terminating; and a
// precondition is an assertion about the object as it is now, which a copy
// taken earlier cannot answer. Either of those buys back the second read.
func (w *WritableREST) deleteEach(ctx context.Context, items []unstructured.Unstructured, options *metav1.DeleteOptions) error {
	reuse := w.finalizers == "" && (options == nil || options.Preconditions == nil)

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(w.deleteConcurrency())

	for i := range items {
		item := &items[i]
		group.Go(func() error {
			var known runtime.Object
			if reuse {
				known = item
			}
			// Something else removing it first is the outcome asked for.
			if _, _, err := w.deleteObject(ctx, item.GetName(), known, nil, options); err != nil &&
				!errors.IsNotFound(err) {
				return err
			}
			return nil
		})
	}
	return group.Wait()
}

// deleteConcurrency bounds the deletes in flight at what the projection allows
// itself to run at once, so a collection delete is not a way around the limit.
func (w *WritableREST) deleteConcurrency() int {
	if limit := w.limiter.Limit(); limit > 0 {
		return limit
	}
	return crispsql.DefaultMaxOpenConns
}

// canDeleteInBulk reports whether one statement can express this request.
// Anything that narrowed the list and the statement never sees — a selector,
// or a page limit — would delete more than was asked for, so those cases fall
// back to deleting the listed objects individually.
func (w *WritableREST) canDeleteInBulk(listOptions *metainternalversion.ListOptions) bool {
	if w.deleteCollection == nil {
		return false
	}

	// A single statement cannot tell which rows something is still holding
	// onto. Deleting the collection one object at a time is slower and is the
	// only way a finalizer means anything here: `delete --all` must not step
	// past what a single delete would respect.
	if w.finalizers != "" {
		return false
	}

	if listOptions == nil {
		return true
	}

	// A limit is a selector by another name. The objects were listed with it
	// applied, the response reports that page, and admission was only ever
	// shown that page — but the bulk statement has no idea a page was asked
	// for and removes the whole collection. `delete --all --chunk-size=N`
	// would report N objects and empty the table, with nothing in the response
	// to say so. A continue token is the same request seen from the middle:
	// the rows before and after that page were never listed and never
	// validated, so neither of them may be deleted by a statement that cannot
	// be told where the page begins or ends.
	if listOptions.Limit > 0 || listOptions.Continue != "" {
		return false
	}

	if listOptions.FieldSelector != nil && !listOptions.FieldSelector.Empty() {
		return false
	}
	if listOptions.LabelSelector != nil && !listOptions.LabelSelector.Empty() {
		return w.deleteCollection.declares("labelSelector")
	}
	return true
}

// isDryRun reports whether the request asked for a rehearsal.
func isDryRun(dryRun []string) bool {
	for _, value := range dryRun {
		if value == metav1.DryRunAll {
			return true
		}
	}
	return false
}

func fieldValidationOf(options *metav1.CreateOptions) string {
	if options == nil {
		return ""
	}
	return options.FieldValidation
}

func updateFieldValidation(options *metav1.UpdateOptions) string {
	if options == nil {
		return ""
	}
	return options.FieldValidation
}

// pruneUnknownFields removes anything the schema does not describe, and reports
// what it removed according to the request's fieldValidation.
//
// Silently discarding a field the client wrote is the behaviour worth avoiding:
// the default warns, and Strict rejects.
func (w *WritableREST) pruneUnknownFields(ctx context.Context, obj *unstructured.Unstructured, fieldValidation string) error {
	if w.structural == nil {
		return nil
	}

	opts := structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: true}
	pruned := pruning.PruneWithOptions(obj.Object, w.structural, true, opts)
	if len(pruned) == 0 {
		return nil
	}

	sort.Strings(pruned)
	switch fieldValidation {
	case metav1.FieldValidationStrict:
		return errors.NewInvalid(w.gvk.GroupKind(), obj.GetName(), field.ErrorList{
			field.Invalid(nil, strings.Join(pruned, ", "), "unknown fields, which this projection does not describe"),
		})
	case metav1.FieldValidationIgnore:
		return nil
	default:
		warning.AddWarning(ctx, "", fmt.Sprintf("unknown fields were dropped: %s", strings.Join(pruned, ", ")))
		return nil
	}
}

// prepare normalises an incoming object and checks it belongs in this request's
// namespace.
func (w *WritableREST) prepare(ctx context.Context, obj runtime.Object) (*unstructured.Unstructured, error) {
	incoming, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("expected an unstructured object, got %T", obj))
	}
	incoming = incoming.DeepCopy()

	if w.NamespaceScoped() {
		namespace := namespaceFrom(ctx, true)
		if incoming.GetNamespace() == "" {
			incoming.SetNamespace(namespace)
		} else if incoming.GetNamespace() != namespace {
			return nil, errors.NewBadRequest(fmt.Sprintf(
				"metadata.namespace %q does not match the request namespace %q", incoming.GetNamespace(), namespace))
		}
	}

	incoming.SetGroupVersionKind(w.gvk)
	return incoming, nil
}

// markTerminating writes the deletion timestamp and leaves the row in place.
func (w *WritableREST) markTerminating(ctx context.Context, obj *unstructured.Unstructured) (runtime.Object, error) {
	ctx, done := w.startQuery(ctx, "delete")
	result, rows, err := w.write(ctx, w.markDeleted, obj, "markDeleted")
	done(int(rows), err)
	return result, err
}

// finishDeletion removes a row whose last finalizer has just been cleared.
//
// This is the other half of the contract: an object stops existing when
// nothing is left holding it, not when someone asks for it to go. Doing it here
// means the client that cleared the finalizer is told the object is gone,
// rather than being handed one that no longer has a reason to exist.
func (w *WritableREST) finishDeletion(ctx context.Context, obj *unstructured.Unstructured) (runtime.Object, error) {
	if _, _, err := w.Delete(ctx, obj.GetName(), nil, &metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		return nil, err
	}
	return obj, nil
}

// awaitingFinalizers reports whether an update has left an object that is
// terminating with nothing more to wait for.
func (w *WritableREST) releasedByUpdate(previous, updated *unstructured.Unstructured) bool {
	return w.finalizers != "" &&
		!previous.GetDeletionTimestamp().IsZero() &&
		len(previous.GetFinalizers()) > 0 &&
		len(updated.GetFinalizers()) == 0
}

// write runs a write statement and returns the resulting object. Statements
// that use RETURNING answer from their own result; otherwise the row is read
// back so the response reflects what the database actually stored.
// The row count it returns is what the statement really touched — the rows a
// RETURNING clause produced, or the affected count otherwise — so the caller
// records what happened rather than assuming one row.
func (w *WritableREST) write(ctx context.Context, query *compiledQuery, obj *unstructured.Unstructured, verb string) (runtime.Object, int64, error) {
	namespace := obj.GetNamespace()
	name := obj.GetName()

	args := w.builtinArgs(ctx, namespace)
	args["name"] = name
	// Bound so a projection can make the precondition atomic in SQL rather
	// than relying on the read-then-write check above.
	args["resourceVersion"] = nullIfEmpty(obj.GetResourceVersion())

	params, err := w.mapper.Params(obj)
	if err != nil {
		return nil, 0, errors.NewInvalid(w.gvk.GroupKind(), name, nil)
	}
	// A column mapped both as a label and as a field can only be written from
	// one of them, and the field wins. Said out loud, because the write is
	// answered 200 either way and the part that was ignored is otherwise
	// invisible — kubectl reports "labeled" for a row that did not move.
	if dropped := w.mapper.DroppedOnWrite(obj); len(dropped) > 0 {
		warning.AddWarning(ctx, "", strings.Join(dropped, "; "))
	}
	for k, v := range params {
		args[k] = v
	}
	if err := w.applyParameters(ctx, args, query.parameters, namespace, name, obj); err != nil {
		return nil, 0, errors.NewInternalError(err)
	}

	// A statement with RETURNING answers from its own result; anything else is
	// executed for its row count and the object is read back afterwards. The
	// distinction matters: for a RETURNING statement "no rows" means nothing
	// matched, while for the others only the affected count can say that.
	release, err := w.acquire(ctx)
	if err != nil {
		return nil, 0, err
	}

	// The slot and the invalidation both belong to the statement, not to the
	// request. A cached read must never outlive the write that invalidates it,
	// and only the namespace that was written needs to pay for it; reads
	// already in flight are detached for the same reason, since one of them
	// started before this write and cannot reflect it.
	//
	// All three end here rather than when write returns, because the read back
	// below is an ordinary read: it must not wait for a slot this request is
	// holding — at maxConcurrentQueries: 1 it would wait out the whole acquire
	// timeout and be shed, reporting 429 for a write that committed — and it
	// must not be answered from a cache this write has not dropped yet.
	var once sync.Once
	finish := func() {
		once.Do(func() {
			release()
			w.flights.detach(namespace)
			w.cache.invalidate(namespace)
		})
	}
	// Deferred as well, so the error paths that return before the read back
	// still drop what the statement invalidated.
	defer finish()

	rows, affected, err := w.run(ctx, query, args, w.session(ctx, namespace, name))
	finish()

	// What the statement touched: the rows it returned, or the count it
	// reported. A driver that cannot report one gives -1, which is not a row
	// count and is recorded as zero rather than as minus one object.
	touched := affected
	if query.returnsRows {
		touched = int64(len(rows))
	}
	if touched < 0 {
		touched = 0
	}

	w.auditWrite(ctx, verb, query, touched)
	if err != nil {
		return nil, touched, translateWriteError(err, w.groupResource(), name, verb)
	}

	if query.returnsRows {
		if len(rows) == 0 {
			return nil, touched, w.missedRows(query, obj, verb, name)
		}

		result, err := w.mapper.Row(rows[0])
		if err != nil {
			return nil, touched, errors.NewInternalError(fmt.Errorf("mapping returned row: %w", err))
		}
		return result, touched, nil
	}

	// A driver that cannot report the count returns -1; treat that as success
	// rather than inventing a conflict.
	if affected == 0 {
		return nil, touched, w.missedRows(query, obj, verb, name)
	}

	// Read back from the primary, and not through the caches.
	//
	// This is the response to the write, which is a stronger claim than any
	// other read makes: the client is being told what their own write produced.
	// A shared read answers that from the read replica when one is configured,
	// and a replica is behind by definition — so a create could be answered 404
	// for the row it had just committed, and an update with the object as it
	// was before, carrying the resourceVersion from before. A client that takes
	// that version as the base for its next write is then in a conflict loop it
	// cannot see the cause of, until something makes it relist.
	//
	// The cache is skipped for the same reason rather than as a side effect.
	// finish() has already dropped this namespace's entries, so there is
	// nothing here to gain from it and only a racing reader's entry to lose to.
	result, err := w.read(ctx, name, fresh, "")
	return result, touched, err
}

// run executes a write, as one statement or as a transaction, and reports
// whichever of rows and affected count the caller can use.
func (w *WritableREST) run(
	ctx context.Context,
	query *compiledQuery,
	args map[string]any,
	session []crispsql.SessionVariable,
) ([]crispsql.Row, int64, error) {
	if query.transactional() {
		return w.pool.TransactWith(ctx, session, query.all(), args)
	}
	if query.returnsRows {
		rows, err := w.pool.QueryWith(ctx, session, query.statement, args)
		return rows, int64(len(rows)), err
	}
	affected, err := w.pool.ExecWith(ctx, session, query.statement, args)
	return nil, affected, err
}

// missedRows explains a write that matched nothing. When the client asserted a
// resourceVersion and the statement carries that precondition, something else
// changed the row first; otherwise the row is simply gone.
func (w *WritableREST) missedRows(query *compiledQuery, obj *unstructured.Unstructured, verb, name string) error {
	if verb == "update" && obj.GetResourceVersion() != "" && query.declares("resourceVersion") {
		return errors.NewConflict(w.groupResource(), name, fmt.Errorf("%s", registry.OptimisticLockErrorMsg))
	}
	return errors.NewNotFound(w.groupResource(), name)
}

// translateWriteError turns the driver errors clients are most likely to hit
// into the matching API status, so a duplicate insert reads as AlreadyExists
// rather than as an opaque internal error.
func translateWriteError(err error, gr schema.GroupResource, name, verb string) error {
	switch {
	case goerrors.Is(err, crispsql.ErrTooBusy):
		return tooManyRequests(gr)
	case goerrors.Is(err, context.DeadlineExceeded), crispsql.IsStatementTimeout(err):
		return timedOut(gr)
	case crispsql.IsUnavailable(err):
		return unavailable(gr, err)
	case crispsql.IsSerializationFailure(err):
		// A conflict, like a stale resourceVersion, but not the same one, and
		// the cause has to say which: there the client holds an object that has
		// moved on and has to read it again, while here the write it sent was
		// never applied and can be sent again exactly as it is. Both are 409
		// because both mean "not now, try again", which is what makes a
		// controller requeue instead of treating it as a server fault — and a
		// 500 is what it got before, for the one failure the database raises
		// specifically to say no harm was done.
		return errors.NewConflict(gr, name, fmt.Errorf(
			"the database could not run this write alongside a concurrent one and rolled it back; "+
				"nothing was changed and it can be retried unchanged: %w", err))
	case crispsql.IsUniqueViolation(err):
		return errors.NewAlreadyExists(gr, name)
	case crispsql.IsForeignKeyViolation(err):
		return errors.NewConflict(gr, name, err)
	default:
		// A collection delete has no one object to name, and "deletecollection
		// : ..." reads like something went missing from the message.
		subject := verb
		if name != "" {
			subject += " " + name
		}
		return errors.NewInternalError(fmt.Errorf("%s: %w", subject, err))
	}
}

// Audit annotation keys. They record which projection and which statement
// answered a write, never the values bound into it.
const (
	auditProjection = "crisp.kubecrisp.io/projection"
	auditResource   = "crisp.kubecrisp.io/resource"
	auditVerb       = "crisp.kubecrisp.io/verb"
	auditDataSource = "crisp.kubecrisp.io/datasource"
	auditStatement  = "crisp.kubecrisp.io/statement"
	auditRows       = "crisp.kubecrisp.io/rows"
)

// auditWrite records the statement a write produced.
//
// Delegated authentication already says who made the request; this says what it
// did to the database, which is the half a reviewer of a system like this asks
// for. Bound values are deliberately left out: they are the caller's data, and
// the statement text is what identifies the operation.
func (w *WritableREST) auditWrite(ctx context.Context, verb string, query *compiledQuery, rows int64) {
	statement := ""
	if query != nil {
		statement = collapseWhitespace(query.statement.SQL)
	}

	audit.AddAuditAnnotationsMap(ctx, map[string]string{
		auditProjection: w.projection,
		auditResource:   w.label,
		auditVerb:       verb,
		auditDataSource: w.pool.Name(),
		auditStatement:  statement,
		auditRows:       strconv.FormatInt(rows, 10),
	})

	klog.V(4).InfoS("projected write",
		"projection", w.projection, "resource", w.label, "verb", verb,
		"datasource", w.pool.Name(), "rows", rows, "statement", statement)
}

// collapseWhitespace puts a statement on one line, since audit annotations are
// read in aggregate.
func collapseWhitespace(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// tooManyRequests tells the client to come back rather than leaving it to wait
// out its own timeout.
func tooManyRequests(gr schema.GroupResource) error {
	return errors.NewTooManyRequests(
		fmt.Sprintf("%s is at its query concurrency limit", gr.String()), 1)
}

// queryError turns a read failure into the status a client can act on.
func (r *REST) queryError(err error, verb string) error {
	switch {
	case isStatusError(err):
		// Already an answer: a request shed at the concurrency limit reaches
		// here as a 429, and wrapping it would turn it into a 500.
		return err
	case goerrors.Is(err, crispsql.ErrTooBusy):
		return tooManyRequests(r.groupResource())
	case goerrors.Is(err, context.DeadlineExceeded), crispsql.IsStatementTimeout(err):
		return timedOut(r.groupResource())
	case crispsql.IsSerializationFailure(err):
		return serializationFailed(r.groupResource())
	case crispsql.IsUnavailable(err):
		return unavailable(r.groupResource(), err)
	default:
		return errors.NewInternalError(fmt.Errorf("%s %s: %w", verb, r.resource.Plural, err))
	}
}

// isStatusError reports whether an error already carries the status a client
// should be given.
func isStatusError(err error) bool {
	var status errors.APIStatus
	return goerrors.As(err, &status)
}

// timedOut reports that the query ran out of time.
//
// A database that is unreachable usually refuses the connection, but one that
// has gone away without closing its sockets simply never answers. Either way
// the client is better served by a timeout it can retry than by a 500 that
// suggests the projection is broken.
//
// Deliberately without a Retry-After, which is the one thing separating this
// from the 429 and 503 above. RetryAfterSeconds becomes a Retry-After header,
// and client-go retries any response over 500 that carries one — ten times, by
// default, at the interval the header asks for. On a shed request that is free,
// and on a refused connection it is nearly so, because both fail before the
// database does any work. A timeout is the opposite: every attempt runs the
// query for its whole budget before giving up, so advertising one here turns a
// single LIST against a slow database into eleven, spending eleven times the
// budget to return the error it was always going to return. That is the load a
// struggling database can least afford, arriving exactly when it is struggling.
//
// Measured against PostgreSQL rather than reasoned about: a 500ms query on a
// table that takes five seconds cost 11 attempts and 15.6s of client wait, and
// one attempt once this stopped asking for them.
//
// The reason stays Timeout, so a client that wants to retry still knows it may.
// What changes is that the server no longer tells every client to.
func timedOut(gr schema.GroupResource) error {
	return errors.NewTimeoutError(
		fmt.Sprintf("the database behind %s did not answer in time", gr.String()), 0)
}

// reasonSerializationFailure marks the answer a contended read gets.
//
// A reason of its own rather than ServiceUnavailable, which is what the code
// would otherwise imply, because the metric label is derived from it and the two
// are not the same event: unavailable is a database that could not be reached,
// and this is one that answered, and answered that it could not serialise this
// read against a concurrent transaction. KubeCrispDatabaseUnreachable is a
// critical alert on the first, so letting a hot table produce it would page
// somebody about an outage that is not happening.
const reasonSerializationFailure metav1.StatusReason = "SerializationFailure"

// serializationFailed reports a read the database rolled back rather than
// serialise.
//
// A write in the same position answers 409: the caller re-reads and reapplies,
// which is a thing to do. A read has nothing to reapply, so what it needs to be
// told is that nothing is wrong with the request and the answer may exist on
// another attempt -- which is 503, and why the status is not reused from the
// write path.
//
// Deliberately without a Retry-After. The rule this project settled on is that
// one is set where a retry is cheap, and a shed request or a refused connection
// costs the database nothing. This costs it a whole query: the database ran the
// transaction and threw the work away. Telling every client to retry ten times
// over would put ten times the load on a database that is already too contended
// to serialise the first attempt, which is the same mistake measured on
// timeouts. The status still says the request may be repeated, so a client that
// wants to may; the server does not instruct it to.
func serializationFailed(gr schema.GroupResource) error {
	return &errors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusServiceUnavailable,
		Reason: reasonSerializationFailure,
		Message: fmt.Sprintf(
			"the database behind %s rolled back a read rather than serialise it against a concurrent transaction",
			gr.String()),
	}}
}

// isSerializationFailure reports whether an answer is the one above.
func isSerializationFailure(err error) bool {
	var status errors.APIStatus
	return goerrors.As(err, &status) && status.Status().Reason == reasonSerializationFailure
}

// unavailable reports that the database is unreachable.
//
// The resource keeps existing while its database is down: withdrawing the API
// group would make a transient outage look like the API had never been defined,
// and would take discovery, RBAC, and any controller watching it with it.
func unavailable(gr schema.GroupResource, err error) error {
	status := errors.NewServiceUnavailable(
		fmt.Sprintf("the database behind %s is unreachable: %v", gr.String(), err))
	status.ErrStatus.Details = &metav1.StatusDetails{
		Group:             gr.Group,
		Kind:              gr.Resource,
		RetryAfterSeconds: 1,
	}
	return status
}

// callerArgs renders the authenticated identity into bind values.
//
// The username alone answers "who is this" only as far as the authenticator's
// naming; a policy that has to survive a username being reassigned keys on the
// UID, and one that scopes rows the way RBAC scopes verbs keys on the groups.
// The collections travel as JSON, which is the one shape every supported driver
// can take apart — Postgres with jsonb operators, MySQL with JSON_CONTAINS.
func callerArgs(ctx context.Context) map[string]any {
	args := map[string]any{
		"user":       nil,
		"userUID":    nil,
		"userGroups": nil,
		"userExtra":  nil,
	}

	user, ok := genericapirequest.UserFrom(ctx)
	if !ok {
		return args
	}

	args["user"] = user.GetName()
	if uid := user.GetUID(); uid != "" {
		args["userUID"] = uid
	}
	if groups := user.GetGroups(); len(groups) > 0 {
		if encoded, err := json.Marshal(groups); err == nil {
			args["userGroups"] = string(encoded)
		}
	}
	if extra := user.GetExtra(); len(extra) > 0 {
		if encoded, err := json.Marshal(extra); err == nil {
			args["userExtra"] = string(encoded)
		}
	}
	return args
}

// builtinArgs seeds the parameters every query may reference.
func (r *REST) builtinArgs(ctx context.Context, namespace string) map[string]any {
	args := map[string]any{
		// A cluster-wide read binds NULL, so a list query can be written as
		// "WHERE (:namespace IS NULL OR tenant = :namespace)" and serve both
		// namespaced and cross-namespace requests, which is what watch needs.
		"namespace": namespaceArg(namespace),
		"name":      nil,
		"name_not":  nil,
		"limit":     nil,
		"offset":    nil,
	}
	for name, value := range callerArgs(ctx) {
		args[name] = value
	}
	return args
}

// query runs a read, sharing it with any identical read already in flight.
//
// The concurrency slot is taken by the query that actually runs, so requests
// that join one do not each hold the projection's limited capacity for work
// nobody is doing twice.
func (r *REST) query(
	ctx context.Context,
	stmt *crispsql.Statement,
	args map[string]any,
	namespace string,
	session []crispsql.SessionVariable,
	mode readMode,
) ([]crispsql.Row, error) {
	results, err := r.queryAll(ctx, []*crispsql.Statement{stmt}, args, namespace, session, mode)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

// queryAll runs several reads as one, so their results describe one moment.
//
// A single statement takes the ordinary path, with its prepared statement and
// no transaction. More than one has to share a transaction, or the results
// cannot be compared: a page and a count taken separately can disagree about
// how many objects there are, which is exactly what remainingItemCount is for.
func (r *REST) queryAll(
	ctx context.Context,
	stmts []*crispsql.Statement,
	args map[string]any,
	namespace string,
	session []crispsql.SessionVariable,
	mode readMode,
) ([][]crispsql.Row, error) {
	pool := r.poolFor(mode)

	role := crispmetrics.RolePrimary
	if pool == r.readPool {
		role = crispmetrics.RoleReplica
	}
	crispmetrics.QueriesRouted.WithLabelValues(r.projection, r.label, role).Inc()

	execute := func(ctx context.Context, against *crispsql.Pool) ([][]crispsql.Row, error) {
		if len(stmts) == 1 {
			rows, err := against.QueryWith(ctx, session, stmts[0], args)
			if err != nil {
				return nil, err
			}
			return [][]crispsql.Row{rows}, nil
		}
		return against.QueryAllWith(ctx, session, stmts, args)
	}

	run := func(ctx context.Context) ([][]crispsql.Row, error) {
		release, err := r.acquire(ctx)
		if err != nil {
			return nil, err
		}
		defer release()

		results, err := execute(ctx, pool)
		if err == nil || pool == r.pool || !crispsql.IsUnavailable(err) {
			return results, err
		}

		// The replica is unreachable. A read the primary can answer should not
		// fail because the copy that exists to spare it load has gone away:
		// without a replica configured this read would have gone to the primary,
		// so that is where it goes now. Only reachability falls back — a
		// statement the replica rejected would be rejected there too.
		r.replicaDownUntil.Store(time.Now().Add(replicaRetryAfter).UnixNano())
		crispmetrics.ReplicaFallbacks.WithLabelValues(r.projection, r.label).Inc()
		klog.V(2).InfoS("the read replica is unreachable; answering from the primary",
			"resource", r.label, "retryAfter", replicaRetryAfter, "err", err)

		return execute(ctx, r.pool)
	}

	// A read taken as the base of a write cannot join a query that was already
	// running: that query may have started before an earlier write and cannot
	// reflect it, which would leave a conflict check comparing against a
	// version the row no longer has.
	if mode == fresh {
		return run(ctx)
	}

	// The session is part of the key. Two requests that differ only in the
	// tenant they set see different rows, and sharing one query between them
	// would hand a client rows the database would never have shown it.
	var key strings.Builder
	for _, stmt := range stmts {
		key.WriteString(flightKey(stmt, args))
	}
	key.WriteString(sessionKey(session))
	// The same statement against the primary and against a replica are two
	// different reads: one of them may not have the write the other has.
	key.WriteString("\x00pool=")
	key.WriteString(pool.Name())

	return r.flights.Do(ctx, key.String(), namespace, run)
}

// poolFor picks the database a read should go to.
//
// A replica is behind the primary by however long replication takes, which is
// fine for a list, a get, or a watch poll — all of them are already a snapshot
// of a moment that has passed. It is not fine for the read a write is based on:
// a resourceVersion checked against a lagging replica is checked against a
// version the row may have left behind, and the merge of an object's untouched
// half would write back state the primary has already moved past. Those ask for
// a fresh read, and a fresh read means the primary.
func (r *REST) poolFor(mode readMode) *crispsql.Pool {
	if mode == fresh || r.readPool == nil {
		return r.pool
	}
	// Found unreachable a moment ago, so it is skipped rather than tried and
	// waited on again.
	if time.Now().UnixNano() < r.replicaDownUntil.Load() {
		return r.pool
	}
	return r.readPool
}

// acquire takes a query slot for this projection, reporting the status a shed
// request should receive.
func (r *REST) acquire(ctx context.Context) (func(), error) {
	// Spanned separately from the query it precedes. Time spent here is time
	// spent waiting for another request to finish, not time the database was
	// slow, and a trace that ran them together would send every investigation
	// of a saturated projection to the database first.
	_, span := tracing.Start(ctx, "kube-crisp.acquire",
		attribute.Int("kube_crisp.concurrency_limit", r.limiter.Limit()))

	release, err := r.limiter.Acquire(ctx)
	if err != nil {
		span.RecordError(err)
	}
	span.End(slowQuerySpanLog)

	if err == nil {
		return release, nil
	}
	if goerrors.Is(err, crispsql.ErrTooBusy) {
		crispmetrics.QueriesShed.WithLabelValues(r.projection, r.label).Inc()
		return nil, tooManyRequests(r.groupResource())
	}
	return nil, errors.NewInternalError(err)
}

// startQuery opens a span for one database round trip and returns the function
// that closes it and records the metrics.
//
// Span and metric share a boundary on purpose: they answer the same question at
// different resolutions, and two boundaries would eventually disagree about
// what a "query" is.
//
// The span carries the projection, which the apiserver's own request span
// cannot: it knows the resource being served, and which projection was asked to
// serve it is exactly what a reader of the trace is trying to find out. The
// child spans in pkg/sql hang beneath this one, so a slow read reads as one
// tree — which projection, which statement, and how much of it was spent
// waiting for a connection rather than for the database.
func (r *REST) startQuery(ctx context.Context, verb string) (context.Context, func(rows int, err error)) {
	start := time.Now()

	ctx, span := tracing.Start(ctx, "kube-crisp."+verb,
		attribute.String("kube_crisp.projection", r.projection),
		attribute.String("kube_crisp.resource", r.label),
		attribute.String("kube_crisp.verb", verb),
	)

	return ctx, func(rows int, err error) {
		if err != nil {
			span.RecordError(err)
		} else {
			span.AddEvent("answered", attribute.Int("kube_crisp.rows", rows))
		}
		span.End(slowQuerySpanLog)
		r.observe(verb, start, rows, err)
	}
}

// slowQuerySpanLog is how long a request has to take before its trace is worth
// printing on its own. Only consulted when this span is the root, which for a
// served request it is not — the apiserver's handler span is.
const slowQuerySpanLog = 500 * time.Millisecond

// observe records one database round trip, labelled with what the caller was
// told.
func (r *REST) observe(verb string, start time.Time, rows int, err error) {
	crispmetrics.QueryDuration.WithLabelValues(r.projection, r.label, verb, resultFor(err)).
		Observe(time.Since(start).Seconds())
	if err == nil {
		crispmetrics.QueryRows.WithLabelValues(r.projection, r.label, verb).Observe(float64(rows))
	}
}

// resultFor classifies a finished query for the result label.
//
// Keyed off the status the client was given rather than off the underlying
// driver error, and deliberately: the two agree, because queryError has already
// done the classifying, and reading it back from the status means the metric
// cannot come to a different conclusion than the caller was given. A projection
// whose graphs say "unavailable" while its clients are being told something
// else would be worse than no label at all.
func resultFor(err error) string {
	switch {
	case err == nil:
		return crispmetrics.ResultSuccess
	case errors.IsNotFound(err):
		return crispmetrics.ResultNotFound
	case errors.IsTimeout(err), errors.IsServerTimeout(err):
		return crispmetrics.ResultTimeout
	case isSerializationFailure(err):
		return crispmetrics.ResultContended
	case errors.IsServiceUnavailable(err):
		return crispmetrics.ResultUnavailable
	case errors.IsTooManyRequests(err):
		return crispmetrics.ResultShed
	case errors.IsInvalid(err), errors.IsBadRequest(err):
		return crispmetrics.ResultInvalid
	case errors.IsConflict(err), errors.IsAlreadyExists(err):
		return crispmetrics.ResultConflict
	default:
		// An InternalError, which for a read is almost always a statement the
		// database refused — the projection's SQL, not the database's health.
		return crispmetrics.ResultError
	}
}

func (r *REST) groupResource() schema.GroupResource {
	return schema.GroupResource{Group: r.resource.Group, Resource: r.resource.Plural}
}

// applyParameters resolves declared parameters into bind values.
func (r *REST) applyParameters(
	ctx context.Context,
	args map[string]any,
	params []crispv1alpha1.QueryParameter,
	namespace, name string,
	obj *unstructured.Unstructured,
) error {
	// The caller's identity is resolved at most once, however many parameters
	// read it. Building it marshals the caller's groups and extra into JSON, and
	// doing that again for every parameter that asks is work with one answer.
	//
	// Deliberately not read back out of args, even though builtinArgs has
	// already put it there: a projection is free to declare a parameter of its
	// own called "user", and reading through args would then hand the next
	// parameter that asks for the caller that projection's literal instead.
	var caller map[string]any
	identity := func(key string) any {
		if caller == nil {
			caller = callerArgs(ctx)
		}
		return caller[key]
	}

	for _, p := range params {
		switch p.From {
		case crispv1alpha1.ParameterSourceValue:
			// The declared type matters: a driver binding "1000" as text where
			// the database expects a number is a syntax error, not a cast.
			value, err := projection.CoerceValue(p.Value, p.Type)
			if err != nil {
				return fmt.Errorf("parameter %q: %w", p.Name, err)
			}
			args[p.Name] = value
		case crispv1alpha1.ParameterSourceRequestNamespace:
			args[p.Name] = namespace
		case crispv1alpha1.ParameterSourceRequestName:
			args[p.Name] = name
		case crispv1alpha1.ParameterSourceRequestUser:
			args[p.Name] = identity("user")
		case crispv1alpha1.ParameterSourceRequestUserUID:
			args[p.Name] = identity("userUID")
		case crispv1alpha1.ParameterSourceRequestUserGroups:
			args[p.Name] = identity("userGroups")
		case crispv1alpha1.ParameterSourceRequestUserExtra:
			args[p.Name] = identity("userExtra")
		case crispv1alpha1.ParameterSourceLabelSelector:
			if _, ok := args[p.Name]; !ok {
				args[p.Name] = nil
			}
		case crispv1alpha1.ParameterSourceField:
			if obj == nil {
				return fmt.Errorf("parameter %q reads a field, which is only available on writes", p.Name)
			}
			value, err := projection.FieldValue(obj, p.Path, p.Type)
			if err != nil {
				return fmt.Errorf("parameter %q: %w", p.Name, err)
			}
			args[p.Name] = value
		default:
			return fmt.Errorf("unknown parameter source %q for parameter %q", p.From, p.Name)
		}
	}
	return nil
}

// alwaysSelectableFields are guaranteed for every Kubernetes resource.
var alwaysSelectableFields = map[string]bool{
	"metadata.name":      true,
	"metadata.namespace": true,
}

// validateFieldSelector rejects selectors this projection cannot honour.
// Returning everything for a selector we do not understand would be worse than
// an error: the client would act on a set that is not the set it asked for.
func (r *REST) validateFieldSelector(selector fields.Selector) error {
	if selector == nil || selector.Empty() {
		return nil
	}

	for _, requirement := range selector.Requirements() {
		if alwaysSelectableFields[requirement.Field] {
			continue
		}
		if _, declared := r.selectable[requirement.Field]; declared {
			continue
		}
		return errors.NewBadRequest(fmt.Sprintf(
			"field selector %q is not supported; %s can be selected on %s",
			requirement.Field, r.groupResource().String(), strings.Join(r.selectableKeys(), ", ")))
	}
	return nil
}

// selectableKeys lists everything this projection can be filtered on, sorted.
func (r *REST) selectableKeys() []string {
	keys := make([]string, 0, len(r.selectable)+len(alwaysSelectableFields))
	for key := range alwaysSelectableFields {
		keys = append(keys, key)
	}
	for key := range r.selectable {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// fieldsOf is the set a selector is matched against: the two guaranteed fields
// plus whatever the projection declared.
func (r *REST) fieldsOf(obj *unstructured.Unstructured) fields.Set {
	set := fields.Set{
		"metadata.name":      obj.GetName(),
		"metadata.namespace": obj.GetNamespace(),
	}

	for key := range r.selectable {
		value, found, err := unstructured.NestedFieldNoCopy(obj.Object, strings.Split(key, ".")...)
		if err != nil || !found || value == nil {
			set[key] = ""
			continue
		}
		set[key] = fmt.Sprint(value)
	}
	return set
}

// matchesFields applies a validated field selector to one object.
func (r *REST) matchesFields(obj *unstructured.Unstructured, selector fields.Selector) bool {
	if selector == nil || selector.Empty() {
		return true
	}
	return selector.Matches(r.fieldsOf(obj))
}

// bindSelectableFields offers the requested values to the query, so a
// projection that declares a column can filter in the database instead of
// after mapping.
func (r *REST) bindSelectableFields(args map[string]any, selector fields.Selector) {
	for _, field := range r.selectable {
		if field.Column == "" {
			continue
		}
		// Always bound, so a statement can reference them unconditionally:
		// "WHERE (:customer::text IS NULL OR customer = :customer)".
		args[field.Column] = nil
		args[field.Column+"_not"] = nil
	}
	if selector == nil {
		return
	}

	// A field selector understands = and != and nothing else, so those are the
	// two that can be pushed down.
	for _, requirement := range selector.Requirements() {
		field, declared := r.selectable[requirement.Field]
		if !declared || field.Column == "" {
			continue
		}
		switch requirement.Operator {
		case selection.Equals, selection.DoubleEquals:
			args[field.Column] = requirement.Value
		case selection.NotEquals:
			args[field.Column+"_not"] = requirement.Value
		}
	}
}

// bindIdentitySelector offers a metadata.name or metadata.namespace selector to
// the query.
//
// These two are selectable on every resource, so unlike a declared field there
// is no column to hang them off — but a projection already binds both: `:name`
// on every query, and `:namespace` as what a list is scoped by. Offering the
// requested value through them turns `--field-selector metadata.name=x` from a
// scan that is filtered down to one row into the lookup a get would have done.
//
// As with every other pushdown here, the values are bound whether or not the
// client asked and the result is filtered again after mapping, so a statement
// that ignores them stays correct and one that uses them stays correct too.
func (r *REST) bindIdentitySelector(args map[string]any, selector fields.Selector, namespace string) {
	if selector == nil {
		return
	}

	for _, requirement := range selector.Requirements() {
		switch requirement.Field {
		case "metadata.name":
			switch requirement.Operator {
			case selection.Equals, selection.DoubleEquals:
				args["name"] = requirement.Value

				// A composite identity is bound one column at a time, exactly
				// as a get does, so the query can compare the columns rather
				// than the name they were joined into. A value that does not
				// split names no object at all — the post-filter says so, and
				// leaving the columns unbound keeps the statement valid.
				identity, err := r.mapper.SplitName(requirement.Value)
				if err != nil {
					continue
				}
				for column, value := range identity {
					args[column] = value
				}
			case selection.NotEquals:
				args["name_not"] = requirement.Value
			}

		case "metadata.namespace":
			// Only when the read is not already scoped to one. A namespaced
			// request has its namespace from the path, and a selector naming a
			// different one matches nothing — which the post-filter decides.
			// Overwriting the binding there would have the query read a
			// namespace the request was never for.
			if namespace != "" {
				continue
			}
			if requirement.Operator == selection.Equals || requirement.Operator == selection.DoubleEquals {
				args["namespace"] = requirement.Value
			}
		}
	}
}

// bindLabelSelector offers a label selector to the query one label at a time.
//
// :labelSelector hands the whole selector over as text, which a projection can
// do something with only by parsing it in SQL. A label mapped to a column can
// be filtered directly instead, and that is what turns a selective list from a
// full scan into an index lookup.
//
// Everything is bound whether or not the client asked for it, and everything is
// filtered again after mapping — so a statement that ignores these parameters
// stays correct, and one that uses some of them stays correct too.
func (r *REST) bindLabelSelector(args map[string]any, selector labels.Selector) {
	for _, column := range r.labelColumns {
		args["label_"+column] = nil
		args["label_"+column+"_not"] = nil
		args["label_"+column+"_in"] = nil
	}
	if selector == nil {
		return
	}

	requirements, selectable := selector.Requirements()
	if !selectable {
		return
	}

	for _, requirement := range requirements {
		column, mapped := r.labelColumns[requirement.Key()]
		if !mapped {
			continue
		}
		values := requirement.ValuesUnsorted()

		switch requirement.Operator() {
		case selection.Equals, selection.DoubleEquals:
			if len(values) == 1 {
				args["label_"+column] = values[0]
			}
		case selection.NotEquals:
			if len(values) == 1 {
				args["label_"+column+"_not"] = values[0]
			}
		case selection.In:
			// A JSON array, which is the one shape every supported driver can
			// take apart.
			if encoded, err := json.Marshal(values); err == nil {
				args["label_"+column+"_in"] = string(encoded)
			}
		}
	}
}

// continueToken describes where the next page starts. After is used for keyset
// paging, which is stable under concurrent inserts; Offset is the fallback for
// projections that can only skip rows by position.
type continueToken struct {
	Offset int64 `json:"offset,omitempty"`

	// After is the last page's value of the keyset column, whatever type that
	// column has. It is carried as it came out of the database so the next page
	// compares like with like.
	After    any   `json:"after,omitempty"`
	Consumed int64 `json:"consumed,omitempty"`
}

func encodeContinue(token continueToken) string {
	raw, err := json.Marshal(token)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeContinue(encoded string) (continueToken, error) {
	if encoded == "" {
		return continueToken{}, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return continueToken{}, fmt.Errorf("invalid continue token")
	}

	// JSON has one number type, and decoding it into an any gives a float64.
	// A key that left as an integer has to come back as one, or the database
	// compares it against a float — and above 2^53 a float64 cannot hold it at
	// all. Rounding it first and then checking whether the result is a whole
	// number says yes to a value that has already lost its last digits, so the
	// next page starts from a key that was never in the table.
	// CockroachDB's unique_rowid() is squarely in that range.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var token continueToken
	if err := decoder.Decode(&token); err != nil || token.Offset < 0 || token.Consumed < 0 {
		return continueToken{}, fmt.Errorf("invalid continue token")
	}

	if number, ok := token.After.(json.Number); ok {
		value, err := number.Int64()
		if err == nil {
			token.After = value
		} else {
			fractional, err := number.Float64()
			if err != nil {
				return continueToken{}, fmt.Errorf("invalid continue token")
			}
			token.After = fractional
		}
	}
	return token, nil
}

// namespaceArg maps the empty namespace onto NULL for binding.
func namespaceArg(namespace string) any {
	if namespace == "" {
		return nil
	}
	return namespace
}

func namespaceFrom(ctx context.Context, namespaced bool) string {
	if !namespaced {
		return ""
	}
	ns, _ := genericapirequest.NamespaceFrom(ctx)
	return ns
}

func listKind(res crispv1alpha1.ProjectedResource) string {
	if res.ListKind != "" {
		return res.ListKind
	}
	return res.Kind + "List"
}

func durationOf(d *metav1.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.Duration
}

func maxRowsOf(v *int32) int {
	if v == nil {
		return 0
	}
	return int(*v)
}

// maxBytesOf turns a declared quantity into a byte count, or 0 for the default.
//
// A negative or absurd value is treated as unset rather than refused, because
// this bounds a read rather than describing one: the safe reading of a limit
// nobody can have meant is the limit the server would have applied anyway.
func maxBytesOf(q *resource.Quantity) int {
	if q == nil {
		return 0
	}
	value, ok := q.AsInt64()
	if !ok || value <= 0 || value > maxInt {
		return 0
	}
	return int(value)
}

// maxInt bounds the conversion below, since a quantity is an int64 and this
// build's int may not be.
const maxInt = int64(^uint(0) >> 1)
