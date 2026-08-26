package dynamic

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/klog/v2"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	"github.com/mrueg/kube-crisp/pkg/projection"
	projectionregistry "github.com/mrueg/kube-crisp/pkg/registry/projection"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// Compiler turns CustomResourceProjection objects into servable resources.
type Compiler struct {
	// Pools supplies connection pools, shared between projections that name
	// the same data source.
	Pools *crispsql.PoolCache

	// Resolver turns a data source reference into a connection string.
	Resolver projection.DSNResolver

	// Schemas resolves a schema borrowed from a CustomResourceDefinition. It is
	// nil when no client is available, in which case schemaFrom is rejected
	// rather than silently ignored.
	Schemas projection.SchemaResolver

	// MaxOpenConns bounds connections to any one database, whatever a
	// projection asks for. Zero leaves the projection's own setting alone.
	MaxOpenConns int
}

// Prepared is a projection's resolved data source plus what identifies the
// resources compiled from it.
type Prepared struct {
	// Pool answers this projection's queries, shared with every other
	// projection reaching the same database.
	Pool    *crispsql.Pool
	PoolKey string

	// ReadPool answers the reads that can tolerate replication lag, and is nil
	// unless the projection names a replica. ReadPoolKey is its pool key, so a
	// replica nobody references any more is released like any other.
	ReadPool    *crispsql.Pool
	ReadPoolKey string

	// Fingerprint changes whenever anything the compiled resources depend on
	// does: the projection's spec, or the connection string its data source
	// resolves to. Equal fingerprints mean a recompile would produce the same
	// storage, so the existing storage can be kept — along with its watch
	// cache, its read cache, and the queries it has in flight.
	Fingerprint string
}

// Prepare resolves a projection's data source and reports what identifies the
// resources it would compile to.
//
// It is separate from Compile so that a sync can decide whether to recompile at
// all. Recompiling builds fresh storage, and fresh storage means an empty watch
// cache, an empty read cache, and every watcher relisting — which is the right
// answer when a projection changed and the wrong one when it did not.
func (c *Compiler) Prepare(ctx context.Context, p *crispv1alpha1.CustomResourceProjection) (*Prepared, error) {
	if err := projection.Validate(p); err != nil {
		return nil, err
	}

	pool, poolKey, err := c.pool(ctx, p.Spec.DataSource)
	if err != nil {
		return nil, fmt.Errorf("connecting data source: %w", err)
	}

	prepared := &Prepared{Pool: pool, PoolKey: poolKey}

	// The replica is resolved the same way, so a rotated replica credential
	// changes the fingerprint and rebuilds the storage exactly as the
	// primary's would.
	readDSN, hasReplica, err := projection.ResolveRead(ctx, c.Resolver, p.Spec.DataSource)
	if err != nil {
		return nil, fmt.Errorf("resolving the read replica: %w", err)
	}
	if hasReplica {
		readPool, readKey, err := c.poolFor(p.Spec.DataSource, readDSN)
		if err != nil {
			return nil, fmt.Errorf("connecting the read replica: %w", err)
		}
		prepared.ReadPool, prepared.ReadPoolKey = readPool, readKey
	}

	spec, err := json.Marshal(p.Spec)
	if err != nil {
		return nil, fmt.Errorf("fingerprinting projection %s: %w", p.Name, err)
	}
	digest := sha256.Sum256(append(spec, []byte("\x00"+poolKey+"\x00"+prepared.ReadPoolKey)...))
	prepared.Fingerprint = hex.EncodeToString(digest[:])

	return prepared, nil
}

// Check reports whether a projection could be served, without building anything
// to serve it with.
//
// The same two questions Compile asks — does the spec validate, and can the
// database run the statements — and the same answers, so an admission webhook
// using this cannot accept something the server would then refuse.
//
// A database that cannot be reached is not an objection. The question this
// answers is whether the projection is wrong, and an outage is not evidence
// either way; refusing on it would make every projection unapplyable while a
// database was down, including the ones that would fix it.
func (c *Compiler) Check(ctx context.Context, p *crispv1alpha1.CustomResourceProjection) error {
	used, err := c.check(ctx, p)

	// A pool closed underneath the check is this server's bookkeeping, not
	// anything about the projection.
	//
	// Pools are shared and released when no installed projection references
	// them any more — and a projection being admitted is not installed, so a
	// sync landing mid-check can close the very pool the check is using. That
	// came back as "sql: database is closed" and was reported as SQL the
	// database could not run, which refused a perfectly good projection.
	//
	// The closed pool is still the one the cache would hand out, so it is
	// dropped before trying again. Once: if it happens twice something other
	// than a race is going on, and the check gives way rather than objecting to
	// a projection on grounds that are not about the projection.
	if isClosedPool(err) {
		klog.V(2).InfoS("the data source pool was closed while checking a projection; retrying",
			"projection", p.Name)
		if used != nil {
			// Compare-and-delete, so a pool that has already been replaced is
			// left alone. Evicting by key would close whichever pool is under
			// it now, which may be one live projections are serving through.
			c.Pools.EvictIf(used.PoolKey, used.Pool)
			if used.ReadPoolKey != "" && used.ReadPool != nil {
				c.Pools.EvictIf(used.ReadPoolKey, used.ReadPool)
			}
		}
		if _, err = c.check(ctx, p); isClosedPool(err) {
			klog.V(2).InfoS("the data source pool was closed again while checking a projection; not objecting",
				"projection", p.Name)
			return nil
		}
	}
	return err
}

// check is one attempt at Check, reporting which pools it used so the caller can
// drop exactly those and no others if they turn out to have been closed.
func (c *Compiler) check(ctx context.Context, p *crispv1alpha1.CustomResourceProjection) (*Prepared, error) {
	prepared, err := c.Prepare(ctx, p)
	if err != nil {
		return nil, err
	}
	if err := prepared.Pool.Ping(ctx); err != nil {
		// A closed pool is not an unreachable database, and treating it as one
		// would quietly skip the check. Returned so the caller can drop it and
		// try again with a live one.
		if isClosedPool(err) {
			return prepared, err
		}
		klog.V(2).InfoS("data source unreachable while checking a projection; not objecting",
			"projection", p.Name, "err", err)
		return prepared, nil
	}

	if err := checkStatements(ctx, prepared.Pool, p); err != nil {
		var unreachable *unreachableError
		if goerrors.As(err, &unreachable) {
			return prepared, nil
		}
		return prepared, err
	}
	return prepared, nil
}

// isClosedPool reports whether an error is a pool that was closed while it was
// being used, rather than anything the database said about a statement.
func isClosedPool(err error) bool {
	if err == nil {
		return false
	}
	// database/sql answers a closed pool with this sentinel, and wraps it in
	// nothing — but the message travels through the query error this package
	// builds, so the string is checked too.
	return goerrors.Is(err, sql.ErrConnDone) || strings.Contains(err.Error(), "sql: database is closed")
}

// Compile validates a projection, connects its data source, and builds the
// REST storage for every version it serves.
func (c *Compiler) Compile(ctx context.Context, p *crispv1alpha1.CustomResourceProjection) ([]Resource, error) {
	prepared, err := c.Prepare(ctx, p)
	if err != nil {
		return nil, err
	}
	return c.CompileWith(ctx, p, prepared)
}

// CompileWith builds the REST storage for every version a projection serves,
// against a data source Prepare already resolved.
func (c *Compiler) CompileWith(ctx context.Context, p *crispv1alpha1.CustomResourceProjection, prepared *Prepared) ([]Resource, error) {
	pool, poolKey := prepared.Pool, prepared.PoolKey

	// An unreachable database is not a broken projection. The resource stays
	// installed and answers 503 until the database comes back, rather than
	// disappearing from discovery and taking its watchers with it.
	reachable := true
	var reachErr error
	if err := pool.Ping(ctx); err != nil {
		reachable, reachErr = false, err
		klog.InfoS("data source is unreachable; the projection will serve 503 until it recovers",
			"projection", p.Name, "driver", p.Spec.DataSource.Driver, "err", err)
	}
	// A replica that is down is reported the same way. It is not a lesser
	// failure: reads are what a projection mostly does, and they all go there.
	if prepared.ReadPool != nil {
		if err := prepared.ReadPool.Ping(ctx); err != nil {
			reachable, reachErr = false, err
			klog.InfoS("read replica is unreachable; the projection will serve 503 for reads until it recovers",
				"projection", p.Name, "driver", p.Spec.DataSource.Driver, "err", err)
		}
	}

	// Every statement, put to the database before anything is served.
	//
	// Only when it is reachable: an unreachable database cannot answer the
	// question, and refusing to compile because nobody was there to ask would
	// turn an outage into a projection that disappears from discovery.
	if reachable {
		if err := checkStatements(ctx, pool, p); err != nil {
			var unreachable *unreachableError
			if goerrors.As(err, &unreachable) {
				// The database went away between the Ping and this. Same
				// situation as above, and the same answer.
				reachable, reachErr = false, unreachable.err
				klog.InfoS("data source went away while checking statements; the projection will serve 503 until it recovers",
					"projection", p.Name, "err", unreachable.err)
			} else {
				return nil, err
			}
		}
	}

	versions, err := c.versions(ctx, p)
	if err != nil {
		return nil, err
	}

	// One set of shared read state for the whole projection: its versions are
	// different views of the same rows, and should not each read them.
	shared := projectionregistry.NewShared(
		p.Name, projectionregistry.ResourceLabel(p.Spec.Resource), notifier(p, pool))

	resources := make([]Resource, 0, len(versions))
	for _, version := range versions {
		// Each version is compiled from a spec of its own: same queries and
		// same rows, but its own schema and mapping.
		spec := p.Spec.DeepCopy()
		spec.Resource.Version = version.Name
		spec.Resource.Schema = version.Schema
		spec.Resource.SchemaFrom = nil
		spec.Resource.AdditionalPrinterColumns = version.PrinterColumns
		spec.Resource.Versions = nil
		if version.Mapping != nil {
			spec.Mapping = *version.Mapping
		}

		storages, err := projectionregistry.New(p.Name, *spec, pool, prepared.ReadPool, shared)
		if err != nil {
			return nil, fmt.Errorf("version %s: %w", version.Name, err)
		}

		res := spec.Resource

		resources = append(resources, Resource{
			Group:            res.Group,
			Version:          res.Version,
			Plural:           res.Plural,
			Kind:             res.Kind,
			Singular:         Singular(res),
			ListKind:         listKind(res),
			Schema:           res.Schema,
			PrinterColumns:   res.AdditionalPrinterColumns,
			Namespaced:       res.Scope == crispv1alpha1.NamespaceScoped,
			ShortNames:       res.ShortNames,
			Categories:       res.Categories,
			SelectableFields: res.SelectableFields,
			Storage:          storages.Resource,
			StatusStorage:    storages.Status,
			ScaleStorage:     storages.Scale,
			ScaleSubresource: scaleSubresource(res),
			ProjectionName:   p.Name,
			PoolKey:          poolKey,
			ReadPoolKey:      prepared.ReadPoolKey,
			DataSourceReady:  reachable,
			DataSourceError:  reachErr,
		})
	}

	return resources, nil
}

// notifier builds the subscription a watched projection is woken by, or nil
// when it has not asked for one.
//
// A notification is only a hint to poll now, so this is additive: without it a
// watch lags by its poll interval, with it by a round trip, and if the
// subscription fails the interval is still what it was.
func notifier(p *crispv1alpha1.CustomResourceProjection, pool *crispsql.Pool) projectionregistry.Notifier {
	if p.Spec.Watch == nil || p.Spec.Watch.Notify == nil || p.Spec.Watch.Disabled {
		return nil
	}
	channel := p.Spec.Watch.Notify.Channel

	return func(ctx context.Context) (<-chan struct{}, error) {
		return pool.Listen(ctx, channel)
	}
}

// compiledVersion is one served version with its schema resolved.
type compiledVersion struct {
	Name           string
	Schema         *apiextensionsv1.JSONSchemaProps
	Mapping        *crispv1alpha1.Mapping
	PrinterColumns []apiextensionsv1.CustomResourceColumnDefinition
}

// versions expands a projection into the versions it serves, resolving any
// schema borrowed from a CustomResourceDefinition along the way.
func (c *Compiler) versions(ctx context.Context, p *crispv1alpha1.CustomResourceProjection) ([]compiledVersion, error) {
	res := p.Spec.Resource

	primary := compiledVersion{
		Name:           res.Version,
		Schema:         res.Schema,
		PrinterColumns: res.AdditionalPrinterColumns,
	}
	if primary.Schema == nil && res.SchemaFrom != nil {
		borrowed, err := c.borrow(ctx, *res.SchemaFrom)
		if err != nil {
			return nil, fmt.Errorf("version %s: %w", res.Version, err)
		}
		primary.Schema = borrowed
	}

	versions := []compiledVersion{primary}
	for _, extra := range res.Versions {
		if extra.Served != nil && !*extra.Served {
			continue
		}
		if extra.Name == res.Version {
			return nil, fmt.Errorf("version %s is declared twice", extra.Name)
		}

		compiled := compiledVersion{
			Name:           extra.Name,
			Schema:         extra.Schema,
			Mapping:        extra.Mapping,
			PrinterColumns: extra.AdditionalPrinterColumns,
		}
		if compiled.PrinterColumns == nil {
			compiled.PrinterColumns = res.AdditionalPrinterColumns
		}
		if compiled.Schema == nil && extra.SchemaFrom != nil {
			borrowed, err := c.borrow(ctx, *extra.SchemaFrom)
			if err != nil {
				return nil, fmt.Errorf("version %s: %w", extra.Name, err)
			}
			compiled.Schema = borrowed
		}
		if compiled.Schema == nil {
			return nil, fmt.Errorf("version %s: one of schema or schemaFrom is required", extra.Name)
		}

		versions = append(versions, compiled)
	}
	if err := checkRoundTrip(res, p.Spec.Mapping, versions); err != nil {
		return nil, err
	}
	return versions, nil
}

// checkRoundTrip refuses versions that map different columns.
//
// There is no conversion between the versions of a projected kind: each reads
// the same rows and maps them its own way. That is only safe while they cover
// the same columns — otherwise a client writing through one version drops a
// value another version displays, and neither of them can tell. Saying so at
// load time beats letting a controller discover it.
func checkRoundTrip(res crispv1alpha1.ProjectedResource, base crispv1alpha1.Mapping, versions []compiledVersion) error {
	if res.Conversion == crispv1alpha1.ConversionNone || len(versions) < 2 {
		return nil
	}

	// A version without a mapping of its own uses the projection's, which is
	// what makes an added version cheap when only the schema differs.
	effective := func(version compiledVersion) sets.Set[string] {
		if version.Mapping == nil {
			return mappedColumns(&base)
		}
		return mappedColumns(version.Mapping)
	}

	primary := effective(versions[0])
	for _, version := range versions[1:] {
		columns := effective(version)
		if missing := primary.Difference(columns); missing.Len() > 0 {
			return fmt.Errorf(
				"version %s does not map %s, which %s does: a write through %s would drop it. Map the same columns, or set conversion: None to say the versions differ on purpose",
				version.Name, strings.Join(sets.List(missing), ", "), versions[0].Name, version.Name)
		}
		if extra := columns.Difference(primary); extra.Len() > 0 {
			return fmt.Errorf(
				"version %s maps %s, which %s does not: a write through %s would drop it. Map the same columns, or set conversion: None to say the versions differ on purpose",
				version.Name, strings.Join(sets.List(extra), ", "), versions[0].Name, versions[0].Name)
		}
	}
	return nil
}

// mappedColumns is every column a version reads or writes through its mapping.
func mappedColumns(mapping *crispv1alpha1.Mapping) sets.Set[string] {
	columns := sets.New[string]()
	if mapping == nil {
		return columns
	}

	for _, column := range []string{
		mapping.Name, mapping.Namespace, mapping.UID, mapping.ResourceVersion,
		mapping.CreationTimestamp, mapping.DeletionTimestamp, mapping.Generation,
		mapping.Finalizers, mapping.OwnerReferences, mapping.ManagedFields,
		mapping.LabelsFrom, mapping.AnnotationsFrom,
	} {
		if column != "" {
			columns.Insert(column)
		}
	}
	columns.Insert(mapping.NameColumns...)
	for _, column := range mapping.Labels {
		columns.Insert(column)
	}
	for _, column := range mapping.Annotations {
		columns.Insert(column)
	}
	for _, field := range mapping.Fields {
		columns.Insert(field.Column)
	}
	return columns
}

// borrow resolves a schema from a CustomResourceDefinition.
func (c *Compiler) borrow(ctx context.Context, ref crispv1alpha1.CRDReference) (*apiextensionsv1.JSONSchemaProps, error) {
	if c.Schemas == nil {
		return nil, fmt.Errorf("schemaFrom needs a cluster connection to read the CustomResourceDefinition")
	}

	borrowed, err := c.Schemas.Resolve(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("resolving schemaFrom: %w", err)
	}
	return borrowed, nil
}

// pool returns the shared pool for a data source, opening it on first use.
//
// The connection string is resolved before the lookup rather than inside it, so
// that a rotated credential produces a different key and therefore a new pool.
// Resolving costs one Secret read per projection per sync.
func (c *Compiler) pool(ctx context.Context, ds crispv1alpha1.DataSource) (*crispsql.Pool, string, error) {
	dsn, err := c.Resolver.Resolve(ctx, ds)
	if err != nil {
		return nil, "", fmt.Errorf("resolving data source: %w", err)
	}
	return c.poolFor(ds, dsn)
}

// poolFor returns the shared pool for one already-resolved connection string.
// The primary and a read replica differ only in that, so they open the same way
// and are keyed the same way — two projections pointed at one replica share its
// pool exactly as they would share a primary.
func (c *Compiler) poolFor(ds crispv1alpha1.DataSource, dsn string) (*crispsql.Pool, string, error) {
	key := projection.PoolKey(ds, dsn)
	pool, err := c.Pools.Get(key, func() (*crispsql.Pool, error) {
		opts := crispsql.PoolOptions{
			Name:   projection.PoolLabel(ds, dsn),
			Driver: ds.Driver,
			DSN:    dsn,
			// Prepared statements and a keep-alive are on by default: both
			// matter for read latency and neither changes semantics.
			PreparedStatements: ds.PreparedStatements == nil || *ds.PreparedStatements,
			KeepAliveInterval:  crispsql.DefaultKeepAlive,
			// Off unless asked for: it puts every query in a transaction.
			StatementTimeout: ds.StatementTimeout != nil && *ds.StatementTimeout,
		}
		if ds.KeepAliveInterval != nil {
			opts.KeepAliveInterval = ds.KeepAliveInterval.Duration
		}
		if ds.MaxOpenConns != nil {
			opts.MaxOpenConns = int(*ds.MaxOpenConns)
		}
		// The server's ceiling wins, so no projection can raise the number of
		// connections opened against a database beyond what it can take.
		if c.MaxOpenConns > 0 && (opts.MaxOpenConns == 0 || opts.MaxOpenConns > c.MaxOpenConns) {
			if opts.MaxOpenConns > c.MaxOpenConns {
				klog.V(2).InfoS("capping a projection's connection pool",
					"requested", opts.MaxOpenConns, "limit", c.MaxOpenConns)
			}
			opts.MaxOpenConns = c.MaxOpenConns
		}
		if ds.MaxIdleConns != nil {
			opts.MaxIdleConns = int(*ds.MaxIdleConns)
		}
		if ds.ConnMaxLifetime != nil {
			opts.ConnMaxLifetime = ds.ConnMaxLifetime.Duration
		}
		if ds.ConnMaxIdleTime != nil {
			opts.ConnMaxIdleTime = ds.ConnMaxIdleTime.Duration
		}

		return crispsql.Open(opts)
	})
	if err != nil {
		return nil, "", err
	}
	return pool, key, nil
}

// scaleSubresource restates a projection's scale paths in the shape the
// OpenAPI builder expects.
func scaleSubresource(res crispv1alpha1.ProjectedResource) *apiextensionsv1.CustomResourceSubresourceScale {
	if res.Subresources == nil || res.Subresources.Scale == nil {
		return nil
	}

	scale := &apiextensionsv1.CustomResourceSubresourceScale{
		SpecReplicasPath:   res.Subresources.Scale.SpecReplicasPath,
		StatusReplicasPath: res.Subresources.Scale.StatusReplicasPath,
	}
	if path := res.Subresources.Scale.LabelSelectorPath; path != "" {
		scale.LabelSelectorPath = &path
	}
	return scale
}

// listKind returns the list kind for a projected resource.
func listKind(res crispv1alpha1.ProjectedResource) string {
	if res.ListKind != "" {
		return res.ListKind
	}
	return res.Kind + "List"
}

// Singular returns the singular resource name for a projected resource.
func Singular(res crispv1alpha1.ProjectedResource) string {
	if res.Singular != "" {
		return res.Singular
	}
	return strings.ToLower(res.Kind)
}

// unreachableError marks a statement check that failed because the database
// could not be reached, rather than because the statement was wrong.
type unreachableError struct{ err error }

func (e *unreachableError) Error() string { return e.err.Error() }
func (e *unreachableError) Unwrap() error { return e.err }

// checkStatements asks the database whether every statement a projection would
// run is one it could run.
//
// This is the difference between finding out at kubectl apply and finding out
// from a client. Without it a projection whose SQL has outlived its schema — a
// renamed column, a dropped table, a migration that landed — compiles, reports
// Ready, appears in discovery, and fails every single request with a 500. The
// author gets no signal at all; the first person to know is whoever called it.
//
// A failure here fails the compilation, which leaves the previous configuration
// serving if there was one and reports CompilationFailed on the projection.
// That is the same treatment a projection whose spec does not validate gets,
// and for the same reason: it cannot work, and saying so where the author is
// looking is the whole point.
func checkStatements(ctx context.Context, pool *crispsql.Pool, p *crispv1alpha1.CustomResourceProjection) error {
	for _, q := range namedQueries(&p.Spec.Queries) {
		for _, statement := range q.statements() {
			if err := pool.Check(ctx, statement); err != nil {
				if crispsql.IsUnavailable(err) {
					return &unreachableError{err: err}
				}
				return fmt.Errorf("queries.%s: the database cannot run this statement: %w", q.name, err)
			}
		}
	}
	return nil
}

// namedQuery is one of a projection's queries with the field it came from, so a
// failure names the query the author has to go and fix.
type namedQuery struct {
	name  string
	query *crispv1alpha1.Query
}

// statements returns every statement the query would run: the multi-statement
// form when it has one, otherwise the single statement.
func (q namedQuery) statements() []string {
	if len(q.query.Statements) > 0 {
		return q.query.Statements
	}
	if q.query.SQL == "" {
		return nil
	}
	return []string{q.query.SQL}
}

// namedQueries lists every query a projection declares. Named one at a time
// rather than by reflection, so a query added to the API without being checked
// here is a compile error rather than a statement that quietly stops being
// validated.
func namedQueries(qs *crispv1alpha1.Queries) []namedQuery {
	out := []namedQuery{{name: "list", query: &qs.List}}
	for _, candidate := range []struct {
		name  string
		query *crispv1alpha1.Query
	}{
		{"get", qs.Get},
		{"create", qs.Create},
		{"update", qs.Update},
		{"delete", qs.Delete},
		{"markDeleted", qs.MarkDeleted},
		{"deleteCollection", qs.DeleteCollection},
		{"updateStatus", qs.UpdateStatus},
		{"count", qs.Count},
	} {
		if candidate.query != nil {
			out = append(out, namedQuery{name: candidate.name, query: candidate.query})
		}
	}
	return out
}
