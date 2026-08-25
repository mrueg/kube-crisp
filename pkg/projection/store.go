package projection

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	crispv1alpha1 "github.com/mrueg/kube-crisp/pkg/apis/crisp/v1alpha1"
	crispsql "github.com/mrueg/kube-crisp/pkg/sql"
)

// LoadDir reads every .yaml or .yml file under dir as a CustomResourceProjection.
//
// This is the bootstrap path: it serves projections that exist as files rather
// than as cluster objects, which is what makes the server runnable without a
// cluster at all. Projections that do live in the cluster are watched and
// installed while the server runs, by pkg/controller/projection.
func LoadDir(dir string) ([]crispv1alpha1.CustomResourceProjection, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading projection directory %s: %w", dir, err)
	}

	var out []crispv1alpha1.CustomResourceProjection
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		// The directory is one the operator named on the command line and the
		// entries come from reading it, so neither half of this path is
		// attacker-controlled and no name can climb out of it.
		path := filepath.Join(dir, e.Name())

		loaded, err := LoadFile(path)
		if err != nil {
			return nil, err
		}
		for i := range loaded {
			if err := Validate(&loaded[i]); err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
		}
		out = append(out, loaded...)
	}

	return out, nil
}

// LoadFile reads every CustomResourceProjection in one YAML file.
//
// Parsing only: what comes back is what the file says, not what has been
// checked. LoadDir validates on top of this because a projection it cannot
// serve is one it must refuse; `kube-crisp-apiserver validate` wants the parsed
// projections whether or not they pass, so it can report each one rather than
// stopping at the first.
func LoadFile(path string) ([]crispv1alpha1.CustomResourceProjection, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	documents, err := splitDocuments(data)
	if err != nil {
		return nil, fmt.Errorf("splitting %s: %w", path, err)
	}

	var out []crispv1alpha1.CustomResourceProjection
	for i, doc := range documents {
		if len(strings.TrimSpace(string(doc))) == 0 {
			continue
		}
		// Check the kind before decoding strictly, so a directory may also hold
		// Secrets, ConfigMaps, or anything else without the loader rejecting
		// the file.
		var typeMeta metav1.TypeMeta
		if err := yaml.Unmarshal(doc, &typeMeta); err != nil {
			return nil, fmt.Errorf("parsing %s document %d: %w", path, i, err)
		}
		if typeMeta.Kind != "CustomResourceProjection" {
			continue
		}

		var p crispv1alpha1.CustomResourceProjection
		if err := yaml.UnmarshalStrict(doc, &p); err != nil {
			return nil, fmt.Errorf("parsing %s document %d: %w", path, i, err)
		}
		out = append(out, p)
	}

	return out, nil
}

// splitDocuments separates a multi-document YAML file.
//
// It goes through the YAML reader rather than splitting on "---" textually: a
// projection is mostly SQL, and a statement holding a line that begins with a
// comment such as "--- rows written by the importer" would otherwise be cut in
// half, taking the rest of the projection with it.
func splitDocuments(data []byte) ([][]byte, error) {
	reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))

	var docs [][]byte
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return docs, nil
		}
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
}

// Validate checks the invariants the apiserver relies on before installing a
// projection. It complements, rather than replaces, CRD schema validation.
func Validate(p *crispv1alpha1.CustomResourceProjection) error {
	res := p.Spec.Resource

	switch {
	case res.Group == "":
		return fmt.Errorf("projection %s: spec.resource.group is required", p.Name)
	case res.Version == "":
		return fmt.Errorf("projection %s: spec.resource.version is required", p.Name)
	case res.Kind == "":
		return fmt.Errorf("projection %s: spec.resource.kind is required", p.Name)
	case res.Plural == "":
		return fmt.Errorf("projection %s: spec.resource.plural is required", p.Name)
	case res.Plural != strings.ToLower(res.Plural):
		return fmt.Errorf("projection %s: spec.resource.plural must be lowercase", p.Name)
	}

	if res.Scope != crispv1alpha1.NamespaceScoped && res.Scope != crispv1alpha1.ClusterScoped {
		return fmt.Errorf("projection %s: spec.resource.scope must be Namespaced or Cluster", p.Name)
	}

	if res.Schema == nil && res.SchemaFrom == nil {
		return fmt.Errorf("projection %s: exactly one of spec.resource.schema or spec.resource.schemaFrom is required", p.Name)
	}

	seen := map[string]bool{res.Version: true}
	for _, version := range res.Versions {
		switch {
		case version.Name == "":
			return fmt.Errorf("projection %s: spec.resource.versions[].name is required", p.Name)
		case seen[version.Name]:
			return fmt.Errorf("projection %s: version %s is declared twice", p.Name, version.Name)
		case version.Schema == nil && version.SchemaFrom == nil:
			return fmt.Errorf("projection %s: version %s needs one of schema or schemaFrom", p.Name, version.Name)
		case version.Schema != nil && version.SchemaFrom != nil:
			return fmt.Errorf("projection %s: version %s sets both schema and schemaFrom", p.Name, version.Name)
		}
		seen[version.Name] = true

		if version.Mapping != nil {
			if _, err := NewMapper(res, *version.Mapping); err != nil {
				return fmt.Errorf("projection %s: version %s: %w", p.Name, version.Name, err)
			}
		}
	}
	if res.Schema != nil && res.SchemaFrom != nil {
		return fmt.Errorf("projection %s: spec.resource.schema and spec.resource.schemaFrom are mutually exclusive", p.Name)
	}

	if strings.TrimSpace(p.Spec.Queries.List.SQL) == "" {
		return fmt.Errorf("projection %s: spec.queries.list.sql is required", p.Name)
	}

	// A session variable's value is resolved from the request, and only some
	// sources have anything to give it. Field reads the submitted object, which
	// a read has none of; LabelSelector is a list's filter, not an identity.
	// Both resolve to the empty string, and an empty setting is the dangerous
	// kind of wrong: a row-level security policy comparing against it does not
	// fail, it just matches nothing — or, written the other way round,
	// everything.
	if _, known := crispsql.Lookup(p.Spec.DataSource.Driver); !known {
		return fmt.Errorf("projection %s: spec.dataSource.driver is %q; this build knows %s",
			p.Name, p.Spec.DataSource.Driver, strings.Join(crispsql.RegisteredDrivers(), ", "))
	}

	if timeout := p.Spec.DataSource.StatementTimeout; timeout != nil && *timeout &&
		!crispsql.SupportsStatementTimeout(p.Spec.DataSource.Driver) {
		return fmt.Errorf(
			"projection %s: spec.dataSource.statementTimeout is supported on the postgres driver only, not %q: "+
				"MySQL bounds read-only SELECTs rather than every statement, and SQLite has no equivalent",
			p.Name, p.Spec.DataSource.Driver)
	}

	for i, variable := range p.Spec.DataSource.SessionVariables {
		switch variable.From {
		case crispv1alpha1.ParameterSourceValue, crispv1alpha1.ParameterSourceRequestNamespace,
			crispv1alpha1.ParameterSourceRequestName, crispv1alpha1.ParameterSourceRequestUser,
			crispv1alpha1.ParameterSourceRequestUserUID, crispv1alpha1.ParameterSourceRequestUserGroups,
			crispv1alpha1.ParameterSourceRequestUserExtra:
		default:
			return fmt.Errorf(
				"projection %s: spec.dataSource.sessionVariables[%d].from is %q, which has no value outside a write; use Value, RequestNamespace, RequestName, or one of the RequestUser sources",
				p.Name, i, variable.From)
		}
		if err := crispsql.ValidateSessionVariableName(variable.Name); err != nil {
			return fmt.Errorf("projection %s: spec.dataSource.sessionVariables[%d]: %w", p.Name, i, err)
		}
	}

	if p.Spec.Watch != nil && p.Spec.Watch.Query != nil {
		if strings.TrimSpace(p.Spec.Watch.Query.SQL) == "" {
			return fmt.Errorf("projection %s: spec.watch.query.sql is required when a watch query is given", p.Name)
		}
		if p.Spec.Mapping.ResourceVersion == "" {
			return fmt.Errorf("projection %s: spec.watch.query needs mapping.resourceVersion, which is the value it pages through", p.Name)
		}
		if err := validateVersionIsDatabaseAssigned(p); err != nil {
			return err
		}
	}

	if p.Spec.Watch != nil && p.Spec.Watch.Notify != nil {
		if !crispsql.SupportsNotifications(p.Spec.DataSource.Driver) {
			return fmt.Errorf(
				"projection %s: spec.watch.notify needs a driver that can push a change notification, and %q cannot",
				p.Name, p.Spec.DataSource.Driver)
		}
		if err := crispsql.ValidateNotifyChannel(p.Spec.Watch.Notify.Channel); err != nil {
			return fmt.Errorf("projection %s: spec.watch.notify: %w", p.Name, err)
		}
		if p.Spec.Watch.Disabled {
			return fmt.Errorf(
				"projection %s: spec.watch.notify wakes a watch, and spec.watch.disabled turns watching off",
				p.Name)
		}
	}

	if p.Spec.Watch != nil && p.Spec.Watch.DeletedQuery != nil {
		if strings.TrimSpace(p.Spec.Watch.DeletedQuery.SQL) == "" {
			return fmt.Errorf("projection %s: spec.watch.deletedQuery.sql is required when a deletion query is given", p.Name)
		}
		if p.Spec.Watch.Query == nil {
			// Without an incremental poll every poll is a full read, which sees
			// deletions already; a deletion query would only add load.
			return fmt.Errorf(
				"projection %s: spec.watch.deletedQuery needs spec.watch.query, since a full poll already sees deletions", p.Name)
		}
	}

	// Turning the resync off means nothing else is looking for rows that
	// disappeared, so something has to be.
	if p.Spec.Watch != nil && p.Spec.Watch.Query != nil &&
		p.Spec.Watch.FullResyncInterval != nil && p.Spec.Watch.FullResyncInterval.Duration == 0 &&
		p.Spec.Watch.DeletedQuery == nil {
		return fmt.Errorf(
			"projection %s: spec.watch.fullResyncInterval is 0, which disables the only thing that notices a deleted row; "+
				"give spec.watch.deletedQuery a statement, or leave the resync enabled", p.Name)
	}

	// Mapping validity is enforced by NewMapper, which the apiserver calls when
	// it compiles the projection into REST storage.
	if _, err := NewMapper(res, p.Spec.Mapping); err != nil {
		return fmt.Errorf("projection %s: %w", p.Name, err)
	}

	return nil
}

// DSNResolver produces the connection string for a projection's data source.
type DSNResolver interface {
	Resolve(ctx context.Context, ds crispv1alpha1.DataSource) (string, error)
}

// ReadReplicaResolver is the part of resolving a data source that only matters
// to a projection naming a read replica.
//
// It is separate from DSNResolver so that a resolver which has no notion of
// replicas — a test stub, a file-backed one — does not have to say so. A
// resolver that does not implement it simply never has a replica to offer.
type ReadReplicaResolver interface {
	// ResolveRead produces the read replica's connection string, reporting
	// false when the projection names no replica.
	ResolveRead(ctx context.Context, ds crispv1alpha1.DataSource) (string, bool, error)
}

// ResolveRead asks a resolver for a replica, treating one that cannot answer as
// one that has none.
func ResolveRead(ctx context.Context, resolver DSNResolver, ds crispv1alpha1.DataSource) (string, bool, error) {
	replicas, ok := resolver.(ReadReplicaResolver)
	if !ok {
		return "", false, nil
	}
	return replicas.ResolveRead(ctx, ds)
}

// readDataSource restates a data source as its replica, so one resolver path
// serves both.
func readDataSource(ds crispv1alpha1.DataSource) (crispv1alpha1.DataSource, bool) {
	if ds.ReadSecretRef == nil {
		return ds, false
	}

	replica := ds
	replica.SecretRef = *ds.ReadSecretRef
	if ds.ReadDSNKey != "" {
		replica.DSNKey = ds.ReadDSNKey
	}
	replica.ReadSecretRef = nil
	replica.ReadDSNKey = ""
	return replica, true
}

// OptInLabel marks a Secret as usable as a projection's data source.
//
// A CustomResourceProjection is cluster-scoped and carries arbitrary SQL, so
// whoever can create one would otherwise be able to point at any Secret the
// server can read and run any statement with those credentials. Requiring the
// Secret's owner to opt in puts that decision back where it belongs.
const (
	OptInLabel = "crisp.kubecrisp.io/allow-projection"
	OptInValue = "true"
)

// SecretCache reads a data source Secret from a local cache instead of the API
// server.
//
// The connection string is resolved once per projection on every sync, which is
// a request per projection every ten minutes and on every change — against
// Secrets the server is already watching. A miss falls back to the client, so
// an unlabelled or not-yet-cached Secret behaves exactly as it did.
type SecretCache interface {
	Secret(namespace, name string) (*corev1.Secret, bool)
}

// SecretDSNResolver reads the DSN from a Secret in the cluster. This is the
// production path: credentials stay in Secrets and never appear in the
// CustomResourceProjection object.
type SecretDSNResolver struct {
	Client kubernetes.Interface

	// Cache answers from the informers watching data source Secrets, when
	// there are any. Nil falls back to reading through Client every time.
	Cache SecretCache

	// AllowedNamespaces restricts where a data source Secret may live. An
	// empty set allows any namespace, which is only safe when the server's own
	// RBAC is already narrow.
	AllowedNamespaces sets.Set[string]

	// RequireOptIn demands the OptInLabel on the Secret.
	RequireOptIn bool
}

// Resolve fetches the referenced Secret and returns the DSN it holds.
func (r *SecretDSNResolver) Resolve(ctx context.Context, ds crispv1alpha1.DataSource) (string, error) {
	if ds.SecretRef.Name == "" {
		return "", fmt.Errorf("dataSource.secretRef.name is required")
	}
	namespace := ds.SecretRef.Namespace
	if namespace == "" {
		return "", fmt.Errorf("dataSource.secretRef.namespace is required")
	}

	if len(r.AllowedNamespaces) > 0 && !r.AllowedNamespaces.Has(namespace) {
		return "", fmt.Errorf(
			"data source secrets may only be read from %s; %s/%s is outside that",
			strings.Join(sets.List(r.AllowedNamespaces), ", "), namespace, ds.SecretRef.Name)
	}

	secret, cached := r.cached(namespace, ds.SecretRef.Name)
	if !cached {
		// Not in the cache: it may not carry the opt-in label the informers
		// select on, or the informers may not have synced yet. Either way the
		// client is the authority, and the opt-in check below still applies.
		live, err := r.Client.CoreV1().Secrets(namespace).Get(ctx, ds.SecretRef.Name, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("reading secret %s/%s: %w", namespace, ds.SecretRef.Name, err)
		}
		secret = live
	}

	if r.RequireOptIn && secret.Labels[OptInLabel] != OptInValue {
		return "", fmt.Errorf(
			"secret %s/%s is not marked as usable by kube-crisp; label it %s=%s to allow projections to connect with it",
			namespace, ds.SecretRef.Name, OptInLabel, OptInValue)
	}

	key := ds.DSNKey
	if key == "" {
		key = "dsn"
	}
	value, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", namespace, ds.SecretRef.Name, key)
	}
	return string(value), nil
}

// cached looks a Secret up in the informer cache, if there is one.
func (r *SecretDSNResolver) cached(namespace, name string) (*corev1.Secret, bool) {
	if r.Cache == nil {
		return nil, false
	}
	return r.Cache.Secret(namespace, name)
}

// ResolveRead fetches the replica's Secret, when the projection names one.
func (r *SecretDSNResolver) ResolveRead(ctx context.Context, ds crispv1alpha1.DataSource) (string, bool, error) {
	replica, ok := readDataSource(ds)
	if !ok {
		return "", false, nil
	}
	dsn, err := r.Resolve(ctx, replica)
	return dsn, err == nil, err
}

// EnvDSNResolver reads the DSN from an environment variable named after the
// referenced Secret, for local development without a cluster. A Secret named
// orders-db with key dsn maps to ORDERS_DB_DSN.
type EnvDSNResolver struct{}

// Resolve looks up the derived environment variable.
func (EnvDSNResolver) Resolve(_ context.Context, ds crispv1alpha1.DataSource) (string, error) {
	key := ds.DSNKey
	if key == "" {
		key = "dsn"
	}
	name := strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(ds.SecretRef.Name + "_" + key))
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return value, nil
}

// ResolveRead looks up the replica's environment variable.
func (e EnvDSNResolver) ResolveRead(ctx context.Context, ds crispv1alpha1.DataSource) (string, bool, error) {
	replica, ok := readDataSource(ds)
	if !ok {
		return "", false, nil
	}
	dsn, err := e.Resolve(ctx, replica)
	return dsn, err == nil, err
}

// PoolKey identifies a shared connection pool.
//
// It is derived from the connection string rather than from the Secret that
// held it, so two projections reaching the same database share one pool even
// when they reference different Secrets. That is what makes a per-database
// connection limit mean anything: without it, N projections open N pools
// against one database and nothing bounds the total.
//
// Keying on the connection string is also what makes credential rotation work:
// a changed Secret produces a different key, so the next sync opens a pool with
// the new credentials and releases the old one. Only a hash of it appears, so
// the key is safe to log.
// The driver and the connection string, and nothing else.
//
// Prepared statements and the statement timeout used to be part of it, so that
// two projections disagreeing about either got pools of their own. That made
// one database into as many as four pools, each with its own MaxOpenConns —
// and so made --max-open-conns-per-datasource a bound on a pool rather than on
// a database, which is neither what it says nor what anyone sizing a database
// would assume. Both settings are now carried on the statement, where they
// always belonged: a prepared statement is cached by its SQL text, and the
// statement timeout is set with SET LOCAL inside the transaction that runs the
// query, so neither is a property of the connection.
func PoolKey(ds crispv1alpha1.DataSource, dsn string) string {
	digest := sha256.Sum256([]byte(dsn))
	return ds.Driver + "#" + hex.EncodeToString(digest[:4])
}

// PoolLabel names a data source in metrics. It is the pool key, which carries
// no credentials: a hash identifies the database without naming it.
func PoolLabel(ds crispv1alpha1.DataSource, dsn string) string {
	return PoolKey(ds, dsn)
}

// validateVersionIsDatabaseAssigned rejects a projection that polls
// incrementally while letting the client decide the resourceVersion.
//
// An incremental poll reads strictly forward from the highest version it has
// seen. A client-supplied version is not monotonic, so a row written with an
// older value is never returned and the change is silently missed. Catching it
// here is the difference between a startup error and a watch that quietly
// skips events.
func validateVersionIsDatabaseAssigned(p *crispv1alpha1.CustomResourceProjection) error {
	column := p.Spec.Mapping.ResourceVersion

	// Ordered rather than ranged over a map, so a projection that breaks this
	// in more than one place is told about the same one every time.
	writes := []struct {
		verb  string
		query *crispv1alpha1.Query
	}{
		{"create", p.Spec.Queries.Create},
		{"update", p.Spec.Queries.Update},
		{"updateStatus", p.Spec.Queries.UpdateStatus},
	}

	for _, write := range writes {
		// Every statement, not only the first: a write may be a transaction of
		// several, and the one binding the version need not be the one that
		// returns the row.
		for _, stmt := range queryStatements(write.query) {
			_, params, err := crispsql.Rewrite(stmt, p.Spec.DataSource.Driver)
			if err != nil {
				// The driver is validated elsewhere; nothing to check here.
				continue
			}

			for _, name := range params {
				if name != column {
					continue
				}
				return fmt.Errorf(
					"projection %s: spec.queries.%s binds :%s, which writes the client's resourceVersion, "+
						"but spec.watch.query polls forward from the highest version seen and would skip such a row. "+
						"Let the database assign it, for example \"SET %s = clock_timestamp()\", and keep the client's "+
						"value in the WHERE clause as :resourceVersion",
					p.Name, write.verb, column, column)
			}
		}
	}
	return nil
}

// queryStatements is every statement a query runs, in order.
//
// A write is written either as one sql or as a transaction of several, and a
// check that reads only the first field silently passes everything expressed
// the other way.
func queryStatements(query *crispv1alpha1.Query) []string {
	switch {
	case query == nil:
		return nil
	case len(query.Statements) > 0:
		return query.Statements
	case query.SQL != "":
		return []string{query.SQL}
	default:
		return nil
	}
}
