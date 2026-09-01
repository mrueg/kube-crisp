# Reference

Everything a `CustomResourceProjection` can say, and why you would say it. The
[README](../README.md) is the short version; the
[tutorials](tutorial-postgresql.md) are the worked ones.

## Bind parameters

`:namespace`, `:name`, `:limit`, `:offset`, `:labelSelector` and the caller's identity —
`:user`, `:userUID`, `:userGroups`, `:userExtra` — are always available, and
on writes every mapped column is bound by its own name. More can be declared under
`queries.*.parameters`, including values read out of the submitted object.

Values are always passed as driver placeholders and never interpolated into the statement, so a
request cannot alter the query. String literals — including backslash escapes and PostgreSQL's
dollar-quoted `$$…$$` — quoted identifiers, comments, and the `::` cast are left untouched by the
rewriter.

Mapping `namespace` to a tenant column is the idiom worth knowing: it makes ordinary namespace RBAC
apply to database rows, and the identity parameters let a projection scope rows to the caller.

`:user` is the username, `:userUID` the half of an identity that does not get reassigned to someone
else, and `:userGroups` and `:userExtra` arrive as JSON — an array and an object of arrays — because
that is the one shape all three drivers can take apart. Authorization in Kubernetes is mostly by
group, so a projection that wants its rows scoped the way RBAC scopes its verbs wants `:userGroups`:

```sql
WHERE owner_group IN (SELECT value FROM json_each(:userGroups))
```

A projection whose `list`, `watch.query` or `watch.deletedQuery` binds any of them cannot be
watched, and is refused at load time unless it sets `watch.disabled: true`. A watch is served from a
cache filled by one poll and shared by every watcher, and there is no request behind a poll: the
query would run with whatever context the first watcher brought, and every watcher after that would
be handed that caller's rows for as long as the stream stayed open. `dataSource.sessionVariables`
that depend on the request are refused alongside watch for the same reason.

## Schema and printing

`spec.resource.schema` is not decoration. It is compiled into a validator that rejects writes which
do not match it, and published as OpenAPI v3 for the group version, which is what makes
`kubectl explain orders.spec.customer` return something. A projection that borrows its schema with
`schemaFrom` is resolved from the named CustomResourceDefinition, and is then validated and
explained exactly as a declared one is — the CRD is only read, never served for the projected group.
Resolution needs a cluster connection, so `schemaFrom` is refused rather than ignored when the
server is running without one.

Editing the referenced CustomResourceDefinition is picked up. The borrowed schema is part of what
identifies a compiled projection, alongside its spec and the connection string its data source
resolves to, so a changed one rebuilds the storage exactly as a changed spec would — and an
unchanged one does not, which is what keeps a watch cache alive across a sync.

The CustomResourceDefinitions projections borrow from are watched, so an edit is picked up when it
happens. The watch carries metadata only — the schema itself is read when a projection is prepared,
and holding a copy of every CRD in the cluster to notice an edit is a cost with no return. Only a
CRD some projection actually names is worth a sync; the rest of a cluster's CRDs are edited by
things that have nothing to do with this server. Running without a cluster connection, or with
`--watch-projections=false`, an edit still lands within `ResyncPeriod`.

Schemas are published to `/openapi/v3` only. The v2 document is served, because the aggregation
layer downloads it regardless, but it carries no projected schemas: a client old enough to read
only v2 will not be able to explain a projected kind.

`additionalPrinterColumns` works the same way it does for a CRD: declare the columns and kubectl
prints them. A projection without any prints just the name.

Defaults in the schema are applied to writes, exactly as they are for a custom resource: a field the
client left out arrives with the value the schema declares, and it reaches the database. Reads are
not defaulted. A projection whose premise is that the rows are the truth should not invent values
that are not in them — a column that is NULL reads as absent, not as the default.

Validation is **ratcheted** on updates. A part of the object that did not change is not re-judged,
so tightening a schema after the fact does not make every unrelated edit to an older row
impossible; writing the offending field is still checked. This applies to both the structural schema
and CEL rules, and never to a create, which has no previous object to forgive.

CEL rules under `x-kubernetes-validations` are compiled once and evaluated on every write, including
transition rules that compare against the stored object. The same schema also drives server-side
apply: with it, field management knows which lists merge by key and which maps are atomic, rather
than deducing structure from whichever object happens to arrive.

Ownership needs somewhere to live. An object is rebuilt from its row on every read, so unless a
column holds `metadata.managedFields` there is no record of who owns which field — and an apply then
merges correctly but never conflicts, which is the half of server-side apply that keeps two
controllers from overwriting each other. Map a column and it works as it does anywhere else:

```yaml
mapping:
  managedFields: managed_fields   # a JSON array; make the column wide
```

With it, a second manager writing a field the first owns is refused with a conflict, and
`--force-conflicts` takes ownership over. Without it, apply is still a convenient way to write part
of an object — it is just not a way to divide one between owners.


## Subresources

Enabling `subresources.status` splits the object the same way it does for a CRD: a write to the
main resource cannot change status, and a write to `/status` cannot change anything else. As with a
CRD, the ignored half is dropped silently rather than rejected, so a client that sends a whole
object to the wrong endpoint gets a success and no change.

`queries.updateStatus` is optional; when status lives in the same row, the update statement serves
both.

`subresources.scale` serves `/scale` from two paths into the object, and the endpoint answers with
an `autoscaling/v1` `Scale` — the only kind `kubectl scale` and the horizontal pod autoscaler
understand:

```yaml
subresources:
  scale:
    specReplicasPath: .spec.replicas       # written by a scale request
    statusReplicasPath: .status.replicas   # reported back
    labelSelectorPath: .status.selector    # optional; an autoscaler needs it
```

A scale write changes the replica path and nothing else, through the projection's ordinary `update`
statement, so `specReplicasPath` has to be a mapped column. Admission sees the `Scale`, not the row
behind it, which is what a policy written for the subresource expects.

## Writes

A write can run several statements as one transaction, which is what lets a
projected kind span more than one table:

```yaml
create:
  statements:
    - INSERT INTO order_events (id, tenant, event) VALUES (:id, :tenant, 'created')
    - |
      INSERT INTO orders (id, tenant, customer, total_cents, updated_at)
      VALUES (:id, :tenant, :customer, :total_cents, clock_timestamp()::text)
      RETURNING id, tenant, customer, total_cents, updated_at
```

Either all of them take effect or none do. Only the last may return rows, since
only its result can be the object the client is answered with; the rest run for
their effect, and a `RETURNING` on one of them is rejected at load time rather
than silently discarded. Every statement binds whichever `:name` parameters it
declares.

Reads cannot use `statements`: they are shared between concurrent requests and
may be answered from a cache, neither of which a transaction survives. A
transactional write also skips the prepared-statement cache, which is per pool
while a transaction holds a connection of its own.

`dryRun` behaves as it does anywhere else in Kubernetes: the request is validated and answered with
the object that would have been stored, and nothing is written.

Fields the schema does not describe are pruned. What happens next follows the request's
`fieldValidation`: the default warns, `Strict` rejects, `Ignore` is silent. Dropping a field a user
wrote without telling them is the thing worth avoiding.

### Writing labels and annotations

Every mapped column is bound on writes as well as reads, so `kubectl label` and `kubectl annotate`
persist — provided the write statement sets the column. `mapping.labels` and `mapping.annotations`
bind one column per key, with `NULL` rather than `""` for a key the object does not carry;
`mapping.labelsFrom` and `mapping.annotationsFrom` bind a JSON column holding whatever has no column
of its own, and `NULL` rather than `{}` for an object with none.

**A column can only be written from one place.** Mapping one column both as a label and as a field is
a reasonable way to read it — select on it as a label, show it as a field — and on the way out both
are filled from the same column, so they always agree. On a write only one of them reaches the
column, and the field does, because it names an exact path while the label is a view of it.

The write still succeeds, and says what it ignored:

```
Warning: label "store.example.com/status" was not written: it shares column "status"
with field status.phase, which the write set to "shipped"
```

Without that the request was answered `200`, `kubectl` reported `labeled`, and the row had not
moved. The projection also says so once when it loads, naming the columns mapped both ways.

### Which verbs are advertised

Discovery offers exactly the verbs a projection has queries for, and nothing else:

| Declared | Advertised |
| --- | --- |
| `list` | `get`, `list` |
| `get`, `list` | `get`, `list` |
| `create` | adds `create` |
| `update` | adds `update` and `patch` |
| `delete` | adds `delete` and `deletecollection` |
| `deleteCollection` alone | adds `deletecollection` |
| `watch` not disabled | adds `watch` |

`get` is always advertised, including for a projection that declares only `list`: a read of one
object is then answered by filtering the collection, which works, and costs what the collection
costs.

This matters beyond tidiness. The garbage collector picks what to collect out of discovery, so a
resource advertising `delete` that refuses it is one the collector retries indefinitely; `kubectl
delete --all` and `apply --prune` fail on a projection that never offered to be deleted; and an
informer on a projection with `watch.disabled` would list, watch, be refused, and never sync.

## Row-level security

A projection can set variables on the connection before every query, from the
request that caused it:

```yaml
dataSource:
  sessionVariables:
    - {name: app.tenant, from: RequestNamespace}
```

With a PostgreSQL policy reading `current_setting('app.tenant')`, the database
decides which rows exist for the query. That is a stronger claim than the
mapping makes on its own: a mistake in a projection's `WHERE` clause cannot hand
one tenant another's rows, because the database never offers them.

```sql
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;
CREATE POLICY orders_tenant ON orders USING (tenant = current_setting('app.tenant', true));
```

The values are always bound, never interpolated, and the names are validated
before they are used, since no driver takes a setting's name as a parameter.
PostgreSQL scopes a setting to the transaction; MySQL has no transaction-local
settings, so the variable is a connection one and is cleared before the
transaction ends — the connection goes back to a pool every projection reaching
that database shares, and a value left on it is one a later request could read.
Setting any of these moves every query into a transaction — the only way a
setting can be scoped to one request rather than left on a pooled connection for
whoever gets it next — and folds the resolved values into the cache and
coalescing keys, so nothing is ever shared across a difference in them.

A variable that depends on the request cannot be combined with watch, and the
server refuses the projection rather than serving it: a poll runs on a timer
with nobody behind it, so a policy keyed on the user or the namespace would show
it nothing, and an empty read is indistinguishable from every row having been
deleted. PostgreSQL and MySQL only; SQLite has no session state.

## Rows that cannot be mapped

A row whose name is not a valid object name, whose identity column is NULL, or whose value does not
fit the type the mapping declares cannot become an object. It is left out of the collection rather
than failing the read: one such row would otherwise make `kubectl get` return 500 for the whole
table, with no way to see the rest of it.

Nothing is dropped silently. The response carries a warning naming how many rows were skipped and
why the first of them was, `kube_crisp_query_rows_unmappable_total` counts them, and the log records
them at `-v=2`. A watch poll skips them the same way, with no request to warn — there the metric is
the signal.

The one case that still fails outright is a paged list that has no `keysetColumn`, since the key it
would page on is the name of the row it could not map.

Where a partial answer is worse than none, say so:

```yaml
  mapping:
    onUnmappableRow: Fail        # default: Skip
```

A collection that quietly omits rows is one a client cannot tell from a collection that genuinely
has fewer, so anything reconciling towards it deletes what it cannot see. `Fail` makes the read
return an error naming the row and the setting, and accepts that one bad row takes the projection
out for everybody — including the watch, whose poll fails the same way.

## Naming a row

`mapping.name` names one column. A table whose key is composite names several:

```yaml
mapping:
  nameColumns: [region, order_no]   # "eu-1042"
  nameSeparator: "-"                # the default
```

The name is the only handle the API has on a row, so it has to be reversible.
Requests split it back into its parts and bind each under its own column name,
so a query reads `WHERE region = :region AND order_no = :order_no`. A value
carrying the separator is refused rather than escaped, because two different
rows would otherwise produce one name; `generateName` is refused for the same
reason, since a random suffix does not split into the identity columns.

## Lifecycle

Two more pieces of metadata can be mapped, and both are owned by the database:

```yaml
mapping:
  generation: generation          # advance it when the spec changes
  deletionTimestamp: deleted_at    # NULL unless the row is on its way out
```

`metadata.generation` is what a controller compares against its
`status.observedGeneration` to know whether it has caught up. A client cannot
set it: a write never carries a client's value back to the row, so for the
comparison to mean anything the column has to advance when the spec changes and
stay put when only status does.

`metadata.deletionTimestamp` is for projections that delete softly — the row is
marked rather than removed, usually by writing `delete` as an `UPDATE`. An
object carrying one is terminating, is still listed and readable, and a second
delete is answered with the object rather than run again, so the clock is not
restarted. That is the same contract a custom resource with a finalizer keeps,
and it is what makes an ordinary controller work against a table that never
really deletes anything.

## Finalizers and owners

A projection can hold `metadata.finalizers` in a column, and then a delete
behaves the way it does for any other Kubernetes object:

```yaml
mapping:
  deletionTimestamp: deleted_at
  finalizers: finalizers        # a JSON array of strings
  ownerReferences: owners       # a JSON array of ownerReferences
queries:
  markDeleted:
    sql: |
      UPDATE orders SET deleted_at = now()
      WHERE tenant = :namespace AND id = :name AND deleted_at IS NULL
```

A delete of an object that still has a finalizer marks it as terminating and
leaves it there. Whoever put the finalizer on it removes it when its work is
done, and clearing the last one is what finally deletes the row — so the client
that let go is the one told the object is gone. Adding a finalizer to an object
that is already terminating is refused, since it would hold open something whose
deletion has already been accepted. All three pieces are required together: the
server refuses a projection that maps finalizers without a way to mark an object
or to clear one.

Labels and annotations can come out of one column too. `mapping.labels` names one key per column,
which is right for a fixed set — a status, a tier — and useless for a table whose labels vary per
row. `mapping.labelsFrom` reads the whole map out of a single JSON column instead:

```yaml
mapping:
  labelsFrom: labels            # {"team":"payments"}
  annotationsFrom: annotations
  labels:
    store.example.com/status: status   # still its own column, and it wins
```

The two combine: anything `mapping.labels` names is read from its own column and overrides the same
key in the JSON, so a label can be promoted out without moving the rest. On a write the JSON column
receives only what has no column of its own — storing a key twice is how the two copies come to
disagree — and an object with nothing left over writes NULL rather than `{}`.

`managedFields` is stored the same way when `mapping.managedFields` names a column, which is what
lets server-side apply detect a conflict rather than silently overwriting another manager's field.

`ownerReferences` are stored and served when `mapping.ownerReferences` names a
column, which is what lets the cluster's garbage collector reach projected
objects — it deletes a child whose owner is gone through the same API as any
other client. Where that column exists they are held to the same rules the
kube-apiserver applies to any object: `apiVersion`, `kind`, `name` and `uid` are
required, only one reference may be the controller, and the same owner cannot
appear twice. A reference the collector cannot resolve is how objects get
deleted by surprise, so it is refused on write and on read rather than stored.
What is not checked is whether the owner exists — that is the collector's
question, asked continuously, and answering it here would only be a guess with a
race attached. Where the column does not exist there is nowhere to put the
field: a write carrying `ownerReferences` is accepted and the references are
dropped, unvalidated, rather than refused, so an object that reads back without
the owner its client set is a projection that maps no column for them.

A collection delete on a projection with finalizers removes objects one at a
time rather than in a single statement, because one statement cannot tell which
rows are still held. The deletes run concurrently, bounded by the projection's
own query limit, so `delete --all` is not a way around it.

## Cascading deletes

`propagationPolicy` decides what happens to the objects this one owns.
`Background` — the default — asks nothing of storage: the object goes and the
garbage collector cleans up afterwards. `Foreground` and `Orphan` are expressed
the way Kubernetes expresses them, as a finalizer that holds the object while the
collector deals with its dependents.

Holding an object needs somewhere to record that finalizer, so a projection that
maps no `mapping.finalizers` column **refuses** those two rather than quietly
doing a background delete. That is the whole change: a client asking for its
dependents to be waited on or orphaned now finds out that this projection cannot,
instead of being told it worked.

The deprecated `orphanDependents` boolean is read too, since clients that predate
the policy still send it.

## Concurrency

Writes are checked against `metadata.resourceVersion` when the client supplies one, and rejected
with a conflict when it is stale — the same contract as any other Kubernetes resource. An empty
resourceVersion means the client is not asserting anything, and the write proceeds.

The check is a read followed by a write, so it closes the window rather than eliminating it. That
read always goes to the database — it is never answered from `cacheTTL` or joined to a query already
in flight — because a conflict check against a cached object is a check against a version the row
may no longer have. To make it atomic, bind the version in the statement itself;
`:resourceVersion` carries what the client sent:

```sql
UPDATE orders
SET customer = :customer, updated_at = clock_timestamp()
WHERE tenant = :namespace AND id = :name
  AND (:resourceVersion::text IS NULL OR updated_at = :resourceVersion)
RETURNING id, tenant, customer, updated_at
```

With `RETURNING`, a statement that matches nothing reports a conflict rather than a lost update.
Let the database assign the new version; the client's value belongs in the `WHERE` clause.

## Selectors

Label selectors are applied to mapped objects, or pushed into SQL through `:labelSelector` if the
projection would rather filter in the database.

Field selectors work on `metadata.name`, `metadata.namespace`, and whatever the projection declares:

```yaml
resource:
  selectableFields:
    - jsonPath: .spec.customer
      column: customer          # optional: lets the query filter in the database
```

With a column, the requested value is bound under that name, so the query can filter where the data
is:

```sql
WHERE (:customer::text IS NULL OR customer = :customer)
```

A field selector understands `=` and `!=` and nothing else, and both are pushed down: the requested
value is bound under the column's name, and the excluded one under `<column>_not`.

`metadata.name` and `metadata.namespace` need no declaration — every resource accepts them, and both
are pushed down through the parameters a projection already has. A name selector binds `:name`, and
the identity columns behind it, exactly as a get does; `metadata.namespace` binds `:namespace`, but
only on a cluster-wide read, since a namespaced request already has its namespace from the path:

```sql
WHERE (:name IS NULL OR id = :name)
  AND (:name_not IS NULL OR id <> :name_not)
```

So a list statement written that way answers `--field-selector metadata.name=order-1001` with the
lookup a get would do, rather than a scan filtered down to one row.

Label selectors go further, because their grammar does. A label mapped to a column is bound as
`:label_<column>`, `:label_<column>_not`, and `:label_<column>_in` — the last a JSON array — so
`status in (shipped,pending)` becomes a query the database can answer:

```sql
WHERE (:label_status_in IS NULL OR status IN (SELECT value FROM json_each(:label_status_in)))
```

Results are filtered again after mapping either way, so a query that ignores any of these is still
correct, only slower. Anything *not* declared is **rejected** with a 400 rather than ignored:
returning an unfiltered collection to a client that asked for a subset is worse than an error,
because the client cannot tell.

## Versions

`spec.resource.versions` adds further versions of the same kind, each with its own schema and,
optionally, its own mapping:

```yaml
resource:
  version: v1alpha1
  schema: {...}
  versions:
    - name: v1beta1
      schema: {...}
      mapping:                      # optional; defaults to spec.mapping
        name: id
        namespace: tenant
        fields:
          - {column: total_cents, path: spec.amount.cents, type: integer}
```

There is no conversion to write and none to configure. Every read goes back to the database, so a
request for `v1beta1` maps the same rows through the `v1beta1` mapping. A field that only one
version describes is simply absent from the other.

Versions are ranked the way Kubernetes ranks them, so `v1` is preferred over `v1beta1` over
`v1alpha1`, whatever order they appear in. Each served version gets its own APIService.

## Versions and conversion

There is no conversion webhook here, and no stored version. Every version reads
the same rows and maps them its own way, so a v1beta1 that puts the amount under
`spec.amount.cents` is not a translation of the v1alpha1 that puts it under
`spec.totalCents` — both are views of one column.

That is only safe while the versions cover the same columns. If one maps a
column the other does not, a client writing through the poorer version drops a
value the richer one displays, and neither can tell. So the versions are checked
when the projection is compiled:

```yaml
resource:
  conversion: RoundTrip   # the default: every version maps the same columns
```

`conversion: None` allows them to differ, for a version that deliberately
exposes less. Nothing translates between them either way — the setting only
decides whether the projection is allowed to say so.

## Caching

Identical reads that arrive together are collapsed into one query: the second
and later requests wait for the one already in flight rather than opening
connections of their own. It needs no configuration and adds no staleness —
every waiter is answered by a query that was still running when it asked — and
`kube_crisp_query_coalesced_total` counts how much duplicate work concurrency
was creating. A write detaches the reads it could have invalidated, so a client
that writes and then reads is never answered by a query that predates its write.

A kind served at several versions polls its table once rather than once per
version: the versions are different views of the same rows, so their watch
caches are driven from one timer and their queries are shared.

`spec.cacheTTL` caches reads for a bounded time. A write drops what it could have changed — the
object, that namespace's lists, and any cluster-wide list — while other namespaces keep their
entries. A write with no namespace, meaning a cluster-scoped kind or a collection delete across all
namespaces, drops everything.

That invalidation reaches one replica: the one that served the write. The cache lives in the
process and there is nothing between replicas, so with more than one — the chart deploys two — a
read routed elsewhere can be answered from an entry written before the change, for up to the TTL.
Read-after-write holds for a replica, not for a set of them, and a client cannot tell which one it
reached: it talks to the kube-apiserver, which proxies to a Service that spreads requests across
them.

Writes are not exposed to this, only reads. The row a write is based on is always read from the
database rather than from the cache, so a client that read a stale copy and then writes it back
sends the older `resourceVersion` and is refused with a conflict rather than overwriting the newer
row. What a stale read costs is a client seeing an old value, and being told to retry if it acts on
one.

A watched projection is not left at the TTL, though, and it needs nothing new to escape it. Watching
already makes every replica poll the table on its own — the leader at the projection's interval, a
follower at `watch.followerPollInterval` — and a poll that comes back with rows has found out that
the data moved without any replica having told it. So a poll that observes a change drops the read
cache for the namespaces those rows are in, on whichever replica ran it. The staleness a read can
carry on a replica that served no write falls from the whole `cacheTTL` to at most one poll
interval, and a poll that observed nothing drops nothing: dropping on every tick regardless would
leave the TTL meaning almost nothing and put the load back that the cache was configured to remove.

It reaches exactly as far as watching does. Polling starts with the first watcher, so a projection
nobody watches never polls and keeps the full-TTL window. Polling it anyway would turn a read cache
into a standing query on every replica, which is the opposite of what `cacheTTL` is asked for. Nor
is it a general answer even for a watched one: the invalidation is still each replica noticing for
itself rather than being told, so the window is a poll interval and not zero, and making it zero
would need the replicas to talk to each other — a channel this does not have.

So a projection whose clients read back what they have just written and that nothing watches wants
either one replica or no `cacheTTL`.

Running with `--enable-leader-election` — which is the operator saying there are peers — such a
projection is named in the log and counted by `kube_crisp_projections_cache_unshared`, the same
treatment `kube_crisp_projections_unversioned` gives the other hazard that only exists with more
than one replica. The gauge counts every projection with a `cacheTTL`, watched or not, because
whether anything is watching is a property of the clients connected at that moment rather than of
the projection: a watch that ends puts the projection back on the full TTL, and a gauge that
flickered with the informers attached to it would say nothing an alert could act on. It clears when
the `cacheTTL` is removed, so an alert on it stops firing when the projection is fixed rather than
when the process restarts.

Caching is off unless asked for: it trades freshness for load, and
`kube_crisp_cache_reads_total` is how you judge whether the trade paid off — and
`kube_crisp_cache_evictions_total` says why when it did not: entries expiring is the cache working
as configured, entries dropped because it was full means the key space is larger than the cache
(a client paging is the usual cause, since every continue token is a key of its own), and entries
`invalidated` mean the projection changes faster than the TTL it was given. That last reason covers
both ways an entry is dropped for having gone stale — a write served by this replica, and a poll on
this replica that saw a change some other replica's write made — because they say the same thing
about the projection.

Cached collections are handed out as views rather than copies — the items are
shared and treated as immutable, the same contract the kube-apiserver's watch
cache keeps. Copying 10,000 objects costs about as much as querying for them,
which would have made the cache pointless at the size it matters most.

Neither the cache nor coalescing ever answers the read a write is based on. A
`kubectl edit`, a patch, or a delete reads the row it is about to change from
the database, so its `resourceVersion` check and the half of the object it does
not own are decided on the row as it is now.

The cache is bounded, and a full one drops entries that have expired, then the
ones closest to expiring — not all of them. A client paging through a large
collection produces a distinct key per page, and dropping everything would have
let one such client clear every other client's entries on a regular cadence.

## Identity

Every projected object has a `metadata.uid`. Map a column to it when the table
already carries one; otherwise kube-crisp derives a stable UUIDv5 from the group,
kind, namespace, name, and — if the projection maps one — the creation timestamp.
The version is deliberately not part of it: a row served at two versions is one
object, so an owner reference written against one keeps resolving through the other.

This is not cosmetic. Controllers use the UID for owner references and to tell
"same name, different object" apart, so an empty one produces malformed
`ownerReferences` and misleads any client that caches by UID. Deriving it means
every replica and every restart agree on the same value. Without a mapped
creation timestamp, a row deleted and recreated under the same name keeps its
UID, which is the one case worth mapping a real identity column for.

## Load shedding

`dataSource.maxConcurrentQueries` caps in-flight queries per projection, defaulting to the pool
size. Requests over the cap wait one second and are then rejected with `429` and a `Retry-After`,
rather than piling up behind a slow query until every client times out.
`kube_crisp_query_shed_total` counts them. The limit is per projection, not per pool: projections
reaching the same database share a pool, so a limit on the pool would be whichever projection
opened it first. Connections are bounded separately.

`kube_crisp_query_duration_seconds` carries a `result` label separating a timeout from an
unreachable database from a statement the database refused, which is what says whether to look at
the schema, the database, or the projection's own SQL. See
[Operating](operating.md#what-result-distinguishes).

A `Retry-After` is an instruction rather than a hint: client-go retries any response over `500` that
carries one, ten times over, and most clients here are client-go. So it is set only where a retry is
cheap. A shed request is refused before the query runs, and an unreachable database refuses the
connection outright — both cost the database nothing, and both clear on their own, which is exactly
what a retry is for.

A timeout is deliberately not in that set. Every attempt runs the query for its whole budget before
giving up, so advertising a retry turned one `LIST` against a slow table into eleven queries and
15.6 seconds of waiting to return the error it was always going to return — eleven times the load,
arriving exactly when the database could least afford it. The status is still `Timeout`, so a client
that wants to retry may; what changed is that the server no longer tells every client to.

Pools are keyed by driver and connection string, and by nothing else, so every projection reaching
the same database shares one pool — however many Secrets point at it, and whatever the projections
disagree about. `--max-open-conns-per-datasource` is the ceiling no projection can raise, so it is
the number of connections one replica opens against that database.

The key used to carry the `preparedStatements` and `statementTimeout` settings too, so that two
projections disagreeing about either got pools of their own. That made one database into as many as
four pools, each bounded separately — and so made the flag a limit on a pool rather than on a
database, which is not what an operator sizing `max_connections` would assume. Neither setting was
ever a property of the connection: a prepared statement is cached by its SQL text, and a statement
timeout is applied with `SET LOCAL` inside the transaction that runs the query, so it dies with that
transaction. Both are carried on the statement now, and projections that disagree share the pool
while keeping their own behaviour.

## Resuming a watch

A watcher that reconnects with a `resourceVersion` is normally answered from an in-memory ring of
recent changes, bounded by `watch.historySize`. Beyond it the honest answer is `410` and a relist.

### Deletions that do not move the version

Where the version comes from a mapped column, a deletion has nothing to raise it with: the row that
carried the version is the one that is gone. A resync noticing a row that stopped coming back, or a
tombstone that records only the identity columns, both remove an object and leave the version
exactly where it was — so a client resuming from that version is indistinguishable from one that has
already seen the removal.

Such a deletion is therefore replayed to anyone resuming from that version, which means a client
that had already seen it is handed it twice. A watch is allowed to repeat an event and every
informer tolerates it; the alternative was a client keeping a row that no longer exists, with no
`410` to tell it to relist. A tombstone that carries the mapped `resourceVersion` column moves the
mark like any other change and does not go through this at all, which is one more reason to write
one.

Only removals. An `Added` or `Modified` at the current version carries a row whose own version *is*
that version, and a client at it either listed the row or was sent it.

### What a list reports as its resourceVersion

A list stamps the watch cache's version, which is the point a watch can resume from — so the usual
list-then-watch is answered from the cache rather than replaying the collection.

The cache only polls while something is watching, so the first list after a restart may arrive
before it has read anything. It is primed at that point rather than quoting a version it cannot
stand behind: one query, once per projection per process. Before this it reported its internal
counter — the number `1` — and the watch that followed was refused with `too old resource version: 1`,
which is an informer's first sync, on every projection with a mapped `resourceVersion`, after every
restart.

A projection with `watch.disabled` has no cache to ask. It reports the newest `resourceVersion`
among the rows it returned, which is a version this server has genuinely observed for that
collection, drawn from the same mapped column a watch would have used. Two things it is not: a
resumable point, since these projections do not advertise `watch` at all; and the collection maximum
when a selector or a page limit narrowed the response, only the maximum of what came back —
understating it is the safe direction. A projection that maps no `resourceVersion` at all reports
none, because there is nothing to report and an invented ordering would be worse than an absent one.

That ring dies with the process and differs on every replica, so a rolling restart used to make
every informer re-read its whole collection at once. Where the projection maps a `resourceVersion`
and has a `deletedQuery`, the database can answer instead: the incremental query says what changed
since that version and the tombstone query says what went away, which together is exactly what the
client missed.

```yaml
  mapping:
    resourceVersion: updated_at
  watch:
    query: {sql: "... WHERE updated_at > :since ORDER BY updated_at ASC"}
    deletedQuery: {sql: "SELECT id, tenant FROM order_tombstones WHERE deleted_at > :since"}
```

Both are required. Without tombstones a replay would report every change and no removal, so a client
would keep objects that are gone — silently, for as long as it stayed connected — and `410` is worse
service and better behaviour. `kube_crisp_watch_database_replays_total` counts the watchers answered
this way.

The same pair also decides how much the watch cache holds. A tombstone that carries the mapped
columns describes the row it removed, so the cache no longer has to keep whole objects just to have
something to put in a `Deleted` event — it keeps the key and the version, which is all the diff
compares. Measured at 1.83x less held per row, and it is what removes the requirement that `maxRows`
exceed the row count. Include the mapped columns in the tombstone query to get it:

```yaml
    deletedQuery:
      sql: |
        SELECT id, tenant, customer, total_cents, status
        FROM order_tombstones WHERE deleted_at > :since
```

What is kept is the identity, the version, the kind and the labels — a watch event that carries an
object with no kind cannot be encoded, and a watcher with a label selector filters deletions on
labels. Everything the row itself holds comes from the tombstone, which is why a deletion is answered
from it rather than from the cache when the cache is lightweight: a field selector over a mapped
column can be matched against the tombstone's row and could not be matched against a trimmed one.

The trade is that a new watcher's initial state is read rather than remembered, so a watcher that
asks for the collection costs a query. A tombstone holding only the identity columns is still
supported and still the documented minimum; the cache simply keeps the objects in that case.

Two things to know about what arrives. Changes come as `Modified` rather than `Added`, because that
is what the database said: the row changed at or after this version, and whether the client had
already seen it is not something a row can answer. client-go turns an update for an object its store
does not hold into an add, so an informer ends up in the same state either way. And a deletion
carries only the identity, since the row is gone from the table and the tombstone records only which
row it was.

A replay larger than the collection is refused, because past that point relisting is cheaper than
being handed the difference.

## Read replicas

Reads are almost all of what a projected kind does — every list, every get, and a watch poll on a
timer — and they all land on the same database that has to serve the writes. Point them somewhere
else:

```yaml
dataSource:
  secretRef: {name: orders-db, namespace: kube-crisp}
  readSecretRef: {name: orders-db-replica, namespace: kube-crisp}
```

What it costs is replication lag, and two things are deliberately kept on the primary because they
cannot tolerate it. Writes, obviously. And the read a write is based on: a `resourceVersion` checked
against a lagging replica is checked against a version the row may already have left behind, and the
untouched half of a merged object would be written back from state the primary has moved past. Those
go to the primary whatever the projection says.

Everything else can be behind, and already is — a list is a snapshot of a moment that has passed
either way. But a client that writes and then reads may see the row as it was before its own write,
and no amount of cache invalidation here can fix that; it is the trade the replica exists to make.

Replicas are pooled like any other data source, so two projections reading the same replica share
its connections, and a replica nobody references any more is released.

A replica that becomes unreachable costs latency, not availability: the read is answered by the
primary instead — which is where it would have gone had no replica been configured — and the replica
is then left alone for a few seconds before being tried again, so an outage costs one failed read per
interval rather than one per request. Only reachability falls back: a statement the replica rejected
would be rejected by the primary too, and retrying it there would double the load and still fail.
`kube_crisp_query_replica_fallback_total` counts it.

## Waking a watch instead of polling for it

Polling is what makes a watch work without a change feed, and it is also the
slowest thing here: a change surfaces within one `watch.pollInterval`. PostgreSQL
can say when something happened, which turns that interval into a round trip.

```yaml
  watch:
    pollInterval: 5s
    notify:
      channel: orders_changed
```

Something has to send them, which is a trigger:

```sql
CREATE FUNCTION orders_changed() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('orders_changed', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_notify
    AFTER INSERT OR UPDATE OR DELETE ON orders
    FOR EACH STATEMENT EXECUTE FUNCTION orders_changed();
```

`FOR EACH STATEMENT`, not `FOR EACH ROW`: the payload is never read, so one
notification per statement says everything a thousand would.

This is additive. A notification carries no data — it means "ask again" — so the
poll is still what reads what changed, and `pollInterval` still runs underneath.
A notification that is never delivered costs latency, not events, which is what
makes it safe to depend on: the subscription reconnects on its own, and if it
cannot, the projection is back to the interval it would have had.
`kube_crisp_watch_notifications_total` counts them — but a counter cannot tell a
dead subscription from a table nobody is writing to, and both leave it flat.
`kube_crisp_watch_listener_connected` is the one to watch: 0 means the
projection is subscribed to nothing and its watches are back on the timer, while
reads and watches both carry on working and report nothing wrong.

The subscription reconnects on its own, five seconds after it drops, and
`kube_crisp_watch_listener_reconnects_total` counts how often it has had to.

The e2e suite checks this against a control: two projections over identical
tables, both polling every 60 seconds, one subscribed and one not, with the
change made in SQL so nothing kube-crisp does can find it early. The subscribed
watch has the change within a couple of hundred milliseconds — most of which is
the `kubectl exec` that made the write — and the unsubscribed one is still
waiting. If both saw it, something other than the notification would be finding
changes, and the test would be measuring nothing.

The subscription holds a connection for as long as a watcher is connected, and
nothing while none is — the same property polling has. That connection is its
own, outside the pool: `LISTEN` occupies a session, so a subscription taking a
pooled connection would be one the projection could never run a query on, and on
a small pool that is every connection it has. A watched projection with `notify`
therefore costs one connection more than `dataSource.maxOpenConns`, which bounds
the connections doing query work.

## Bounding a query at the database

A query's `timeout` stops kube-crisp waiting. It does not stop PostgreSQL working: the context is
cancelled, the client is answered, and a statement with a bad plan carries on burning CPU that
everything queued behind it is waiting for.

`dataSource.statementTimeout: true` makes the deadline the database's as well, so it aborts the
statement itself and says so — the client gets a timeout rather than an opaque 500, and the work
actually stops.

```yaml
  dataSource:
    driver: postgres
    statementTimeout: true
```

It costs a transaction per query, because that is the only scope a setting can be confined to rather
than left behind on a connection every projection reaching the same database shares — and a
transaction does not use the prepared statement cache. So it trades a little read latency for a bound
on the worst case, and is off unless asked for. kube-crisp waits slightly longer than the database
does, so that the database's answer is the one that arrives rather than the two deadlines racing.

What it does not change is the ordinary case. On a healthy connection the driver already cancels the
query when the deadline passes, and PostgreSQL honours that — measured side by side against a control
projection, the query dies at the same moment either way, and the caller sees the same timeout. The
difference is only visible in the database's own log, which names which bound fired:

```
canceling statement due to statement timeout   -- statementTimeout: true
canceling statement due to user request        -- the driver cancelled it
```

The case it is for is the one that cannot be staged in a test: a cancellation is a second connection
carrying a request the server may never act on. When the network is why the query is slow, that is
exactly when it fails to arrive — and then the only thing that stops the query is a bound the database
was already holding. Turn it on for the worst case, not for the ordinary one.

PostgreSQL only: MySQL's `max_execution_time` covers read-only `SELECT`s rather than every statement,
and SQLite has no equivalent, so asking for it on either is refused rather than quietly ignored. A
single fixed bound for every query on a data source needs none of this — put
`options=-c statement_timeout=5000` in the connection string instead.

## Credentials

The connection string is part of a pool's identity, so rotating the Secret takes effect on its own:
the next sync resolves the new value, opens a pool for it, and releases the old one.

The Secrets are watched, so a rotation lands when it happens rather than at the next resync — which
matters for credentials that expire on their own, like an IAM token with fifteen minutes to live.
Only the Secrets carrying the opt-in label, in the namespaces `--datasource-namespaces` allows, are
watched: those are the ones a projection could read anyway. An update that changes no credential
does not rebuild anything, so relabelling a Secret is free. Without the RBAC to list and watch
them (see `manifests/20-rbac.yaml`) rotation still lands within the 10 minute resync.

### Passwords that are minted rather than stored

A managed cloud database increasingly has no password to put in a Secret at all. AWS RDS IAM, Cloud
SQL and Entra all authenticate with a token minted on demand from the identity the process already
has, and it lives about a quarter of an hour. `dataSource.auth` says where such a token comes from:

```yaml
dataSource:
  driver: postgres
  secretRef:
    name: orders-db
    namespace: kube-crisp
  auth:
    provider: aws-rds-iam
    options:
      region: eu-central-1
```

The Secret is still required and still carries the connection string — the host, the port, the user,
the database, and the TLS settings. What `auth` changes is only where the password comes from.

The distinction that matters is that the token is a property of a *connection* and not of a pool.
Rotation as described above works by the connection string changing, and that is exactly what must
not happen here: the pool is keyed by a hash of the connection string, so a token inside it would
produce a new pool every fifteen minutes — every live connection dropped, every prepared statement
thrown away, the database asked to authenticate everything again, four times an hour, for a
credential that changed and a database that did not. Written in once and never refreshed it is worse
still: the pool survives and every connection opened after the token expires fails to authenticate,
which looks like the database going down rather than like a credential running out.

So the pool is opened from a `database/sql` connector rather than from a connection string, and the
token is minted inside it, once per new connection. The pool key never sees one. A connection that is
already open keeps working for as long as it is open; only a connection actually being opened pays
for a token. Which is also why a provider should cache: connections are opened in bursts after an
idle period, and `maxIdleConns` defaults to `maxOpenConns` precisely so that those bursts are rare.

**Which providers exist is a property of the build.** Every one of them is a cloud SDK linked into
the binary, and kube-crisp links no dependency a given build does not need — the same reason a driver
is a registration rather than a switch — so the image published from this repository registers none.
Naming a provider needs a binary that registers it; see [Adding a credential
provider](../README.md#adding-a-credential-provider). A projection naming one this build does not
have is refused when it is compiled, by name, with the providers it does have — the same place, and
the same `Ready` condition, as a projection naming an unknown driver.

`postgres`, `cockroach` and `mysql` only. SQLite is a local file with no password and no connection,
and the CRD refuses `auth` on it outright.

**IAM authentication over an unencrypted connection is refused, not warned about.** An ordinary
plaintext connection is only logged about, because a password in a Secret is a shared secret and an
operator who puts one on a plaintext connection has decided something about the network it crosses —
a unix socket, a sidecar proxy, a host it never leaves. A minted token is a bearer credential: valid for
whoever holds it, for the quarter of an hour it lives, with nothing else standing between it and the
database. It is also sent as typed — an RDS IAM token is a signed URL checked as a cleartext password
— so there is no challenge-response hiding it on the wire. Refusing costs nothing, either, because
the cloud refuses it too: RDS requires TLS for IAM authentication and drops the connection itself. So
this is not a policy imposed on an operator, it is the database's own rule, said while the projection
is being compiled and with the name of the setting in it. Write `sslmode=require` for PostgreSQL or
`tls=true` for MySQL.

That refusal is what makes the MySQL side work at all: an IAM token is checked with the
`mysql_clear_password` plugin, which the driver will not use unless it is told to, and kube-crisp
turns it on for a data source with `auth` — safely, because it has already established that the
connection is encrypted.

### AWS RDS IAM

The one provider this repository ships, in `providers/aws`. RDS accepts, in place of a password, a
URL signed with SigV4 by an identity holding `rds-db:connect` on the database user; it is valid for
fifteen minutes and there is no extra service to run.

It is a **Go module of its own**, and the binary published here does not contain it. Linking it pulls
in fifteen AWS SDK modules and about 4 MB of binary that a build projecting a SQLite file can never
reach. A build tag would not have helped: a file excluded by a tag still contributes its imports to
the module graph, so `go.mod` and `go.sum` would carry the SDK either way and so would every project
that depends on kube-crisp as a library. A separate module is the only shape in which the cost is
paid by the builds that want it.

Building one is a `main` function and a `go.mod` — see
[`providers/aws/cmd/kube-crisp-apiserver`](../providers/aws/cmd/kube-crisp-apiserver/main.go), which
is the stock server plus `rdsiam.Register()`:

```go
import rdsiam "github.com/mrueg/kube-crisp/providers/aws"

if err := rdsiam.Register(); err != nil { ... }
```

On the cluster side you need an identity with `rds-db:connect` on
`arn:aws:rds-db:<region>:<account>:dbuser:<resource-id>/<user>` — on EKS, a service account annotated
for IRSA or Pod Identity — and a database user created with the `rds_iam` role
(`GRANT rds_iam TO orders_app;` for PostgreSQL, `IDENTIFIED WITH AWSAuthenticationPlugin` for MySQL).
Give the pod nothing else: the Secret still carries the connection string, and the token comes from
the pod's own identity, so no long-lived database password exists to be leaked or rotated.

The projection states as little as possible:

```yaml
dataSource:
  driver: postgres
  secretRef: {name: orders-db, namespace: kube-crisp}
  auth:
    provider: aws-rds-iam
```

The endpoint and the database user are read out of the connection string, through the drivers' own
parsers, so they are stated once and not twice. The region is read off the endpoint —
`<name>.<suffix>.<region>.rds.amazonaws.com` — and falls back to the pod's AWS configuration. Three
options override any of that when the guess is wrong: `region`, `user` and `endpoint`. An option this
provider does not understand is an error rather than a default quietly taken, because a misspelt
`region` is a projection signing tokens for the wrong place and nothing about the failure would lead
back to the typo.

Tokens are cached for ten of their fifteen minutes. The five minutes of margin is not politeness: a
token handed out at fourteen minutes and fifty seconds expires while the connection carrying it is
still shaking hands, and the result is an intermittent authentication failure under load, which is
about the least diagnosable thing this could do. Signing is local — SigV4 over a URL, with no request
to AWS — so the cache is not about the cost of a token; it is about not signing eight of them when a
pool refills after an idle period.

## Registration

kube-crisp creates the `APIService` that delegates each projected group to it, corrects it if it
drifts, and deletes it when the last projection in that group goes away. Objects it did not create
are never adopted — it only touches APIServices labelled `app.kubernetes.io/managed-by: kube-crisp` —
so an existing registration is left exactly as it is.

Point it at the right Service with `--apiservice-service-name`, `--apiservice-service-namespace`,
and `--apiservice-service-port`, supply `--apiservice-ca-bundle-file` if you have a real serving
certificate, or turn the whole thing off with `--manage-apiservices=false` — in which case
`examples/apiservice.yaml` is the object to write per group.

### Two projections claiming one resource

`plural.group/version` identifies an API path, and only one projection can answer it. When two claim
the same one, the projection already serving it keeps it and the other is failed by name, with the
conflict in its `Ready` condition and its ClusterRole left alone. Nothing else changes: every other
projection installs, and the server becomes ready as usual.

Whoever is serving wins, because the mistake is in the object just applied and applying it must not
take a working API group away from whichever projection had it. On a cold start nothing is serving
yet, so the older projection wins instead — usually re-electing whoever was serving before the
restart — with the name as the tie-break, so every replica settles on the same answer.

A projection loses whole. One claiming two resources and conflicting on one of them serves neither,
since a half-installed projection is a surface whose missing half looks exactly like a projection
nobody applied.

Applying one by hand while the reconciler is on is the case worth avoiding: it carries no
`managed-by` label, so the server will not touch it, and it outlives the projection it was written
for. The aggregation layer then reports that group as unavailable, which degrades `kubectl
api-resources` for the whole cluster rather than only for that group.

## Pagination

Bind `:after` in the list query and pages are keyset-based, which is what makes them stable:

```sql
WHERE tenant = :namespace AND (:after::text IS NULL OR id > :after)
ORDER BY id
LIMIT COALESCE(:limit, 500)
```

The continue token carries the last key of the page, so rows inserted meanwhile cannot shift the
window or make a client skip an object. Binding `:offset` instead still works and is simpler, but
concurrent inserts do shift it.

The key has to be the column the query orders by, and `queries.list.keysetColumn` is how a list
ordered by anything other than the name says so:

```yaml
list:
  sql: |
    SELECT id, tenant, created_at FROM orders
    WHERE tenant = :namespace AND (:after::timestamptz IS NULL OR created_at > :after)
    ORDER BY created_at
    LIMIT :limit
  keysetColumn: created_at
```

It defaults to the column `mapping.name` reads, which is right for the usual `ORDER BY id` list and
wrong for everything else: a token carrying a name while the query orders by a timestamp compares
the two against each other, and pages silently skip and repeat rows. A composite identity has no
single column to page on, so it has to be named.

Define `queries.count` and a paged list also reports `remainingItemCount`; the count only runs when
a client actually pages.

A continue token sent without a limit is a request in its own right, as it is against etcd: the read
resumes where the token points and returns everything left, in one answer and with no further token.

A projection that can do neither ignores the client's limit and returns everything, rather than
truncating the collection while telling the client it saw all of it.

## Watch

SQL has no general change feed, so watches are served by polling the list query and diffing
consecutive snapshots. Polling starts when the first watcher connects and stops when the last one
leaves, so a projection nobody watches costs nothing.

Two things follow from that. Events lag a change in the database by up to `watch.pollInterval`
(5s by default). And the mapped `resourceVersion` column is how a poll tells a changed row from an
unchanged one, so every write must move it — a projection without one still works, but each poll
then compares whole objects.

For one poll to serve every namespace, write the list query so it accepts a NULL namespace:

```sql
WHERE (:namespace::text IS NULL OR tenant = :namespace)
```

**Poll incrementally for anything large.** Give `watch.query` a statement that reads forward from
`:since`, and a steady state costs a query that returns nothing instead of a full scan:

```yaml
watch:
  pollInterval: 1s
  fullResyncInterval: 1m
  query:
    sql: |
      SELECT id, tenant, customer, status, updated_at
      FROM orders
      WHERE (:since::text IS NULL OR updated_at > :since)
      ORDER BY updated_at ASC
```

**Index the column `:since` reads.** The statement above only avoids a full scan if the database can
seek to `:since` and stop; without an index on `updated_at` it scans the table and sorts it on every
poll, which is the thing incremental polling exists to avoid. Measured against PostgreSQL at 10,000
rows, one poll: 6.0ms and 116 shared buffers without the index, 0.16ms and 10 with it — and the first
number grows with the table while the second does not. The tutorials create the index alongside the
table for this reason.

```sql
CREATE INDEX orders_updated_at_idx ON orders (updated_at);
```

**`maxRows` does not bound memory; `maxBytes` does.** A row can carry a megabyte of JSON or text, so
a modest row count is still gigabytes — and `resultFormat: JSONArray` returns the whole collection as
a single row, where `maxRows` never applies at all. `maxBytes` defaults to `64Mi` per result set and
takes a quantity, so `512Ki` and `1Gi` both work. Exceeding it fails the read rather than truncating
it, exactly as `maxRows` does.

```yaml
queries:
  list:
    maxBytes: 16Mi
    sql: SELECT id, tenant, document FROM orders WHERE tenant = :namespace
```

**Raise `maxRows` for anything large.** A full resync reads the whole collection, so `queries.list`
(or `watch.query`) has to be allowed to return all of it: a projection over 20,000 rows with the
default `maxRows` of 5,000 fails every resync, and its watchers stop seeing deletions. The whole
collection is also held in memory by the watch cache, once per projection rather than once per
version.

An idle watch receives a bookmark every `watch.bookmarkInterval` (1m by default) carrying the
current resourceVersion, so a client that reconnects resumes from a recent point instead of
replaying.

A watch resumes from where it left off. The cache keeps the last `watch.historySize` changes (1000
by default), so a client that reconnects is handed what it missed rather than being told to start
over. An unset or `"0"` version replays the current contents first, as it does for any other
resource.

Beyond that window there is nothing to replay, and the answer is `410 Gone` so the client relists —
which is also what happens for a version the cache has never reached. `resourceVersionMatch=NotOlderThan`
is always satisfiable because every read goes to the database; `Exact` is refused, since a table's
current contents cannot be rewound to an arbitrary past version.

A deleted row simply stops being returned, so deletions are invisible to an incremental read; the
full query still runs every `fullResyncInterval` to catch them. That is the one asymmetry to plan
around: creates and updates surface within a poll interval, deletions within a resync interval.

Unless the projection can be asked. `watch.deletedQuery` reads what was removed since a version —
from a tombstone table, or from rows a soft delete marked — and an incremental poll then sees
deletions at the same rate as everything else:

```yaml
watch:
  query: {...}
  deletedQuery:
    sql: |
      SELECT id, tenant FROM order_tombstones
      WHERE (:since::text IS NULL OR deleted_at > :since)
      ORDER BY deleted_at ASC
  # Nothing else has to look for removals now.
  fullResyncInterval: "0s"
```

Returning the mapped columns as well as the identity is worth it: a tombstone that describes the row
lets a deletion be answered from the table rather than from memory, and lets the watch cache keep
only keys and versions. Turning the resync off without a `deletedQuery` is refused rather than
accepted, since nothing would then ever notice a row disappearing.

What the resync also catches is a row committed out of stamp order. A version from
`clock_timestamp()` records when the statement ran, not when the transaction committed, so a writer
that stamps a row and commits seconds later has stamped it *behind* rows committed in between — and
once `:since` has moved past that stamp, `> :since` never returns it. Measured against PostgreSQL: a
row stamped 1ms before the watermark and committed 3s after it was invisible to every later
incremental poll, permanently.

kube-crisp's own writes are single statements, where the two moments are microseconds apart. Set
`fullResyncInterval: "0s"` where this table is written only by kube-crisp or by writers that commit
in one statement; leave the resync on where anything holds a transaction open across the write. A
version assigned at commit needs no resync at all.

This requires the version column to be monotonic, which means **the database must assign it**. Let
the statement compute it — `updated_at = clock_timestamp()` — and keep the client's value in the
`WHERE` clause as `:resourceVersion`. A write that stores the client's version instead is not
monotonic, and a forward-reading poll will never return the row again.

That mistake is easy to make and silent, so it is checked rather than left to documentation:

- A projection whose `create`, `update`, or `updateStatus` statement binds the version column while
  `watch.query` is set is **rejected at load time**, with a message naming the query and the column.
- Anything the check cannot see — another writer inserting a row with a stale version, a clock
  moving backwards — is caught at runtime. A full resync that finds a change at or below the
  incremental high-water mark counts it in `kube_crisp_watch_missed_events_total` and logs it.
  Watchers stay correct either way, because the resync still delivers the event; the counter is
  what tells you the projection is leaning on the resync instead of the incremental read.

`kube_crisp_watch_polls_total{mode}` shows whether a projection really is polling incrementally.

## Media types

Projected objects are served as JSON, YAML or CBOR, and never as protobuf. They are unstructured,
and unstructured has no protobuf encoding — which is why custom resources cannot be served as
protobuf either. Every Kubernetes client already handles this for custom resources, so nothing needs
configuring.

It is worth knowing what happens if a server does offer it. Advertising a format that cannot be
produced turns a clean refusal during content negotiation into a `406` after the response has been
assembled — and the namespace controller's metadata client accepts protobuf, so it negotiated
protobuf for the `deletecollection` it issues while emptying a namespace and got an encoding error
every time. It retries indefinitely, and the namespace never leaves `Terminating`.

Not only namespaces holding projected objects: the controller sweeps every resource that advertises
`deletecollection`, so a namespace containing nothing but a ConfigMap was stuck in exactly the same
way. Installing a server that made that mistake stopped every namespace in the cluster from being
deleted.

### Patch types

A projected resource accepts a JSON Patch (`kubectl patch --type=json`), a merge patch
(`--type=merge`) and a server-side apply (`kubectl apply --server-side`). It does not accept a
strategic merge patch — which is exactly what `kubectl patch` sends when no `--type` is given, so
that is the one form of the command that has to be spelled out.

The reason is the reason protobuf is missing. A strategic merge patch decides how to merge each
field by reading `patchStrategy` and `patchMergeKey` struct tags off the Go type the object decodes
into, and a projected object decodes into an unstructured map, which has neither. Custom resources
have the identical problem and give the identical answer: the type is never offered, so a client
that sends one is refused during content negotiation with a `415` naming the three that do work,
rather than accepted and then failed halfway through the merge.

## What a projection needs from the database

kube-crisp does not create tables. A projection whose table is missing compiles against the database
and reports `Ready=False` with reason `CompilationFailed`, carrying the database's own message —
naming the query and the column. It keeps retrying, so the projection starts serving on its own once
the table appears; nothing has to be reapplied.

The other half is in `status.requiredSchema`: what the table would have to contain, gathered from the
queries and the mapping so nobody has to read the SQL to find out.

```console
$ kubectl get crp orders -o jsonpath='{.status.requiredSchema}' | jq
{
  "tables": ["orders"],
  "columns": [
    {"name": "customer",    "type": "string",  "usedFor": "field"},
    {"name": "id",          "type": "string",  "usedFor": "identity"},
    {"name": "status",      "type": "string",  "usedFor": "label"},
    {"name": "tenant",      "type": "string",  "usedFor": "identity"},
    {"name": "total_cents", "type": "integer", "usedFor": "field"},
    {"name": "updated_at",  "type": "string",  "usedFor": "metadata"}
  ]
}
```

`usedFor` separates the columns a row cannot become an object without — `identity` — from the ones
that fill in metadata, labels, or fields.

`kubectl crisp schema` reads it back as a checklist, over every projection at once, and over
manifests that have never reached a cluster:

```console
$ kubectl crisp schema -f examples/orders/
orders  orders.store.example.com/v1alpha1
  tables: orders
  COLUMN       TYPE     READ FOR
  created_at   string   metadata
  currency     string   field
  customer     string   field
  id           string   identity
  ...
```

It derives the same answer rather than reading the field, by calling the function the controller
fills the field with — so a projection still in a file, or one whose status has not been written
because it cannot compile, reports what it would need. `-o json` gives the same data per projection.

Two things it deliberately is not:

- **A schema.** These are *result* columns. A list query that computes one, `SELECT total_cents AS
  observed`, requires `observed` in its results and no such column in any table.
- **Complete.** Table names are read out of the statement text, so a name inside a string literal or
  a comment is not one — but a common table expression is listed alongside the tables it reads, and a
  table reached only through a view is not listed at all, because nothing in the statement says so.

### Creating the table

Use a schema tool and let the two converge. [SchemaHero](https://schemahero.io) and the
[Atlas Operator](https://atlasgo.io/integrations/kubernetes/operator) both manage database schema
through custom resources; either can own the table while a `CustomResourceProjection` projects it.
Apply both together — ordering does not matter, because a projection whose table is not there yet
retries until it is.

Owning DDL is deliberately out of scope here. Migration ordering, drift, and what to do about a
destructive change are a separate problem with mature answers, and a table outlives the projection
over it: several projections may read one table, and deleting a projection must never drop it. The
credentials are the other reason — the Secret a projection reads is one that opted in to being
projected, and giving it rights to drop tables would hand that to every projection sharing it.

## Everything else

The prose above covers the fields worth explaining. These are the rest — real,
supported, and mostly things you set once and forget. `kubectl explain` is
authoritative and carries the same text this table summarises:

```console
$ kubectl explain customresourceprojection.spec.dataSource
$ kubectl explain customresourceprojection.spec.mapping.creationTimestamp
```

### Naming the resource

| Field | |
|---|---|
| `resource.singular` | The singular name kubectl accepts. Defaults to the lower-cased kind |
| `resource.listKind` | Defaults to `Kind` + `List` |
| `resource.shortNames` | kubectl aliases, so `kubectl get ord` works |
| `resource.categories` | kubectl categories, so `kubectl get all` can include the kind |

### Reaching the database

| Field | |
|---|---|
| `dataSource.dsnKey` | The key within the Secret holding the connection string. Defaults to `dsn` |
| `dataSource.readDsnKey` | The same for the replica's Secret. Defaults to `dsnKey` |
| `dataSource.preparedStatements` | Caches a prepared statement per query on each pooled connection, so repeated reads skip parsing and planning. On by default |
| `dataSource.maxIdleConns` | Idle connections the pool keeps. Defaults to `maxOpenConns` |
| `dataSource.connMaxLifetime` | Bounds connection reuse. Defaults to 30m |
| `dataSource.connMaxIdleTime` | Closes a connection idle this long, before its lifetime is up |
| `dataSource.keepAliveInterval` | Pings idle connections so a request never pays connection setup, and a firewall does not drop them unnoticed. Defaults to 30s |

The pool defaults suit a projection under steady load. The two worth revisiting
are `maxOpenConns`, which is what a busy projection runs out of first, and
`preparedStatements`, which is worth turning off only if something between here
and the database cannot cope with them — a connection pooler in transaction
mode, most often.

### Mapping

| Field | |
|---|---|
| `mapping.creationTimestamp` | The column providing `metadata.creationTimestamp`, which is what gives `kubectl get` an `AGE` column |
| `mapping.fields[].omitEmpty` | Drops the field when the column is NULL rather than emitting an explicit `null` |
| `mapping.fields[].type` | `string`, `integer`, `number` or `boolean` — how the column's value is carried into JSON |

An object with no `creationTimestamp` shows an empty age, which is the usual
reason a projection looks subtly wrong in `kubectl get` while being correct in
`-o yaml`.

### Choosing a field type for a numeric column

`number` is a float64, because that is what JSON has. PostgreSQL `NUMERIC` and
MySQL `DECIMAL` can carry more significant digits than a float64 keeps, so a
currency or measurement column mapped as `number` loses its last places — with
no error and no warning, because nothing about the value is invalid. Map such a
column as `string` to carry it exactly, and let the client parse it.

`integer` covers the full signed 64-bit range. A MySQL `BIGINT UNSIGNED` above
that cannot be a JSON number at all; the row is refused with an error naming
`type: string` as the way to carry it, rather than being dropped from the
collection. A `string` holds the whole unsigned range exactly, whatever width
the driver picked. That is also what makes `id BIGINT UNSIGNED` usable as
`mapping.name`, since every mapped identity field is rendered as text.
