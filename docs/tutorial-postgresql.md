# Projecting a PostgreSQL table

A working `orders` table becomes a Kubernetes resource in about five minutes.
Everything here assumes kube-crisp is already installed from `manifests/`.

## 1. The table

```sql
CREATE TABLE orders (
    id          TEXT PRIMARY KEY,
    tenant      TEXT        NOT NULL,
    customer    TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',
    total_cents BIGINT      NOT NULL,
    updated_at  TEXT        NOT NULL
);
CREATE INDEX orders_tenant_idx ON orders (tenant);
-- The column mapped to resourceVersion, because the watch query filters and
-- orders by it on every poll. Without this the poll is a sequential scan and a
-- sort of the whole table, once per pollInterval: measured at 10,000 rows, 6.0ms
-- and 116 buffers per poll against 0.16ms and 10 with the index, and the gap
-- grows with the table rather than staying flat.
CREATE INDEX orders_updated_at_idx ON orders (updated_at);
```

`tenant` becomes the Kubernetes namespace, and `updated_at` becomes
`resourceVersion`. Neither is required, but both are worth having: the first
makes ordinary namespace RBAC apply to rows, and the second is what lets a watch
tell a changed row from an unchanged one.

## 2. The credentials

The Secret has to opt in. kube-crisp refuses to use one that has not, because a
`CustomResourceProjection` is cluster-scoped and carries arbitrary SQL: whoever
owns the credentials decides whether they may be projected.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: orders-db
  namespace: kube-crisp
  labels:
    crisp.kubecrisp.io/allow-projection: "true"
type: Opaque
stringData:
  dsn: postgres://crisp:secret@postgres.databases.svc:5432/store?sslmode=require
```

## 3. The projection

```yaml
apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: orders
spec:
  dataSource:
    driver: postgres
    secretRef: {name: orders-db, namespace: kube-crisp}

  resource:
    group: store.example.com
    version: v1alpha1
    kind: Order
    plural: orders
    scope: Namespaced
    schema:
      type: object
      properties:
        spec:
          type: object
          properties:
            customer: {type: string}
            totalCents: {type: integer, format: int64}
        status:
          type: object
          properties:
            phase: {type: string}
    additionalPrinterColumns:
      - {name: Customer, type: string, jsonPath: .spec.customer}
      - {name: Total, type: integer, jsonPath: .spec.totalCents}
    selectableFields:
      - jsonPath: .spec.customer
        column: customer

  queries:
    list:
      sql: |
        SELECT id, tenant, customer, status, total_cents, updated_at
        FROM orders
        WHERE (:namespace::text IS NULL OR tenant = :namespace)
          AND (:after::text IS NULL OR id > :after)
          AND (:customer::text IS NULL OR customer = :customer)
        ORDER BY id
        LIMIT :limit
    get:
      sql: |
        SELECT id, tenant, customer, status, total_cents, updated_at
        FROM orders WHERE tenant = :namespace AND id = :name
    count:
      sql: SELECT COUNT(*) FROM orders WHERE (:namespace::text IS NULL OR tenant = :namespace)
    create:
      sql: |
        INSERT INTO orders (id, tenant, customer, status, total_cents, updated_at)
        VALUES (:id, :tenant, :customer, :status, :total_cents,
                (extract(epoch from clock_timestamp()) * 1000000)::bigint::text)
        RETURNING id, tenant, customer, status, total_cents, updated_at
    update:
      sql: |
        UPDATE orders
        SET customer = :customer, total_cents = :total_cents,
            updated_at = (extract(epoch from clock_timestamp()) * 1000000)::bigint::text
        WHERE tenant = :namespace AND id = :name
          AND (:resourceVersion::text IS NULL OR updated_at = :resourceVersion)
        RETURNING id, tenant, customer, status, total_cents, updated_at
    delete:
      sql: DELETE FROM orders WHERE tenant = :namespace AND id = :name

  watch:
    pollInterval: 1s
    query:
      # maxRows has to cover the whole table, not one poll: the periodic full
      # resync reads all of it, and that is what detects deletions.
      maxRows: 50000
      sql: |
        SELECT id, tenant, customer, status, total_cents, updated_at
        FROM orders
        WHERE (:since::text IS NULL OR updated_at > :since)
        ORDER BY updated_at ASC
        LIMIT :limit

  mapping:
    name: id
    namespace: tenant
    resourceVersion: updated_at
    labels:
      store.example.com/status: status
    fields:
      - {column: customer,    path: spec.customer}
      - {column: total_cents, path: spec.totalCents, type: integer}
      - {column: status,      path: status.phase}
```

```console
$ kubectl apply -f orders-projection.yaml
$ kubectl get customresourceprojection orders
NAME     RESOURCE   GROUP               DRIVER     READY   AGE
orders   orders     store.example.com   postgres   True    5s
```

The APIService registers itself. Give the aggregation layer a moment, then:

```console
$ kubectl get orders -n acme
NAME         CUSTOMER   TOTAL
order-1001   ada        4999

$ kubectl get orders -n acme --field-selector spec.customer=ada
$ kubectl explain orders.spec.customer
$ kubectl create -f order.yaml && kubectl delete order order-1001 -n acme
```

## Writing to more than one table

A write can run several statements as one transaction, which is what a kind
spanning two tables needs:

```yaml
    create:
      statements:
        - INSERT INTO order_events (id, tenant, event) VALUES (:id, :tenant, 'created')
        - |
          INSERT INTO orders (id, tenant, customer, status, total_cents, updated_at)
          VALUES (:id, :tenant, :customer, :status, :total_cents,
                  (extract(epoch from clock_timestamp()) * 1000000)::bigint::text)
          RETURNING id, tenant, customer, status, total_cents, updated_at
```

Either both rows land or neither does. Only the last statement may return rows,
because only its result can be the object the client is answered with.

## Deleting softly

Rows are often marked rather than removed. Map the column and clients see what
they expect — an object with a `deletionTimestamp` is terminating:

```yaml
    delete:
      sql: |
        UPDATE orders SET deleted_at = now()
        WHERE tenant = :namespace AND id = :name AND deleted_at IS NULL
  mapping:
    deletionTimestamp: deleted_at
    generation: generation
```

The object stays listed and readable while it is terminating, and deleting it
again is answered with the object rather than restamping the column. `generation`
is the counter a controller compares against its `status.observedGeneration`; the
database owns it, so advance it in the `UPDATE` when the spec changes.

## Scaling a row

The same projection can serve `/scale`, which is what `kubectl scale` and the horizontal pod
autoscaler drive. Add a replica column to the table and two paths to the resource:

```yaml
    schema:
      type: object
      properties:
        spec:
          type: object
          properties:
            replicas: {type: integer, format: int64, default: 1}
    subresources:
      scale:
        specReplicasPath: .spec.replicas
        statusReplicasPath: .status.replicas
```

`specReplicasPath` has to be a mapped column, because a scale request writes it through the
projection's ordinary `update` statement:

```console
$ kubectl scale orders/order-1001 -n acme --replicas=4
order.store.example.com/order-1001 scaled
```

The `default: 1` above is applied to writes, so a client that omits `spec.replicas` still lands a
value in the column. Reads are never defaulted: a NULL column reads as absent.

## Finishing work before an object goes away

A controller that has to drain something before its row disappears puts a
finalizer on the object. Hold them in a column and the ordinary Kubernetes flow
works:

```sql
ALTER TABLE orders ADD COLUMN deleted_at  TEXT;
ALTER TABLE orders ADD COLUMN finalizers  TEXT;
ALTER TABLE orders ADD COLUMN owners      TEXT;
```

```yaml
  queries:
    markDeleted:
      sql: |
        UPDATE orders SET deleted_at = now()
        WHERE tenant = :namespace AND id = :name AND deleted_at IS NULL
  mapping:
    deletionTimestamp: deleted_at
    finalizers: finalizers
    ownerReferences: owners
```

A delete on an object holding a finalizer marks it terminating and leaves the
row; the controller does its work, removes the finalizer with an ordinary
update, and that is what deletes the row. `ownerReferences` are stored the same
way, which is what lets the cluster's garbage collector delete a projected
object when its owner goes away.

## Letting the database enforce the boundary

The projection above trusts its own `WHERE tenant = :namespace`. PostgreSQL can
enforce it instead, which is a stronger claim: a mistake in the SQL cannot leak
rows the database never offers.

```sql
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;
CREATE POLICY orders_tenant ON orders
    USING (tenant = current_setting('app.tenant', true));
```

```yaml
  dataSource:
    driver: postgres
    secretRef: {name: orders-db, namespace: kube-crisp}
    sessionVariables:
      - {name: app.tenant, from: RequestNamespace}
  watch:
    disabled: true
```

kube-crisp sets `app.tenant` on the connection from the request before every
query, inside the transaction the query runs in, so it cannot be left behind on
a pooled connection. Watch has to be off: a poll runs on a timer with no request
behind it, so there is no tenant to set and the policy would show it nothing.

## Scoping rows to the caller

`app.tenant` above comes from the request's namespace. The caller's own identity
is available the same way, and there is more of it than a username:

```yaml
  dataSource:
    sessionVariables:
      - {name: app.user,   from: RequestUser}
      - {name: app.uid,    from: RequestUserUID}
      - {name: app.groups, from: RequestUserGroups}
```

`app.user` is the name the authenticator gave, `app.uid` the half of an identity
that does not get reassigned to somebody else, and `app.groups` a JSON array —
which is what you want, because authorization in Kubernetes is mostly by group:

```sql
CREATE POLICY orders_team ON orders
    USING (owner_team IN (
        SELECT jsonb_array_elements_text(current_setting('app.groups', true)::jsonb)
    ));
```

The same values are available to any query as `:user`, `:userUID`,
`:userGroups` and `:userExtra`, without row-level security in the picture.

## Labels that vary per row

`mapping.labels` names one label key per column, which is right when the set is
fixed. A table whose rows carry different labels wants a single JSON column
instead:

```sql
ALTER TABLE orders ADD COLUMN labels jsonb;
UPDATE orders SET labels = '{"team":"payments"}' WHERE id = 'order-1001';
```

```yaml
  mapping:
    labelsFrom: labels
    labels:
      store.example.com/status: status
```

Both at once is the useful shape: everything lives in the JSON except the keys
worth promoting to a column of their own, and those win. A promoted key is also
one a selector can filter on in the database — kube-crisp binds
`:label_status`, `:label_status_not` and `:label_status_in` for it, so
`kubectl get orders -l store.example.com/status in (shipped,pending)` becomes a
query rather than a filter over everything.

## Setting labels and annotations

Labels and annotations are not read-only. Every mapped column is bound on writes
as well as reads, so `kubectl label` and `kubectl annotate` reach the database —
if the write statement sets the column. Annotations work the same way through
`mapping.annotations` and `mapping.annotationsFrom`.

```sql
ALTER TABLE orders ADD COLUMN annotations jsonb;
```

```yaml
  queries:
    update:
      sql: |
        UPDATE orders
        SET customer = :customer,
            total_cents = :total_cents,
            labels = :labels,
            annotations = :annotations,
            updated_at = (extract(epoch from clock_timestamp()) * 1000000)::bigint::text
        WHERE tenant = :namespace AND id = :name
          AND (:resourceVersion::text IS NULL OR updated_at = :resourceVersion)
        RETURNING id, tenant, customer, labels, annotations, total_cents, updated_at

  mapping:
    labelsFrom: labels
    annotationsFrom: annotations
```

```console
$ kubectl label order order-1001 -n acme team=platform region=eu
order.store.example.com/order-1001 labeled
$ kubectl annotate order order-1001 -n acme owner=ada
order.store.example.com/order-1001 annotated
```

```
 labels                                | annotations
---------------------------------------+------------------
 {"team": "platform", "region": "eu"}   | {"owner": "ada"}
```

Removing one persists too: `kubectl label order order-1001 region-` rewrites the
column without that key. The JSON column holds only what has no column of its
own, so a key promoted into `mapping.labels` is not stored twice, and an object
carrying no labels writes `NULL` rather than `{}`.

**One column cannot be written from two places.** In the projection at the top
of this tutorial `status` is mapped twice — as the label
`store.example.com/status` and as the field `status.phase`. That is a good way
to *read* it, and on the way out both are filled from the same column so they
always agree. On a write only one of them can reach the column, and the field is
the one that does:

```console
$ kubectl label order order-1001 -n acme store.example.com/status=cancelled --overwrite
order.store.example.com/order-1001 labeled
Warning: label "store.example.com/status" was not written: it shares column
"status" with field status.phase, which the write set to "shipped"
```

The change to `status.phase` is what moves that column, and the label follows it
on the next read. Map the key only one way if you want to write it as a label.

## Noticing a deletion without re-reading everything

The incremental watch above reads forward from `:since`, so it never sees a row
that is gone — which is why the full list still runs every
`fullResyncInterval`. On a large table that scan is the expensive part. If the
database can say what was deleted, it does not have to happen:

The tombstone's timestamp has to live in the same value space as the mapped
`resourceVersion`, because kube-crisp binds the highest version it has seen as
`:since` for both queries. `updated_at` above is epoch microseconds held as
text, so `deleted_at` is too — declaring it `timestamptz` and casting
`:since::timestamptz` would hand PostgreSQL a sixteen-digit string to parse as a
date, and every deletion poll would fail.

Two things about the key. It is `(id, deleted_at)` rather than `id`, because a
name can be deleted, created again and deleted again — the same identity twice
over — and a primary key on `id` alone makes that second delete fail with a
duplicate key for as long as the first tombstone exists, which is forever. The
row then cannot be deleted at all. And the trigger says `ON CONFLICT DO NOTHING`
so that two deletions landing on the same microsecond do not abort the `DELETE`
that caused them.

The table records the mapped columns as well as the identity. A tombstone that
names the row and nothing else leaves the watch cache as the only place the
deleted object exists, so a client resuming against a restarted server is handed
a `Deleted` event carrying a bare name.

```sql
CREATE TABLE order_tombstones (
    id         text NOT NULL,
    tenant     text NOT NULL,
    customer   text NOT NULL,
    updated_at text NOT NULL,
    deleted_at text NOT NULL
        DEFAULT ((extract(epoch from clock_timestamp()) * 1000000)::bigint::text),
    PRIMARY KEY (id, deleted_at)
);

CREATE FUNCTION record_deleted_order() RETURNS trigger AS $$
BEGIN
    INSERT INTO order_tombstones (id, tenant, customer, updated_at)
        VALUES (OLD.id, OLD.tenant, OLD.customer, OLD.updated_at)
        ON CONFLICT DO NOTHING;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_tombstone AFTER DELETE ON orders
    FOR EACH ROW EXECUTE FUNCTION record_deleted_order();
```

```yaml
  watch:
    pollInterval: 1s
    query: {...}
    deletedQuery:
      sql: |
        SELECT id, tenant, customer, updated_at FROM order_tombstones
        WHERE (:since::text IS NULL OR deleted_at > :since)
        ORDER BY deleted_at ASC
    fullResyncInterval: "0s"
```

It returns the mapped columns as well as the identity, so a deletion can be
described from the table rather than from memory — which is also what lets the
watch cache keep only keys and versions instead of the whole collection.

Worth knowing what `fullResyncInterval: "0s"` costs, because it removes the only
safety net two different failures share. A deletion query that errors leaves
deleted objects in every watcher's cache indefinitely, with only
`kube_crisp_watch_poll_errors_total` climbing to say so. And a row stamped
before the watermark but committed after it — see the note on `clock_timestamp()`
below — is skipped by every later incremental poll, with nothing to say so at
all.

Both are recovered by the periodic full read. Set it to `0s` when this table is
written only by kube-crisp, or by writers that commit in a single statement;
leave it on where anything holds a transaction open.

## Waking the watch instead of polling for it

The poll above runs every second, so a change surfaces within a second of
happening. PostgreSQL can say when it happened instead:

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

```yaml
  watch:
    pollInterval: 5s
    notify:
      channel: orders_changed
```

`FOR EACH STATEMENT` rather than `FOR EACH ROW`: the payload is never read, so a
thousand-row update should say so once.

The notification carries nothing — it means "ask again" — so the incremental
query above is still what reads what changed, and `pollInterval` still runs
underneath as the backstop. That is what makes the interval worth *lengthening*
here rather than shortening: notifications handle the common case, and the timer
is only there for the notification that never arrived.

`kube_crisp_watch_notifications_total` is how you tell the difference. A
projection configured for them whose count is flat is polling on the timer alone.

## Reading from a replica

Reads are almost all of what a projected kind does. Sending them somewhere other
than the database serving the writes is one line:

```yaml
  dataSource:
    secretRef:     {name: orders-db, namespace: kube-crisp}
    readSecretRef: {name: orders-db-replica, namespace: kube-crisp}
```

Writes stay on the primary, and so does the read a write is based on — a
`resourceVersion` checked against a lagging replica is checked against a version
the row may already have left behind. Everything else can be behind, and already
is. The cost is that a client which writes and then reads may see the row as it
was before its own write; `kube_crisp_query_routed_total` shows how the reads
divided.

## What is particular to PostgreSQL

- **Cast NULL comparisons.** `:namespace IS NULL` needs `:namespace::text IS NULL`,
  or the server cannot infer the parameter's type.
- **`RETURNING` is worth using** on every write: the statement answers with the
  row it wrote, which saves a second round trip.
- **Let the database stamp the version.** Writing back the client's value breaks
  incremental watches, and kube-crisp rejects that combination at load time. But
  know what `clock_timestamp()` orders by: the moment the *statement* ran, not
  the moment the transaction *committed*. A writer that stamps a row and commits
  three seconds later has stamped it earlier than rows committed in between, and
  once the poll's `:since` has moved past it, `updated_at > :since` never
  returns it again. Measured against PostgreSQL: a row stamped 1ms before the
  watermark and committed 3s after it was invisible to every later incremental
  poll.

  It matters for writers holding transactions open — kube-crisp's own writes are
  single statements, where stamp and commit are microseconds apart. If other
  systems write to the table inside longer transactions, keep
  `fullResyncInterval` non-zero: the periodic full read is what recovers those
  rows. A version assigned at commit — a sequence advanced in the same statement
  read behind a gapless watermark, or logical decoding — is the shape that needs
  no net at all.
- **`json_agg` is available** via `resultFormat: JSONArray`, though it measured
  slower than row scanning in this project's benchmark. Measure before adopting.
