# Projecting a SQLite file

SQLite has no server: the database is a file, so the projection is only as
reachable as the volume holding it. That makes it the quickest way to try
kube-crisp, and a reasonable way to project something that already lives on disk
next to the apiserver — a catalogue, an export, a read-mostly reference table.

## 1. The file

The apiserver needs the file on a volume it mounts. Add one to the deployment,
and seed it however you like — an init container running `sqlite3`, or Python,
which ships with the `sqlite3` module:

The file has to be writable by the user the apiserver runs as. With a non-root
container and an init container that writes as root, that means `fsGroup`:

```yaml
securityContext:
  fsGroup: 65532          # or whatever the apiserver runs as
initContainers:
  - name: seed-sqlite
    image: python:3.13-alpine
    command: ["python", "/seed/seed.py"]
    volumeMounts:
      - {name: sqlite, mountPath: /var/lib/kube-crisp/sqlite}
      - {name: sqlite-seed, mountPath: /seed}
```

```sql
CREATE TABLE items (
    id         TEXT PRIMARY KEY,
    tenant     TEXT    NOT NULL,
    label      TEXT    NOT NULL,
    quantity   INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX items_tenant_idx ON items (tenant);
-- The column mapped to resourceVersion, because the watch query filters and
-- orders by it on every poll. Without this the poll is a sequential scan and a
-- sort of the whole table, once per pollInterval: measured at 10,000 rows, 6.0ms
-- and 116 buffers per poll against 0.16ms and 10 with the index, and the gap
-- grows with the table rather than staying flat.
CREATE INDEX items_updated_at_idx ON items (updated_at);
```

## 2. The credentials

There are none, but the Secret still carries the connection string — a path —
and still has to opt in:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: inventory-db
  namespace: kube-crisp
  labels:
    crisp.kubecrisp.io/allow-projection: "true"
type: Opaque
stringData:
  dsn: /var/lib/kube-crisp/sqlite/inventory.db
```

## 3. The projection

```yaml
apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: items
spec:
  dataSource:
    driver: sqlite
    secretRef: {name: inventory-db, namespace: kube-crisp}
    # SQLite serialises writers, so the pool is one connection — and the query
    # limit matches it. Allowing more queries in flight than there are
    # connections only builds a queue inside database/sql, where a request waits
    # out its statement timeout; matching the two turns contention into a fast
    # 429 instead.
    maxOpenConns: 1
    maxConcurrentQueries: 1

  resource:
    group: store.example.com
    version: v1alpha1
    kind: Item
    plural: items
    scope: Namespaced
    schema:
      type: object
      properties:
        spec:
          type: object
          properties:
            label: {type: string}
            quantity: {type: integer, format: int64}
    selectableFields:
      - jsonPath: .spec.label
        column: label

  queries:
    list:
      sql: |
        SELECT id, tenant, label, quantity, updated_at
        FROM items
        WHERE (:namespace IS NULL OR tenant = :namespace)
          AND (:after IS NULL OR id > :after)
          AND (:label IS NULL OR label = :label)
        ORDER BY id
        LIMIT :limit
    get:
      sql: |
        SELECT id, tenant, label, quantity, updated_at
        FROM items WHERE tenant = :namespace AND id = :name
    count:
      sql: SELECT COUNT(*) FROM items WHERE (:namespace IS NULL OR tenant = :namespace)
    create:
      sql: |
        INSERT INTO items (id, tenant, label, quantity, updated_at)
        VALUES (:id, :tenant, :label, :quantity,
                (SELECT COALESCE(MAX(updated_at), 0) + 1 FROM items))
        RETURNING id, tenant, label, quantity, updated_at
    delete:
      sql: DELETE FROM items WHERE tenant = :namespace AND id = :name

  mapping:
    name: id
    namespace: tenant
    resourceVersion: updated_at
    fields:
      - {column: label,    path: spec.label}
      - {column: quantity, path: spec.quantity, type: integer}
```

```console
$ kubectl apply -f items-projection.yaml
$ kubectl get items -n acme --field-selector spec.label=label-3
```

## What else this projection could do

Most of what the [PostgreSQL tutorial](tutorial-postgresql.md) shows applies
here too: labels read from a JSON column, selectors pushed down, the caller's
identity as bind parameters, and a deletion query so an incremental watch sees
removals without re-reading the file.

Two do not:

- **Session variables**, and so row-level security, because SQLite has no
  session state. A projection asking for them is refused at load time rather
  than served with a setting that does nothing.
- **Read replicas**, because there is no second copy to read from. A file is a
  file.

JSON parameters arrive as text, and `json_each` takes them apart:
`WHERE owner_team IN (SELECT value FROM json_each(:userGroups))`.

## What is particular to SQLite

- **The DSN is a path.** Anything the driver accepts works, including
  `file:/path/to.db?mode=ro` for a read-only projection.
- **One writer at a time.** Keep `maxOpenConns` at 1 and `maxConcurrentQueries`
  with it — the limit only sheds what exceeds it, so a higher value just moves
  the wait from a 429 into database/sql. kube-crisp also gives every SQLite
  connection a 5 second busy timeout unless the DSN sets one, so a reader and a
  writer that overlap wait rather than failing outright.
- **File permissions are part of the setup.** `fsGroup` gives the containers a
  common group, but the file's mode has to allow that group to write, and so
  does the directory — SQLite writes a journal beside the database. Getting
  either wrong shows up as `attempt to write a readonly database` on the first
  create, with reads working perfectly.
- **`RETURNING` works** on SQLite 3.35 and newer, so writes can answer from the
  statement.
- **The file is per replica.** A projection over a local file cannot be served
  by more than one replica unless the volume is shared, and a shared volume
  brings SQLite's locking limitations with it.
- **Versions have to come from somewhere.** There is no `clock_timestamp()`;
  `(SELECT COALESCE(MAX(updated_at), 0) + 1 FROM items)` is the usual answer.
