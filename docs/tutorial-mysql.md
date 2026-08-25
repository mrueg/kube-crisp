# Projecting a MySQL table

The same shape as the [PostgreSQL tutorial](tutorial-postgresql.md), with the
differences MySQL forces. They are small but each one is a hard error rather
than a silent difference, so they are worth reading before writing the queries.

## 1. The table

```sql
CREATE TABLE widgets (
    id           VARCHAR(64) PRIMARY KEY,
    tenant       VARCHAR(64) NOT NULL,
    colour       VARCHAR(64) NOT NULL,
    weight_grams BIGINT      NOT NULL,
    updated_at   VARCHAR(32) NOT NULL,
    INDEX widgets_tenant_idx (tenant),
    -- The column mapped to resourceVersion, because the watch query filters and
    -- orders by it on every poll. Without it the poll is a full scan and a sort
    -- of the whole table, once per pollInterval.
    INDEX widgets_updated_at_idx (updated_at)
);
```

## 2. The credentials

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: widgets-db
  namespace: kube-crisp
  labels:
    crisp.kubecrisp.io/allow-projection: "true"
type: Opaque
stringData:
  # go-sql-driver's DSN format, not a URL.
  dsn: crisp:secret@tcp(mysql.databases.svc:3306)/store?parseTime=true
```

## 3. The projection

```yaml
apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: widgets
spec:
  dataSource:
    driver: mysql
    secretRef: {name: widgets-db, namespace: kube-crisp}

  resource:
    group: store.example.com
    version: v1alpha1
    kind: Widget
    plural: widgets
    scope: Namespaced
    schema:
      type: object
      properties:
        spec:
          type: object
          properties:
            colour: {type: string}
            weightGrams: {type: integer, format: int64}
    additionalPrinterColumns:
      - {name: Colour, type: string, jsonPath: .spec.colour}

  queries:
    list:
      sql: |
        SELECT id, tenant, colour, weight_grams, updated_at
        FROM widgets
        WHERE (:namespace IS NULL OR tenant = :namespace)
          AND (:after IS NULL OR id > :after)
        ORDER BY id
        LIMIT :limit
    get:
      sql: |
        SELECT id, tenant, colour, weight_grams, updated_at
        FROM widgets WHERE tenant = :namespace AND id = :name
    count:
      sql: SELECT COUNT(*) FROM widgets WHERE (:namespace IS NULL OR tenant = :namespace)
    create:
      sql: |
        INSERT INTO widgets (id, tenant, colour, weight_grams, updated_at)
        VALUES (:id, :tenant, :colour, :weight_grams,
                CAST(UNIX_TIMESTAMP(NOW(6)) * 1000000 AS UNSIGNED))
    delete:
      sql: DELETE FROM widgets WHERE tenant = :namespace AND id = :name

  watch:
    pollInterval: 1s
    query:
      # maxRows has to cover the whole table, not one poll: the periodic full
      # resync reads all of it, and that is what detects deletions. The default
      # is 5000, so a table past that fails its resync with "result set exceeded
      # maxRows" while ordinary reads carry on working.
      maxRows: 50000
      sql: |
        SELECT id, tenant, colour, weight_grams, updated_at
        FROM widgets
        WHERE (:since IS NULL OR updated_at > :since)
        ORDER BY updated_at ASC
        LIMIT :limit

  mapping:
    name: id
    namespace: tenant
    resourceVersion: updated_at
    fields:
      - {column: colour,       path: spec.colour}
      - {column: weight_grams, path: spec.weightGrams, type: integer}
```

```console
$ kubectl apply -f widgets-projection.yaml
$ kubectl get widgets -n acme
NAME       COLOUR
widget-1   red
```

## What else this projection could do

The [PostgreSQL tutorial](tutorial-postgresql.md) works through the features
that are not driver-specific, and they apply here unchanged: labels read from a
JSON column, selectors pushed down onto the columns behind them, the caller's
identity as bind parameters, a read replica, and a deletion query that lets an
incremental watch see removals without re-reading the table.

Two are shaped differently on MySQL:

- **Session variables are connection-scoped**, not transaction-scoped, so
  kube-crisp clears them before the transaction ends. Read them with
  `@app_tenant` — a user variable cannot hold a dot, so `app.tenant` becomes
  `app_tenant`.
- **JSON parameters** — `:userGroups`, `:label_<column>_in` — are strings holding
  a JSON array. `JSON_CONTAINS(:userGroups, JSON_QUOTE(owner_team))` is the
  idiom, where PostgreSQL would use `jsonb_array_elements_text`.

## What is particular to MySQL

- **No `RETURNING`.** Write statements are executed and the row is read back
  afterwards, which kube-crisp does automatically. The only visible difference
  is one extra round trip per write.
- **`LIMIT` takes a literal or a placeholder, never an expression.** `LIMIT
  COALESCE(:limit, 1000)` is a syntax error. Write `LIMIT :limit`: kube-crisp
  always binds a value, using the query's `maxRows` when a client did not ask
  for a page.
- **No casts needed** on `:param IS NULL`; MySQL binds `?` positionally, and
  kube-crisp repeats the value for each occurrence.
- **Duplicate keys** come back as error 1062, which is translated to
  `AlreadyExists` like any other driver's equivalent.
