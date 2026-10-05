# kube-crisp

**C**ustom **R**esource **I**nterface for **S**QL **P**rojections — serve any SQL database as Kubernetes custom resources.

![kube-crisp: a crisp packet branded with a ship's wheel around a database, SQL flavour, 100% custom resources](logo.png)

kube-crisp is an aggregated Kubernetes API server. You give it a resource shape and a set of
queries; it answers `kubectl get` by executing those queries against your database and mapping
result rows onto API objects. Nothing is copied into etcd and nothing is synchronised in the
background: reads are answered from the data source at request time, and writes go straight back
to it.

```console
$ kubectl get films
NAME               TITLE              RATING   RATE   MINUTES   BREAK-EVEN
academy-dinosaur   ACADEMY DINOSAUR   PG       0.99   86        22
ace-goldfinger     ACE GOLDFINGER     G        4.99   48        3
adaptation-holes   ADAPTATION HOLES   NC-17    2.99   50        7

$ kubectl explain films.spec.title
GROUP:      pagila.example.com
KIND:       Film
VERSION:    v1alpha1

FIELD: title <string>
```

Those objects are rows in a PostgreSQL table.

![Listing a thousand films out of PostgreSQL and reading one of them with kubectl](docs/demo/pagila.gif)

A thousand of them, out of a [DVD-rental sample database](docs/tutorial-pagila.md) projected whole —
listed, sliced by label, and read in full. No CRD, no controller, and nothing copied into etcd.

## Why not a controller that syncs rows into CRs?

A sync controller has to own a copy of your data, reconcile it, and answer awkward questions about
staleness, deletion, and write conflicts. A projection has none of that: the database stays the
single source of truth, the API server holds no state, and a row that changes is visible on the
next read.

## How it works

```
kubectl ──▶ kube-apiserver ──▶ APIService ──▶ kube-crisp-apiserver ──▶ SQL database
              (aggregation layer)                  │
                                                   └── CustomResourceProjection
                                                       (resource shape + queries + mapping)
```

1. A `CustomResourceProjection` declares the projected group, version, and kind, the data source,
   the SQL that answers each verb, and how columns map onto object fields.
2. An `APIService` delegates that API group to kube-crisp-apiserver.
3. On each request the matching query runs with request-derived bind parameters, and every row
   becomes one object.

Projections are watched, not loaded once: creating a `CustomResourceProjection` installs its API
group while the server runs, and deleting it takes the group away again. No restart, no redeploy.

## What is supported

| | |
|---|---|
| **Read** | `get`, `list`, label and field selectors pushed down to the database where a column backs them, `resourceVersionMatch`, metadata-only requests, keyset pagination with `remainingItemCount`, optional caching |
| **Write** | `create` (including `generateName`), `update`, `patch` (JSON Patch, merge patch and apply — a custom resource's set, so not strategic merge), `delete`, `deleteCollection`, `dryRun`, with optimistic concurrency, and multi-statement writes in a transaction |
| **Subresources** | `/status`, owned separately from the rest of the object, and `/scale`, so `kubectl scale` and the horizontal pod autoscaler work |
| **Watch** | incremental polling — or `LISTEN`/`NOTIFY`, so a change wakes the watch in milliseconds rather than at the next tick — resumable from recent history, with periodic bookmarks and the `WatchList` protocol, so client-go informers work |
| **Schema** | enforced on writes including `x-kubernetes-validations` CEL rules and ratcheting, defaults applied, unknown fields pruned or rejected, published as OpenAPI, and used by server-side apply |
| **Versions** | several versions of a kind, each with its own schema and mapping, checked to map the same columns so a write through one does not lose what another shows |
| **Admission** | opt-in: `ValidatingAdmissionPolicy`, `MutatingAdmissionPolicy`, admission webhooks, and namespace lifecycle apply to projected writes; and a webhook of its own that checks a projection's SQL against the database, so a broken one is refused at `kubectl apply` rather than reported afterwards |
| **Registration** | the APIService for each projected group is created, corrected, and removed automatically |
| **Access** | authorization is the cluster's: `kubectl crisp rbac` writes the ClusterRoles a projected group needs, granting each kind exactly the verbs its projection can serve |
| **Lifecycle** | map columns onto `metadata.generation`, `deletionTimestamp`, `finalizers`, and `ownerReferences`, so soft deletes, `observedGeneration`, finalizer flows, and garbage collection work as clients expect |
| **Multi-tenancy** | map a tenant column to `metadata.namespace` and ordinary namespace RBAC applies, or set session variables and let row-level security enforce it in the database; the caller's name, UID, groups and extra are all bindable |
| **Scale-out** | reads can go to a read replica while writes stay on the primary, and leader election leaves one replica polling at full rate |
| **Identity** | one column, or several joined into a name for a table with a composite key |
| **Drivers** | PostgreSQL (pgx), MySQL, SQLite — all three are covered by the e2e suite, and the set is a registry rather than a switch |
| **Credentials** | a connection string in a Secret, watched so a rotation lands when it happens — or, for a managed database that has no password to store, a registered provider that mints one per connection, so a fifteen-minute IAM token does not rebuild the pool four times an hour |
| **Observability** | Prometheus metrics, audited writes, and OTLP traces carrying a span per statement, so a slow read names the projection and the query rather than ending at the handler |

## Tutorials

A complete walk-through per driver, each ending in a working `kubectl get`:

- [PostgreSQL](docs/tutorial-postgresql.md) — the fullest example: writes with `RETURNING`,
  incremental watch, keyset paging.
- [MySQL](docs/tutorial-mysql.md) — the same, minus `RETURNING`, plus what `LIMIT` will and will
  not accept.
- [SQLite](docs/tutorial-sqlite.md) — a file on a volume, no server, and what that costs.

And one that is not about a driver at all:

- [Pagila](docs/tutorial-pagila.md) — a whole schema somebody else designed, modelled as ten kinds.
  Which tables become resources and which become fields, names that have to survive real data,
  and a `kubectl scale` over a table that does not exist.

## Example

```yaml
apiVersion: crisp.kubecrisp.io/v1alpha1
kind: CustomResourceProjection
metadata:
  name: pagila-films
spec:
  dataSource:
    driver: postgres
    secretRef: {name: pagila-db, namespace: kube-crisp}
  resource:
    group: pagila.example.com
    version: v1alpha1
    kind: Film
    plural: films
    scope: Cluster
    schema: {...}
    # Bound into the statement, so the database does the filtering rather than
    # the server discarding rows it has already read.
    selectableFields:
      - {jsonPath: .spec.rating, column: rating}
  queries:
    list:
      sql: |
        SELECT f.film_id,
               lower(regexp_replace(f.title, '[^a-zA-Z0-9]+', '-', 'g')) AS slug,
               f.title, f.rating::text AS rating, f.rentals_to_breakeven
        FROM film f
        WHERE (:rating::text IS NULL OR f.rating::text = :rating)
        ORDER BY f.film_id
    get:
      sql: ...
  mapping:
    name: slug          # ACADEMY DINOSAUR is not a valid object name
    uid: film_id
    labels:
      pagila.example.com/rating: rating
    fields:
      - {column: title,  path: spec.title}
      - {column: rating, path: spec.rating}
      # A generated column: PostgreSQL computes it, so it belongs in status.
      - {column: rentals_to_breakeven, path: status.rentalsToBreakEven, type: integer}
```

The full version is in [`examples/pagila/`](examples/pagila), which projects that whole schema as ten
kinds — the [tutorial](docs/tutorial-pagila.md) walks through why each one is shaped the way it is.
A smaller, writable, single-table example is in [`examples/orders/`](examples/orders), with its table
in [`examples/orders/schema.sql`](examples/orders/schema.sql).

Every field a projection can carry — bind parameters, schemas, subresources,
writes, row-level security, finalizers, selectors, versions, caching, replicas,
pagination, and watch — is in **[docs/reference.md](docs/reference.md)**.

## Documentation

| | |
|---|---|
| [Installing](docs/installation.md) | The server with Helm, plain manifests or `go run`, the optional manifests, and the `kubectl crisp` plugin |
| [Tutorials](docs/tutorial-postgresql.md) | A worked walk-through per driver, each ending in a working `kubectl get`, and [one whole schema](docs/tutorial-pagila.md) |
| [Reference](docs/reference.md) | Every field a projection can carry, and why you would use it |
| [Operating](docs/operating.md) | Admission, leader election, fair queueing, outages, health, metrics, tracing, and the security model |
| [Limitations](docs/limitations.md) | What it does not do, or does with a condition attached, and why |
| [Extending](docs/extending.md) | Registering a database driver or a credential provider in a build of your own |
| [Performance](docs/performance.md) | What `make bench` measures, and what it found |
| [Development](docs/development.md) | The test suites, `validate`, the make targets, and the repository layout |
| [Contributing](CONTRIBUTING.md) | Running the tests, regenerating code, and what good looks like here |
| [Security](SECURITY.md) | Reporting a vulnerability, the assumptions this makes, and a hardening checklist |

## Quick start

These project a sample database this repository does not carry — run
[`./hack/fetch-pagila.sh`](third_party/pagila) and load it into a PostgreSQL 18 server first, or
point the DSN at a database of your own and apply [`examples/orders/`](examples/orders) instead,
which needs one `CREATE TABLE`.

```console
$ helm install kube-crisp oci://ghcr.io/mrueg/charts/kube-crisp --namespace kube-crisp --create-namespace
$ kubectl apply -f examples/pagila/00-secret.yaml     # edit the DSN first
$ kubectl apply -f examples/pagila/10-catalogue.yaml
$ kubectl get films                                   # the APIService registers itself
```

That last command works as cluster-admin. Authorization is the cluster's, so everyone else needs a
ClusterRole naming the group first — `kubectl crisp rbac | kubectl apply -f -` writes it.

[Installing](docs/installation.md) covers running it locally without a cluster, plain manifests,
the optional pieces, and the `kubectl crisp` plugin.

## Status

Early, and interfaces will change. Reads, writes, watch, dynamic registration, admission, tracing,
and the mapping layer are implemented, with unit tests and an e2e suite that runs against
PostgreSQL, MySQL and SQLite in a kind cluster — covering a database outage, row-level security,
finalizer flows, server-side apply conflicts, a dropped `LISTEN`/`NOTIFY` subscription, generated
RBAC that is applied and then used to make the requests it authorizes, and a run against a server
built with the race detector. The correctness half of that suite takes a few minutes; the rest is
benchmarks.

Built against Kubernetes libraries v0.37.0 and Go 1.26.
