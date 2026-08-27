# kube-crisp

![kube-crisp: a crisp packet branded with a ship's wheel around a database, SQL flavour, 100% custom resources](logo.png)

**C**ustom **R**esource **I**nterface for **S**QL **P**rojections — serve any SQL database as Kubernetes custom resources.

kube-crisp is an aggregated Kubernetes API server. You give it a resource shape and a set of
queries; it answers `kubectl get` by executing those queries against your database and mapping
result rows onto API objects. Nothing is copied into etcd and nothing is synchronised in the
background: reads are answered from the data source at request time, and writes go straight back
to it.

```console
$ kubectl get orders -n acme
NAME         CUSTOMER   PHASE     TOTAL   AGE
order-1001   ada        shipped   4999    3d
order-1002   grace      pending   1250    2d

$ kubectl explain orders.spec.customer
GROUP:      store.example.com
KIND:       Order
VERSION:    v1alpha1

FIELD: customer <string>
```

Those objects are rows in a PostgreSQL table.

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
| **Write** | `create` (including `generateName`), `update`, `patch`, `delete`, `deleteCollection`, `dryRun`, with optimistic concurrency, and multi-statement writes in a transaction |
| **Subresources** | `/status`, owned separately from the rest of the object, and `/scale`, so `kubectl scale` and the horizontal pod autoscaler work |
| **Watch** | incremental polling — or `LISTEN`/`NOTIFY`, so a change wakes the watch in milliseconds rather than at the next tick — resumable from recent history, with periodic bookmarks and the `WatchList` protocol, so client-go informers work |
| **Schema** | enforced on writes including `x-kubernetes-validations` CEL rules and ratcheting, defaults applied, unknown fields pruned or rejected, published as OpenAPI, and used by server-side apply |
| **Versions** | several versions of a kind, each with its own schema and mapping, checked to map the same columns so a write through one does not lose what another shows |
| **Admission** | opt-in: `ValidatingAdmissionPolicy`, admission webhooks, and namespace lifecycle apply to projected writes; and a webhook of its own that checks a projection's SQL against the database, so a broken one is refused at `kubectl apply` rather than reported afterwards |
| **Registration** | the APIService for each projected group is created, corrected, and removed automatically |
| **Lifecycle** | map columns onto `metadata.generation`, `deletionTimestamp`, `finalizers`, and `ownerReferences`, so soft deletes, `observedGeneration`, finalizer flows, and garbage collection work as clients expect |
| **Multi-tenancy** | map a tenant column to `metadata.namespace` and ordinary namespace RBAC applies, or set session variables and let row-level security enforce it in the database; the caller's name, UID, groups and extra are all bindable |
| **Scale-out** | reads can go to a read replica while writes stay on the primary, and leader election leaves one replica polling at full rate |
| **Identity** | one column, or several joined into a name for a table with a composite key |
| **Drivers** | PostgreSQL (pgx), MySQL, SQLite — all three are covered by the e2e suite, and the set is a registry rather than a switch |
| **Observability** | Prometheus metrics, audited writes, and OTLP traces carrying a span per statement, so a slow read names the projection and the query rather than ending at the handler |

## Tutorials

A complete walk-through per driver, each ending in a working `kubectl get`:

- [PostgreSQL](docs/tutorial-postgresql.md) — the fullest example: writes with `RETURNING`,
  incremental watch, keyset paging.
- [MySQL](docs/tutorial-mysql.md) — the same, minus `RETURNING`, plus what `LIMIT` will and will
  not accept.
- [SQLite](docs/tutorial-sqlite.md) — a file on a volume, no server, and what that costs.

## Example

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
    schema: {...}
  queries:
    list:
      sql: SELECT id, tenant, customer, status FROM orders WHERE tenant = :namespace
    get:
      sql: SELECT id, tenant, customer, status FROM orders WHERE tenant = :namespace AND id = :name
    create:
      sql: |
        INSERT INTO orders (id, tenant, customer, status) VALUES (:id, :tenant, :customer, :status)
        RETURNING id, tenant, customer, status
  mapping:
    name: id
    namespace: tenant
    fields:
      - {column: customer, path: spec.customer}
      - {column: status,   path: status.phase}
```

The full version is in [`examples/orders-projection.yaml`](examples/orders-projection.yaml), with
its table in [`examples/schema.sql`](examples/schema.sql).

Every field a projection can carry — bind parameters, schemas, subresources,
writes, row-level security, finalizers, selectors, versions, caching, replicas,
pagination, and watch — is in **[docs/reference.md](docs/reference.md)**.

## Documentation

| | |
|---|---|
| [Tutorials](docs/tutorial-postgresql.md) | A worked walk-through per driver, each ending in a working `kubectl get` |
| [Reference](docs/reference.md) | Every field a projection can carry, and why you would use it |
| [Operating](docs/operating.md) | Admission, leader election, fair queueing, outages, health, metrics, tracing, and the security model |
| [Performance](docs/performance.md) | What `make bench` measures, and what it found |
| [Contributing](CONTRIBUTING.md) | Running the tests, regenerating code, and what good looks like here |
| [Security](SECURITY.md) | Reporting a vulnerability, the assumptions this makes, and a hardening checklist |

## Quick start

Local, against a database you can already reach:

```console
$ export ORDERS_DB_DSN='postgres://user:pass@localhost:5432/store?sslmode=disable'
$ go run ./cmd/kube-crisp-apiserver \
    --projection-dir=examples --local-dsn-from-env \
    --watch-projections=false --secure-port=8443 --authentication-skip-lookup
```

In a cluster, with Helm:

```console
$ helm install kube-crisp ./charts/kube-crisp --namespace kube-crisp --create-namespace
$ kubectl apply -f examples/orders-db-secret.yaml   # edit the DSN first
$ kubectl apply -f examples/orders-projection.yaml
$ kubectl get orders -n acme                        # the APIService registers itself
```

`helm show values ./charts/kube-crisp` lists what can be turned on: admission,
fair queueing, a `ServiceMonitor`, an egress `NetworkPolicy`, a real CA bundle.

Or with plain manifests:

```console
$ kubectl apply -f manifests/
$ kubectl apply -f examples/orders-db-secret.yaml   # edit the DSN first
$ kubectl apply -f examples/orders-projection.yaml
$ kubectl get orders -n acme                        # the APIService registers itself
```

`manifests/optional/` is deliberately not picked up by the apply above — `kubectl apply -f` on a
directory is not recursive. Everything in it is a decision rather than a default:

| File | What it is for | Why it is not applied by default |
| --- | --- | --- |
| `networkpolicy.yaml` | Restricts traffic to and from the server | Needs your cluster's CIDRs and namespace labels |
| `servicemonitor.yaml`, `prometheusrule.yaml` | Scrape config and alert rules | Need the Prometheus Operator's CRDs |
| `admission-rbac.yaml` | Lets the API surface project admission configuration | Watches webhook configurations and namespaces cluster-wide |
| `flowcontrol-rbac.yaml` | Lets it project FlowSchemas and PriorityLevelConfigurations | Writes to `flowschemas/status` |
| `webhook-rbac.yaml` | Lets the server manage its own `ValidatingWebhookConfiguration` | Creates and updates a cluster-scoped admission object |

The last three used to sit in the main directory as `60-`, `70-` and `80-`, which meant the base
install granted them — while the documentation described each as a grant to make deliberately.

### Adding a driver

`spec.dataSource.driver` names a registered driver, and the registry is open:

```go
sql.Register(sql.Driver{
    Name:             "clickhouse",
    SQLDriver:        "clickhouse",   // what the database/sql driver registered as
    Placeholders:     sql.PlaceholderQuestion,
    SessionVariables: false,
    StatementTimeout: false,
    Notifications:    false,
})
```

Everything that differs between databases is stated there rather than scattered
through switch statements, so adding one is a registration rather than an edit in
six places. What a driver declares is what the rest of the server will offer: a
projection asking for session variables, a statement timeout, or notifications
from a driver that does not claim them is refused rather than silently served
without.

You are building your own binary either way — a `database/sql` driver has to be
linked in — so the same build regenerates the CRD, whose `driver` enum lists what
that build accepts.

## Development

The e2e suite is split so it does not have to be run whole. `make e2e-up` provisions the cluster and
the three databases; after that:

```console
$ make e2e-correctness              # 62 tests, a few minutes
$ make e2e-bench SHARD=reads        # one benchmark shard
$ make bench                        # every benchmark
$ make e2e                          # all of it
```

A projection can be checked without any of that:

```console
$ kube-crisp-apiserver validate examples/ path/to/projection.yaml
ok  examples/: orders (orders.store.example.com/v1alpha1)

1 projection(s) validated
```

It needs no cluster and no database, exits non-zero if anything is rejected, and reports every
projection rather than stopping at the first — so it works as a commit gate. What it cannot check is
whether the database can run the statements; that needs the database, and the server checks it when
the projection is compiled.

The correctness half is the part that says whether the code works, and it answers in a minute; the
rest is benchmarks, which take twenty. CI runs the five shards as parallel jobs with `BENCH_RUNS=1`,
since there it matters that the benchmarks run rather than that their numbers are quotable.
`make e2e-bench-check` fails if a benchmark is in no shard, which would otherwise be a benchmark that
silently stopped running.


```console
$ make codegen     # deepcopy, clientset, listers, informers
$ make verify      # fmt, vet, unit tests
$ make cover       # unit tests with -race and a coverage profile
$ make lint        # golangci-lint
$ make image       # local container image, built by goreleaser through ko
$ make e2e         # kind + PostgreSQL, MySQL, and SQLite, seeded, then the full suite
$ make e2e-race    # the same minus the benchmarks, against a race-built server
$ make bench       # the CRD-versus-projection comparisons, latency and throughput
$ make e2e-down
```

The API types are the source of truth: `pkg/apis/crisp/v1alpha1/zz_generated.deepcopy.go` and
everything under `pkg/generated` come from `hack/update-codegen.sh`, so change the types and
re-run rather than editing the output.

There is no Dockerfile: every image, local or released, is built by goreleaser driving
[ko](https://ko.build), so the e2e image and the released image come off the same path. Releases
are cut by tagging; the workflow signs the checksums and the image with cosign (keyless) and
attaches build provenance.

## Layout

| Path | Contents |
|---|---|
| `pkg/apis/crisp/v1alpha1` | `CustomResourceProjection` API types |
| `pkg/apiserver` | Aggregated server, scheme, and the dynamic router that swaps the served API surface |
| `pkg/controller/projection` | Watches projections and installs or removes API groups at runtime |
| `pkg/registry/projection` | REST storage: reads, writes, and the polling watch cache |
| `pkg/projection` | Row-to-object mapping, projection loading, validation, DSN resolution |
| `pkg/sql` | Pooling, prepared statements, `:named` parameter binding, JSON aggregation |
| `pkg/generated` | Typed clientset, listers, and informers, produced by `hack/update-codegen.sh` |
| `pkg/webhook` | Admission endpoint that checks a projection against its database before the cluster accepts it |
| `pkg/metrics` | Prometheus metrics |
| `pkg/controller/projection` | Also owns APIService registration for served groups |
| `charts/kube-crisp/` | Helm chart, with the optional pieces behind values |
| `manifests/` | CRD, RBAC, Deployment, Service — everything `kubectl apply -f manifests/` should install |
| `manifests/optional/` | Monitoring, network policy, and the RBAC for features that are off by default: each needs a decision or a cluster's own details |
| `examples/` | A projection, its table, its Secret, and a hand-written `APIService` for the rare case of registering groups yourself |
| `test/e2e` | Cluster suite: three drivers, watch, admission, a database outage, and the benchmarks |
| `docs/` | Tutorials per driver, plus the reference, operating and performance documents |
| `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md` | How to contribute, the [CNCF Community Code of Conduct](CODE_OF_CONDUCT.md) this project follows, and how to report a vulnerability |

## Limitations

- **Projected objects are not served as protobuf.** They are unstructured, and unstructured cannot be
  encoded to protobuf — the same reason custom resources cannot be. Clients negotiate JSON, YAML or
  CBOR instead, which is what every Kubernetes client already does for a custom resource.
- **Adding a driver is a registration, within limits.** `spec.dataSource.driver` names a registered
  driver and the registry is open, which covers a database that differs only in placeholders,
  session variables, or whether it can be told to abort a statement — CockroachDB ships as its own
  driver for exactly that reason, being PostgreSQL without `LISTEN`/`NOTIFY`. It does not cover one
  that writes back rows differently: detecting whether a statement returns rows looks for
  `RETURNING`, so SQL Server's `OUTPUT` needs more than a registration. Registering one is not quite
  all of it either — `spec.dataSource.driver` carries an enum, so the CRD has to list the name as
  well or the API server rejects the projection before any of this is reached. A test compares the
  two.
- **The table has to exist.** kube-crisp projects rows; it does not create or migrate tables. A
  projection whose table is missing reports `CompilationFailed` with the database's own message and
  keeps retrying, so it starts serving once the table appears — and `status.requiredSchema` says what
  the table would have to contain, for handing to whatever manages the schema.
- **No subresource beyond `/status` and `/scale`.**
- **Watch history is bounded** by `watch.historySize` and lives in memory, so it is lost on restart
  and not shared between replicas — but a projection that maps a `resourceVersion` and has a
  `deletedQuery` is resumed from the database instead of relisting, which survives both. Without
  tombstones a replay could not report removals, so those clients still relist.
- **Multiple replicas need a mapped `resourceVersion`.** The version a list reports is derived from
  the data, so every replica agrees — but only when the projection maps a version column. Without
  one, each replica falls back to its own counter and must run alone. With leader election on — the
  operator saying there are peers — such a projection is reported in the log and by
  `kube_crisp_projections_unversioned`, rather than failing silently and looking like a client bug.
- **Every replica polls, though not at the same rate.** `--enable-leader-election` gives the lease
  holder the configured interval and leaves the others at `watch.followerPollInterval` (1m by
  default). Followers slow down rather than stop: a watcher is served from the cache of whichever
  replica it connected to, so one that stopped polling would leave its watchers seeing nothing, with
  no error to notice. Versions of one kind do share a poll.
- **Admission and fair queueing are opt-in** and each needs the extra RBAC above. Without admission,
  cluster policy does not apply to projected writes; without fair queueing, the only backpressure is
  a projection's own concurrency limit, which sheds indiscriminately.
- **Owner references are validated, not resolved.** The shape is checked, but whether the owner
  exists is the garbage collector's question rather than this server's.
- **Server-side apply tracks ownership only when asked to.** Without `mapping.managedFields` there
  is nowhere to keep it, so applies merge but never conflict.
- **`--projection-dir` is re-read while running.** A file changing is picked up the way a projection
  changing in the cluster is: the directory is watched and re-read on every sync. A file that does
  not parse keeps the last good set rather than taking every file-backed projection out of service.
  Backed by a ConfigMap, the wait is the kubelet's rather than this server's — around a minute in the
  e2e cluster, with no restart.
- **A watched projection holds its whole collection in memory** and needs `maxRows` set above the
  row count, since the periodic full resync reads all of it. A projection that maps a
  `resourceVersion` and has a `deletedQuery` keeps only keys and versions instead — the diff needs
  the version, and the tombstone describes what was deleted — and reads a new watcher's initial
  state rather than remembering it. Measured at 1.83x less held per row — the identity, the
  version, the kind and the labels are kept, because a watch event has to carry a kind and a label
  selector filters deletions on labels.
- **Rows that cannot be mapped are skipped** by default, with a warning on the response and a count
  in `kube_crisp_query_rows_unmappable_total`, rather than failing the whole collection. Set
  `mapping.onUnmappableRow: Fail` where a partial answer is worse than none — a collection that
  silently omits rows is one a client cannot tell from a smaller collection, so anything reconciling
  towards it deletes what it cannot see.
- **A projection that cannot be served does not fail a probe.** It is reported by
  `kube_crisp_projections{state="failed"}`, by the projection's status conditions, and in the log.
  Making it a health check would have the kubelet restart the server over one broken projection and
  take every healthy one with it; `--require-all-projections` promotes it to a readiness gate, where
  the server drains instead.

## Status

Early, and interfaces will change. Reads, writes, watch, dynamic registration, admission, tracing,
and the mapping layer are implemented and covered by 414 unit tests plus a 74-test e2e suite that
runs against PostgreSQL, MySQL and SQLite in a kind cluster — including a database outage, row-level
security, finalizer flows, server-side apply conflicts, a dropped `LISTEN`/`NOTIFY` subscription, and
a run against a server built with the race detector. The correctness half of that suite takes a few
minutes; the rest is benchmarks.

Built against Kubernetes libraries v0.36.4 and Go 1.26.
