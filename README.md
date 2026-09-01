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
| **Admission** | opt-in: `ValidatingAdmissionPolicy`, admission webhooks, and namespace lifecycle apply to projected writes; and a webhook of its own that checks a projection's SQL against the database, so a broken one is refused at `kubectl apply` rather than reported afterwards |
| **Registration** | the APIService for each projected group is created, corrected, and removed automatically |
| **Access** | authorization is the cluster's: `kubectl crisp rbac` writes the ClusterRoles a projected group needs, granting each kind exactly the verbs its projection can serve |
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
| [Tutorials](docs/tutorial-postgresql.md) | A worked walk-through per driver, each ending in a working `kubectl get`, and [one whole schema](docs/tutorial-pagila.md) |
| [Reference](docs/reference.md) | Every field a projection can carry, and why you would use it |
| [Operating](docs/operating.md) | Admission, leader election, fair queueing, outages, health, metrics, tracing, and the security model |
| [Performance](docs/performance.md) | What `make bench` measures, and what it found |
| [Contributing](CONTRIBUTING.md) | Running the tests, regenerating code, and what good looks like here |
| [Security](SECURITY.md) | Reporting a vulnerability, the assumptions this makes, and a hardening checklist |

## Quick start

These project a sample database this repository does not carry — run
[`./hack/fetch-pagila.sh`](third_party/pagila) and load it into a PostgreSQL 18 server first, or
point the DSN at a database of your own and apply [`examples/orders/`](examples/orders) instead,
which needs one `CREATE TABLE`.

Local, against a database you can already reach:

```console
$ export PAGILA_DB_DSN='postgres://user:pass@localhost:5432/pagila?sslmode=disable'
$ go run ./cmd/kube-crisp-apiserver \
    --projection-dir=examples/pagila --local-dsn-from-env \
    --watch-projections=false --secure-port=8443 --authentication-skip-lookup
```

In a cluster, with Helm — one chart per release, published beside the image:

```console
$ helm install kube-crisp oci://ghcr.io/mrueg/charts/kube-crisp --namespace kube-crisp --create-namespace
$ kubectl apply -f examples/pagila/00-secret.yaml     # edit the DSN first
$ kubectl apply -f examples/pagila/10-catalogue.yaml
$ kubectl get films                                   # the APIService registers itself
```

That last command works as cluster-admin. Authorization is the cluster's, so everyone else needs a
ClusterRole naming the group first — `kubectl crisp rbac | kubectl apply -f -` writes it.

`--version` pins a release; without it Helm takes the newest published. The chart is signed with
cosign keylessly, the same as the image, and both of its version numbers are stamped at release
time — so the image a default install deploys is always the one that release built.

The copy in this repository is the development one and is not a release: it is versioned
`0.0.0-dev`, and its `appVersion` is `latest`, so `helm install ./charts/kube-crisp` from a checkout
deploys the newest released image rather than a number frozen at whenever the file was last edited.
Use it for a change to the chart that is not released yet, with `--set image.tag=` to pin a version
or name an image you built.

`helm show values oci://ghcr.io/mrueg/charts/kube-crisp` lists what can be turned on: admission,
fair queueing, a `ServiceMonitor`, an egress `NetworkPolicy`, a real CA bundle.

Or with plain manifests:

```console
$ kubectl apply -f manifests/
$ kubectl apply -f examples/pagila/00-secret.yaml     # edit the DSN first
$ kubectl apply -f examples/pagila/10-catalogue.yaml
$ kubectl get films                                   # the APIService registers itself
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

### Installing the kubectl plugin

`kubectl crisp` writes the RBAC a projected group needs to be reachable, shows who may reach it,
finds the roles a deleted projection left behind, says what a projection needs from its database, and
says why one is not answering.
kubectl finds it by name: any executable called `kubectl-crisp` on `PATH` becomes `kubectl crisp`.

Releases carry one archive per platform holding the plugin alone, for Linux, macOS and Windows — it
links no database driver, so unlike the server there is no reason to build your own:

```console
$ VERSION=0.2.0; OS=linux; ARCH=amd64
$ curl -sSLO "https://github.com/mrueg/kube-crisp/releases/download/v$VERSION/kubectl-crisp_${VERSION}_${OS}_${ARCH}.tar.gz"
$ tar xzf "kubectl-crisp_${VERSION}_${OS}_${ARCH}.tar.gz" kubectl-crisp
$ sudo install kubectl-crisp /usr/local/bin/
$ sudo ln -s kubectl-crisp /usr/local/bin/kubectl_complete-crisp   # optional; see below
$ kubectl crisp --help
```

The link is what makes `kubectl crisp <TAB>` complete. kubectl asks a plugin for completions by
looking up an executable named `kubectl_complete-<plugin>` on `PATH` and offers nothing without one,
however much the plugin itself knows — so the plugin answers to that name as well, and the link is
the whole of it. There is no second program to install or to keep in step. Skipping it costs the
completion and nothing else.

What completes: the subcommands, the projection names `rbac`, `can-i` and `schema` take — read from
the cluster and described by the resource each one serves — the output formats each command accepts,
`--context` from the kubeconfig, and `-n` from the cluster's namespaces. A completion that cannot
reach the cluster says nothing and offers no filenames, since a filename is never the right guess in
a position that wants a projection.

On Windows the lookup goes through `PATHEXT`, so the copy has to be named `kubectl_complete-crisp.exe`
to be found.

Each release also carries a `checksums.txt`, signed with cosign keylessly, which is what to check the
download against.

Or with Go, which reports its version as `dev` — the real one is stamped at release time:

```console
$ go install github.com/mrueg/kube-crisp/cmd/kubectl-crisp@latest
$ ln -s kubectl-crisp "$(go env GOPATH)/bin/kubectl_complete-crisp"   # completion, as above
```

From a checkout, `make build` puts it in `bin/` beside the server, with the completion link made.

It is not on [krew](https://krew.sigs.k8s.io) yet.

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
$ make e2e-correctness              # 92 tests, a few minutes
$ make e2e-bench SHARD=reads        # one benchmark shard
$ make bench                        # every benchmark
$ make e2e                          # all of it
```

A projection can be checked without any of that:

```console
$ kube-crisp-apiserver validate examples/pagila/
ok  examples/pagila/10-catalogue.yaml: pagila-films (films.pagila.example.com/v1alpha1)
...
ok  examples/pagila/50-reporting.yaml: pagila-store-sales (storesales.pagila.example.com/v1alpha1)

10 projection(s) validated
```

It takes files and directories, needs no cluster and no database, exits non-zero if anything is
rejected, and reports every projection rather than stopping at the first — so it works as a commit
gate. What it cannot check is whether the database can run the statements; that needs the database,
and the server checks it when the projection is compiled.

The other half of getting a projection in front of somebody is RBAC, which is the kubectl plugin's:

```console
$ kubectl crisp rbac -f examples/pagila/ | kubectl apply -f -
```

Ten kinds become two ClusterRoles, each granting exactly the verbs its projection can serve — a
projection with no `create` query refuses `create` whatever a role says. `kubectl crisp rbac` with
no arguments reads the projections in the cluster instead. `kubectl crisp can-i` then shows who may
do what, including the case neither gate can see alone: a verb RBAC grants and the projection cannot
serve, which is authorized and returns 405. `kubectl crisp prune` finds the roles a deleted
projection left behind — `--apiservices` finds the registrations one left behind instead — and
`kubectl crisp schema` says what a projection needs from its database,
for handing to whatever manages the tables. `kubectl crisp status` answers the other question — why a
projection is not answering — by joining its conditions to the `APIService` behind its group, which is
the half of the answer that lives somewhere else. It is a separate binary because it needs
neither a database nor a driver, unlike `validate`, whose answer depends on which drivers the build
linked in.

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
| `cmd/kubectl-crisp` | The kubectl plugin: generates the RBAC a projected group needs to be reachable |
| `pkg/metrics` | Prometheus metrics |
| `pkg/controller/projection` | Also owns APIService registration for served groups |
| `charts/kube-crisp/` | Helm chart, with the optional pieces behind values |
| `manifests/` | CRD, RBAC, Deployment, Service — everything `kubectl apply -f manifests/` should install |
| `manifests/optional/` | Monitoring, network policy, and the RBAC for features that are off by default: each needs a decision or a cluster's own details |
| `examples/orders/` | One writable table projected end to end: its schema, its Secret, and the projection |
| `examples/pagila/` | The ten projections this README shows and the [tutorial](docs/tutorial-pagila.md) walks through: a whole schema nobody designed for this |
| `examples/apiservice.yaml` | A hand-written `APIService`, for the rare case of registering groups yourself |
| `test/e2e` | Cluster suite: three drivers, watch, admission, a database outage, and the benchmarks |
| `docs/` | Tutorials per driver, plus the reference, operating and performance documents |
| `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md` | How to contribute, the [CNCF Community Code of Conduct](CODE_OF_CONDUCT.md) this project follows, and how to report a vulnerability |

## Limitations

- **Projected objects are not served as protobuf.** They are unstructured, and unstructured cannot be
  encoded to protobuf — the same reason custom resources cannot be. Clients negotiate JSON, YAML or
  CBOR instead, which is what every Kubernetes client already does for a custom resource.
- **Adding a driver is a registration, within limits.** `spec.dataSource.driver` names a registered
  driver and the registry is open, which covers a database whose differences are the ones the
  `Driver` struct already states — which of session variables, statement timeouts and notifications
  it claims, and what its connection string needs before it is opened. CockroachDB ships as its own
  driver for exactly that reason, being PostgreSQL without `LISTEN`/`NOTIFY`. Five things are more
  than a registration, and each of them is quiet in its own way. Placeholders are a choice of two,
  `$N` and `?`, and the rewriter emits nothing else, so SQL Server's `@pN` is a third style to add
  rather than a field to set. Detecting whether a statement answers with the rows it wrote looks for
  the word `RETURNING`, so SQL Server's `OUTPUT` is read as a write with nothing to return, and the
  client is told its object was not found for a write that in fact landed. How the text of a
  statement is read — what closes a string literal, what opens a comment — is likewise keyed on the
  driver's name and not on `Placeholders`, since MySQL and SQLite share `?` and agree on almost none
  of it; a driver that is not in that set is read with every dialect's rule at once, which never
  reads a `RETURNING` out of a comment but can miss one a literal it does not recognise ran past, so
  a write that does answer with its row is run for its effect instead. Session variables are set
  by a statement picked from a closed set keyed on the driver's name, so a driver registered with
  `SessionVariables: true` that is not in that set loads cleanly and then fails on the first request
  that binds one. And the CRD pins driver names in more than the enum: CEL rules allow `watch.notify`
  only for `postgres` and `statementTimeout` only for `postgres` and `cockroach`, so a driver
  registered with `Notifications: true` and added to the enum — everything
  [Adding a driver](#adding-a-driver) asks for — is still refused by the API server the moment a
  projection configures the capability that driver declared. Tests compare the enum and both rules against what the registry claims, so a gap
  between them is a CI failure rather than something found in a cluster.
- **The table has to exist.** kube-crisp projects rows; it does not create or migrate tables. A
  projection whose table is missing reports `CompilationFailed` with the database's own message and
  keeps retrying, so it starts serving once the table appears — and `status.requiredSchema` says what
  the table would have to contain, for handing to whatever manages the schema.
- **`/status` and `/scale` are the only subresources**, which is parity rather than a shortfall:
  those two are the only ones Kubernetes defines for a custom resource, so there is no third a
  projection could be missing. Both are served — `/status` owned separately from the rest of the
  object, so a controller writing status cannot walk over a spec, and `/scale` so `kubectl scale`
  and the horizontal pod autoscaler work against a table.
- **Watch history is bounded** by `watch.historySize` and lives in memory, so it is lost on restart
  and not shared between replicas — but a projection that maps a `resourceVersion` and has a
  `deletedQuery` is resumed from the database instead of relisting, which survives both for as long
  as the gap is a short one: past the greater of the collection size and 100 changed rows the replay
  would cost more than the relist it is avoiding and is refused, as it is if either query errors.
  Without tombstones a replay could not report removals, so those clients still relist.
- **Multiple replicas need a mapped `resourceVersion`.** The version a list reports is derived from
  the data, so every replica agrees — but only when the projection maps a version column. Without
  one, each replica falls back to its own counter and must run alone. With leader election on — the
  operator saying there are peers — such a projection is reported in the log and by
  `kube_crisp_projections_unversioned`, rather than failing silently and looking like a client bug.
- **`cacheTTL` is invalidated per replica, and a watch is what shortens that.** A write drops the
  entries it could have invalidated in the replica that served it and in no other, the cache being
  in process. With more than one replica — the chart deploys two — a read can be answered from an
  entry older than a write the same client just made. How much older depends on whether anything is
  watching the projection. A watched projection polls on every replica, follower included, and a
  poll that comes back with changed rows drops the entries for the namespaces it saw move — so the
  window is one poll interval rather than the TTL, without any replica having to tell another
  anything. A projection nobody watches does not poll at all, and there the window is still the full
  TTL. Writes are unaffected either way, since the row a write is based on is always read from the
  database, so a client acting on a stale read is refused with a conflict rather than overwriting. A
  projection whose clients read back what they wrote and is not watched wants one replica, or no
  `cacheTTL`. With leader election on — the operator saying there are peers — such a projection is
  reported in the log and by `kube_crisp_projections_cache_unshared`, rather than being a
  documented limitation nothing checks.
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
- **`--projection-dir` reads a tree.** Every `.yaml` and `.yml` file under the directory is loaded,
  subdirectories included, so pointing it at a directory of folders works — which it did not, and a
  directory holding only folders used to load nothing and say nothing, as this repository's own
  `examples/` did once it grew subfolders. Directories whose name starts with a dot are skipped,
  which is what makes the flag safe to point at a mounted ConfigMap: the mount keeps its real files
  in a timestamped `..`-prefixed directory beside the symlinks that name them, and reading both
  would load every projection twice.
- **`--projection-dir` is re-read while running.** A file changing is picked up the way a projection
  changing in the cluster is: the directory and everything under it is watched, and re-read on every
  sync. A file that does
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
and the mapping layer are implemented and covered by 594 unit tests plus a 105-test e2e suite that
runs against PostgreSQL, MySQL and SQLite in a kind cluster — including a database outage, row-level
security, finalizer flows, server-side apply conflicts, a dropped `LISTEN`/`NOTIFY` subscription,
generated RBAC that is applied and then used to make the requests it authorizes, and a run against a
server built with the race detector. The correctness half of that suite takes a few
minutes; the rest is benchmarks.

Built against Kubernetes libraries v0.37.0 and Go 1.26.
