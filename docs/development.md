# Development

Building, testing and checking kube-crisp from a checkout. [CONTRIBUTING](../CONTRIBUTING.md) says what
a pull request needs; this is the longer tour.

## Testing

The e2e suite is split so it does not have to be run whole. `make e2e-up` provisions the cluster and
the three databases; after that:

```console
$ make e2e-correctness              # a few minutes
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
projection left behind, counting a group as served when a projection declares it or an
`APIService` kube-crisp wrote for it exists, which is how one loaded from `--projection-dir` shows
up — `--apiservices` finds the registrations one left behind instead, and goes first, since a
registration holds its group's roles up until it is removed — and
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

## Repository layout

| Path | Contents |
|---|---|
| `pkg/apis/crisp/v1alpha1` | `CustomResourceProjection` API types |
| `pkg/apiserver` | Aggregated server, scheme, and the dynamic router that swaps the served API surface |
| `pkg/controller/projection` | Watches projections, installs or removes API groups at runtime, and owns APIService registration for served groups |
| `pkg/registry/projection` | REST storage: reads, writes, and the polling watch cache |
| `pkg/projection` | Row-to-object mapping, projection loading, validation, DSN resolution |
| `pkg/sql` | Pooling, prepared statements, `:named` parameter binding, JSON aggregation |
| `pkg/generated` | Typed clientset, listers, and informers, produced by `hack/update-codegen.sh` |
| `pkg/webhook` | Admission endpoint that checks a projection against its database before the cluster accepts it |
| `cmd/kubectl-crisp` | The kubectl plugin: generates the RBAC a projected group needs to be reachable |
| `pkg/metrics` | Prometheus metrics |
| `charts/kube-crisp/` | Helm chart, with the optional pieces behind values |
| `manifests/` | CRD, RBAC, Deployment, Service — everything `kubectl apply -f manifests/` should install |
| `manifests/optional/` | Monitoring, network policy, and the RBAC for features that are off by default: each needs a decision or a cluster's own details |
| `examples/orders/` | One writable table projected end to end: its schema, its Secret, and the projection |
| `examples/pagila/` | The ten projections the [README](../README.md) shows and the [tutorial](tutorial-pagila.md) walks through: a whole schema nobody designed for this |
| `examples/apiservice.yaml` | A hand-written `APIService`, for the rare case of registering groups yourself |
| `test/e2e` | Cluster suite: three drivers, watch, admission, a database outage, and the benchmarks |
| `docs/` | Tutorials per driver, plus the reference, operating and performance documents |
| `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md` | How to contribute, the [CNCF Community Code of Conduct](../CODE_OF_CONDUCT.md) this project follows, and how to report a vulnerability |
