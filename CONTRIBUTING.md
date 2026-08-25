# Contributing

This project follows the [CNCF Community Code of Conduct](CODE_OF_CONDUCT.md).
Report an incident to the maintainer at <manuel@rueg.eu>.

Thanks for looking. This is a young project and interfaces still move, so the
most useful contributions are the ones that tell us something we did not know:
a projection shape that does not work, a database that behaves differently from
the three that are tested, a number that disagrees with the ones in the README.

## Before a pull request

```console
$ make verify     # fmt, vet, and the unit tests
$ make lint       # golangci-lint, with the config in .golangci.yml
$ make cover      # the race detector and a coverage summary
```

CI runs those plus `make codegen-check`, `make tidy-check`, `make helm-lint`,
`make alerts-check`, `make vulncheck`, and the e2e suite. Running `make verify`
and `make lint` locally catches most of what CI would.

`-race` is not decoration here. The watch cache is refreshed by a poll loop
while watchers read from it, the served API surface is swapped under live
requests, and the connection pool is shared by every projection reaching the
same database. A change to any of those wants `make cover` rather than
`make test`.

## Generated code

The CRD, the deepcopy functions, the typed client, and the chart's copy of the
CRD all come from the API types in `pkg/apis/crisp/v1alpha1`:

```console
$ make codegen
```

Edit the types and re-run; do not edit the output. `make codegen-check` fails if
the committed output is not what the types produce, which is the check that
stops a field being added to the types and silently pruned by the API server.

The alert rules work the same way: `charts/kube-crisp/files/alerts.yaml` is the
source, and `manifests/optional/prometheusrule.yaml` is generated from it.

## Tests

The e2e suite is split so it does not have to be run whole:

```console
$ make e2e-up                       # kind, PostgreSQL, MySQL, SQLite
$ make e2e-correctness              # 62 tests, a few minutes
$ make e2e-bench SHARD=reads        # one benchmark shard
$ make e2e-down
```

Before any of that, a projection file can be checked on its own. It needs no
cluster and no database, and exits non-zero if anything is rejected:

```console
$ go run ./cmd/kube-crisp-apiserver validate test/e2e/manifests/
```

The correctness half is what says whether the code works. The benchmarks take
twenty minutes and are what says whether it is worth using.

Two things are worth knowing before adding a test:

**Fixtures have to go back exactly as they were.** Several tests assert exact
object counts, and the suite runs against a cluster that is reused between runs.
A test that restores a column but not the mapped `resourceVersion` passes once
and fails on every run after — that is a bug we have actually shipped.

**A test that passes is not necessarily measuring anything.** The watch cache
polls on its first watcher, so seeding `cache.items` directly and then opening a
watch empties it before the snapshot is taken; two tests written that way passed
while asserting nothing. If a test cannot fail, it is not a test.

New benchmarks have to be added to a shard in the `Makefile`, or CI will not run
them. `make e2e-bench-check` fails if one is orphaned.

## What good looks like here

**Comments say why, not what.** The code is readable; the reasoning behind a
decision usually is not. A comment explaining why a lock is *not* held across a
database round trip is worth more than one restating the line below it.

**Measurements over assertions.** The README's performance numbers come from
`make bench`, and the claims in it are ones the suite checks. If you improve
something, a before-and-after benchmark is the argument; if you cannot measure
it, say so.

**Failure modes are the interesting part.** A projection is a database behind an
API, so most of what can go wrong is partial: a replica that has gone away, a
row that cannot be mapped, a watch that stopped advancing. Behaviour that
degrades visibly beats behaviour that degrades silently, every time.

## Reporting something

For a bug, the most useful report is the projection that triggers it, with
credentials removed, and the driver and database version. For a security issue,
please use [SECURITY.md](SECURITY.md) rather than the issue tracker.

## Licence

Contributions are under the Apache 2.0 licence in [LICENSE](LICENSE).
