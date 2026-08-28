# Operating

Running kube-crisp in a cluster: what it watches, what it needs permission for,
what it does when a database goes away, and what it reports while doing it.

## Admission

Cluster policy does not reach a projection unless you let it in. `--enable-admission` runs the
admission chain for projected writes, so a `ValidatingAdmissionPolicy` matching
`store.example.com/orders` is enforced here exactly as it would be for a CustomResourceDefinition,
admission webhooks are consulted, and `NamespaceLifecycle` stops a projection writing rows for a
tenant the cluster does not have.

It is off by default because the plugins watch webhook configurations, admission policies, and
namespaces cluster-wide. Those are configuration rather than credentials, but it is a grant worth
making deliberately, so it lives in its own manifest:

```console
$ kubectl apply -f manifests/optional/admission-rbac.yaml
```

## Leader election

Polling is the one thing a projection does with no request behind it, which makes it the only load
several replicas multiply for nothing. `--enable-leader-election` elects one to poll at the
projection's own interval:

```console
$ kubectl apply -f manifests/20-rbac.yaml   # includes the Lease permissions
$ kube-crisp-apiserver --enable-leader-election
```

The replicas that do not hold the lease keep polling, at `watch.followerPollInterval` instead. That
is the part worth understanding: a watch is served from the cache of whichever replica the client
reached, so a follower that stopped polling would leave its watchers seeing nothing at all — not an
error, just silence. Slowing them down instead means their watchers lag by that interval, and the
total load falls from N fast polls to one fast and N-1 slow.

Losing the lease is not fatal. Nothing about serving depends on it, so a replica that cannot renew
slows down and carries on rather than taking a working API down with it.

The Lease is `kube-crisp-poller` in `$POD_NAMESPACE`. Two kube-crisp deployments in one namespace —
a second one serving a different set of projections, say — would otherwise contend for it, and
whichever lost would run every replica as a follower. `--leader-election-name` and
`--leader-election-namespace` are what keep them apart:

```console
$ kube-crisp-apiserver --enable-leader-election --leader-election-name=kube-crisp-reporting-poller
```

`kube_crisp_poll_leader` is per Lease, so once the names differ each deployment has a leader of its
own and `KubeCrispNoPollingLeader` stops firing for the one that lost.

## Checking projections at admission

A projection whose SQL has outlived its schema — a renamed column, a dropped table, a migration that
landed — is refused when it is applied, rather than accepted and then reported:

```console
$ kubectl apply -f orders-projection.yaml
The request is invalid: admission webhook "projections.crisp.kubecrisp.io" denied the request:
queries.list: the database cannot run this statement: ERROR: column "customer_name" does not exist
(SQLSTATE 42703)
```

`--enable-projection-webhook` turns it on. It asks the database to *prepare* every statement, which
parses it and resolves every name against the catalogue without touching a row, so the answer is the
database's own and costs it nothing. The same check runs at compile time regardless; the webhook
only moves it to where the mistake was made.

The server registers the `ValidatingWebhookConfiguration` itself, which is what
`--manage-projection-webhook` (on by default) does, and needs the RBAC in
`manifests/optional/webhook-rbac.yaml`. It has to, because of the certificate: a `ValidatingWebhookConfiguration`
has no `insecureSkipTLSVerify` — the escape hatch the `APIService` path uses for self-signed
certificates does not exist here — so the configuration must carry a CA bundle that verifies this
server, and with a generated certificate nobody knows what that is until the server has started.
`--projection-webhook-ca-bundle-file` supplies one from a real CA instead; without it the server's
own certificate is used, which is correct for a self-signed one and pins the webhook to that
certificate otherwise. `--projection-webhook-name` names the configuration, for a cluster running
more than one kube-crisp.

The configuration is kept reconciled rather than registered once, because a generated certificate
belongs to the pod that generated it. During a rolling update the pod on its way out can write its
certificate after its replacement wrote theirs, leaving the cluster told to trust a certificate
nothing serves — and since the policy is `Ignore`, that is silent: admission is skipped and a
projection the server would have refused is accepted. The reconcile runs every 30 seconds and writes
only when the configuration disagrees with what the server can actually answer.

**More than one replica needs a real certificate.** With a generated one every replica signs its
own, and the configuration can name only one CA — so the replicas take turns correcting it to
theirs, and whichever the Service picks for a given request may be one the cluster does not trust.
Give every replica the same certificate and pass its CA with
`--projection-webhook-ca-bundle-file`, or run the webhook on a single replica.
`kube_crisp_projections` does not show this, because from the server's side nothing failed.

Its failure policy is `Ignore`, deliberately. This server is what serves the webhook, so `Fail`
would mean that while it is down or rolling nobody can create or edit a projection — including the
edit that would fix it. Nothing depends on the webhook having run: the server refuses to serve a
projection it cannot compile either way.

## Fair queueing

`--enable-priority-and-fairness` puts the generic API priority-and-fairness filter in front of
projected requests, so they are queued by `FlowSchema` and one client cannot take a projection's
whole capacity. It matters more here than for most extension servers: a projection's capacity is a
connection pool, and a slow database backs requests up behind it. With fair queueing the requests
that caused the slowdown are the ones that wait.

It is off by default for the same reason admission is — the filter watches `FlowSchemas` and
`PriorityLevelConfigurations` cluster-wide — and it needs a kubeconfig, since it builds informers of
its own:

```console
$ kubectl apply -f manifests/optional/flowcontrol-rbac.yaml
```

Without it the only backpressure is a projection's own `maxConcurrentQueries`, which sheds whatever
arrives once it is full rather than the requests responsible for filling it.

Webhooks add a network round trip to every write, which weighs more here than for an etcd-backed
resource, since the write is already a database round trip.

## Outages

A database that goes away does not take its API with it. The projection stays installed and
answers `503` with a `Retry-After`, `DataSourceConnected` goes false on the projection while
`Ready` stays true, and reads recover on their own when the database returns. Discovery, RBAC, and
anything watching the resource are untouched throughout — a transient outage should not look like
an API that never existed.

A database that has gone away without closing its sockets does not refuse connections, it simply
stops answering; that surfaces as `504` once the query's timeout expires. A statement the database
*rejects* still returns 500, because that is the projection's fault and retrying will not help.

## What a projection reports about itself

`kubectl get customresourceprojections` is the first place to look, and the
object carries more than the printed columns:

```console
$ kubectl get crp orders -o jsonpath='{.status}' | jq
{
  "observedGeneration": 3,
  "servedPaths": ["/apis/store.example.com/v1alpha1/orders"],
  "requiredSchema": {
    "tables": ["orders"],
    "columns": [{"name": "id", "type": "string", "usedFor": "identity"}]
  },
  "conditions": [
    {"type": "Ready", "status": "True", "reason": "Serving"},
    {"type": "Registered", "status": "True", "reason": "Routed"},
    {"type": "DataSourceConnected", "status": "True", "reason": "Connected"},
    {"type": "SchemaResolved", "status": "True", "reason": "SchemaAccepted"}
  ]
}
```

`servedPaths` is what the projection is actually answering on, one entry per
served version — which is how you tell an extra version that is installed from
one that is only declared.

`requiredSchema` is what the projection reads from the database, gathered from
its queries and its mapping. kube-crisp creates nothing, so this is what to hand
to whatever does — see
[the reference](reference.md#what-a-projection-needs-from-the-database). It is
reported whether or not the projection is serving, which matters because a
projection that failed to compile because its table is missing is exactly the
one whose required schema someone wants to read.

The four conditions say different things and are worth reading separately.
`SchemaResolved` covers the shape, including a schema borrowed with
`schemaFrom`. `DataSourceConnected` going false means the database is
unreachable and requests are getting `503`, while the API group stays installed.
`Ready` false with reason `CompilationFailed` means the projection is not being
served at all; `Ready` false with reason `ServingPreviousConfiguration` means the
generation in the cluster did not compile and the previous one is still
answering — requests work, and the spec answering them is not the spec you
applied.

`Registered` is about the aggregation layer rather than about this server.
Compiling a projection and installing its handlers is only half of serving it:
until an `APIService` exists and the aggregator reports it available, a request
for the projected resource never arrives here at all. `Registered` false with
reason `NotRouted` carries the aggregator's own message — a Service with no
endpoints, a CA bundle that no longer matches, a certificate that does not name
the Service — and takes `Ready` with it, because nothing can reach the
projection. `Registered` `Unknown` with reason `Pending` means the registration
exists and nothing has dialled it yet, which is the ordinary state for the first
second of a projection's life and the permanent state in a cluster with no
aggregation layer; `Ready` stands on its own in that case.

Compiling includes asking the database whether it could run each of the
projection's statements. Preparing them resolves every name against the
catalogue without touching a row, so a projection whose SQL has outlived its
schema — a renamed column, a dropped table, a migration that landed — is
`CompilationFailed` with the database's own message, naming the query:

```
queries.list: the database cannot run this statement: ERROR: column
"customer_name" does not exist (SQLSTATE 42703)
```

Before this it compiled, reported `Ready`, appeared in discovery, and failed
every request with a `500`. The author got no signal; the first person to find
out was whoever called it.

An unreachable database is not a failed compilation. The check only runs when the
database answered, so an outage leaves `DataSourceConnected` false and the
resource installed, rather than withdrawing a projection because nobody was there
to ask.

## Health

`/readyz` reports whether the server can take traffic; it goes green once the projected API surface
is installed. Whether *every* projection made it into that surface is a separate question, answered
by `kube_crisp_projections{state="failed"}`, by each projection's status conditions, and by the log.

It is deliberately not a probe. `AddHealthChecks` registers into `/livez` as well, so a projection
with a typo in its query would fail the liveness probe and have the kubelet restart the server —
taking every healthy projection down with it. One broken projection should not do that, so it is
reported rather than gated. Pass `--require-all-projections` to promote it to a readiness gate,
where the server leaves the endpoints and nothing restarts it.


## Metrics

Published on the apiserver's own `/metrics`, alongside the standard apiserver request metrics:

| Metric | What it tells you |
|---|---|
| `kube_crisp_query_duration_seconds` | Database round trip, by projection, resource, verb, and result — see below |
| `kube_crisp_query_rows` | Rows returned — usually what explains a slow list |
| `kube_crisp_projections` | Projections serving versus rejected |
| `kube_crisp_watch_watchers` | Connected watchers; also explains query load with no request behind it |
| `kube_crisp_watch_events_total` | Events emitted, by type |
| `kube_crisp_watch_notifications_total` | Change notifications the database pushed; a counter, so see the gauge below rather than reading a flat line |
| `kube_crisp_watch_listener_connected` | 1 while a notification subscription is connected — the one thing that distinguishes a dead subscription from a quiet table |
| `kube_crisp_watch_listener_reconnects_total` | Subscriptions re-established after dropping; one is a database restart, a climbing number is a subscription that cannot stay up |
| `kube_crisp_watch_polls_total` | Polls by mode, so you can see whether incremental polling is in effect |
| `kube_crisp_projections_unversioned` | Projections that map no resourceVersion while this server has peers — the version two replicas then disagree about |
| `kube_crisp_watch_database_replays_total` | Watchers resumed from the database rather than being asked to relist — what a rolling restart no longer costs |
| `kube_crisp_watch_missed_events_total` | Changes only a full resync found — a resourceVersion that is not monotonic |
| `kube_crisp_watch_poll_errors_total` | Polls that failed or were shed — a watch that has quietly stopped advancing |
| `kube_crisp_query_rows_unmappable_total` | Rows skipped because they could not be turned into objects |
| `kube_crisp_cache_reads_total` | Read cache hits and misses, when `cacheTTL` is set |
| `kube_crisp_cache_entries` | What the cache holds; sitting at its bound means it is evicting entries a request was about to ask for |
| `kube_crisp_cache_evictions_total` | Entries dropped, by reason — `expired` is the cache working, `full` means it is smaller than the key space, `invalidated` means writes outpace the TTL |
| `kube_crisp_query_shed_total` | Requests rejected at a projection's concurrency limit |
| `kube_crisp_query_coalesced_total` | Reads answered by a query another request had already started |
| `kube_crisp_query_routed_total` | Reads by the database that answered them — primary or replica |
| `kube_crisp_query_replica_fallback_total` | Reads the primary answered because the replica was unreachable |
| `kube_crisp_poll_leader` | 1 on the replica holding the polling lease. Exactly one should be, and none for long means every watch is lagging |
| `kube_crisp_projections{state="failed"}` | Projections defined but not served |
| `kube_crisp_projections{state="stale"}` | Projections still serving what they last compiled, having failed to recompile since |
| `kube_crisp_watch_poll_duration_seconds` | Cost of one poll and diff |
| `kube_crisp_projection_state` | The state of each projection **by name**, one series per state with exactly one set. `kube_crisp_projections` counts them and answers whether anything is wrong; this answers which |
| `kube_crisp_admission_reviews_total` | Admission reviews the projection webhook answered, by result. Going flat at zero is how a webhook the cluster cannot call looks from here — its policy is `Ignore`, so nothing else reports it |
| `kube_crisp_admission_duration_seconds` | Time to answer one. The check reaches the database, inside a request the cluster gives ten seconds |
| `kube_crisp_datasource_connections` | Pool state, so exhaustion shows up before latency does |
| `kube_crisp_datasource_connections_closed_total` | Connections discarded rather than reused, by reason — `max_idle` climbing with request volume means the pool is smaller than the concurrency it serves, and every query past the limit is paying to reconnect |
| `kube_crisp_datasource_wait_seconds_total` | Time spent waiting for a connection — with the wait count, the average wait |
| `kube_crisp_datasource_prepared_statements` | Size of the statement cache |

`result` separates `not_found` from `error`, so a projection serving ordinary 404s does not look
unhealthy. `verb` covers the API verbs plus two of the server's own: `watch` for a poll, and
`watchDeleted` for the deletion query that runs beside it.

Metrics are served by the aggregated apiserver itself, so a scrape is an authenticated, authorized
request like any other — a scraper without a grant on the `/metrics` non-resource URL gets a 403 and
shows as a target that is simply down. `manifests/optional/servicemonitor.yaml` carries a
`ServiceMonitor` and that grant; edit the binding to name your Prometheus service account.

### What `result` distinguishes

`success` and `not_found` are both healthy — a projection serving ordinary 404s is working. The
failures are split because they point at different people:

| `result` | What it means | Who fixes it |
|---|---|---|
| `timeout` | Reachable, answering too slowly for the query's own timeout | Whoever owns the schema: a plan that stopped being good |
| `unavailable` | Could not be reached at all | Whoever owns the database, or the network to it |
| `shed` | Refused at `maxConcurrentQueries` before it ran | Raise the limit, or reduce the load |
| `invalid` | The projection's schema or CEL rules rejected the request | The client sending it |
| `conflict` | A write lost to a concurrent one | Nobody; the client retries |
| `error` | The database refused the statement | The projection author: a renamed column, a dropped table |

The last one is the one worth wiring to a pager first. It does not clear on its own, retrying does
not help, and no amount of database capacity changes it — but before this split it was
indistinguishable from an outage, so the only signal was a rate going up with nothing to say which
of the six had happened. `KubeCrispDatabaseUnreachable`, `KubeCrispQueriesTimingOut` and
`KubeCrispQueriesRejected` are the three alerts that separate them.

`manifests/optional/prometheusrule.yaml` carries alerts for the ways a projection goes quietly
wrong, which is most of them: a watch that stopped advancing looks exactly like a database where
nothing is happening, rows skipped for being unmappable leave clients seeing fewer objects than the
table holds, and a replica that has gone away keeps answering — from the primary. The chart renders
the same rules behind `prometheusRule.enabled`. One threshold, the p99 query time, cannot be right by
default; set `prometheusRule.slowQuerySeconds` to what your slowest projection is allowed to take.


## Tracing

`--tracing-config-file` takes the standard
[`TracingConfiguration`](https://kubernetes.io/docs/tasks/administer-cluster/kubelet-tracing/), the
same one the kube-apiserver takes, and exports over OTLP:

```yaml
apiVersion: apiserver.config.k8s.io/v1beta1
kind: TracingConfiguration
endpoint: otel-collector.observability:4317
samplingRatePerMillion: 10000   # 1%
```

The chart takes the same thing as values:

```console
$ helm upgrade --install kube-crisp charts/kube-crisp \
    --set crisp.tracing.enabled=true \
    --set crisp.tracing.endpoint=otel-collector.observability:4317
```

which renders the configuration into a ConfigMap and mounts it. Enabling it without an endpoint is
refused at render time rather than producing a server that starts and exports nowhere.

A traced request carries a span per verb and a span per statement beneath it:

```
"List" resource:orders (total time: 509ms):
  "kube-crisp.list"             kube_crisp.projection:timed-orders  509ms
    "kube-crisp.sql.transaction"  db.system:postgres                509ms
      "kube-crisp.sql.query"      db.statement:SELECT id, ...       508ms
```

Which is the point: the verb span says which projection served the request — the request's own span
knows only the resource — and the statement spans say where the time went. A projection whose kind
spans several tables gets one span per statement, so a slow write names the slow table rather than
the transaction it was part of.

Waiting for a query slot is spanned separately from the query, because the two send an investigation
in opposite directions: one is `maxConcurrentQueries` set too low for the traffic, the other is the
database. A span under its share of the total is left out of the log summary but is still exported.

Bind values are deliberately not recorded, because they are the request's own data and a trace goes
somewhere an audit log does not. The statement text is, since it comes from the projection's spec
and is already on the audit event.

Nothing is sampled or exported without `--tracing-config-file`. The spans are taken from whatever
span the request already carries, so with tracing unconfigured they derive from a no-op and there is
no separate switch to turn them off.

That is not the same as costing nothing. The export is free; the span object is not. Measured at
**13 allocations and about 760 bytes per span**, on a server that has never heard of a collector,
because `tracing.Start` allocates a span and nests into the trace the apiserver already keeps
whether or not OpenTelemetry is configured. A read opens three or four:

| | single-object GET | LIST of 500 |
|---|---|---|
| tracing's share of the allocations | ~20% | ~0.008% |
| tracing's share of the time | ~0.1% | ~0.002% |

Which is the trade being made: a fifth of the allocations of the cheapest possible read, for a
thousandth of its latency. It matters if a projection is serving point reads at a rate where the
garbage collector is the constraint, and not otherwise.
`BenchmarkTracingStartUnderRequestTrace` in `pkg/registry/projection` is what those numbers come
from, so a future version of component-base that changes them will not do it quietly.

The same spans also nest into the trace lines the server already logs at `--v=2`, so a slow request
shows its query breakdown in the log without a collector deployed at all.


## Events

A projection records an Event when its state changes: `Serving`, `CompilationFailed`,
`ServingPreviousConfiguration`, `NotRouted`. Conditions say what the state is now, which is what a
controller reconciling against it needs; an Event says that it changed and when, which is what
`kubectl describe` shows and what anything watching for failures reacts to.

```console
$ kubectl describe crp orders | tail -4
Events:
  Type     Reason              Age   From        Message
  ----     ------              ----  ----        -------
  Warning  CompilationFailed   2m    kube-crisp  queries.list: the database cannot run this statement: ERROR: relation "orders" does not exist (SQLSTATE 42P01)
```

Only on a change. A sync runs whenever anything moves and a projection is usually where it was, so
announcing every sync would bury the one that matters. Recording needs the `events` rule in
`manifests/20-rbac.yaml`; without it the controller reports through conditions and the log alone, and
carries on.

A projection loaded from `--projection-dir` records nothing, because there is no object in the
cluster to attach an Event to.

## Uninstalling

Registered `APIService`s are owned by the projections behind them, so removing the projections
removes the registrations:

```console
$ kubectl delete customresourceprojections --all
$ kubectl delete -f manifests/
```

Either order works — deleting the CRD takes every `CustomResourceProjection` with it, and each one
takes the registration it owned. What matters is that they go: an `APIService` left pointing at a
Service that no longer exists is a cluster-scoped object the aggregation layer goes on dialling and
failing to reach, and it is nobody's obvious job to notice.

A group version served by a projection loaded from `--projection-dir` is deliberately left unowned,
because there is no object whose deletion should collect it. Those registrations are removed by the
running server when it stops serving the group, so shutting the server down without first removing
the files leaves them behind. To find any that outlived their server:

```console
$ kubectl get apiservices -l app.kubernetes.io/managed-by=kube-crisp
```

## Security notes

- Credentials live in Secrets and are referenced, never inlined in a projection.
- Every write is audited. Auditing needs a Policy as well as a destination — `--audit-log-path` on
  its own records nothing — which the chart supplies:

  ```console
  $ helm upgrade --install kube-crisp charts/kube-crisp --set crisp.audit.enabled=true
  ```

  That writes projected writes at `RequestResponse` and reads at `Metadata` to the container log,
  where whatever already collects logs will find them. Replace `crisp.audit.policy` for anything
  else. With auditing enabled, each write carries
  annotations naming the projection, the resource, the verb, the data source, the statement, and
  the rows affected — so an API call can be tied to the SQL it produced. The values bound into the
  statement are deliberately excluded: they are the caller's data, and the statement text is what
  identifies the operation. The same line is available at `-v=4` without auditing configured.
- Authentication and authorization are delegated to the kube-apiserver, so existing RBAC governs
  who can read or write a projected resource — unless there is no cluster to delegate to. With no
  `--kubeconfig` and no service account the server allows every request and warns that it is doing
  so, because delegated authorization without a client can only deny every one of them. That is for
  running against a local database; the port must not be exposed. See
  [SECURITY.md](../SECURITY.md).
- Admission and API priority-and-fairness are off by default: both watch cluster-wide
  configuration, which is a grant worth making deliberately. Each has its own manifest, and
  enabling one without applying it leaves the server unable to start its informers.
- The `secrets` rule in `manifests/20-rbac.yaml` is a namespaced `Role`, scoped to the namespace
  data source Secrets are read from. Narrow it further to named Secrets with `resourceNames` before
  production use.
- Managing APIServices needs cluster-wide write access to `apiregistration.k8s.io`. Set
  `--manage-apiservices=false` and drop that rule if you would rather register groups yourself.
- The `APIService` the server creates sets `insecureSkipTLSVerify: true` because it self-signs by
  default. Supply `--apiservice-ca-bundle-file` and a real serving certificate instead.
- Profiling is off in the shipped Deployment. `/debug/pprof` is served by default and sits behind
  delegated authorization, but it is a default-on surface that nothing here needs, and a profile of
  this server describes the queries it is running. Turn it on for an investigation rather than
  leaving it on.
- Egress is the control worth adding: this server opens connections to arbitrary databases with
  credentials from Secrets, and a `CustomResourceProjection` is cluster-scoped, so whoever can
  create one chooses the destination. `manifests/optional/networkpolicy.yaml` is the starting
  point — it needs the database rule filled in before it is any use.

Reporting a vulnerability, the assumptions kube-crisp makes, and a hardening checklist are in
[SECURITY.md](../SECURITY.md).
