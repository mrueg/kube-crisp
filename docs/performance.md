# Performance

`make bench` compares the same patterns against 10,000 objects held twice: as native CRs in etcd,
and as PostgreSQL rows behind a projection.

**What to expect before reading any numbers.** A projected read is a database round trip; a native
one is answered from the kube-apiserver's own cache. Single-object reads should therefore be slower,
and a benchmark showing otherwise is measuring a cache somewhere. Collections should go the other
way, because a database is good at returning many rows and there is no per-object trip through etcd.
Writes should be close, since both end up doing one round trip to something durable.

**The traffic is measured as an ordinary ServiceAccount**, not as cluster admin. kube-crisp is an
aggregated API server, so every request it serves is authorized twice — once by the kube-apiserver
before the request is proxied, and again by kube-crisp, which delegates by posting a
`SubjectAccessReview` back to the cluster. A native CRD is authorized once, in process. That second
check is the one `system:masters` never reaches, so measuring as admin leaves out a cost only the
projection pays, which does not merely flatter both columns — it biases the comparison towards the
side being advertised. `BENCH_AS_ADMIN=1` measures the other way for comparison.

Everything below is one run of `make e2e` on a single-node kind cluster where PostgreSQL, MySQL,
SQLite, etcd and both API servers compete for the same cores. Treat the ratios as indicative and the
absolute numbers as worthless off that machine.

## Reads

Three runs of each measurement after a discarded warm-up pass; the median run, with the spread of
each run's p50 beside it:

| Scenario | CRD (etcd) | Projection | p50 range (projection) | |
|---|---|---|---|---|
| GET single object | 1.6ms | 1.8ms | 1.8 – 3.0ms | 1.16x slower |
| LIST first 500 | 152.6ms | 86.3ms | 86.3 – 87.2ms | **1.77x faster** |
| LIST all 10,000 | 3.08s | 1.73s | 1.72 – 1.74s | **1.79x faster** |
| WALK 10,000 in pages of 500 | 3.05s | 1.76s | | **1.73x faster** |

Collections go to the projection, single reads to the CRD — as they should. The GET gap is small
here because a projected get is one indexed round trip and the machine is not fast; under
concurrency it widens to 2.4x fewer requests per second, which the throughput table shows.

Per-object cost stays flat as the collection grows, so this is linear rather than paying something
per collection:

| Objects | CRD (etcd) | Projection | |
|---|---|---|---|
| 100 | 315µs | 191µs | 1.65x faster |
| 1,000 | 309µs | 173µs | 1.78x faster |
| 10,000 | 309µs | 177µs | 1.74x faster |

## Writes

| Scenario | CRD (etcd) | Projection | |
|---|---|---|---|
| CREATE | 3.3ms | 3.3ms | even |
| UPDATE | 5.3ms | 7.6ms | 1.43x slower |
| DELETE | 3.1ms | 4.1ms | 1.32x slower |

Both do one round trip to something durable, and it shows. `RETURNING` is why create holds up at all:
the write answers from its own result instead of costing a second trip to read the row back. Update
and delete are behind because a projected write reads the row first — to apply the update onto it,
and to have something to put in the deletion event — where etcd has it in cache already.

## Watch

| Scenario | CRD (etcd) | Projection | |
|---|---|---|---|
| WATCH propagation | 3.8ms | 1.07s | **283x slower** |

**This is the number to plan around.** etcd pushes a change to a watcher in milliseconds; a
projection polls, so a change surfaces within one `watch.pollInterval` — 1s here, and the
measurement lands almost exactly there. A controller reconciling projected objects reacts on that
timescale.

`watch.notify` is the answer to it: with a trigger sending `pg_notify`, a change wakes the poll
instead of the poll waiting to find it, which brings this down to a round trip. Measured directly
against PostgreSQL at **2ms**, and end to end through a live watch:

| Scenario | Poll interval | Change seen after | |
|---|---|---|---|
| WATCH propagation, `watch.notify` | 60s | 163 – 192ms | **~370x inside the interval** |

Those milliseconds are an upper bound rather than the notification's own latency: the change is made
with `kubectl exec ... psql`, and the round trip to issue it is most of what is being timed. What
the number is good for is ruling out the timer — a projection polling every 60 seconds cannot see a
change 170ms after it happens.

The suite asserts it against a control, a second projection over an identical table with the same
60s interval and no subscription, which must still be waiting when the subscribed one has fired. It
also drops the subscription's PostgreSQL backend mid-test and checks the watch is woken again
afterwards, on a new connection, since a subscription that stops reconnecting fails silently: reads
keep working and the watches simply return to the interval.

## Under concurrency

Sixteen clients saturating both backends for a fixed window, which asks how many clients are
answered before requests start waiting on each other:

| Scenario | | req/s | p50 | p95 | p99 |
|---|---|---|---|---|---|
| GET single object | CRD (etcd) | 1524 | 9.5ms | 16.8ms | 31.4ms |
| GET single object | projection | 644 | 17.1ms | 59.9ms | 76.5ms |
| GET single object | projection with `cacheTTL` | 1507 | 8.3ms | 22.7ms | 58.2ms |
| LIST first 100 | CRD (etcd) | 61 | 254.5ms | 378.0ms | 429.0ms |
| LIST first 100 | projection | 150 | 96.5ms | 183.9ms | 229.5ms |

Single reads are where concurrency hurts: the CRD is answered from a cache that costs nothing per
client, while every projected GET holds a database connection for its duration. The third row is the
answer — a projection with `cacheTTL` matches the CRD exactly, because a cache hit costs neither a
connection nor a round trip. Whether you can have it is a question about your data, and
`kube_crisp_cache_reads_total` is how you judge whether the trade paid off.

## Selectors

A selector the database answers against the same selector filtered after mapping. Both return the
right objects, so the difference is invisible except as latency:

| Scenario | | p50 | |
|---|---|---|---|
| one object by name | LIST everything, filter after | 1.74s | |
| one object by name | field selector, pushed down | 5.2ms | **334x faster** |
| one object by name | GET | 3.0ms | the floor |
| a third of the collection by label | LIST everything, filter after | 1.73s | |
| a third of the collection by label | label selector, pushed down | 573.4ms | 3.02x faster |

A pushed-down name selector lands near the GET, because both are one statement written for the rows
asked for. A projection whose list statement ignores the parameter still returns the right object —
after reading all ten thousand.

## Paging

| Scenario | CRD (etcd) | Projection |
|---|---|---|
| page 1 of 20, 500 per page | 155.1ms | 91.3ms |
| page 20 of 20, 500 per page | 156.1ms | 89.1ms |

A last page that costs what the first did is keyset paging working: the query resumes after a value
rather than skipping rows to reach it. Offset paging would climb.

## Drivers

Same query shape, each over its own 10,000-row collection:

| Scenario | PostgreSQL | MySQL | SQLite |
|---|---|---|---|
| GET single object | 3.0ms | 3.9ms | 3.1ms |
| LIST first 500 | 88.7ms | 90.8ms | 76.6ms |
| LIST all 10,000 | 1.74s | 1.53s | 1.54s |

## What authorization costs

The same read as the same subject, and as the admin the delegating authorizer never reaches:

| Scenario | CRD (etcd) | Projection | |
|---|---|---|---|
| GET as cluster admin | 1.7ms | 3.1ms | 1.79x slower |
| GET as an RBAC subject | 1.6ms | 3.0ms | 1.88x slower |

Within run-to-run spread of each other. On a single node the `SubjectAccessReview` is a same-host
hop of a couple of milliseconds. Where it would show is a deployment in which kube-crisp does not sit
beside the apiserver it delegates to — the authorizer caches on the full attribute record, object
name included, so a get of a distinct object misses almost every time and the cost is per distinct
object touched rather than per request.

## What the rest of the cluster feels

Every number above is a latency. What it is supposed to buy is that the cluster's own apiserver and
etcd stop carrying the objects — so the question that matters is what everything else feels while a
backend is being written to.

Measured by writing and deleting one unrelated ConfigMap every 200ms throughout a fixed 20-second
window, while eight clients write into each backend as fast as they can. A write rather than a read,
because a read of an unchanging object comes from the watch cache and would measure almost nothing.
All three rows are 100 samples over equal windows; both loaded rows wrote about 11,000 objects.

| unrelated write, while | p50 | p95 | p99 |
|---|---|---|---|
| nothing else is happening | 7.8ms | 13.7ms | 14.4ms |
| 11,005 native objects are created | 3.5ms | 7.3ms | 10.2ms |
| 11,005 projected objects are created | 8.5ms | 13.3ms | **24.0ms** |

**Read the tail, not the median, and do not read too much into either.** Both loaded rows can beat
the idle one on the median without anything being wrong: etcd batches concurrent writes into one WAL
fsync, so a write arriving while eight others are in flight rides a commit it would otherwise pay
for alone.

This run says the opposite of the previous one, which is the honest headline. Writing through the
projection made the unrelated write's tail worse — 24ms against 10ms — where it had previously been
better. On a single node the database, both API servers and etcd share the same cores, so writing
11,000 rows through PostgreSQL takes CPU away from the apiserver being probed; that cost is an
artefact of the topology, and it is exactly the cost that disappears when the database is not on the
control-plane node.

So this table does not currently show what it was built to show. Treat it as a measurement that
needs a two-machine setup to mean anything, and treat the storage accounting below — which is not a
timing at all — as the evidence that offloading is real.

## What never reaches etcd

```
objects the cluster's own apiserver reports storing:
  benchorders.bench.example.com   20380
  orders.store.example.com        no line at all

the same projection answers for 10000 objects through the API
```

Not a smaller number — no line, because that apiserver is not storing them. They are not in its
etcd, not in its watch cache, and not in its resource count.

## Caveats

- Single-node kind: PostgreSQL, MySQL, SQLite, etcd and both API servers share the same CPU, so
  every backend is competing with the one it is being compared against.
- Three runs is better than one, but the spread above is a range rather than a confidence interval,
  and every run shares one machine. Where the ranges of two backends overlap, the ratio is noise —
  `LIST all 10,000` is the clearest case.
- `resultFormat: JSONArray` measured even with row scanning here (3.13s against 3.17s) with ranges
  that overlap almost entirely, so this run says nothing about it either way. It moves work from the
  server to the database, and whether that pays depends on the rows.
- 10,000 objects is not much. The regime that matters for offloading is one where a cluster's etcd
  is under real pressure, which this does not reproduce.
- The unrelated-traffic measurement is the right measurement in the wrong place, and the place is
  the whole point.

Five things keep the read path competitive, all on by default: **prepared statements** cached per
connection, **connection pooling with a keep-alive** so a request never pays connection setup,
**coalescing** so identical concurrent reads cost one query between them, **cached collections handed
out as views** rather than copies, and the same trick on the way out, where restating a collection's
kind shares its objects rather than copying them.
