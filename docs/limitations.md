# Limitations

What kube-crisp does not do, or does with a condition attached, and why.

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
  only for `postgres`, `statementTimeout` only for `postgres` and `cockroach`, and `dataSource.auth`
  only for those two and `mysql`, so a driver
  registered with `Notifications: true` and added to the enum — everything
  [Adding a driver](extending.md#adding-a-driver) asks for — is still refused by the API server the moment a
  projection configures the capability that driver declared. Tests compare the enum and all three rules against what the registry claims, so a gap
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
  The counter also does not survive a restart: it starts from the wall clock in microseconds, so a
  version handed out by an earlier process is below anything the next one hands out and a watch
  resuming from it is refused with `410` and relists, rather than being admitted at a point that
  describes a different state of the table. A wall clock stepped backwards across the restart is the
  one case that check cannot see.
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
- **Admission and fair queueing are opt-in** and each needs the extra RBAC in
  [`manifests/optional/`](installation.md#the-server). Without admission,
  cluster policy does not apply to projected writes; without fair queueing, the only backpressure is
  a projection's own concurrency limit, which sheds indiscriminately.
- **Owner references are validated, not resolved.** The shape is checked, but whether the owner
  exists is the garbage collector's question rather than this server's.
- **Server-side apply tracks ownership only when asked to.** Without `mapping.managedFields` there
  is nowhere to keep it, so applies merge but never conflict.
- **`--projection-dir` reads a tree.** Every `.yaml` and `.yml` file under the directory is loaded,
  subdirectories included, so pointing it at a directory of folders works — which it did not, and a
  directory holding only folders used to load nothing and say nothing, as this repository's own
  `examples/` did once it grew subfolders. Files and directories whose name starts with a dot are
  skipped, which is what makes the flag safe to point at a mounted ConfigMap: the mount keeps its
  real files in a timestamped `..`-prefixed directory beside the symlinks that name them, and
  reading both would load every projection twice. It also keeps an editor's lock file, such as
  emacs's dangling `.#orders.yaml`, from failing the directory while a file is being edited.
  Symlinked directories are followed, the directory named by the flag included, so a
  `current -> releases/42` link and a ConfigMap whose items name nested paths are both read and
  watched; each directory is read once by where it really is, so a link back up the tree is not a
  loop. A directory named by the flag that is a link is watched from its parent too, so swapping
  the link to another target is picked up as promptly as an edit.
- **`--projection-dir` is re-read while running.** A file changing is picked up the way a projection
  changing in the cluster is: the directory and everything under it is watched, and re-read on every
  sync. A file that does
  not parse keeps the last good set rather than taking every file-backed projection out of service.
  A file that parses but will not validate fails that one projection, by name, and the files beside
  it are served — at startup too, where it used to stop the server starting. A file that does not
  parse at startup still does: there is no last good set yet to keep, and starting with none would
  serve no file-backed projection without saying why. Without a cluster there is nothing to fail a
  projection by name against, so there every projection has to compile for the server to start.
  Backed by a ConfigMap, the wait is the kubelet's rather than this server's — around a minute in the
  e2e cluster, with no restart.
- **A file-backed projection's name is reserved.** A `CustomResourceProjection` in the cluster with
  the same `metadata.name` as a file under `--projection-dir` is not served: the file keeps serving,
  and the object reports `Ready` false with reason `NameReservedByFile`, whichever of the two came
  first. Without that, creating an object of the file's name replaced the file, and its consumers
  were served whatever the object pointed at.
- **A watched projection holds its whole collection in memory** and needs `maxRows` set above the
  row count, since the periodic full resync reads all of it. A projection that maps a
  `resourceVersion` and has a `deletedQuery` whose tombstones carry every mapped column keeps only
  keys and versions instead — the diff needs the version, and the tombstone describes what was
  deleted — and reads a new watcher's initial state rather than remembering it. Measured at 1.83x
  less held per row — the identity, the version, the kind and the labels are kept, because a watch
  event has to carry a kind and a label selector filters deletions on labels. That saves memory
  only: the first poll and a watcher asking for the collection still read the whole table, so
  `maxRows` has to exceed the row count all the same. The cache switches once it has seen a
  tombstone map in full; with identity-only tombstones it keeps whole objects.
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
