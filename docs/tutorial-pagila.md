# Projecting a whole schema: Pagila

The other tutorials project one table. This one takes a database that already
exists — [Pagila](https://github.com/xzilla/pagila), the PostgreSQL port of the
Sakila DVD-rental sample — and models the whole of it, which is a different
exercise. A schema you did not design does not arrange itself into resources;
deciding what is a resource, and what is merely a column somewhere, is most of
the work.

Everything here assumes kube-crisp is installed from `manifests/`. The
projections are in [`examples/pagila/`](../examples/pagila); the database is
somebody else's, so it is fetched rather than kept here.

![Listing Pagila's films and reading one of them with kubectl](demo/pagila.gif)

A thousand films that only exist as rows in PostgreSQL, listed, selected on by
label and read in full — with no CRD behind them and nothing copying rows into
etcd. The recording is [`docs/demo/pagila.tape`](demo/pagila.tape); `vhs` on it
rebuilds the GIF against a cluster from `hack/e2e-up.sh`.

## The decision everything else follows from

Pagila has two stores. Almost every table that records something happening —
customer, staff, inventory, rental, payment — carries `store_id`, directly or
one join away. Mapping it to `metadata.namespace` is the choice that makes the
rest fall out:

```
namespace store-1   →   store_id = 1
namespace store-2   →   store_id = 2
```

Ordinary Kubernetes RBAC then means "which store you work at". A role bound in
`store-1` cannot read `store-2`'s customers, and nobody had to write an
authorization layer to get that.

It also splits the schema in two. A film is not the property of a store, so the
catalogue — film, actor, category — is **cluster-scoped**. What a store does
with the catalogue is **namespaced**.

The tables that become nothing at all are as important. `address`, `city` and
`country` are three tables to hold one postal address; they are normalised for
storage, and a `City` resource would be an API shaped like a foreign key. They
are joined into `Customer`, `StaffMember` and `Store` as fields. `film_actor`
and `film_category` are join tables, and the Kubernetes-shaped answer is
`spec.actors` and `spec.categories` on the film.

Ten kinds, from fifteen tables and a view:

| Kind | Scope | Reads from |
|---|---|---|
| `Store` | Cluster | `store` + address join |
| `Film` | Cluster | `film` + both join tables |
| `Actor` | Cluster | `actor` |
| `Category` | Cluster | `category` |
| `Customer` | Namespaced | `customer` + address join |
| `StaffMember` | Namespaced | `staff` |
| `Rental` | Namespaced | `rental` + `inventory` |
| `Stock` | Namespaced | `inventory`, aggregated |
| `Payment` | Namespaced | `payment` (partitioned) + rental join |
| `StoreSales` | Cluster | the `sales_by_store` view |

## 1. The database

Pagila needs **PostgreSQL 18**: version 18.a added a virtual generated column to
`film`, and virtual generated columns arrived in 18. On 17 the schema fails to
load.

The dump is not in this repository — it is upstream's data, and upstream keeps
the copy of record. Fetch it, at a pinned commit and checked against recorded
SHA-256 sums:

```sh
./hack/fetch-pagila.sh
```

Then load it:

```sh
kubectl -n kube-crisp exec deploy/postgres -- psql -U crisp -d postgres -c "CREATE DATABASE pagila"

# Upstream's schema ends every object with "ALTER ... OWNER TO postgres", which
# is what pg_dump writes on a default install. If your superuser is called
# something else, the role still has to exist or the dump will not load.
kubectl -n kube-crisp exec deploy/postgres -- psql -U crisp -d pagila -c "CREATE ROLE postgres SUPERUSER LOGIN"

kubectl -n kube-crisp exec -i deploy/postgres -- psql -U crisp -d pagila < third_party/pagila/pagila-schema.sql
kubectl -n kube-crisp exec -i deploy/postgres -- psql -U crisp -d pagila < third_party/pagila/pagila-data.sql
```

Two indexes that Pagila does not ship, and that the watch in step 9 needs:

```sql
CREATE INDEX rental_last_update_idx    ON rental (last_update);
CREATE INDEX inventory_store_film_idx  ON inventory (store_id, film_id);
```

Pagila has no index on any `last_update` column, and a watch filters and orders
by the one it maps on every poll. Without the first index every poll is a
sequential scan of `rental`.

## 2. The credentials

```sh
kubectl apply -f examples/pagila/00-secret.yaml
```

The Secret carries `crisp.kubecrisp.io/allow-projection: "true"`, without which
kube-crisp refuses to read it. A projection's author chooses the SQL; the
credential's owner chooses whether it may be used at all.

## 3. The catalogue

```sh
kubectl apply -f examples/pagila/10-catalogue.yaml
kubectl get films
```

```
NAME               TITLE              RATING   RATE   MINUTES   BREAK-EVEN
academy-dinosaur   ACADEMY DINOSAUR   PG       0.99   86        22
ace-goldfinger     ACE GOLDFINGER     G        4.99   48        3
adaptation-holes   ADAPTATION HOLES   NC-17    2.99   50        7
```

Three things in that output are worth stopping on.

**The name is a slug, not the title.** `ACADEMY DINOSAUR` is not a valid object
name — Kubernetes wants a DNS subdomain — so the query builds one:

```sql
lower(regexp_replace(f.title, '[^a-zA-Z0-9]+', '-', 'g')) AS slug
```

**Break-even is a status, and the database computes it.**
`film.rentals_to_breakeven` is a generated column: `replacement_cost /
rental_rate`, maintained by PostgreSQL. Mapping it to `status` rather than
`spec` says exactly what it is — a value the object's owner computes, which no
client can set. `status.revenueProjection` is the other one.

**Two arrays came from three tables.** `spec.categories` and `spec.actors` are
gathered by subqueries over the join tables and mapped with `type: json`:

```sh
kubectl get film academy-dinosaur -o jsonpath='{.spec.actors}'
```

```
["JOHNNY CAGE","ROCK DUKAKIS","CHRISTIAN GABLE","PENELOPE GUINESS", ...]
```

`spec.specialFeatures` is different again: `film.special_features` is a
PostgreSQL `text[]`, which becomes a JSON array with `to_jsonb(...)`.

Every `kubectl get` in this section ran as cluster-admin. For anyone else they are
`Forbidden` until a ClusterRole names the group: authorization is delegated to the
kube-apiserver, and nothing has told it about `films.pagila.example.com` yet.

```sh
kubectl crisp rbac -f examples/pagila/ | kubectl apply -f -
kubectl create clusterrolebinding pagila-view \
  --clusterrole=kube-crisp:pagila.example.com:view --user=alice
```

Two roles for all ten kinds, granting each of them exactly the verbs its projection can serve —
which here is not the same answer twice, and is the subject of
[the operating guide](operating.md#granting-access-to-a-projected-group).

## 4. Names have to identify one row

`Actor` is where the slug stops being enough:

```sh
kubectl get actors | grep susan-davis
```

```
susan-davis-101           SUSAN DAVIS            33
susan-davis-110           SUSAN DAVIS            21
```

Pagila has two actors called Susan Davis. A name built from the slug alone would
have two rows claiming one object — the first would win, the second would
vanish, and nothing would say so. The `actor_id` on the end is ugly and correct.

Films are luckier: all 1000 titles slug uniquely, which is worth checking
rather than assuming. The check is one query:

```sql
SELECT count(*), count(DISTINCT lower(regexp_replace(title, '[^a-zA-Z0-9]+', '-', 'g'))) FROM film;
```

Uniqueness is only half of it. `staff.username` is `Mike` and `Jon` — unique,
readable, and not valid object names, because Kubernetes wants lower case. The
first draft of `StaffMember` mapped the column straight through, and the result
was not an error: both rows were skipped as unmappable and `kubectl get
staffmembers` returned an empty list with a warning nobody was looking at.
`lower(s.username)` fixes it. Any column you did not choose for this purpose
deserves the same suspicion.

## 5. Labels, and selecting on them

`Film` maps three columns to labels — rating, language and primary category —
so the catalogue can be sliced the way any other Kubernetes collection is:

```sh
kubectl get films -l pagila.example.com/rating=PG-13
kubectl get films -l 'pagila.example.com/category in (Documentary,Sports)'
```

Label selection happens after the rows are read. A **field** selector can reach
the database instead, if the projection says which column holds the value:

```yaml
selectableFields:
  - {jsonPath: .spec.rating, column: rating}
```

```sh
kubectl get films --field-selector spec.rating=G
```

Now `:rating` is bound into the statement and PostgreSQL does the filtering. The
result is the same either way; the difference is how many rows crossed the wire
to produce it.

## 6. Namespaces are stores

```sh
kubectl apply -f examples/pagila/20-operations.yaml
kubectl get stores
kubectl get customers -n store-1 | head -3
```

```
NAME      MANAGER        CITY         COUNTRY
store-1   Mike Hillyer   Lethbridge   Canada
store-2   Jon Stephens   Woodridge    Australia
```

```
NAME                 FULL NAME          CITY             ACTIVE   OUT   SINCE
mary-smith-1         MARY SMITH         Sasebo           true     0     20y
patricia-johnson-2   PATRICIA JOHNSON   San Bernardino   true     0     20y
```

326 customers in `store-1`, 273 in `store-2`, and no request can see across the
boundary — the projection binds `:namespace` into every query, and kube-crisp
withholds any row whose mapped namespace does not match what was asked for even
if the SQL forgets to.

**`StaffMember` is the security argument in one projection.** The `staff` table
has a `password` column and a `bytea` `picture`. Neither appears in the SELECT
list:

```sh
kubectl get staffmember mike -n store-1 -o yaml | grep -i password
# nothing
```

There is no field selector, no label selector and no clever request that reaches
a column the query never asked for. The statement is the allow-list, and it is
the only one.

## 7. Renting a film

```sh
kubectl apply -f examples/pagila/30-rental.yaml
```

Pagila stores a rental as a `tsrange`: `["2005-05-24 22:53:30",)` is out,
`["2005-05-24 22:53:30","2005-05-26 22:04:30")` is returned. So the shape of the
write comes from the data rather than being imposed on it — renting opens the
range, returning closes it, and **returning a film is a status write**.

```sh
cat <<'EOF' | kubectl create -f -
apiVersion: pagila.example.com/v1alpha1
kind: Rental
metadata:
  generateName: rental-
  namespace: store-1
spec:
  film: academy-dinosaur
  customer: MARY SMITH
EOF
```

The create statement resolves the film slug and the customer name to ids, finds
a copy in this store that nobody has out, and inserts. If there is no free copy
it matches no row, and kube-crisp answers 404 — which is the truth: the object
the client asked to create does not exist to be created.

`RETURNING` is doing real work here. The database assigns `rental_id`, and the
object is named after it, so without `RETURNING` there would be nothing to read
the new object back by.

Returning the film:

```sh
kubectl patch rental rental-16050 -n store-1 --subresource=status --type=merge \
  -p '{"status":{"phase":"Returned"}}'
```

```sh
kubectl get rentals -n store-1 --field-selector status.phase=Out
```

Returning it twice is a 404, not a silent success: the statement requires
`upper_inf(rental_period)`, so a rental already closed matches nothing.

**Deleting is where the database answers back.** A rental with a payment against
it mostly cannot be deleted — `payment.rental_id` references it — and kube-crisp
turns a foreign key violation into `409 Conflict`:

```
Error from server (Conflict): Operation cannot be fulfilled on
  rentals.pagila.example.com "rental-1476": executing statement: ERROR: update or
  delete on table "rental" violates foreign key constraint
  "payment_p2007_01_rental_id_fkey" on table "payment_p2007_01" (SQLSTATE 23503)
```

The constraint it names belongs to a *partition*, and that is not decoration.
Pagila declares the foreign key on six of `payment`'s eight partitions;
`payment_p0000_default` and `payment_p2007_07_max` carry none. So a rental paid
for in 2007-03 cannot be deleted, and one paid for in 2006-12 can be — the
delete succeeds and leaves a payment pointing at nothing.

Nothing in the projection restates that rule, and nothing in it papers over the
gap either. The API inherits the constraints the schema has, and exactly those:
a projection is not a place to re-implement integrity, and a schema that
enforces a rule unevenly gives you an API that enforces it unevenly. Worth
knowing before promising a client that a reference is safe.

## 8. Scaling stock

```sh
kubectl apply -f examples/pagila/40-stock.yaml
kubectl get stock -n store-1 academy-dinosaur
```

```
NAME               TITLE              DESIRED   COPIES   ON-LOAN   AVAILABLE
academy-dinosaur   ACADEMY DINOSAUR   4         4        0         4
```

There is no `stock` table. A `Stock` is one row per (film, store) assembled from
a count over `inventory` — and because the projection declares the `scale`
subresource, it scales:

```sh
kubectl scale stock/academy-dinosaur --replicas=8 -n store-1
kubectl scale stock/academy-dinosaur --replicas=4 -n store-1
```

Scaling up inserts inventory rows; scaling down deletes them. It is one
statement, a CTE that deletes and inserts and then reports the count, because
the number the client is answered with has to be the count after both halves ran
— a second read taken between them could report a number that was never true.

Scaling down will not delete a copy that has ever been rented: the rental rows
reference it, and losing a store's history to a `kubectl scale` would be a
strange thing for it to do. So a scale-down removes what it can and the status
reports what actually happened, which is why `spec.replicas` and
`status.replicas` can differ.

## 9. Watching

`Rental` is the only projection here with watch enabled, because it is the only
table that changes on its own. The others are read on request.

```sh
kubectl get rentals -n store-1 --watch
```

The watch polls `watch.query`, which reads forward from a high-water mark rather
than re-reading the table:

```sql
WHERE :since::text IS NULL
   OR (extract(epoch FROM r.last_update) * 1000000)::bigint > :since::bigint
ORDER BY r.last_update
```

Three things make that work, and all three are worth knowing before turning
watch on anywhere else.

**The version has to be a number that only goes up.** `last_update` is a
timestamp, and kube-crisp compares resourceVersions as integers when it can, so
the query renders it as microseconds since the epoch. Pagila helps here: every
table has a `BEFORE UPDATE` trigger that sets `last_update = now()`, so a write
gets a fresh version without the projection having to set one.

**`now()` is transaction time.** Rows written in one transaction share a
version, and an incremental poll cannot tell them apart. For a sample database
that is fine. For a table under real write load, use `clock_timestamp()` or a
sequence.

**Deletions need a tombstone.** A forward-reading poll cannot see a row that is
gone. Pagila has no tombstone table, so deletions are only noticed by the
periodic full resync — which is the default, and the reason `fullResyncInterval`
exists. A projection that needs deletions promptly wants a `deleted_rentals`
table and a `watch.deletedQuery`; the [PostgreSQL
tutorial](tutorial-postgresql.md#noticing-a-deletion-without-re-reading-everything)
has the shape.

One thing you cannot have: if you scope rows to the caller — binding `:user`, or
setting a session variable for row-level security — the projection is refused
with watch enabled. A watch is served from one poll shared by every watcher, and
there is no request behind a poll. Set `watch.disabled: true` and the projection
loads.

## 10. Things that are not tables

```sh
kubectl apply -f examples/pagila/50-reporting.yaml
```

`payment` is partitioned by month. The projection selects from the parent and
PostgreSQL decides which partitions to touch; nothing in the projection knows
partitioning exists.

Deciding its namespace took a second look. `payment` has no `store_id`, and
there are two ways to reach one: the staff member who took the money, or the
copy that was rented. They disagree for half the rows — Pagila's staff serve
customers from both stores — and the first draft took the staff member, which
put the money in one namespace and the `Rental` it names in the other. The join
to use was the one Pagila's own `sales_by_store` view uses, through
`rental → inventory → store`, and it is the one that keeps `spec.rental`
pointing inside the namespace that holds it. When a table has no column for the
namespace, the join you pick *is* the modelling decision — and a reference that
leaves its own namespace is the sign you picked wrong.

What the projection does declare is paging, because this is the only collection
here big enough to need it:

```sh
kubectl get payments -n store-1 --chunk-size=200
```

`keysetColumn: payment_id` makes the continue token carry the last row's key
rather than an offset, so a payment inserted while a client is paging cannot
shift a row onto a page it has already seen. `maxRows` and `maxBytes` bound what
one read may cost.

One trap worth the paragraph, because it hides so well. `:after` appears twice
in that query, and both casts have to agree:

```sql
AND (:after::int IS NULL OR p.payment_id > :after::int)
```

PostgreSQL takes a parameter's type from its first use. Written `:after::text IS
NULL`, the driver is asked to send `payment_id` 667 as text and refuses — but
only from the *second* page onwards, because on the first `:after` is NULL and
NULL encodes as anything. A first page that works is not evidence that paging
works.

Which is why the watch in step 9 can write `:since::text IS NULL` and be right:
a resourceVersion is a string, so text is what the driver is holding. `:after`
carries the keyset column's own value, and `payment_id` is an integer. The rule
is not "always cast to text" — it is that the cast has to match what will
actually be bound.

`StoreSales` projects the `sales_by_store` **view**. A projection does not care
whether the relation it selects from is a table:

```sh
kubectl get storesales
```

```
NAME                  STORE                  MANAGER        SALES
lethbridge-canada     Lethbridge, Canada     Mike Hillyer   33679.79
woodridge-australia   Woodridge, Australia   Jon Stephens   33726.77
```

Everything on it is `status`, which is honest: a view has no spec, nothing can
write to it, and every value is computed.

## What Pagila taught that a designed schema would not

- **Most tables are not resources.** Fifteen tables and a view became ten kinds,
  and three of the tables became fields on other kinds.
- **Real data breaks names.** Two Susan Davises is not a hypothetical.
- **The schema's constraints become the API's constraints**, for free, and they
  reach the client as the right status code — and so do the constraints it turns
  out not to have. Two of `payment`'s eight partitions carry no foreign key, so
  the rule the other six enforce is one the API can only half promise.
- **Nothing indexes what a watch needs**, because nothing was watching before.
- **The interesting resources are the ones with no table.** `Stock` is a count,
  and it is the only kind here you can `kubectl scale`.
- **When a table has no column for the namespace, the join you take is the
  modelling decision.** `payment` can be reached from a store two ways, they
  disagree for half the rows, and only one of them keeps a payment's
  `spec.rental` inside the namespace holding it.

Three things cost an afternoon each, and all three are in the projections now:

- **A dump assumes its own server.** Pagila's schema ends every object with
  `ALTER ... OWNER TO postgres`, 65 times. On a server whose superuser is called
  something else the load stops at the first one.
- **A data-modifying CTE cannot see itself.** `Stock`'s write inserts rows and
  then reports the new `max(last_update)` — and the `SELECT` sees the table as it
  was before the insert, because that is what PostgreSQL guarantees. Reporting
  that as the resourceVersion made the *next* write conflict against a version
  the previous one had invented. The fix is to read the version out of the
  inserting CTE rather than out of the table — and then the same statement was
  found doing it a second time, counting `status.available` off the table it had
  just added to, so a scale to 7 answered "7 copies, 4 available". Once a
  statement writes, every count in it is suspect until you have asked which side
  of the write it sees.
- **A column mapped twice can only be written once.** The rental phase started as
  both a label and a status field; every write warned that the label had been
  ignored. It is a selectable field now, which is the half that can change.
