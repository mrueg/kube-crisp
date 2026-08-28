# Pagila

The sample database the [PostgreSQL tutorial for Pagila](../../docs/tutorial-pagila.md)
projects. A "DVD rental store" schema — films, actors, stores, customers,
rentals and payments — with enough shape to be worth modelling: composite keys,
a partitioned table, generated columns, an array, a range type and a handful of
views.

| | |
|---|---|
| Upstream | <https://github.com/xzilla/pagila> |
| Commit | `fc7a86771a7ff213597139942f1f57c36125d37d` |
| Version | 18.a |
| License | The PostgreSQL License. Portions © 2006–2026 Robert Treat, portions © 2006 MySQL AB. |

## Nothing is here

`pagila-schema.sql` and `pagila-data.sql` are **not** in this repository. They
are somebody else's data, 2.9 MB of it, and upstream keeps the copy of record.
Fetch them:

```sh
./hack/fetch-pagila.sh
```

Which downloads both files at the commit pinned above and checks them against
recorded SHA-256 sums, so the contents cannot change under this repository
without the pin being changed first. Already-correct files are left alone, so
`hack/e2e-up.sh` calls it on every run and pays for it once.

A mismatch is an error rather than a warning, and the partial download is
removed rather than left where something might load it. If upstream has moved
on deliberately, update `COMMIT` and the two sums in the script together.

What arrives is upstream's, byte for byte — 1000 films, 4581 copies on the
shelves, 16044 rentals and as many payments. Nothing is trimmed: the tutorial
quotes real numbers out of the real database, and a reader who fetches it gets
the same ones.

## PostgreSQL 18

The schema needs it. Pagila 18.a adds a *virtual* generated column to `film`
(`rentals_to_breakeven`), and virtual generated columns arrived in PostgreSQL
18 — on 17 the schema fails to load with a syntax error at the `GENERATED
ALWAYS AS` clause, which is why the e2e cluster runs 18.

It also installs a LOGIN event trigger that greets every new connection with a
NOTICE. Harmless, and a fair demonstration that a projection takes the database
as it finds it.
