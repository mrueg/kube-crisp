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

## What is here

Both files are upstream's, byte for byte — 1000 films, 4581 copies on the
shelves, 16044 rentals and as many payments. Nothing is trimmed: the tutorial
quotes real numbers out of the real database, and a reader who loads it gets the
same ones.

To refresh after an upstream bump:

```sh
curl -O https://raw.githubusercontent.com/xzilla/pagila/master/pagila-schema.sql
curl -O https://raw.githubusercontent.com/xzilla/pagila/master/pagila-data.sql
```

## PostgreSQL 18

The schema needs it. Pagila 18.a adds a *virtual* generated column to `film`
(`rentals_to_breakeven`), and virtual generated columns arrived in PostgreSQL
18 — on 17 the schema fails to load with a syntax error at the `GENERATED
ALWAYS AS` clause, which is why the e2e cluster runs 18.

It also installs a LOGIN event trigger that greets every new connection with a
NOTICE. Harmless, and a fair demonstration that a projection takes the database
as it finds it.
