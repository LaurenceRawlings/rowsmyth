# Upgrading to rowsmyth 2.0

rowsmyth 2.0 optimises row generation and adds extra referential integrity checks.
Most fixtures need only small edits; this guide lists every
breaking change with a before/after.

## Contents

- [Upgrading to rowsmyth 2.0](#upgrading-to-rowsmyth-20)
  - [Contents](#contents)
  - [Why 2.0](#why-20)
  - [At a glance](#at-a-glance)
  - [1. Spark work is deferred until you read](#1-spark-work-is-deferred-until-you-read)
  - [2. `dataset.dataframes` is gone](#2-datasetdataframes-is-gone)
  - [3. Integrity is enforced by default](#3-integrity-is-enforced-by-default)
  - [4. Callables must be wrapped in `lazy()`](#4-callables-must-be-wrapped-in-lazy)
  - [5. Column values are type-checked](#5-column-values-are-type-checked)
  - [6. Pools resolve in Python](#6-pools-resolve-in-python)
  - [7. Factories are copy-on-write](#7-factories-are-copy-on-write)
  - [8. `count(0)` registers an empty table](#8-count0-registers-an-empty-table)
  - [9. Column names may not shadow the `Model` API](#9-column-names-may-not-shadow-the-model-api)
  - [10. Renamed and removed symbols](#10-renamed-and-removed-symbols)
  - [New in 2.0](#new-in-20)
  - [Migration checklist](#migration-checklist)

---

## Why 2.0

In 1.x every `create()` call rebuilt the whole table: one `createDataFrame` plus
one `createOrReplaceTempView` per row, over a payload that grew by one row each
time. Generating *n* rows serialised `n(n+1)/2` rows and issued `2n` Spark
round trips, so the documented "create rows one at a time" pattern was
quadratic. A 10,000-row fixture spent tens of minutes issuing tens of thousands
of sub-100ms queries.

2.0 keeps generated rows in the driver as plain dicts and touches Spark only
when a table is read. The same fixture now costs one `createDataFrame` and one
temp view registration **per table**, whichever way you create rows.

| Rows created one at a time | 1.x `createDataFrame` calls | 2.0 |
|---:|---:|---:|
| 100 | 100 | 1 |
| 400 | 400 | 1 |
| 1,600 | 1,600 | 1 |

| Rows created one at a time | 1.x rows serialised | 2.0 |
|---:|---:|---:|
| 100 | 5,050 | 100 |
| 400 | 80,200 | 400 |
| 1,600 | 1,280,800 | 1,600 |

## At a glance

| Change | Action |
|--------|--------|
| Spark work deferred to first read | Call `dataset.flush()` before `spark.table()` / `spark.sql()` on generated tables |
| `dataset.dataframes` removed | Use `dataset.tables()` or `dataset.dataframe(name)` |
| Duplicate primary keys raise | Fix the fixture, or pass `integrity="warn"` / `"off"` |
| `__foreign_keys__` validated on block exit | Declare them, or leave them undeclared for no checks |
| Bare callables are no longer invoked | Wrap with `rowsmyth.lazy(...)` |
| Values are type-checked per row | Pass values matching the declared Spark type |
| `pool.choice()` returns a value, not a token | Remove any `PoolChoice` / `UnresolvedPoolError` handling |
| Factory methods return copies | Keep the returned factory: `f = f.count(3)` |
| `count(0)` registers an empty table | Expect an empty DataFrame instead of `TableNotFoundError` |
| Columns may not shadow the `Model` API | Rename columns such as `key`, `pk`, `attrs`, `create` |
| `DataframeNotFoundError` renamed | Catch `TableNotFoundError` |

---

## 1. Spark work is deferred until you read

`Model.create()` and `Factory.create()` no longer call Spark. Tables are
materialised on the first read - `dataset.dataframe(name)`, `dataset.tables()`,
`dataset.write_all()` - or when you call `dataset.flush()`.

Nothing changes if you read through the dataset:

```python
with Base.dataset(spark, seed=42) as dataset:
    Customer.factory().count(100).create()
    customers_df = dataset.dataframe("customers")  # materialises here
```

Code that reads a generated temp view through Spark **during** generation needs
an explicit flush:

```python
# 1.x - the temp view was always current
with Base.dataset(spark) as dataset:
    Role.create(name="admin")
    spark.table("roles").show()

# 2.0
with Base.dataset(spark) as dataset:
    Role.create(name="admin")
    dataset.flush()            # or dataset.flush("roles")
    spark.table("roles").show()
```

Better still, read rows without Spark at all (see [New in 2.0](#new-in-20)):

```python
dataset.rows("roles")   # list[dict]
dataset.count("roles")  # int
```

If you never need temp views, pass `views=False` to halve the remaining round
trips:

```python
with Base.dataset(spark, seed=42, views=False) as dataset:
    ...
```

## 2. `dataset.dataframes` is gone

The dict attribute was a snapshot of eagerly built frames. Use the accessors:

```python
# 1.x
for name, df in dataset.dataframes.items():
    df.write.saveAsTable(f"main.bronze.{name}")

# 2.0
for name, df in dataset.tables().items():
    df.write.saveAsTable(f"main.bronze.{name}")

# or let rowsmyth do it
dataset.write_all(name=lambda table: f"main.bronze.{table.__table_name__}")
```

`dataset.table_names()` returns the created table names without materialising
anything.

## 3. Integrity is enforced by default

Datasets run with `integrity="raise"`.

**Primary keys** are checked as rows are created:

```python
with Base.dataset(spark) as dataset:
    Thing.create(id=7, name="first")
    Thing.create(id=7, name="second")   # DuplicatePrimaryKeyError
```

**Foreign keys** are checked when the dataset block exits cleanly, so children
may still be created before their parents. Declare them per model:

```python
class Answer(Base):
    __table_name__ = "answers"
    __primary_key__ = ("id",)
    __foreign_keys__ = {
        "survey_id": "surveys.id",
        "question_id": "questions.id",
    }
```

Undeclared foreign keys are not checked, so existing models keep working until
you declare them. Call `dataset.check_integrity()` at any point to fail closer
to the code that created the rows.

To migrate a large suite gradually, downgrade the mode:

```python
Base.dataset(spark, seed=42, integrity="warn")   # RowsmythWarning instead
Base.dataset(spark, seed=42, integrity="off")    # no checks at all
```

## 4. Callables must be wrapped in `lazy()`

1.x called *any* callable column value with the row context, so a class,
builtin or `functools.partial` passed as data was silently invoked - and for a
string column, Spark stored the repr of whatever came back.

```python
# 1.x
.where(score=lambda ctx: ctx.random.random())

# 2.0
from rowsmyth import lazy

.where(score=lazy(lambda ctx: ctx.random.random()))
```

A bare callable is now treated as data, so it fails the type check for its
column with a message pointing at `lazy()`.

## 5. Column values are type-checked

Every generated value is checked against the column's declared Spark type
before the row is committed, so a bad value names its own table, column and
type instead of failing later inside `createDataFrame`:

```
ColumnTypeError: roles.name: string column expects str, got int
```

String columns require `str` - 1.x let Spark stringify anything. Convert
explicitly:

```python
{"id": str(uuid4())}       # not uuid4()
{"price": Decimal("9.99")}  # not 9.99 for a decimal column
```

## 6. Pools resolve in Python

`pool.choice()` used to return a deferred `PoolChoice` token resolved with a
Spark window join at commit time. It now returns a concrete value immediately:

- A pool name that matches a table in your declarative base resolves from rows
  already generated in this dataset - no Spark at all.
- Any other name is read once from the Spark session and cached for the rest of
  the dataset block.

```python
"role_id": ctx.pool("roles", "id").choice()   # unchanged in your generator
```

Removed: `rowsmyth.pool.PoolChoice` and `UnresolvedPoolError`. Pooling a table
that has no rows yet now raises `EmptyPoolError` naming the table, and pooling a
column the table does not declare raises `UnknownColumnError`.

Seeded pool selections differ from 1.x: the same seed produces a different (but
still deterministic) choice. Re-baseline any golden files that pin pooled
values.

## 7. Factories are copy-on-write

`count()`, `where()`, `has()`, `variant()` and the new `from_rows()` return a
new factory instead of mutating the receiver, so a factory is a safe template:

```python
template = Thing.factory().count(1)
a = template.where(tag="A").create()[0]   # tag = "A"
b = template.create()[0]                  # default tag, not "A"
```

Code that relied on mutation must keep the result:

```python
# 1.x - mutated in place
factory = Thing.factory()
factory.count(3)
factory.create()          # 2.0: creates 1 row

# 2.0
factory = Thing.factory().count(3)
factory.create()
```

## 8. `count(0)` registers an empty table

A zero-row factory now registers an empty DataFrame carrying the declared
schema, so downstream pipeline code exercises the same path:

```python
Thing.factory().count(0).create()
dataset.dataframe("things")   # 1.x: DataframeNotFoundError, 2.0: 0 rows
```

## 9. Column names may not shadow the `Model` API

Columns named after `Model` members (`key`, `pk`, `attrs`, `create`, `factory`,
`registry`, `dataset`, `fqn`, `generator`, ...) used to either raise from deep
inside row construction or silently shadow the API. They are now rejected at
class definition:

```
ReservedColumnError: things: column names collide with the Model API: ['key', 'pk']
```

Rename the column in `__definition__` (for example `key` to `row_key`).

## 10. Renamed and removed symbols

| 1.x | 2.0 |
|-----|-----|
| `DataframeNotFoundError` | `TableNotFoundError` |
| `Dataset.dataframes` | `Dataset.tables()` / `Dataset.dataframe(name)` |
| `rowsmyth.pool.PoolChoice` | removed - `Pool.choice()` returns a value |
| `UnresolvedPoolError` | removed |
| bare callable column values | `rowsmyth.lazy(fn)` |

`DatasetContextError` and `DatasetLookupError` now share a `DatasetError`
parent; `RowsmythError` still roots the whole hierarchy.

## New in 2.0

| API | Purpose |
|-----|---------|
| `dataset.rows(name)` / `dataset.count(name)` | Read generated rows in Python, no Spark |
| `dataset.tables()` / `dataset.table_names()` | Every created table, by name |
| `dataset.flush(name=None)` | Materialise pending rows and register temp views |
| `dataset.write_all(...)` | Write every created table with `saveAsTable` or `save` |
| `dataset.check_integrity()` | Run foreign key checks early |
| `dataset.view_name(name)` | Temp view name for a table |
| `Model.bulk_create(rows)` / `Factory.from_rows(rows)` | Create precomputed rows without `generator()` |
| `Model.__foreign_keys__` | Declared `{column: "table.column"}` relationships |
| `rowsmyth.lazy(fn)` | Explicit deferred column value |
| `ctx.dataset` | The active `Dataset` from inside `generator()` |
| `Base.dataset(..., integrity=, views=, view_prefix=)` | Integrity mode, temp view control and namespacing |
| `RowsmythWarning` | Warning class used by `integrity="warn"` |

## Migration checklist

1. Replace `dataset.dataframes` with `dataset.tables()`.
2. Add `dataset.flush()` before any direct `spark.table()` / `spark.sql()` read
   of a generated table inside a dataset block.
3. Wrap callable column values in `lazy(...)`.
4. Catch `TableNotFoundError` instead of `DataframeNotFoundError`.
5. Delete any `PoolChoice` or `UnresolvedPoolError` handling.
6. Keep the return value of every factory method call.
7. Run the suite. Fix `DuplicatePrimaryKeyError`, `ForeignKeyViolationError`,
   `ColumnTypeError` and `ReservedColumnError` failures - each names the table,
   column and value at fault. Use `integrity="warn"` to triage a large suite in
   one pass.
8. Replace hand-maintained table lists with `dataset.tables()` or
   `dataset.write_all(...)`.
