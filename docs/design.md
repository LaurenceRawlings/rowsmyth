# Design & Architecture

Rowsmyth generates relational test and seed datasets as Spark DataFrames **row by row**, with referential integrity between tables. Row generation runs in the driver in pure Python; Spark is touched only when a table is read. Writing to Unity Catalog (or elsewhere) stays under your control.

## Influences

| Idea | Source |
|------|--------|
| Create a declarative base, subclass it, auto-register in a catalog | SQLAlchemy declarative bases |
| Faker, weights, column ergonomics | dbldatagen (but rowsmyth is not vectorized and supports cross-table FKs) |

dbldatagen optimises for throughput on a single DataFrame. Rowsmyth trades that for per-row control and real parent/child relationships.

## Core invariant: generation is Spark-free

`Model.create()`, `Model.bulk_create()` and `Factory.create()` append plain
dicts to the active `Dataset`. Nothing is serialised, no temp view is
registered, no query is issued. A table is materialised - one
`createDataFrame`, plus one `createOrReplaceTempView` when `views=True` - the
first time it is read, and cached until new rows arrive for it.

This makes the cost of a fixture independent of how its rows were created:

| Rows | `createDataFrame` calls | Rows serialised |
|-----:|------------------------:|----------------:|
| 100 | 1 | 100 |
| 1,600 | 1 | 1,600 |
| 10,000 | 1 | 10,000 |

There is no memory trade-off: the rows were always held in the driver, and
`createDataFrame` always shipped them from there.

Two consequences worth knowing:

- Reading a generated table through Spark directly (`spark.table`, `spark.sql`)
  inside a dataset block needs `dataset.flush()` first.
- Reading rows back in Python (`dataset.rows`, `dataset.count`) is free, so
  fixture code never needs a Spark action to look up what it just created.

## Public API

Import from `rowsmyth`:

| Symbol | Role |
|--------|------|
| `declarative_base` | Creates a scoped declarative base with its own registry |
| `Model` | Abstract model machinery inherited by declarative bases |
| `variant` | Decorator for named partial row overrides |
| `lazy` / `Lazy` | Explicit marker for deferred, context-dependent column values |
| `Factory` | Fluent, copy-on-write builder (`Model.factory()` is the usual entry) |
| `RowCtx` | Per-row context in `generator()` and variants |
| `Dataset` | Object yielded by `Base.dataset()` (rows, tables, pools, integrity) |
| `Pool` | Distinct values from a dataset table or a Spark temp view |
| `RowsmythError` and subclasses | Domain errors for datasets, schemas, integrity, factories, variants and pools |
| `RowsmythWarning` | Warning class used by `integrity="warn"` |

`Model.create()` and `Factory.create()` must run inside `Base.dataset(...)` for the same declarative base. Calling either outside raises `DatasetContextError`; using a model from another base raises `WrongDeclarativeBaseError`.

## Package layout

```
src/rowsmyth/
  model.py       Model, declarative_base(), variant, registry, fqn(), metadata helpers
  factory.py     Factory - fluent copy-on-write API, create() and from_rows()
  dataset.py     Dataset, RowCtx, materialisation, pools, integrity, active context
  resolution.py  FK resolution, lazy resolution, row validation
  lazy.py        Lazy marker and lazy()
  pool.py        Pool and its incremental distinct-value index
  errors.py      Rowsmyth exception hierarchy
```

## Defining a table

Create a base with `declarative_base()`, subclass it, declare schema metadata on the class and implement `generator()` for one row.

```python
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import declarative_base, variant

Base = declarative_base()


class Role(Base):
    __table_name__ = "roles"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        return {
            "id": ctx.sequence(),
            "name": ctx.random.choice(["admin", "user", "guest"]),
        }


class User(Base):
    __table_name__ = "users"
    __catalog__ = "main"
    __schema__ = "app"
    __comment__ = "Application users"
    __primary_key__ = ("id",)
    __foreign_keys__ = {"role_id": "roles.id"}
    __table_tags__ = {"layer": "silver", "pii": "true"}
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("role_id", LongType(), False, metadata={"comment": "FK -> roles.id"}),
        StructField("full_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("status", StringType(), False),
    ])

    def generator(self, ctx) -> dict:
        first = ctx.faker.first_name()
        last = ctx.faker.last_name()
        return {
            "id": ctx.sequence(),
            "role_id": Role.factory(),
            "full_name": f"{first} {last}",
            "email": ctx.faker.unique.ascii_email(),
            "status": ctx.random.choices(["active", "inactive"], weights=[9, 1])[0],
        }

    @variant
    def churned(self, ctx) -> dict:
        return {"status": "inactive"}
```

Because each row is a single `dict`, columns that depend on each other use normal local variables (`first` / `last` above). There is no column-ordering DSL.

### Class attributes

| Attribute | Required | Purpose |
|-----------|----------|---------|
| `__table_name__` | yes | Registry key and temp-view name |
| `__definition__` | yes | `StructType` - types, nullability, UC column metadata |
| `__primary_key__` | yes | Tuple of PK column names (one or more); uniqueness enforced |
| `__foreign_keys__` | no | `{column: "parent_table.parent_column"}`; validated per dataset |
| `__catalog__`, `__schema__` | no | Used by `Model.fqn()` |
| `__comment__` | no | Table comment for UC / Lakeflow |
| `__table_tags__` | no | `{key: value}` for UC table tags |
| `__expectations__` | no | `{name: sql}` dict for Lakeflow data quality expectations (`expect_all_or_fail`) |

### Registry and validation on subclass

On subclass, if `__table_name__` is set, rowsmyth:

1. caches the field-name set on the class (it is immutable, so per-row work is wasted work);
2. rejects columns starting with `__rowsmyth_` and columns that shadow the `Model` API (`key`, `pk`, `attrs`, `create`, ...), both as `ReservedColumnError`;
3. validates that primary-key columns exist in the Spark schema;
4. validates the shape of every `__foreign_keys__` entry (known column, `"table.column"` target);
5. registers the class in `Base.registry[__table_name__]`;
6. collects inherited plus locally declared `@variant` methods into `_variants`.

Foreign key *targets* resolve when a dataset is checked, not at class definition, so models may reference tables defined later.

If `__table_name__` is omitted (mixin or abstract intermediate base), the class is **not** registered. Concrete children still register normally.

## Factory API

```python
User.factory()
    .count(10)
    .variant("churned")       # UnknownVariantError if unknown
    .where(status="active")    # scalars, Factory FKs, or lazy() values
    .has(Post.factory().count(3), via="author_id")
    .create()                   # list[User]; rows appended to the dataset
```

| Method | Behaviour |
|--------|-----------|
| `count(n)` | Rows to generate for this table; `0` registers an empty table, negative/non-integer values raise `FactoryError` |
| `from_rows(rows)` | One row per mapping, bypassing `generator()`; excludes `count()` and `variant()` |
| `variant(name)` | Merge partial dict from `@variant` method |
| `where(**kwargs)` | Overrides; merged after variant |
| `has(child, via=None)` | For each parent row, generate child rows with parent injected |
| `create()` | Generate every table in the factory tree and return root-table instances |

Every method returns a **copy**. A factory is therefore a value, not a session:
holding one as a template and refining it per call site is safe, which is how
fluent builders are conventionally read.

`.create()` returns concrete model objects for the root factory table. Every table touched by the factory tree - parents created for FKs, children from `.has()`, and tables with zero rows - is registered on the dataset.

## Static and precomputed row creation

```python
Role.create(name="admin")            # one row through generator(), with overrides
Role.bulk_create(rows)               # one row per mapping, generator() skipped
Role.bulk_create(rows, tenant="a")   # ... plus a constant override
```

`Model.create()` builds one row through the same `generator(ctx)`, sequence, lazy, FK resolution and validation pipeline as factories. `Model.bulk_create()` is `Factory.from_rows(rows).where(**cols).create()`: it skips generation but keeps resolution, validation and integrity checks, which makes it the cheapest way to put known rows into a dataset.

## Row materialisation pipeline

For each row:

1. `attrs = table().generator(ctx)` - or `dict(preset)` when the factory came from `from_rows()`
2. If a variant is selected: `attrs.update(variant_method(table_instance, ctx))`
3. `attrs.update(factory._where)` - **`.where()` wins** over generator and variant
4. `ctx.row = attrs` (may still contain `Factory` instances or `Lazy` values)
5. **Resolve** each value in `attrs`:
   - `Factory` -> FK resolution (see below); slot = column name
   - `Lazy` -> `value.fn(ctx)`; siblings visible on `ctx.row`
6. **Validate** the full row: no unknown columns, every NOT NULL column present and non-`None`, and every value matching its declared Spark type

Bare callables are *not* invoked. In 1.x any callable was called with the row context, so a class, builtin or partial passed as data silently became whatever the call returned - for a string column, the repr of a `RowCtx`. `lazy()` makes the intent explicit, and a stray callable now fails the type check for its column with a message naming `lazy()`.

Variants are bound methods: `def churned(self, ctx) -> dict`.

## Integrity

| Constraint | When | Cost | Error |
|------------|------|------|-------|
| `__primary_key__` uniqueness | As rows are committed | One set lookup per row | `DuplicatePrimaryKeyError` |
| `__foreign_keys__` validity | Clean block exit, or `dataset.check_integrity()` | One pass per child table | `ForeignKeyViolationError` |

Primary keys fail fast because the failing `create()` call is the useful stack
frame. Foreign keys are deferred because fixtures legitimately create children
before parents; deferring also means one index build per parent column instead
of one lookup per row.

`Base.dataset(spark, integrity=...)` accepts `"raise"` (default), `"warn"`
(emit `RowsmythWarning` and continue - the migration mode) and `"off"`.

## Foreign keys and referential integrity

### Factory as column value (single-column FK)

Return a parent `Factory` as the column value. Resolution uses slot = **column name** (or injected slot from `.has(..., via=...)`):

- If a model object exists in `ctx._parents` for that slot -> use its primary key
- Otherwise -> create one parent row (recursive), cache it, use its PK

```python
"role_id": Role.factory()
```

**Restriction:** the parent table must have a **single-column** primary key. For compound keys, `resolve_fk` raises `CompoundPrimaryKeyError` with a message to use `ctx.parent()` instead.

### `ctx.parent(table, role=None)`

Resolve the parent once per row (inject-or-create, cached). Slot = `role` or `table.__table_name__`.

```python
def generator(self, ctx):
    order = ctx.parent(Order)
    return {
        "order_id": order.key["order_id"],
        "order_region": order.key["region"],
        "qty": ctx.random.randint(1, 5),
    }
```

Use `order.key` (dict of PK columns), `order.pk` (scalar, single-column PK only; raises `CompoundPrimaryKeyError` for compound keys), or `order.attrs` (full row dict).

### `ctx.pool(view, col)`

Not an FK mode: distinct non-null values from a column.

- **A table in the active declarative base** resolves from rows already generated in this dataset. The index is incremental - each lookup consumes only rows added since the last one - so pooling in a generator stays linear.
- **Any other name** is read from the Spark session once (`spark.table(name)`, ordered for determinism) and cached for the dataset block.

```python
"role_id": ctx.pool("roles", "id").choice()
```

`choice()` returns a concrete value drawn with the dataset RNG. 1.x returned a deferred token resolved later with a Spark window join per commit; removing that machinery removed several Spark actions per row from the documented quick-start path.

Create parents with `Model.create()` or `Factory.create()` before dependants that pool from them. The inject-or-create FK path does not require ordering.

### Disambiguation with `via`

When a child has multiple FKs to the same parent, `via` names the injection slot (usually the child FK column name):

```python
User.factory().has(Post.factory().count(3), via="author_id")
```

In `Post.generator`, either:

```python
"author_id": User.factory()
```

or:

```python
ctx.parent(User, role="author_id")
```

## `Base.dataset(spark, seed=None, integrity="raise", *, views=True, view_prefix=None)`

Activates a dataset via a `ContextVar`. Factories and FK logic use the active session without threading `spark` through every call. The dataset is bound to the declarative base, so model creation can reject models from other bases before commit. A clean exit runs `check_integrity()`.

When `seed` is set, rowsmyth seeds only session-owned objects: `ctx.random` / `dataset.random` and `ctx.faker` / `dataset.faker`. It does not call global `random.seed()` or `Faker.seed()`.

```python
with Base.dataset(spark, seed=42) as dataset:
    Role.create(name="admin")
    users = (
        User.factory()
        .count(10)
        .has(Post.factory().count(3).where(published=True), via="author_id")
        .create()
    )
    dataset.count("posts")            # 30, no Spark
    users_df = dataset.dataframe("users")   # materialises "users" + temp view

# dataset.write_all(name=lambda table: f"main.bronze.{table.__table_name__}")
```

### `RowCtx`

| Member | Description |
|--------|-------------|
| `ctx.faker` | Shared `Faker` instance |
| `ctx.random` | Seeded `random.Random` |
| `ctx.seed` | Seed passed to `Base.dataset()`, or `None` |
| `ctx.spark` | Active `SparkSession` |
| `ctx.dataset` | Active `Dataset` |
| `ctx.index` | 0-based row index for the current factory |
| `ctx.row` | In-progress attribute dict (for lazy values and FK resolution) |
| `ctx.sequence(name=None)` | Monotonic counter; default name = current `__table_name__` |
| `ctx.parent(table, role=None)` | Resolve parent model object |
| `ctx.pool(view, col)` | Pool over a dataset table or temp view |

### `Dataset`

Yielded by `Base.dataset()`. Exposes `spark`, `base`, `registry`, `faker`, `random`, `seed`, `integrity`, plus `rows(name)`, `count(name)`, `table_names()`, `dataframe(name)`, `tables()`, `flush(name=None)`, `view_name(name)`, `write_all(...)`, `check_integrity()`, `next_seq(name)` and `pool(view, col)`. Internal row storage, dirty tracking, key indexes and pool indexes are not part of the public contract.

## Create output

For each table name touched by `Model.create()`, `Model.bulk_create()` or `Factory.create()`:

1. Rows are appended to the dataset and the table is marked dirty (no Spark)
2. On first read: `createDataFrame(rows, dataset.registry[name].__definition__)` - schema is **always** explicit; never inferred
3. On first read with `views=True`: `createOrReplaceTempView(dataset.view_name(name))`

Temp views use the bare `__table_name__` (optionally prefixed by `view_prefix`), not `fqn()`. Because two declarative bases with a same-named table would otherwise overwrite each other's view, rowsmyth tracks view ownership per session and raises `ViewCollisionError` when a second base claims a name.

`dataset.write_all()` writes every created table - `saveAsTable(Model.fqn())` by default, a custom `name(model)`, or `save(path(model))` for volumes and files.

## Errors

| Situation | Exception |
|-----------|-----------|
| `create()` outside `Base.dataset()` | `DatasetContextError` |
| Unsupported `integrity=` mode | `DatasetConfigurationError` |
| Model from another declarative base used in active dataset | `WrongDeclarativeBaseError` |
| Model does not extend `declarative_base()` | `InvalidDeclarativeBaseError` |
| Model primary key or foreign key metadata inconsistent | `InvalidModelDefinitionError` |
| Model declares reserved or `Model`-shadowing columns | `ReservedColumnError` |
| Unknown columns passed to `Model.create()` or constructor | `UnknownColumnError` |
| Table read before it is created in this dataset | `TableNotFoundError` |
| Unknown `.variant(name)` | `UnknownVariantError` |
| NOT NULL column missing or `None` in any generated row | `MissingRequiredColumnError` |
| Value does not match the declared Spark type | `ColumnTypeError` |
| Repeated primary key | `DuplicatePrimaryKeyError` |
| Declared foreign key with no parent row | `ForeignKeyViolationError` |
| Second declarative base claims a temp view name | `ViewCollisionError` |
| `Factory.count()` with a negative or non-integer value, or conflicting builder calls | `FactoryError` |
| `lazy()` given a non-callable | `LazyValueError` |
| Pool with no usable values | `EmptyPoolError` |
| `Pool.sample(k)` cannot sample without replacement | `PoolSampleError` |
| `Factory()` as column value for compound-PK parent | `CompoundPrimaryKeyError` |
| `.pk` used on a compound-PK model | `CompoundPrimaryKeyError` |
| `generator()` not implemented | `NotImplementedError` |

Validation runs on every generated row before it is committed.

## Unity Catalog integration

`StructField.metadata` holds UC column metadata. Use `metadata={"comment": "...", "tags": {...}}` for column comments and tags. `__comment__`, `__table_tags__`, `column_comments()`, `column_tags()` and `uc_tag_sql()` are available on the `Model` class - rowsmyth generates comment/tag SQL strings but does not execute catalog writes.

```python
for statement in User.uc_tag_sql():
    spark.sql(statement)


# Same Model class for Lakeflow / UC declaration:
# @dp.table(name=User.fqn(), comment=User.__comment__, schema=User.__definition__)
```

## Gotchas and non-goals

**Scale.** Driver-side row generation suits dev, test and seed volumes (thousands to low millions). For large-scale synthetic data, prefer vectorised tools.

**Flush before Spark reads.** Generation is Spark-free by design, so a temp view only exists after the table is read or flushed.

**Schema.** `__definition__` is always used with `createDataFrame`. Inference miss-handles `None` and Python `int` vs Spark `LongType`.

**Determinism.** `seed` fixes generated values. Spark may not preserve row order across partitions - sort if you need stable ordering.

**Uniqueness.** Use `ctx.faker.unique.*` or `ctx.sequence()` for unique columns; Faker's `.unique` resets each `Base.dataset()` block.

**FK cycles.** Inject-or-create recurses forever on cyclic `Factory()` graphs. Break cycles by seeding one table and using `ctx.pool()` for the back-reference.

**Ordering.** No topological sort. Call `.create()` in dependency order when using `pool()`; FK inject/create and deferred foreign key checks do not require it.

**Catalog.** `write_all()` writes tables or files on request; comments, tags and grants are never applied automatically.

**Out of scope.** Production-scale throughput, automatic cycle breaking, distributed generation and schema inference.

## Testing (library development)

Integration tests use a session-scoped local `SparkSession` (Java 17+). DataFrame assertions use [chispa](https://github.com/MrPowers/chispa). A counting `SparkSession` stand-in guards the core invariant - generation issues no round trips, and materialisation issues exactly one `createDataFrame` per table. See `tests/` and `CONTRIBUTING.md`. Local development installs PySpark via the `spark` uv dependency group / `rowsmyth[spark]` extra; end users on managed Spark typically install rowsmyth without that extra.
