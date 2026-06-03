# Design & Architecture

Rowsmyth generates relational test and seed datasets as Spark DataFrames **row by row**, with referential integrity between tables. Generation runs in the driver; the library registers temp views in the active session. Writing to Unity Catalog (or elsewhere) is your responsibility.

## Influences

| Idea | Source |
|------|--------|
| Subclass `Model`, auto-register in a catalog | SQLAlchemy declarative bases |
| Faker, weights, column ergonomics | dbldatagen (but rowsmyth is not vectorized and supports cross-table FKs) |

dbldatagen optimises for throughput on a single DataFrame. Rowsmyth trades that for per-row control and real parent/child relationships.

## Public API

Import from `rowsmyth`:

| Symbol | Role |
|--------|------|
| `Model` | Declarative model base and `Model.registry` |
| `variant` | Decorator for named partial row overrides |
| `generate` | Context manager; activates a generation session |
| `Factory` | Fluent builder (`Model.factory()` is the usual entry) |
| `RowCtx` | Per-row context in `generator()` and variants |
| `Generation` | Object yielded by `generate()` (`spark`, `dataframes`, `faker`, `random`, `seed`) |
| `Pool` | Spark-backed distinct values from a temp view (`choice`, `sample`) |

`Model.create()` and `Factory.create()` must run inside `generate()`. Calling either outside raises `RuntimeError`.

## Package layout

```
src/rowsmyth/
  table.py       Model, variant, registry, fqn(), metadata helpers
  factory.py     Factory - fluent API and create()
  context.py     generate(), Generation, RowCtx
  resolution.py  FK resolution, row value resolution, validation
  pool.py        Pool and deferred pool tokens
```

## Defining a table

Subclass `Model`. Declare schema metadata on the class; implement `generator()` for one row.

```python
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import Model, variant


class Role(Model):
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


class User(Model):
    __table_name__ = "users"
    __catalog__ = "main"
    __schema__ = "app"
    __comment__ = "Application users"
    __primary_key__ = ("id",)
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
| `__primary_key__` | yes | Tuple of PK column names (one or more) |
| `__catalog__`, `__schema__` | no | Used by `Model.fqn()` |
| `__comment__` | no | Table comment for UC / Lakeflow |
| `__table_tags__` | no | `{key: value}` for UC table tags |
| `__expectations__` | no | `{name: sql}` dict for Lakeflow data quality expectations (`expect_all_or_fail`) |

### Registry and abstract bases

On subclass, if `__table_name__` is set, the class is registered in `Model.registry[__table_name__]` and `@variant` methods are collected into `_variants`.

If `__table_name__` is omitted (mixin or abstract intermediate base), the class is **not** registered. Concrete children still register normally.

## Factory API

```python
User.factory()
    .count(10)
    .variant("churned")       # KeyError if unknown
    .where(status="active")    # scalars, Factory FKs, or callables
    .has(Post.factory().count(3), via="author_id")
    .create()                   # list[User] + session DataFrames/temp views
```

| Method | Behaviour |
|--------|-----------|
| `count(n)` | Rows to generate for this table |
| `variant(name)` | Merge partial dict from `@variant` method |
| `where(**kwargs)` | Overrides; merged after variant |
| `has(child, via=None)` | For each parent row, generate child rows with parent injected |
| `create()` | Materialise all touched tables; register `createOrReplaceTempView(__table_name__)`; return root-table instances |

`.create()` returns concrete model objects for the root factory table. DataFrames for every touched table (parents created for FKs, children from `.has()` and the root table itself) are stored on the active `Generation` as `gen.dataframes` and are available via `gen.dataframe(name)`.

## Static row creation

Use `Model.create(**cols)` for reference data that should already exist before factories sample from a pool:

```python
with generate(spark, seed=42) as gen:
    admin = Role.create(name="admin")
    user = Role.create(name="user")
    users = User.factory().count(10).create()

    role_ids = {admin.id, user.id}
    assert all(created_user.role_id in role_ids for created_user in users)
    users_df = gen.dataframe("users")
```

`Model.create()` builds one row through the same `generator(ctx)`, sequence, callable, FK resolution and validation pipeline as factories. Explicit columns override generator defaults, so `Role.create(name="admin")` can rely on `generator()` for the `id` sequence.

## Row materialisation pipeline

For each row, `Factory.row()` runs:

1. `attrs = table().generator(ctx)`
2. If a variant is selected: `attrs.update(variant_method(table_instance, ctx))`
3. `attrs.update(factory._where)` - **`.where()` wins** over generator and variant
4. `ctx.row = attrs` (may still contain `Factory` instances or callables)
5. **Resolve** each value in `attrs`:
   - `Factory` → FK resolution (see below); slot = column name
   - `callable` → `value(ctx)`; siblings visible on `ctx.row`
6. **Validate** NOT NULL columns once per table per `generate()` session (first row only)

Variants are bound methods: `def churned(self, ctx) -> dict`.

## Foreign keys and referential integrity

### Factory as column value (single-column FK)

Return a parent `Factory` as the column value. Resolution uses slot = **column name** (or injected slot from `.has(..., via=...)`):

- If a model object exists in `ctx._parents` for that slot → use its primary key
- Otherwise → create one parent row (recursive), cache it, use its PK

```python
"role_id": Role.factory()
```

**Restriction:** the parent table must have a **single-column** primary key. For compound keys, `resolve_fk` raises `TypeError` with a message to use `ctx.parent()` instead.

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

Use `order.key` (dict of PK columns), `order.pk` (scalar, single-column PK only), or `order.attrs` (full row dict).

### `ctx.pool(view, col)`

Not an FK mode: read distinct values from an existing **temp view** in the session. `choice()` returns a deferred token that is resolved in Spark during commit using hidden rowsmyth columns, which are dropped before public DataFrames and temp views are registered. Raises `ValueError` if the view has no values.

```python
"role_id": ctx.pool("roles", "id").choice()
```

Create parents with `Model.create()` or `Factory.create()` before dependents that use `pool`. The inject-or-create FK path does not require ordering.

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

## `generate(spark, seed=None)`

Activates generation via a `ContextVar`. Factories and FK logic use the active session without threading `spark` through every call.

When `seed` is set, rowsmyth seeds only session-owned objects: `ctx.random` / `gen.random` and `ctx.faker` / `gen.faker`. It does not call global `random.seed()` or `Faker.seed()`.

Use `ctx.random` and `ctx.faker` for deterministic custom logic. Avoid unseeded sources (`uuid4`, wall clock) unless intentional.

```python
from rowsmyth import generate

with generate(spark, seed=42) as gen:
    Role.create(name="admin")
    Role.create(name="user")
    users = (
        User.factory()
        .count(10)
        .has(Post.factory().count(3).where(published=True), via="author_id")
        .create()
    )
    users_df = gen.dataframe("users")
    posts_df = gen.dataframe("posts")
    # temp views "users", "posts"; gen.spark is the active SparkSession

# Persist yourself, e.g.:
# users_df.write.mode("overwrite").saveAsTable(User.fqn())
```

### `RowCtx`

| Member | Description |
|--------|-------------|
| `ctx.faker` | Shared `Faker` instance |
| `ctx.random` | Seeded `random.Random` |
| `ctx.seed` | Seed passed to `generate()`, or `None` |
| `ctx.spark` | Active `SparkSession` |
| `ctx.index` | 0-based row index for the current factory |
| `ctx.row` | In-progress attribute dict (for callables and FK resolution) |
| `ctx.sequence(name=None)` | Monotonic counter; default name = current `__table_name__` |
| `ctx.parent(table, role=None)` | Resolve parent model object |
| `ctx.pool(view, col)` | Spark-backed pool from a temp view |

### `Generation`

Yielded by `generate()`. Exposes `spark`, `dataframes`, `dataframe(name)`, `faker`, `random` and `seed`. Internal rows, counters, deferred pool tokens and validation state are not part of the public contract.

## Create output

For each table name touched by `Model.create()` or `Factory.create()`:

1. `createDataFrame(rows, Model.registry[name].__definition__)` - schema is **always** explicit; never inferred
2. `createOrReplaceTempView(__table_name__)`
3. Store the DataFrame in `gen.dataframes[name]`

Temp views use the bare `__table_name__` (not `fqn()`). Factories return instances, not DataFrames.

## Errors

| Situation | Exception |
|-----------|-----------|
| `create()` outside `generate()` | `RuntimeError` |
| `Model.create()` with unknown columns | `ValueError` |
| Unknown `.variant(name)` | `KeyError` |
| NOT NULL column missing on first row of a table | `ValueError` |
| `ctx.pool` on empty view | `ValueError` |
| `Factory()` as column value for compound-PK parent | `TypeError` |
| `generator()` not implemented | `NotImplementedError` |

Validation runs on the **first row** produced for each table in a `generate()` block, then is skipped for subsequent rows of that table.

## Unity Catalog integration

`StructField.metadata` holds UC column metadata. Use `metadata={"comment": "...", "tags": {...}}` for column comments and tags. `__comment__`, `__table_tags__`, `column_comments()`, `column_tags()` and `uc_tag_sql()` are available on the `Model` class - rowsmyth generates SQL strings but does not execute catalog writes.

```python
for statement in User.uc_tag_sql():
    spark.sql(statement)


# Same Model class for Lakeflow / UC declaration:
# @dp.table(name=User.fqn(), comment=User.__comment__, schema=User.__definition__)
```

## Gotchas and non-goals

**Scale.** Driver-side row generation suits dev, test and seed volumes (thousands to low millions). For large-scale synthetic data, prefer vectorised tools.

**Schema.** Always use `__definition__` with `createDataFrame`. Inference miss-handles `None` and Python `int` vs Spark `LongType`.

**Determinism.** `seed` fixes generated values. Spark may not preserve row order across partitions - sort if you need stable ordering.

**Uniqueness.** Use `ctx.faker.unique.*` or `ctx.sequence()` for unique columns; Faker’s `.unique` resets each `generate()` block.

**FK cycles.** Inject-or-create recurses forever on cyclic `Factory()` graphs. Break cycles by seeding one table and using `ctx.pool()` for the back-reference.

**Ordering.** No topological sort. Call `.create()` in dependency order when using `pool()`; FK inject/create does not require it.

**Catalog.** No `saveAsTable`, tags, or grants - only temp views.

**Out of scope.** Production-scale throughput, automatic cycle breaking, distributed generation and schema inference.

## Testing (library development)

Integration tests use a session-scoped local `SparkSession` (Java 17+). DataFrame assertions use [chispa](https://github.com/MrPowers/chispa). Unit tests mock Spark where a JVM is not required. See `tests/` and `CONTRIBUTING.md`.
