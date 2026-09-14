# rowsmyth - usage guide

Full API reference and usage patterns. For a quick overview see the [README](../README.md); for internal design decisions see [design.md](design.md); for migrating from 1.x see the [v2 upgrade guide](upgrade-v2.md).

## Install

- **Local / CI:** `pip install "rowsmyth[spark]"` or `uv add "rowsmyth[spark]"` to install PySpark 4.0+ alongside rowsmyth.
- **Databricks / managed Spark:** `pip install rowsmyth` (no extra) and use the cluster's PySpark. rowsmyth requires PySpark 4.0+ at import time but does not pin it as a core dependency.

## Contents

- [Defining a table](#defining-a-table)
- [Variants](#variants)
- [Factory API](#factory-api)
- [Base.dataset() context manager](#basedataset-context-manager)
- [Reading generated data](#reading-generated-data)
- [Materialisation and temp views](#materialisation-and-temp-views)
- [RowCtx reference](#rowctx-reference)
- [Foreign keys and referential integrity](#foreign-keys-and-referential-integrity)
- [Writing output](#writing-output)
- [Databricks Lakeflow and Unity Catalog](#databricks-lakeflow-and-unity-catalog)
- [Generating test fixtures](#generating-test-fixtures)
- [Error reference](#error-reference)
- [Gotchas](#gotchas)

---

## Defining a table

Create a declarative base with `declarative_base()`, then subclass that base. Declare schema metadata as class attributes; implement `generator()` to return one row as a plain `dict`.

```python
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import declarative_base

Base = declarative_base()


class Role(Base):
    __table_name__ = "roles"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id",   LongType(),   False),
        StructField("name", StringType(), False),
    ])

    def generator(self, ctx):
        return {
            "id":   ctx.sequence(),
            "name": ctx.random.choice(["admin", "user", "guest"]),
        }
```

### Class attributes

| Attribute | Required | Type | Purpose |
|-----------|----------|------|---------|
| `__table_name__` | yes | `str` | Registry key and temp-view name |
| `__definition__` | yes | `StructType` | Column types, nullability and UC column metadata |
| `__primary_key__` | yes | `tuple[str, ...]` | One or more PK column names; uniqueness is enforced |
| `__foreign_keys__` | no | `dict[str, str]` | `{column: "parent_table.parent_column"}`; validated per dataset |
| `__catalog__` | no | `str \| None` | Unity Catalog catalog name; used by `Model.fqn()` |
| `__schema__` | no | `str \| None` | Schema name; used by `Model.fqn()` |
| `__comment__` | no | `str \| None` | Table comment for Unity Catalog / Lakeflow |
| `__table_tags__` | no | `dict[str, str]` | UC table tags |
| `__expectations__` | no | `dict[str, str]` | `{name: sql}` pairs for Lakeflow data quality expectations |

`Model.fqn()` joins the configured catalog parts in order. A model with both
`__catalog__` and `__schema__` returns `catalog.schema.table_name`; a partial
configuration returns the available prefix plus `__table_name__`.

Column names may not collide with the `Model` API (`key`, `pk`, `attrs`,
`create`, `factory`, `dataset`, `fqn`, `generator`, `registry`, ...) or start
with `__rowsmyth_`. Both raise `ReservedColumnError` when the class is defined.

### Registry

Every subclass that declares `__table_name__` is auto-registered in its declarative base registry. Omit `__table_name__` on a mixin or abstract intermediate base; concrete children still register normally.

```python
Base.registry["roles"]  # -> Role class
```

### Column metadata

Attach Unity Catalog column comments and column tags directly on `StructField`.
`uc_tag_sql()` can generate SQL for table comments, table tags, column comments
and column tags (see [Apply Unity Catalog metadata](#apply-unity-catalog-metadata-after-pipeline-run)).

```python
StructField("email", StringType(), False, metadata={
    "comment":  "Customer email, PII",
    "tags": {"pii": "true", "classification": "restricted"},
})
```

### Multi-column relationships in `generator()`

Because each row is a single `dict`, columns that depend on each other use ordinary local variables - no DSL needed:

```python
def generator(self, ctx):
    first = ctx.faker.first_name()
    last  = ctx.faker.last_name()
    return {
        "id":        ctx.sequence(),
        "full_name": f"{first} {last}",
        "email":     f"{first.lower()}.{last.lower()}@example.com",
    }
```

### Column types

Every value is checked against its declared Spark type before the row is
committed, so a mistake names its own table and column:

```
ColumnTypeError: roles.name: string column expects str, got int
```

| Spark type | Accepted Python types |
|------------|-----------------------|
| `StringType` | `str` |
| `BooleanType` | `bool` |
| `ByteType`, `ShortType`, `IntegerType`, `LongType` | `int` |
| `FloatType`, `DoubleType` | `float`, `int` |
| `DecimalType` | `decimal.Decimal` |
| `DateType` | `datetime.date` |
| `TimestampType`, `TimestampNTZType` | `datetime.datetime` |
| `DayTimeIntervalType` | `datetime.timedelta` |
| `BinaryType` | `bytes`, `bytearray` |
| `ArrayType` | `list`, `tuple` |
| `MapType` | `dict` |
| `StructType` | `Row`, `dict`, `list`, `tuple` |

Types not listed are not checked. `None` is allowed for nullable columns and
rejected for NOT NULL ones with `MissingRequiredColumnError`.

---

## Variants

A `@variant` method returns a partial `dict` that is **merged into** the row produced by `generator()`. It lets you describe named states (churn, suspension, premium tier) without duplicating the full row.

```python
from rowsmyth import declarative_base, variant

Base = declarative_base()


class User(Base):
    __table_name__ = "users"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id",     LongType(),   False),
        StructField("email",  StringType(), False),
        StructField("status", StringType(), False),
    ])

    def generator(self, ctx):
        return {
            "id":     ctx.sequence(),
            "email":  ctx.faker.unique.ascii_email(),
            "status": "active",
        }

    @variant
    def churned(self, ctx):
        return {"status": "inactive"}

    @variant
    def suspended(self, ctx):
        return {"status": "suspended"}
```

Activate with `.variant("churned")` in the factory chain. Passing an unknown name raises `UnknownVariantError`.

Merge order (later wins):
1. `generator()`
2. `@variant` return dict
3. `.where()` overrides

---

## Factory API

`Model.factory()` returns a `Factory`. Every method returns a **new** factory, so a configured factory is a reusable template - refining it never changes the factory it came from. Call `.create()` to generate rows and return root model instances.

```python
User.factory()
    .count(10)
    .variant("churned")
    .where(status="inactive", role_id=Role.factory())
    .has(Post.factory().count(3), via="author_id")
    .create()
```

```python
template = User.factory().count(10)
churned  = template.variant("churned").create()   # 10 churned users
regular  = template.create()                      # 10 ordinary users
```

### Methods

| Method | Description |
|--------|-------------|
| `count(n)` | Number of rows to generate (default: 1); `0` registers an empty table, negative/non-integer values raise `FactoryError` |
| `from_rows(rows)` | Generate one row per mapping, bypassing `generator()`; cannot be combined with `count()` or `variant()` |
| `variant(name)` | Apply a named `@variant`; raises `UnknownVariantError` if unknown |
| `where(**kwargs)` | Column overrides; merged last, wins over `generator()` and `@variant`. Values may be scalars, `Factory` instances, or `lazy(...)` callables |
| `has(child_factory, via=None)` | For each parent row, generate child rows and inject this parent. `via` names the injection slot (usually the FK column name) |
| `create()` | Generate all rows into the active dataset and return a list of concrete root model objects |

`.create()` must run inside a `Base.dataset(...)` block for the same declarative base, or it raises `DatasetContextError`. Using a model from another base raises `WrongDeclarativeBaseError`.

`.create()` does not touch Spark - see [Materialisation and temp views](#materialisation-and-temp-views).

### `Model.create()` for static rows

Use `Model.create(**cols)` for reference data that should exist before generated rows sample from `ctx.pool(...)`:

```python
with Base.dataset(spark, seed=42) as dataset:
    admin = Role.create(name="admin")
    user = Role.create(name="user")
    users = User.factory().count(20).create()

    role_ids = {admin.id, user.id}
    assert all(created_user.role_id in role_ids for created_user in users)
```

`Model.create()` returns one concrete model object. Explicit columns override defaults from `generator(ctx)`, so `Role.create(name="admin")` can still use `ctx.sequence()` from `generator()` for the primary key.

### `Model.bulk_create()` for precomputed rows

When the rows already exist as dicts - read from a file, computed by a loop, or
copied from a production sample - skip `generator()` entirely:

```python
rows = [{"id": i, "name": f"role-{i}"} for i in range(500)]

Role.bulk_create(rows)                       # one row per mapping
Role.bulk_create(rows, tenant="acme")        # plus the same override on every row
```

`Model.bulk_create(rows, **cols)` is `Model.factory().from_rows(rows).where(**cols).create()`. Each mapping is one complete row; validation, foreign key resolution and integrity checks run exactly as they do for generated rows. The input mappings are copied, never mutated.

`from_rows()` composes with `.has()` and `.where()`:

```python
Survey.factory().from_rows(survey_rows).has(Answer.factory().count(5), via="survey_id").create()
```

### `.where()` with `lazy()`

Pass `lazy(fn)` for deferred values that depend on the row context. rowsmyth never calls a bare callable - a class, builtin or `functools.partial` passed as a column value is data, not a generator:

```python
from rowsmyth import lazy

.where(
    score=lazy(lambda ctx: round(ctx.random.gauss(0.5, 0.15), 4)),
    grade=lazy(lambda ctx: "pass" if ctx.row["score"] >= 0.4 else "fail"),
)
```

Lazy values are resolved after `generator()` and `@variant`, so `ctx.row` contains the current in-progress attributes when the callable runs.

### `.has()` - parent/child relationships

Generate child rows for each parent:

```python
with Base.dataset(spark) as dataset:
    users = (
        User.factory()
        .count(5)
        .has(Post.factory().count(3), via="author_id")
        .create()
    )
# users - 5 user instances
# dataset.count("posts") - 15 rows (3 per user), each with the correct author_id
```

`via` names the slot used to inject the parent. In `Post.generator()`, reference it as:

```python
"author_id": User.factory()         # slot = "author_id" (column name)
# or:
"author_id": ctx.parent(User, role="author_id").pk
```

---

## `Base.dataset()` context manager

All `.create()` calls must run inside `Base.dataset(...)` for the declarative base that owns the models being created. It activates a session-scoped `ContextVar` so table creation, factories and FK resolution can find the active `SparkSession` and generators without threading them through every call.

```python
with Base.dataset(spark, seed=42) as dataset:
    Role.create(name="admin")
    users = (
        User.factory()
        .count(10)
        .has(Post.factory().count(3).where(published=True), via="author_id")
        .create()
    )
    users_df = dataset.dataframe("users")
# foreign keys are validated here, on a clean exit
```

### Options

| Argument | Default | Purpose |
|----------|---------|---------|
| `seed` | `None` | Seeds `dataset.random` and `dataset.faker` only |
| `integrity` | `"raise"` | `"raise"`, `"warn"` or `"off"` for primary and foreign key checks |
| `views` | `True` | Register a temp view per table when it materialises; `False` skips view registration entirely |
| `view_prefix` | `None` | Prefix for registered temp view names, e.g. `"fixture_"` |

### Seeding

When `seed` is provided, rowsmyth seeds only the active dataset state:

- `ctx.random` / `dataset.random` is a dedicated `random.Random(seed)` instance
- `ctx.faker` / `dataset.faker` is a dedicated Faker instance seeded with `seed`

Rowsmyth does not call global `random.seed()` or `Faker.seed()`. Use `ctx.random` and `ctx.faker` inside `generator()` for deterministic output. Avoid unseeded sources (`uuid4`, wall-clock timestamps) unless non-determinism is intentional.

### `Dataset` object

The object yielded by `with Base.dataset(...) as dataset`:

| Member | Type | Description |
|--------|------|-------------|
| `dataset.spark` | `SparkSession` | Active session |
| `dataset.base` | `type[Model]` | Declarative base bound to this dataset |
| `dataset.registry` | `dict[str, type[Model]]` | Registry for the bound declarative base |
| `dataset.faker` | `Faker` | Shared Faker instance |
| `dataset.random` | `random.Random` | Seeded RNG |
| `dataset.seed` | `int \| None` | Seed passed to `Base.dataset()` |
| `dataset.integrity` | `str` | Active integrity mode |
| `dataset.rows(name)` | `list[dict]` | Generated rows, no Spark |
| `dataset.count(name)` | `int` | Number of generated rows, no Spark |
| `dataset.table_names()` | `list[str]` | Tables created in this dataset, in creation order |
| `dataset.dataframe(name)` | `DataFrame` | Materialise and return one table |
| `dataset.tables()` | `dict[str, DataFrame]` | Materialise and return every table |
| `dataset.flush(name=None)` | `None` | Materialise pending rows and register temp views |
| `dataset.view_name(name)` | `str` | Temp view name used for a table |
| `dataset.write_all(...)` | `dict[str, str]` | Write every table; returns the destination used per table |
| `dataset.check_integrity()` | `None` | Run declared foreign key checks now |
| `dataset.next_seq(name)` | `int` | Next value for a named sequence counter |
| `dataset.pool(view, col)` | `Pool` | Distinct values from a dataset table or temp view |

---

## Reading generated data

Rows live in the driver as plain dicts, so reading back what you just generated costs nothing:

```python
with Base.dataset(spark, seed=42) as dataset:
    Survey.factory().count(60).create()

    dataset.count("surveys")                       # 60
    surveys = dataset.rows("surveys")              # list[dict]
    by_school = {row["school_id"]: row for row in surveys}
```

`rows()` returns copies, so mutating them cannot corrupt the dataset. Prefer it
over `dataset.dataframe(name).filter(...).collect()` for lookups against data
the fixture just created - the DataFrame path is a Spark action per call.

`dataset.dataframe(name)`, `dataset.rows(name)`, `dataset.count(name)` and
`dataset.flush(name)` raise `TableNotFoundError` for a table this dataset has
not created.

---

## Materialisation and temp views

`create()` appends rows in Python. Spark is touched only when a table is read:

| Trigger | Effect |
|---------|--------|
| `dataset.dataframe(name)` | One `createDataFrame` for that table, plus a temp view when `views=True` |
| `dataset.tables()` | The same, for every created table |
| `dataset.write_all(...)` | The same, then writes |
| `dataset.flush(name=None)` | The same, without returning anything |

A materialised table is cached until new rows are created for it, so repeated
reads are free. Creating *n* rows costs one `createDataFrame` per table no
matter how the rows were created.

Read a generated table through Spark directly - `spark.table()`, `spark.sql()`,
a Spark UDF - only after a flush:

```python
with Base.dataset(spark) as dataset:
    Role.create(name="admin")
    dataset.flush()                       # or dataset.flush("roles")
    spark.sql("SELECT * FROM roles").show()
```

Temp views use the bare `__table_name__`, not `fqn()`. Two declarative bases
that declare the same table name would overwrite each other's view, so rowsmyth
raises `ViewCollisionError` when a second base registers a view name another
base already owns in that session. Namespace one of them:

```python
with Base.dataset(spark, view_prefix="cdc2_") as dataset:
    ...
    dataset.view_name("surveys")   # "cdc2_surveys"
```

If you never query temp views, `views=False` skips registering them.

---

## RowCtx reference

`RowCtx` is passed to `generator()` and every `@variant` method. It gives access to generators, sequence counters, parent rows and pool sampling.

| Member | Description |
|--------|-------------|
| `ctx.faker` | Shared `Faker` instance |
| `ctx.random` | Seeded `random.Random` |
| `ctx.seed` | Seed from `Base.dataset()`, or `None` |
| `ctx.spark` | Active `SparkSession` |
| `ctx.dataset` | Active `Dataset` (for `ctx.dataset.rows(...)` and friends) |
| `ctx.index` | 0-based row index for the current factory |
| `ctx.row` | In-progress attribute dict (populated during resolution; useful in `lazy()` values) |
| `ctx.sequence(name=None)` | Monotonic counter; default name is `__table_name__` |
| `ctx.parent(table, role=None)` | Resolve or create a parent model object |
| `ctx.pool(view, col)` | Distinct values from a dataset table or temp view |

### `ctx.sequence()`

Returns an ever-increasing integer, distinct per named counter. Useful for surrogate keys:

```python
"id": ctx.sequence()                    # counter keyed to __table_name__
"order_num": ctx.sequence("order_num")  # named counter, shared across tables
```

### `ctx.faker` and `ctx.random`

```python
"email":  ctx.faker.unique.ascii_email()
"name":   ctx.faker.name()
"score":  ctx.random.uniform(0, 1)
"status": ctx.random.choices(["active", "inactive"], weights=[9, 1])[0]
```

`ctx.faker.unique` resets at the start of each `Base.dataset()` block.

---

## Foreign keys and referential integrity

### Declaring foreign keys

`__foreign_keys__` maps a column to `"parent_table.parent_column"`. Every
non-null value in that column must match a value created for the parent:

```python
class Answer(Base):
    __table_name__ = "answers"
    __primary_key__ = ("id",)
    __foreign_keys__ = {
        "survey_id":   "surveys.id",
        "question_id": "questions.id",
    }
```

Checks run when the dataset block exits cleanly, or whenever you call
`dataset.check_integrity()`. Because they are deferred, children may be created
before their parents. Violations raise `ForeignKeyViolationError` naming the
child column, the target and the offending values.

Primary keys are checked as rows are created: a repeated `__primary_key__` tuple
raises `DuplicatePrimaryKeyError` at the `create()` call that caused it.

### Integrity modes

```python
Base.dataset(spark, seed=42)                     # "raise" (default)
Base.dataset(spark, seed=42, integrity="warn")   # RowsmythWarning per violation
Base.dataset(spark, seed=42, integrity="off")    # no checks
```

`"warn"` is the migration mode: it reports every violation in one run instead of
stopping at the first.

### Pattern 1 - Factory as column value (single-column FK)

Return a `Factory` as a column value. Rowsmyth resolves it to the parent's primary key, creating a new parent row per child row if none has been injected via `.has()`:

```python
class OrderItem(Base):
    ...
    def generator(self, ctx):
        return {
            "id":       ctx.sequence(),
            "order_id": Order.factory(),  # creates one Order per item unless injected
            "qty":      ctx.random.randint(1, 5),
        }
```

Use `.has()` to share the same parent across a batch of children, or `ctx.pool()` for small reference tables (roles, statuses) seeded once at the start:

```python
# Reference tables: create first, sample with pool()
with Base.dataset(spark):
    Role.create(name="admin")
    Role.create(name="user")
    User.factory().count(20).create()  # User.generator() uses ctx.pool("roles", "id").choice()

# Parent/child: use .has() to wire the relationship
with Base.dataset(spark):
    orders = Order.factory().count(10).has(OrderItem.factory().count(3)).create()
```

The column name is the injection slot. Requires a **single-column** primary key on the parent; raises `CompoundPrimaryKeyError` for compound PKs (use `ctx.parent()` instead).

### Pattern 2 - `ctx.parent()` (compound or named FKs)

Use when you need multiple FK columns from the same parent, or when the parent has a compound PK:

```python
def generator(self, ctx):
    order = ctx.parent(Order)
    return {
        "order_id":     order.key["order_id"],
        "order_region": order.key["region"],
        "qty":          ctx.random.randint(1, 5),
    }
```

`order.key` - `dict` of PK columns
`order.pk` - scalar value (single-column PK only; raises `CompoundPrimaryKeyError` otherwise)
`order.attrs` - full row dict

Slot defaults to `table.__table_name__`; pass `role="slot_name"` to disambiguate multiple parents of the same type.

### Pattern 3 - `ctx.pool()` (sample existing values)

Read distinct non-null values from a column. Pools do **not** create rows:

```python
"role_id": ctx.pool("roles", "id").choice()
```

| Name | Source |
|------|--------|
| A table in this declarative base | Rows already generated in this dataset - pure Python, always current |
| Anything else | The Spark session (`spark.table(name)`), read once and cached for the dataset |

```python
pool = dataset.pool("roles", "id")
pool.values      # distinct non-null values, in creation order (tables) or sorted (views)
pool.choice()    # one value, chosen with the dataset RNG
pool.sample(3)   # k distinct values without replacement
```

Errors: `EmptyPoolError` when the table has no rows yet or the view has no
non-null values, `UnknownColumnError` for a column the table does not declare,
`PoolSampleError` when `k` exceeds the available values.

Create parents before dependants that pool from them. The inject-or-create FK
path does not require ordering.

### Disambiguation with `via`

When a child has multiple FKs to the same parent table, use `via` to name the slot:

```python
User.factory().has(Post.factory().count(3), via="author_id")
```

Then in `Post.generator()`:

```python
"author_id": User.factory()               # slot = "author_id" (column name)
# or:
author = ctx.parent(User, role="author_id")
"author_id": author.pk
```

---

## Writing output

`write_all()` writes every table the dataset created, so a new model never needs
a new line in a hand-maintained list:

```python
with Base.dataset(spark, seed=42) as dataset:
    Customer.factory().count(50).has(Order.factory().count(3), via="customer_id").create()

    # Managed tables, one per model, named by Model.fqn()
    dataset.write_all()

    # Custom table names
    dataset.write_all(name=lambda table: f"main.bronze.raw_{table.__table_name__}")

    # Files, e.g. a Unity Catalog volume
    dataset.write_all(
        format="parquet",
        options={"compression": "snappy"},
        path=lambda table: f"/Volumes/main/bronze/ingest/{table.__table_name__}/",
    )
```

| Argument | Default | Purpose |
|----------|---------|---------|
| `mode` | `"overwrite"` | Spark save mode |
| `format` | `None` | Spark format; omit for the session default |
| `options` | `None` | Extra writer options |
| `name` | `None` | `Model -> table name`; defaults to `Model.fqn()` |
| `path` | `None` | `Model -> path`; uses `save()` instead of `saveAsTable()` |

It returns `{table_name: destination}`. For a single table, use the DataFrame
directly:

```python
dataset.dataframe("users").write.mode("overwrite").saveAsTable(User.fqn())
```

---

## Databricks Lakeflow and Unity Catalog

A single `Model` subclass serves as the source of truth for your pipeline declaration, Unity Catalog metadata and test fixtures.

### Full table definition with Lakeflow metadata

```python
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import declarative_base, variant

Base = declarative_base()


class Customer(Base):
    __table_name__ = "customers"
    __catalog__ = "main"
    __schema__ = "commerce"
    __comment__ = "One row per customer account"
    __primary_key__ = ("id",)
    __table_tags__ = {"layer": "silver", "pii": "true"}
    __expectations__ = {
        "id_not_null":    "id IS NOT NULL",
        "email_not_null": "email IS NOT NULL",
        "valid_tier":     "tier IN ('standard', 'premium')",
    }
    __definition__ = StructType([
        StructField("id",    LongType(),   False),
        StructField("email", StringType(), False, metadata={
            "comment":  "Customer email, PII",
            "tags": {"pii": "true", "classification": "restricted"},
        }),
        StructField("tier",  StringType(), False),
    ])

    def generator(self, ctx):
        return {
            "id":    ctx.sequence(),
            "email": ctx.faker.unique.ascii_email(),
            "tier":  ctx.random.choices(["standard", "premium"], weights=[7, 3])[0],
        }

    @variant
    def premium(self, ctx):
        return {"tier": "premium"}
```

### Lakeflow pipeline declaration

Pass class attributes directly to the pipeline decorators - no duplication:

```python
from pyspark import pipelines as dp

from tables.customer import Customer


@dp.table(
    name=Customer.__table_name__,
    comment=Customer.__comment__,
    schema=Customer.__definition__,
)
@dp.expect_all_or_fail(Customer.__expectations__)
def customers():
    return spark.read.table("main.bronze.raw_customers")
```

`__expectations__` is a `dict[str, str]` - keys are constraint names, values are SQL expressions - which maps directly to `expect_all_or_fail`.

### Apply Unity Catalog metadata after pipeline run

rowsmyth stores metadata on the class but does not write to the catalog itself.
Generate comment and tag SQL in a notebook or job that runs after the pipeline:

```python
for statement in Customer.uc_tag_sql():
    spark.sql(statement)
```

### `Model.fqn()`

Returns the table name with any configured catalog/schema prefix:

```python
Customer.fqn()  # -> "main.commerce.customers"
```

---

## Generating test fixtures

Use rowsmyth to create deterministic seed data for integration tests against Lakeflow pipelines.

### Write to a Unity Catalog volume

The pipeline reads from a volume path; write fixture parquet there:

```python
from pyspark.sql import SparkSession

from tables.base import Base
from tables.customer import Customer

spark = SparkSession.builder.getOrCreate()

with Base.dataset(spark, seed=42) as dataset:
    Customer.factory().count(100).create()
    dataset.write_all(
        format="parquet",
        path=lambda table: f"/Volumes/main/bronze/ingest/raw_{table.__table_name__}/",
    )
```

### Write to persistent bronze tables

The pipeline reads from Unity Catalog tables; populate them directly:

```python
with Base.dataset(spark, seed=42) as dataset:
    Customer.factory().count(100).create()
    dataset.write_all(name=lambda table: f"main.bronze.raw_{table.__table_name__}")
```

### Multi-table fixture with related data

Create all tables the pipeline depends on in one session to maintain referential integrity:

```python
from tables.order import Order
from tables.order_item import OrderItem
from tables.customer import Customer

with Base.dataset(spark, seed=42) as dataset:
    Customer.factory().count(50).create()
    Order.factory().count(200).has(OrderItem.factory().count(3), via="order_id").create()
    dataset.write_all(name=lambda table: f"main.bronze.raw_{table.__table_name__}")
```

### Fixture from precomputed rows

Rows loaded from a file or computed up front skip `generator()` entirely:

```python
import json
from pathlib import Path

rows = json.loads(Path("fixtures/customers.json").read_text())

with Base.dataset(spark, seed=42) as dataset:
    Customer.bulk_create(rows)
    dataset.write_all(name=lambda table: f"main.bronze.raw_{table.__table_name__}")
```

### Fixture with variants

Use variants to generate a realistic mix of row states:

```python
with Base.dataset(spark, seed=42) as dataset:
    # 70 standard + 30 premium customers
    Customer.factory().count(70).create()
    Customer.factory().count(30).variant("premium").create()
    all_customers = dataset.dataframe("customers")
```

---

## Error reference

| Situation | Exception | Message |
|-----------|-----------|---------|
| `.create()` called outside `Base.dataset()` | `DatasetContextError` | `rowsmyth factories must be used inside Base.dataset(spark, ...)` |
| `Base.dataset(integrity=...)` with an unsupported mode | `DatasetConfigurationError` | `integrity must be one of ['raise', 'warn', 'off'], got {value!r}` |
| Model from another declarative base used in active dataset | `WrongDeclarativeBaseError` | `{model} belongs to a different declarative base than the active dataset` |
| Model does not extend `declarative_base()` | `InvalidDeclarativeBaseError` | `{model} must extend a rowsmyth declarative base created by declarative_base()` |
| Model declares reserved `__rowsmyth_*` columns | `ReservedColumnError` | `{table}: reserved rowsmyth columns: {cols}` |
| Model declares columns that shadow the `Model` API | `ReservedColumnError` | `{table}: column names collide with the Model API: {cols}` |
| `Model.create()` or model constructor with unknown columns | `UnknownColumnError` | `{table}: unknown columns: {cols}` |
| Table read before it is created in this dataset | `TableNotFoundError` | `{name!r} has not been created in this dataset` |
| Unknown `.variant(name)` | `UnknownVariantError` | `{table} has no variant {name!r}` |
| Model primary key references columns absent from `__definition__` | `InvalidModelDefinitionError` | `{table}: missing primary key columns: {cols}` |
| `__foreign_keys__` entry with an unknown column or malformed target | `InvalidModelDefinitionError` | `{table}: foreign key target for {col!r} must be 'table.column', got {target!r}` |
| `__foreign_keys__` target table or column missing at check time | `InvalidModelDefinitionError` | `{table}.{col}: foreign key target table {parent!r} is not registered on this declarative base` |
| NOT NULL column missing or `None` in any generated row | `MissingRequiredColumnError` | `{table}: NOT NULL columns without a value: {cols}` |
| Generated value does not match the declared Spark type | `ColumnTypeError` | `{table}.{col}: {type} column expects {types}, got {actual}` |
| Two rows share a primary key | `DuplicatePrimaryKeyError` | `{table}: duplicate primary key {cols}` |
| Declared foreign key value has no parent row | `ForeignKeyViolationError` | `{table}.{col} -> {target}: {n} value(s) have no matching parent row: {values}` |
| Two declarative bases register the same temp view | `ViewCollisionError` | `{table}: temp view {view!r} is already registered by declarative base {base!r} in this session` |
| `Factory.count()` with a negative or non-integer value | `FactoryError` | `Factory.count() requires a non-negative integer` |
| `from_rows()` combined with `count()` or `variant()` | `FactoryError` | `Factory.from_rows() cannot be combined with count()` |
| `lazy()` given a non-callable | `LazyValueError` | `lazy() requires a callable taking the row context, got {value!r}` |
| Pool over a table with no rows, or a view with no non-null values | `EmptyPoolError` | `pool({view!r}, {col!r}): no non-null values ...` |
| Pool over a column the table does not declare | `UnknownColumnError` | `pool({view!r}, {col!r}): {col!r} is not a column of {table}` |
| `pool.sample(k)` cannot sample without replacement | `PoolSampleError` | `pool({view!r}, {col!r}): cannot sample {k} values from {n} available values` |
| `Factory()` as value for compound-PK parent | `CompoundPrimaryKeyError` | `{table}: Factory() as column value requires single-column PK; use ctx.parent()` |
| `.pk` used on a compound-PK model | `CompoundPrimaryKeyError` | `{table}: pk requires a single-column primary key` |
| `generator()` not implemented | `NotImplementedError` | `{table} must implement generator()` |

Every domain error inherits `RowsmythError`. Warnings raised by
`integrity="warn"` inherit `RowsmythWarning`.

---

## Gotchas

**Scale.** Row generation runs in the Spark driver, row by row, and rows are
held in the driver until a table is read. This suits dev, test and seed volumes
(thousands to low millions). For large-scale synthetic data prefer vectorised
tools such as dbldatagen.

**Flush before Spark reads.** `create()` never touches Spark. Call
`dataset.flush()` before reading a generated table through `spark.table()` or
`spark.sql()` inside a dataset block.

**Schema.** `__definition__` is always passed to `createDataFrame`; inference is never used. It miss-handles `None` values and conflates Python `int` with Spark `LongType`.

**Determinism.** `seed` fixes generated values, but Spark does not guarantee row order across partitions. Sort the DataFrame if stable ordering matters.

**Uniqueness.** Use `ctx.faker.unique.*` or `ctx.sequence()` for columns with unique constraints. `ctx.faker.unique` resets at the start of each `Base.dataset()` block. Primary key uniqueness is enforced, so a colliding `rng.choice` fails instead of silently duplicating.

**External pools are snapshots.** Values pooled from a view outside the dataset are read once and cached for the block. Pools over dataset tables always see the latest generated rows.

**FK cycles.** Inject-or-create recurses forever on cyclic `Factory()` graphs. Break cycles by creating one side first, then referencing it with `ctx.pool()` for the back-reference.

**Create order.** There is no automatic topological sort. Call `.create()` in dependency order when using `ctx.pool()`. The inject-or-create FK path and declared `__foreign_keys__` checks do not require explicit ordering.

**Temp view names.** Views use the bare `__table_name__` (optionally prefixed with `view_prefix`), not the fully-qualified name.

**No catalog writes by default.** `write_all()` is the only API that writes; comments, tags and grants remain your responsibility.
