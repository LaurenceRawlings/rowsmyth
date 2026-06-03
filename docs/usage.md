# rowsmyth - usage guide

Full API reference and usage patterns. For a quick overview see the [README](../README.md); for internal design decisions see [design.md](design.md).

## Contents

- [Defining a table](#defining-a-table)
- [Variants](#variants)
- [Factory API](#factory-api)
- [Base.dataset() context manager](#basedataset-context-manager)
- [RowCtx reference](#rowctx-reference)
- [Foreign keys and referential integrity](#foreign-keys-and-referential-integrity)
- [Create output](#create-output)
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
| `__primary_key__` | yes | `tuple[str, ...]` | One or more PK column names |
| `__catalog__` | no | `str \| None` | Unity Catalog catalog name; used by `Model.fqn()` |
| `__schema__` | no | `str \| None` | Schema name; used by `Model.fqn()` |
| `__comment__` | no | `str \| None` | Table comment for Unity Catalog / Lakeflow |
| `__table_tags__` | no | `dict[str, str]` | UC table tags |
| `__expectations__` | no | `dict[str, str]` | `{name: sql}` pairs for Lakeflow data quality expectations |

`Model.fqn()` returns `catalog.schema.table_name` when both `__catalog__` and `__schema__` are set, otherwise just `__table_name__`.

### Registry

Every subclass that declares `__table_name__` is auto-registered in its declarative base registry. Omit `__table_name__` on a mixin or abstract intermediate base; concrete children still register normally.

```python
Base.registry["roles"]  # -> Role class
```

### Column metadata

Attach Unity Catalog column comments and column tags directly on `StructField`. The comment is applied automatically when the table is created from the schema; column tags are applied separately (see [Apply Unity Catalog tags](#apply-unity-catalog-tags-after-pipeline-run)).

```python
StructField("email", StringType(), False, metadata={
    "comment":  "Customer email, PII",
    "tags": {"pii": "true", "classification": "restricted"},
})
```

### Multi-column relationships in `definition()`

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

Activate with `.variant("churned")` in the factory chain. Passing an unknown name raises `KeyError`.

Merge order (later wins):
1. `generator()`
2. `@variant` return dict
3. `.where()` overrides

---

## Factory API

`Model.factory()` returns a `Factory` instance. All methods return `self` for chaining; call `.create()` to materialise rows and return root model instances.

```python
User.factory()
    .count(10)
    .variant("churned")
    .where(status="inactive", role_id=Role.factory())
    .has(Post.factory().count(3), via="author_id")
    .create()
```

### Methods

| Method | Description |
|--------|-------------|
| `count(n)` | Number of rows to generate (default: 1) |
| `variant(name)` | Apply a named `@variant`; raises `KeyError` if unknown |
| `where(**kwargs)` | Column overrides; merged last, wins over `generator()` and `@variant`. Values may be scalars, `Factory` instances, deferred pool choices, or `callable(ctx) -> value` |
| `has(child_factory, via=None)` | For each parent row, generate child rows and inject this parent. `via` names the injection slot (usually the FK column name) |
| `create()` | Materialise all touched tables; register temp views; return a list of concrete root model objects |

`.create()` must run inside a `Base.dataset(...)` block for the same declarative base, or it raises `RuntimeError`.

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

`Model.create()` returns one concrete model object and immediately updates `dataset.dataframes[table_name]` plus the matching temp view. Explicit columns override defaults from `generator(ctx)`, so `Role.create(name="admin")` can still use `ctx.sequence()` from `generator()` for the primary key.

### `.where()` with callables

Pass a callable that receives `RowCtx` for deferred/dependent values:

```python
.where(
    score=lambda ctx: round(ctx.random.gauss(0.5, 0.15), 4),
    grade=lambda ctx: "pass" if ctx.row["score"] >= 0.4 else "fail",
)
```

Callables are resolved after `generator()` and `@variant`, so `ctx.row` contains the current in-progress attributes when the callable runs.

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
# dataset.dataframe("posts") - 15 rows (3 per user), each with the correct author_id
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
    Role.create(name="user")
    users = (
        User.factory()
        .count(10)
        .has(Post.factory().count(3).where(published=True), via="author_id")
        .create()
    )
    users_df = dataset.dataframe("users")
    posts_df = dataset.dataframe("posts")
    # dataset.spark, dataset.faker, dataset.random, dataset.seed - shared state
```

### Seeding

When `seed` is provided, rowsmyth seeds only the active dataset state:

- `ctx.random` / `dataset.random` is a dedicated `random.Random(seed)` instance
- `ctx.faker` / `dataset.faker` is a dedicated Faker instance seeded with `seed`

Rowsmyth does not call global `random.seed()` or `Faker.seed()`. Use `ctx.random` and `ctx.faker` inside `generator()` for deterministic output. Avoid unseeded sources (`uuid4`, wall-clock timestamps) unless non-determinism is intentional.

### `Dataset` object

The object yielded by `with Base.dataset(...) as dataset`:

| Attribute | Type | Description |
|-----------|------|-------------|
| `dataset.spark` | `SparkSession` | Active session |
| `dataset.base` | `type[Model]` | Declarative base bound to this dataset |
| `dataset.registry` | `dict[str, type[Model]]` | Registry for the bound declarative base |
| `dataset.dataframes` | `dict[str, DataFrame]` | DataFrames created in this dataset |
| `dataset.dataframe(name)` | `DataFrame` | Checked lookup by table name |
| `dataset.faker` | `Faker` | Shared Faker instance |
| `dataset.random` | `random.Random` | Seeded RNG |
| `dataset.seed` | `int \| None` | Seed passed to `Base.dataset()` |

---

## RowCtx reference

`RowCtx` is passed to `generator()` and every `@variant` method. It gives access to generators, sequence counters, parent rows and pool sampling.

| Member | Description |
|--------|-------------|
| `ctx.faker` | Shared `Faker` instance |
| `ctx.random` | Seeded `random.Random` |
| `ctx.seed` | Seed from `Base.dataset()`, or `None` |
| `ctx.spark` | Active `SparkSession` |
| `ctx.index` | 0-based row index for the current factory |
| `ctx.row` | In-progress attribute dict (populated during resolution; useful in callables) |
| `ctx.sequence(name=None)` | Monotonic counter; default name is `__table_name__` |
| `ctx.parent(table, role=None)` | Resolve or create a parent model object |
| `ctx.pool(view, col)` | Spark-backed pool of distinct values from a temp view |

### `ctx.sequence()`

Returns an ever-increasing integer, distinct per named counter. Useful for surrogate keys:

```python
"id": ctx.sequence()          # counter keyed to __table_name__
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

The column name is the injection slot. Requires a **single-column** primary key on the parent; raises `TypeError` for compound PKs (use `ctx.parent()` instead).

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
`order.pk` - scalar value (single-column PK only; raises `ValueError` otherwise)
`order.attrs` - full row dict

Slot defaults to `table.__table_name__`; pass `role="slot_name"` to disambiguate multiple parents of the same type.

### Pattern 3 - `ctx.pool()` (sample from an existing temp view)

Read distinct values from a temp view already registered in the session. Does **not** create rows:

```python
with Base.dataset(spark):
    Role.create(name="admin")             # must create first
    Role.create(name="user")
    users = User.factory().count(20).create()  # pool() safe to use now

# In User.generator():
"role_id": ctx.pool("roles", "id").choice()
```

`pool.choice()` - a deferred value resolved against Spark during commit
`pool.sample(k)` - immediate `k` distinct values without replacement
Raises `ValueError` if the view is empty.

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

## Create output

`Factory.create()` returns concrete model objects for the root table. DataFrames for every table touched in the factory tree (parents created for FKs, children from `.has()` and the root table itself) are stored on the active dataset.

For each table:

1. `createDataFrame(rows, Model.__definition__)` - schema is **always** explicit; inference is never used
2. `createOrReplaceTempView(__table_name__)` - bare name, not the fully qualified name
3. DataFrame stored in `dataset.dataframes[__table_name__]`

```python
with Base.dataset(spark, seed=42) as dataset:
    users = (
        User.factory()
        .count(10)
        .has(Post.factory().count(3), via="author_id")
        .create()
    )

users                 # list[User], 10 users
dataset.dataframe("users")  # DataFrame, 10 rows
dataset.dataframe("posts")  # DataFrame, 30 rows (3 per user)
```

Persist to Unity Catalog yourself:

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

### Apply Unity Catalog tags after pipeline run

rowsmyth stores metadata on the class but does not write to the catalog itself. Generate tag SQL in a notebook or job that runs after the pipeline:

```python
for statement in Customer.uc_tag_sql():
    spark.sql(statement)
```

### `Model.fqn()`

Returns the fully-qualified catalog name when `__catalog__` and `__schema__` are both set:

```python
Customer.fqn()  # → "main.commerce.customers"
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
    customers_df = dataset.dataframe("customers")

customers_df.write.mode("overwrite").parquet(
    "/Volumes/main/bronze/ingest/raw_customers/"
)
```

### Write to a persistent bronze table

The pipeline reads from a Unity Catalog table; populate it directly:

```python
with Base.dataset(spark, seed=42) as dataset:
    Customer.factory().count(100).create()
    customers_df = dataset.dataframe("customers")

customers_df.write.mode("overwrite").saveAsTable("main.bronze.raw_customers")
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
    customers_df = dataset.dataframe("customers")
    orders_df = dataset.dataframe("orders")
    order_items_df = dataset.dataframe("order_items")

customers_df.write.mode("overwrite").saveAsTable(
    "main.bronze.raw_customers"
)
orders_df.write.mode("overwrite").saveAsTable("main.bronze.raw_orders")
order_items_df.write.mode("overwrite").saveAsTable(
    "main.bronze.raw_order_items"
)
```

### Fixture with variants

Use variants to generate a realistic mix of row states:

```python
with Base.dataset(spark, seed=42) as dataset:
    # 70 standard + 30 premium customers
    Customer.factory().count(70).create()
    Customer.factory().count(30).variant("premium").create()
    all_customers = dataset.dataframe("customers")

all_customers.write.mode("overwrite").saveAsTable("main.bronze.raw_customers")
```

---

## Error reference

| Situation | Exception | Message |
|-----------|-----------|---------|
| `.create()` called outside `Base.dataset()` | `RuntimeError` | `rowsmyth factories must be used inside Base.dataset(spark, ...)` |
| Model from another declarative base used in active dataset | `WrongDeclarativeBaseError` | `{model} belongs to a different declarative base than the active dataset` |
| `Model.create()` with unknown columns | `ValueError` | `{table}: unknown columns: {cols}` |
| Unknown `.variant(name)` | `KeyError` | `{table} has no variant {name!r}` |
| NOT NULL column missing on first row | `ValueError` | `{table}: NOT NULL columns without a value: {cols}` |
| `ctx.pool()` on empty temp view | `ValueError` | `pool({view!r}, {col!r}): no values in temp view` |
| `Factory()` as value for compound-PK parent | `TypeError` | `{table}: Factory() as column value requires single-column PK; use ctx.parent()` |
| `generator()` not implemented | `NotImplementedError` | `{table} must implement generator()` |

NOT NULL validation runs on the **first** row produced for each table in a `Base.dataset()` block, then is skipped for subsequent rows.

---

## Gotchas

**Scale.** Row generation runs in the Spark driver, row by row. This suits dev, test and seed volumes (thousands to low millions). For large-scale synthetic data prefer vectorised tools such as dbldatagen.

**Schema.** Always pass `__definition__` to `createDataFrame`. Schema inference miss-handles `None` values and conflates Python `int` with Spark `LongType`.

**Determinism.** `seed` fixes generated values, but Spark does not guarantee row order across partitions. Sort the DataFrame if stable ordering matters.

**Uniqueness.** Use `ctx.faker.unique.*` or `ctx.sequence()` for columns with unique constraints. `ctx.faker.unique` resets at the start of each `Base.dataset()` block.

**FK cycles.** Inject-or-create recurses forever on cyclic `Factory()` graphs. Break cycles by creating one side with `.create()` first, then referencing it with `ctx.pool()` for the back-reference.

**Create order.** There is no automatic topological sort. Call `.create()` in dependency order when using `ctx.pool()`. The inject-or-create FK path does not require explicit ordering.

**Temp view names.** `createOrReplaceTempView` uses the bare `__table_name__`, not the fully-qualified name. Queries against temp views must use the bare name.

**No catalog writes.** rowsmyth only creates temp views. Writing to Unity Catalog tables, volumes, or applying tags is always your responsibility.
