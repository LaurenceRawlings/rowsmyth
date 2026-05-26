# rowsmyth

**Forged test data for SQLAlchemy models - no boilerplate required.**

A blacksmith forges metal. A rowsmyth forges rows - mythical ones that exist only in your tests. Pronounced *row smith*, `rowsmyth` is a thin orchestration layer on top of [SQLAlchemy](https://www.sqlalchemy.org/) and [factory-boy](https://factoryboy.readthedocs.io/) that eliminates the ceremony of wiring up test factories. Define generators once, co-located with your models. Generate hierarchical or flat datasets with a single call.

```python
from rowsmyth import declarative_base, variant
import factory

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)
    tier: Mapped[str] = mapped_column(String)
    orders: Mapped[list["Order"]] = relationship(back_populates="user")

    @classmethod
    def generators(cls):
        return {
            cls.name: factory.Faker("name"),
            cls.tier: factory.fuzzy.FuzzyChoice(["standard", "premium"]),
        }

    @variant
    def admin(cls):
        return {cls.name: "admin", cls.tier: "premium"}


# Order and OrderItem are defined in the Complete Example section below

# 20 users, each with 1-5 orders, each order with 1-4 items
users = (
    User
    .factory(20)
    .has(Order.factory(1, 5).has(OrderItem.factory(1, 4)))
    .mix(admin=0.1)
    .random_seed(42)
    .create()
)
```

---

## The Problem with factory-boy + SQLAlchemy

factory-boy is excellent at generating realistic values, but wiring it up to SQLAlchemy for multi-table test datasets requires a lot of scaffolding:

```python
# The old way - three separate files just to generate related rows


class UserFactory(SQLAlchemyModelFactory):
    class Meta:
        model = User
        sqlalchemy_session = Session
        sqlalchemy_session_persistence = "commit"

    name = factory.Faker("name")


class OrderFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Order
        sqlalchemy_session = Session
        sqlalchemy_session_persistence = "commit"

    total = factory.Faker("pyfloat", positive=True, max_value=500)
    user = factory.SubFactory(UserFactory)  # creates a NEW user every time


class OrderItemFactory(SQLAlchemyModelFactory):
    class Meta:
        model = OrderItem
        sqlalchemy_session = Session
        sqlalchemy_session_persistence = "commit"

    sku = factory.Faker("ean13")
    order = factory.SubFactory(OrderFactory)  # creates a NEW order every time


# Manual loops to avoid the SubFactory explosion
for _ in range(10):
    user = UserFactory()
    for _ in range(random.randint(1, 5)):
        order = OrderFactory(user=user)
        OrderItemFactory.create_batch(random.randint(1, 4), order=order)
```

Every project ends up maintaining a parallel hierarchy of factory classes, manual session management and hand-rolled loops to control which rows share foreign keys.

`rowsmyth` handles all of this automatically.

---

## Installation

```bash
uv add rowsmyth
```

With PySpark schema support:

```bash
uv add "rowsmyth[spark]"
```

**Requirements:** Python ≥ 3.12, SQLAlchemy ≥ 2.0, factory-boy ≥ 3.3

---

## Core Concepts

### `declarative_base()`

A drop-in replacement for SQLAlchemy's `declarative_base()`. Returns a base class with `rowsmyth` capabilities mixed in - no changes to your model definitions required.

```python
from rowsmyth import declarative_base

Base = declarative_base()
```

### `generators()` - co-located factory declarations

Instead of a separate factory class, define value generators directly on the model:

```python
class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)
    price: Mapped[float]
    category: Mapped[str] = mapped_column(String)

    @classmethod
    def generators(cls):
        return {
            cls.name: factory.Faker("ecommerce_name"),
            cls.price: factory.Faker("pyfloat", positive=True, max_value=999),
            cls.category: factory.fuzzy.FuzzyChoice([
                "electronics",
                "clothing",
                "food",
            ]),
        }
```

Keys are column attributes (`cls.name`, `cls.price`) - not strings. Values are any factory-boy declaration.

### `@variant` - named model variants

Variants override specific generators for a named sub-type of the model:

```python
class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)
    is_active: Mapped[bool]

    @classmethod
    def generators(cls):
        return {
            cls.name: factory.Faker("name"),
            cls.role: "user",
            cls.is_active: True,
        }

    @variant
    def admin(cls):
        return {cls.role: "admin"}

    @variant
    def suspended(cls):
        return {cls.is_active: False}
```

Variants are applied on top of `generators()` - you only override what changes.

---

## Generating Data

### `FactoryBuilder` - hierarchical datasets

Use `Model.factory(n)` to build a `FactoryBuilder`. Call `.create()` to execute and get back a list of the root model instances.

```python
# Exact count
users = User.factory(50).create()

# Random count in range
users = User.factory(40, 60).create()
```

#### `.has()` - parent → child relationships

Chain `.has()` to attach child builders. Foreign keys are resolved automatically from SQLAlchemy's relationship metadata:

```python
users = User.factory(20).has(Order.factory(1, 5)).create()
# 20 users, each with 1-5 orders. Each order's user_id is set automatically.
```

Chains can be arbitrarily deep:

```python
users = User.factory(20).has(Order.factory(1, 5).has(OrderItem.factory(1, 4))).create()
```

Multiple child types at the same level:

```python
users = (
    User
    .factory(20)
    .has(
        Order.factory(1, 5),
        Address.factory(1, 3),
    )
    .create()
)
```

When a model has multiple relationships to the same parent, pass `via=` to disambiguate:

```python
User.factory(10).has(Message.factory(5, via="sent_messages")).create()
```

#### `.mix()` - probabilistic variants

Distribute rows across variants using proportions. Proportions must sum to ≤ 1.0; the remainder uses `generators()` defaults:

```python
# 10% admin, 5% suspended, 85% default
users = User.factory(100).mix(admin=0.1, suspended=0.05).create()
```

#### `.where()` - field overrides

Force specific values on every generated row. Overrides always win over variants and generators:

```python
# All users are in the "enterprise" tier
users = User.factory(50).where({User.tier: "enterprise"}).create()
```

#### `.random_seed()` - reproducible output

Seed the RNG and Faker for deterministic datasets:

```python
users = User.factory(100).mix(admin=0.2).random_seed(42).create()
```

Running the same code twice with the same seed produces identical rows.

---

### `Dataset` - flat multi-table datasets

When tables reference each other laterally (not hierarchically), use `Base.dataset()`. It handles topological ordering and automatically samples foreign keys from already-created rows:

```python
data = (
    Base
    .dataset(
        Customer.factory(20).mix(premium=0.3),
        Product.factory(50),
        Order.factory(200),
    )
    .random_seed(42)
    .create()
)

# Returns a dict keyed by tablename:
# {
#   "customers": [...],
#   "products":  [...],
#   "orders":    [...],   # each order's customer_id points to a real customer
# }
```

`Dataset` creates rows in dependency order, then randomly samples from the created pool when injecting foreign keys. All rows share one in-memory SQLite session.

#### Seeding reference data

Pass raw model instances directly to `Base.dataset()` to seed reference/lookup tables with predetermined rows. Seeded instances are committed to the session first and added to the FK pool so all factories can wire to them:

```python
statuses = [
    Status(code="draft"),
    Status(code="active"),
    Status(code="archived"),
]

data = (
    Base
    .dataset(
        *statuses,
        Article.factory(100),
    )
    .random_seed(42)
    .create()
)

# data["statuses"]  -> the 3 seeded Status rows
# data["articles"]  -> 100 Article rows, each article.status_code points to one of the 3
```

Any mix of raw instances and `FactoryBuilder`s is accepted. Seeded rows always appear before factory-generated rows for the same table in the result dict.

---

## Model Metadata

`rowsmyth` exposes standard SQLAlchemy `comment` and `info` fields as convenient classproperties. This is useful for data catalogues, data quality tooling and documentation generation.

```python
class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("tier IN ('standard', 'premium')", name="ck_users_tier"),
        {
            "comment": "Stores application users",
            "info": {"domain": "auth", "owner": "auth-team"},
        },
    )

    id: Mapped[int] = mapped_column(primary_key=True, info={"pii": False})
    name: Mapped[str] = mapped_column(
        String,
        comment="Display name",
        info={"pii": True, "owner": "auth-team"},
    )
    tier: Mapped[str] = mapped_column(String)
```

```python
User.__comment__
# "Stores application users"

User.__table_info__
# {"domain": "auth", "owner": "auth-team"}

User.__column_info__
# {
#   "id":   {"pii": False},
#   "name": {"pii": True, "owner": "auth-team"},
#   "tier": {},
# }

User.__expectations__
# {"ck_users_tier": "tier IN ('standard', 'premium')"}
```

These are read directly from SQLAlchemy's `Table.comment`, `Table.info`, `Column.comment`, `Column.info` and `CheckConstraint` objects - no duplication required.

---

## PySpark Schema Integration

When working with Spark pipelines, `__spark_schema__` converts your SQLAlchemy model to a PySpark `StructType` automatically:

```python
# uv add "rowsmyth[spark]"

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([], schema=User.__spark_schema__)
```

Supported type mappings: `SmallInteger` → `ShortType`, `Integer` → `IntegerType`, `BigInteger` → `LongType`, `String/Text/Unicode/UnicodeText` → `StringType`, `Double` → `DoubleType`, `Float` → `FloatType`, `Numeric(p,s)` → `DecimalType(p,s)`, `Boolean` → `BooleanType`, `DateTime` → `TimestampType`, `Date` → `DateType`, `Uuid` → `StringType`, `LargeBinary` → `BinaryType`.

Nullable columns are mapped to nullable Spark fields. Column-level `info` dict is merged into the Spark field metadata.

---

## Complete Example

```python
import uuid
import factory
import factory.fuzzy
from sqlalchemy import CheckConstraint, ForeignKey, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship
from rowsmyth import declarative_base, variant

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("tier IN ('standard', 'premium')", name="ck_users_tier"),
        {"comment": "Stores application users", "info": {"domain": "auth"}},
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(
        String, comment="Display name", info={"pii": True}
    )
    tier: Mapped[str] = mapped_column(String)

    orders: Mapped[list["Order"]] = relationship(back_populates="user")

    @classmethod
    def generators(cls):
        return {
            cls.name: factory.Faker("name"),
            cls.tier: factory.fuzzy.FuzzyChoice(["standard", "premium"]),
        }

    @variant
    def admin(cls):
        return {cls.name: "admin", cls.tier: "premium"}


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    total: Mapped[float]
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey(User.id))

    user: Mapped["User"] = relationship(back_populates="orders")
    items: Mapped[list["OrderItem"]] = relationship(back_populates="order")

    @classmethod
    def generators(cls):
        return {
            cls.total: factory.Faker("pyfloat", positive=True, max_value=500),
        }


class OrderItem(Base):
    __tablename__ = "order_items"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    sku: Mapped[str] = mapped_column(String)
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey(Order.id))

    order: Mapped["Order"] = relationship(back_populates="items")

    @classmethod
    def generators(cls):
        return {cls.sku: factory.Faker("ean13")}


# Hierarchical: 20 users → 1-5 orders each → 1-4 items each
users = (
    User
    .factory(20)
    .has(Order.factory(1, 5).has(OrderItem.factory(1, 4)))
    .mix(admin=0.1)
    .random_seed(42)
    .create()
)

# Flat: independent tables, FKs sampled from pool
data = (
    Base
    .dataset(
        User.factory(20).mix(admin=0.1),
        Order.factory(50, 100),
        OrderItem.factory(200, 400),
    )
    .random_seed(42)
    .create()
)
```

---

## API Reference

### `declarative_base(metadata=None, type_annotation_map=None, registry=None)`

Returns a SQLAlchemy declarative base with `rowsmyth` capabilities mixed in.

| Argument | Type | Description |
|----------|------|-------------|
| `metadata` | `MetaData \| None` | Shared `MetaData` instance for all models. |
| `type_annotation_map` | `dict \| None` | Extra Python type → SQLAlchemy type mappings for `Mapped[]` annotations. |
| `registry` | `registry \| None` | Pre-existing mapper registry to share across multiple bases. |

### `@variant`

Decorator for methods in a model class body. The method receives `cls` and returns a dict in the same shape as `generators()`. Collected into `cls.__variants__`.

### `Model.factory(n)` / `Model.factory(min, max)`

Returns a `FactoryBuilder` for the model. `n` generates exactly n rows; `(min, max)` generates a random count in that range per parent.

### `FactoryBuilder`

| Method | Description |
|--------|-------------|
| `.has(*builders, via=None)` | Attach child builders. Use `via="rel_name"` to disambiguate multiple relationships to the same parent. |
| `.mix(**proportions)` | Variant distribution. Values are proportions (0.0-1.0), must sum to ≤ 1.0. |
| `.where(overrides)` | Force field values. `{Model.column: value}`. |
| `.random_seed(value)` | Seed `random` and `Faker` for reproducibility. |
| `.create()` | Execute. Returns `list[RootModel]`. Creates an in-memory SQLite DB automatically. |

### `Base.dataset(*args)`

Returns a `Dataset` for flat multi-table generation. `args` may be any mix of `FactoryBuilder` instances and raw model instances (for seeding reference data).

| Method | Description |
|--------|-------------|
| `.random_seed(value)` | Seed `random` and `Faker`. |
| `.create()` | Execute. Returns `dict[str, list[Model]]` keyed by `__tablename__`. |

### Model classproperties

| Property | Type | Description |
|----------|------|-------------|
| `__comment__` | `str \| None` | Table-level comment from `__table_args__`. |
| `__table_info__` | `dict` | Table-level `info` dict from `__table_args__`. |
| `__column_info__` | `dict[str, dict]` | Per-column `info` dicts, keyed by column name. |
| `__expectations__` | `dict[str, str]` | `CheckConstraint` expressions, keyed by constraint name. |
| `__spark_schema__` | `StructType` | PySpark schema. Requires `rowsmyth[spark]`. |

---

## Real-World Use Case: Databricks Lakeflow Declarative Pipelines

When building data pipelines on Databricks with [Lakeflow Declarative Pipelines](https://docs.databricks.com/en/delta-live-tables/index.html) (formerly DLT), your SQLAlchemy model becomes the single source of truth for the entire table lifecycle: schema, documentation, data quality rules, Unity Catalog tags and test fixtures.

### Model definition

Define your model once with comments, info tags, check constraints and generators:

```python
import factory
import factory.fuzzy
from sqlalchemy import CheckConstraint, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from rowsmyth import declarative_base, variant

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = (
        CheckConstraint("tier IN ('standard', 'premium')", name="ck_customers_tier"),
        CheckConstraint("email LIKE '%@%'", name="ck_customers_email"),
        {
            "comment": "Registered customers with purchase history",
            "info": {"domain": "commerce", "pii": "true", "team": "data-platform"},
        },
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(
        String, comment="Primary contact email", info={"pii": "true"}
    )
    name: Mapped[str] = mapped_column(
        String, comment="Full legal name", info={"pii": "true"}
    )
    tier: Mapped[str] = mapped_column(
        String, comment="Subscription tier", info={"pii": "false"}
    )

    @classmethod
    def generators(cls):
        return {
            cls.email: factory.Faker("email"),
            cls.name: factory.Faker("name"),
            cls.tier: factory.fuzzy.FuzzyChoice(["standard", "premium"]),
        }

    @variant
    def premium(cls):
        return {cls.tier: "premium"}
```

### Lakeflow pipeline definition

Reference model metadata directly in your pipeline decorators - no duplication:

```python
from pyspark import pipelines as dp
from tables.customer import Customer


@dp.table(
    name=Customer.__tablename__,
    comment=Customer.__comment__,
    schema=Customer.__spark_schema__,
)
@dp.expect_all_or_fail(Customer.__expectations__)
def customers():
    return spark.read.table("bronze.raw_customers")
```

`__spark_schema__` gives Lakeflow the authoritative column types and nullability. `__expectations__` maps directly to `@dp.expect_all_or_fail` - your `CheckConstraint` definitions become pipeline data quality rules with no extra work.

### Unity Catalog tags

Set table-level and column-level tags in Unity Catalog from the same `info` dicts:

```python
catalog = "main"
schema = "commerce"
table = Customer.__tablename__

# Table tags
table_tag_pairs = ", ".join(
    f"'{k}' = '{v}'" for k, v in Customer.__table_info__.items()
)
spark.sql(f"""
    ALTER TABLE {catalog}.{schema}.{table}
    SET TAGS ({table_tag_pairs})
""")

# Column tags
for column, info in Customer.__column_info__.items():
    if not info:
        continue
    col_tag_pairs = ", ".join(f"'{k}' = '{v}'" for k, v in info.items())
    spark.sql(f"""
        ALTER TABLE {catalog}.{schema}.{table}
        ALTER COLUMN {column}
        SET TAGS ({col_tag_pairs})
    """)
```

### Test fixtures for pipeline unit tests

Use `rowsmyth` to generate realistic fixture data and write it to the bronze source your pipeline reads from. The same `__spark_schema__` ensures the fixture DataFrame matches the pipeline's expected schema exactly:

```python
from pyspark.sql import SparkSession
from tables.customer import Customer

spark = SparkSession.builder.getOrCreate()


def make_customer_fixture(spark, n=100, seed=42):
    rows = Customer.factory(n).mix(premium=0.3).random_seed(seed).create()
    records = [
        {col: getattr(row, col) for col in Customer.__table__.columns.keys()}
        for row in rows
    ]
    return spark.createDataFrame(records, schema=Customer.__spark_schema__)


# Write fixture to the bronze source the pipeline reads
make_customer_fixture(spark).write.mode("overwrite").saveAsTable("bronze.raw_customers")

# Run the pipeline and assert on the output
# ...
```

Because `Customer.__spark_schema__` is derived from the same model as the pipeline decorator, fixture data and pipeline schema can never drift apart.

---

## License

MIT
