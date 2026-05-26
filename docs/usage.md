# Usage Guide

## Installation

```bash
uv add rowsmyth
```

With PySpark support:

```bash
uv add "rowsmyth[spark]"
```

## Quick Start

Use `declarative_base()` from rowsmyth instead of SQLAlchemy's own. Define
generators co-located with your models using `generators()` and mark named
variants with `@variant`.

```python
from rowsmyth import declarative_base, variant
import factory
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, ForeignKey

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

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    total: Mapped[float] = mapped_column()
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    user: Mapped[User] = relationship(back_populates="orders")

    @classmethod
    def generators(cls):
        return {cls.total: factory.Faker("pyfloat", positive=True, max_value=500)}
```

## Generating Data

### Basic usage

```python
users = User.factory(10).create()
```

### Hierarchical data

```python
users = (
    User.factory(20)
        .has(Order.factory(1, 5))
        .create()
)
```

### Variants and proportions

```python
# ~5 admins, ~95 default users
users = User.factory(100).mix(admin=0.05).create()
```

### Reproducible output

```python
users = User.factory(10).random_seed(42).create()
```

### Fixed overrides

```python
users = User.factory(5).where({User.tier: "premium"}).create()
```

### Full dataset (all tables at once)

```python
result = Base.dataset(
    User.factory(50),
    Order.factory(200),
).random_seed(42).create()

users = result["users"]    # 50 User instances
orders = result["orders"]  # 200 Order instances, FKs wired to users
```

### Seeding reference data

Pass raw model instances directly to `Base.dataset()` to seed reference/lookup
tables with predetermined rows. Seeded instances are persisted first and made
available as FK targets for all factories. They appear in the result dict like
any other rows.

```python
# Seed a lookup table with fixed rows
statuses = [
    Status(code="draft"),
    Status(code="active"),
    Status(code="archived"),
]

result = Base.dataset(
    *statuses,
    Article.factory(100),
).random_seed(42).create()

result["statuses"]  # the 3 seeded Status rows
result["articles"]  # 100 Article rows, each article.status_id points to one of the 3
```

Any mix of raw instances and `FactoryBuilder`s is accepted. Seeded rows always
appear before factory-generated rows for the same table.

## Spark Schema

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
schema = User.__spark_schema__
df = spark.createDataFrame([], schema)
```
