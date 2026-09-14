# rowsmyth

A blacksmith forges metal. A rowsmyth forges rows - mythical ones that exist only in your tests. `rowsmyth` is declarative relational test and seed data for Apache Spark: describe each table once, generate rows with real foreign-key integrity, then materialise ordinary `DataFrame`s and temp views.

Row generation is pure Python. Spark is touched only when you read a table, so
creating rows one at a time costs the same round trips as creating them in
bulk: one `createDataFrame` and one temp view registration per table.

## Install

```bash
uv add "rowsmyth[spark]"
# or
pip install "rowsmyth[spark]"
```

Requires Python 3.12+, PySpark 4.0+ and Java 17+ when running Spark locally.
The `[spark]` extra installs `pyspark`; omit it on Databricks or anywhere you
already have a compatible PySpark on the cluster (avoids version clashes):

```bash
uv add rowsmyth
# or
pip install rowsmyth
```

Java must be on your `PATH` or via `JAVA_HOME` when running Spark locally.

Upgrading from 1.x? See the [v2 upgrade guide](docs/upgrade-v2.md).

## Quick start

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import declarative_base, variant

spark = SparkSession.builder.master("local[*]").getOrCreate()
Base = declarative_base()


class Role(Base):
    __table_name__ = "roles"
    __primary_key__ = ("id",)
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), False),
    ])

    def generator(self, ctx):
        return {
            "id": ctx.sequence(),
            "name": ctx.random.choice(["admin", "user", "guest"]),
        }


class User(Base):
    __table_name__ = "users"
    __primary_key__ = ("id",)
    __foreign_keys__ = {"role_id": "roles.id"}
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField("role_id", LongType(), False),
        StructField("email", StringType(), False),
    ])

    def generator(self, ctx):
        return {
            "id": ctx.sequence(),
            "role_id": ctx.pool("roles", "id").choice(),
            "email": ctx.faker.unique.ascii_email(),
        }

    @variant
    def inactive(self, ctx):
        return {"email": "inactive@example.com"}


with Base.dataset(spark, seed=42) as dataset:
    admin = Role.create(name="admin")
    user = Role.create(name="user")
    users = User.factory().count(10).variant("inactive").create()

    assert dataset.count("users") == 10           # Python, no Spark
    users_df = dataset.dataframe("users")         # materialises + registers "users"
# duplicate primary keys and broken foreign keys fail here
```

## What you get

| Feature | API |
|---------|-----|
| One row at a time | `Role.create(name="admin")` |
| Batches with relationships | `User.factory().count(10).has(Post.factory().count(3), via="author_id").create()` |
| Precomputed rows | `Answer.bulk_create(rows)` |
| Deferred values | `.where(score=lazy(lambda ctx: ctx.random.random()))` |
| Driver-side reads | `dataset.rows("users")`, `dataset.count("users")` |
| Spark output | `dataset.dataframe("users")`, `dataset.tables()` |
| Bulk persistence | `dataset.write_all(name=lambda t: f"main.bronze.{t.__table_name__}")` |
| Referential integrity | `__primary_key__`, `__foreign_keys__`, `dataset.check_integrity()` |

Rows are validated as they are generated: unknown columns, missing NOT NULL
values, wrong column types, duplicate primary keys and broken declared foreign
keys all fail with the table, column and value at fault.

## Databricks Lakeflow

A `Model` subclass carries all the metadata your Lakeflow pipeline and Unity Catalog need - schema, comment, tags and data quality expectations - in one place.

### Define a table

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
        "id_not_null": "id IS NOT NULL",
        "email_not_null": "email IS NOT NULL",
        "valid_tier": "tier IN ('standard', 'premium')",
    }
    __definition__ = StructType([
        StructField("id", LongType(), False),
        StructField(
            "email",
            StringType(),
            False,
            metadata={
                "comment": "Customer email, PII",
                "tags": {"pii": "true", "classification": "restricted"},
            },
        ),
        StructField("tier", StringType(), False),
    ])

    def generator(self, ctx):
        return {
            "id": ctx.sequence(),
            "email": ctx.faker.unique.ascii_email(),
            "tier": ctx.random.choices(["standard", "premium"], weights=[7, 3])[0],
        }

    @variant
    def premium(self, ctx):
        return {"tier": "premium"}
```

### Lakeflow pipeline

Use the class attributes directly in your pipeline declaration:

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

### Apply Unity Catalog metadata

After the pipeline materialises the table, apply tags from the same class:

```python
for statement in Customer.uc_tag_sql():
    spark.sql(statement)
```

`uc_tag_sql()` emits table comments, table tags, column comments and column tags.

### Generate test fixtures

Write fixtures to the source your pipeline reads - either a Unity Catalog volume or a persistent bronze table:

```python
from pyspark.sql import SparkSession

from tables.base import Base
from tables.customer import Customer

spark = SparkSession.builder.getOrCreate()

with Base.dataset(spark, seed=42) as dataset:
    Customer.factory().count(100).create()

    # Option A - ingest volume (pipeline reads parquet from path)
    dataset.write_all(
        format="parquet",
        path=lambda table: f"/Volumes/main/bronze/ingest/raw_{table.__table_name__}/",
    )

    # Option B - persistent bronze tables, one per model
    dataset.write_all(name=lambda table: f"main.bronze.raw_{table.__table_name__}")
```

`write_all()` covers every table the dataset created, so adding a model never
leaves a hand-maintained list behind.

See [docs/usage.md](docs/usage.md) for the complete API reference and
[docs/upgrade-v2.md](docs/upgrade-v2.md) for the 1.x to 2.0 migration.

## Development

```bash
make install
make test        # requires JAVA_HOME / java on PATH
make lint
make typecheck
make security
make pre-commit
make ci          # local equivalent of CI checks
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.
