# rowsmyth

A blacksmith forges metal. A rowsmyth forges rows - mythical ones that exist only in your tests. `rowsmyth` is declarative relational test and seed data for Apache Spark: generate rows **one at a time** with real foreign-key integrity, then materialise ordinary `DataFrame`s and temp views.

## Install

```bash
uv install rowsmyth
```

Requires Python 3.12+ and Java 17+.

## Quick start

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import Model, generate, variant

spark = SparkSession.builder.master("local[*]").getOrCreate()


class Role(Model):
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


class User(Model):
    __table_name__ = "users"
    __primary_key__ = ("id",)
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


with generate(spark, seed=42) as gen:
    admin = Role.create(name="admin")
    user = Role.create(name="user")
    users = User.factory().count(10).variant("inactive").create()

    role_ids = {admin.id, user.id}
    assert all(created_user.role_id in role_ids for created_user in users)
    users_df = gen.dataframe("users")
    # users_df is a DataFrame; temp view "users" is registered
```

## Databricks Lakeflow

A `Model` subclass carries all the metadata your Lakeflow pipeline and Unity Catalog need - schema, comment, tags and data quality expectations - in one place.

### Define a table

```python
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import Model, variant


class Customer(Model):
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

### Generate test fixtures

Write fixtures to the source your pipeline reads - either a Unity Catalog volume or a persistent bronze table:

```python
from pyspark.sql import SparkSession

from rowsmyth import generate
from tables.customer import Customer

spark = SparkSession.builder.getOrCreate()

with generate(spark, seed=42) as gen:
    customers = Customer.factory().count(100).create()
    customers_df = gen.dataframe("customers")

# Option A - ingest volume (pipeline reads parquet from path)
customers_df.write.mode("overwrite").parquet(
    "/Volumes/main/bronze/ingest/raw_customers/"
)

# Option B - persistent bronze table
customers_df.write.mode("overwrite").saveAsTable("main.bronze.raw_customers")
```

See [docs/usage.md](docs/usage.md) for the complete API reference.

## Development

```bash
make install
make test        # requires JAVA_HOME / java on PATH
make lint
make typecheck
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.
