"""End-to-end user scenarios for creating relational Spark fixtures."""

from __future__ import annotations

from chispa import assert_column_equality, assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


def test_factory_creates_related_rows_and_temp_views(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    user_model = models["user"]
    post_model = models["post"]

    with app_base.dataset(spark, seed=50) as dataset:
        users = (
            user_model
            .factory()
            .count(2)
            .has(post_model.factory().count(3).where(published=True), via="author_id")
            .create()
        )
        users_df = dataset.dataframe("users")
        roles_df = dataset.dataframe("roles")
        posts_df = dataset.dataframe("posts")

    assert len(users) == 2
    assert users_df.count() == 2
    assert roles_df.count() == 2
    assert posts_df.count() == 6
    assert spark.catalog.tableExists("users")
    assert spark.catalog.tableExists("roles")
    assert spark.catalog.tableExists("posts")

    user_roles = (
        users_df
        .alias("u")
        .join(roles_df.alias("r"), F.col("u.role_id") == F.col("r.id"))
        .select(F.col("u.role_id"), F.col("r.id").alias("role_pk"))
    )
    assert user_roles.count() == users_df.count()
    assert_column_equality(user_roles, "role_id", "role_pk")

    post_authors = (
        posts_df
        .alias("p")
        .join(users_df.alias("u"), F.col("p.author_id") == F.col("u.id"))
        .select(F.col("p.author_id"), F.col("u.id").alias("user_pk"))
    )
    assert post_authors.count() == posts_df.count()
    assert_column_equality(post_authors, "author_id", "user_pk")
    assert_column_equality(
        posts_df.withColumn("expected", F.lit(True)),
        "published",
        "expected",
    )


def test_model_create_appends_and_pool_choices_use_existing_rows(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    role_model = models["role"]
    pool_consumer_model = models["pool_consumer"]

    with app_base.dataset(spark, seed=42) as dataset:
        admin = role_model.create(name="admin")
        user = role_model.create(name="user")
        consumers = (
            pool_consumer_model
            .factory()
            .count(8)
            .where(fallback_role_id=dataset.pool("roles", "id").choice())
            .create()
        )
        roles_df = dataset.dataframe("roles")
        consumers_df = dataset.dataframe("pool_consumer")

    role_ids = {admin.id, user.id}
    assert roles_df.count() == 2
    assert [row.name for row in roles_df.orderBy("id").collect()] == ["admin", "user"]
    assert all(consumer.role_id in role_ids for consumer in consumers)
    assert all(consumer.fallback_role_id in role_ids for consumer in consumers)
    assert all("__rowsmyth_" not in column for column in consumers_df.columns)
    for row in consumers_df.collect():
        assert row.role_id in role_ids
        assert row.fallback_role_id in role_ids


def test_seeded_datasets_are_deterministic_without_global_seeding(
    spark: SparkSession,
    monkeypatch,
    app_base,
    models,
) -> None:
    import random

    from faker import Faker

    def fail_global_seed(*_args, **_kwargs) -> None:
        msg = "global seed must not be used"
        raise AssertionError(msg)

    monkeypatch.setattr(random, "seed", fail_global_seed)
    monkeypatch.setattr(Faker, "seed", fail_global_seed)

    user_model = models["user"]
    with app_base.dataset(spark, seed=99) as first_dataset:
        user_model.factory().count(3).create()
        first = first_dataset.dataframe("users")

    with app_base.dataset(spark, seed=99) as second_dataset:
        user_model.factory().count(3).create()
        second = second_dataset.dataframe("users")

    assert_df_equality(first, second, ignore_row_order=True)


def test_variants_can_be_inherited_from_abstract_bases(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    stateful_item = models["stateful_item"]

    with app_base.dataset(spark, seed=1) as dataset:
        created = stateful_item.factory().count(2).variant("archived").create()
        items_df = dataset.dataframe("stateful_items")

    assert all(item.status == "archived" for item in created)
    assert_column_equality(
        items_df.withColumn("expected_status", F.lit("archived")),
        "status",
        "expected_status",
    )


def test_named_sequence_is_shared_across_tables(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=1) as dataset:
        models["sequence_left"].factory().count(2).create()
        models["sequence_right"].factory().count(2).create()
        left = dataset.dataframe("sequence_left")
        right = dataset.dataframe("sequence_right")

    shared_values = [
        row.shared_id
        for row in (
            left
            .select("shared_id")
            .union(right.select("shared_id"))
            .orderBy("shared_id")
            .collect()
        )
    ]
    assert shared_values == [1, 2, 3, 4]


def test_callable_overrides_can_use_row_seed_and_spark(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=123) as dataset:
        created = (
            models["role"]
            .factory()
            .where(
                id=lambda ctx: ctx.sequence(),
                name=lambda ctx: f"seed-{ctx.seed}-{ctx.spark is spark}",
            )
            .create()
        )
        roles_df = dataset.dataframe("roles")

    assert created[0].name == "seed-123-True"
    assert roles_df.collect()[0].name == "seed-123-True"


def test_ctx_parent_can_create_default_parent_without_has(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    class Invoice(app_base):
        __table_name__ = "invoice_test_only"
        __primary_key__ = ("id",)
        __definition__ = StructType([
            StructField("id", LongType(), False),
            StructField("order_id", LongType(), False),
            StructField("order_region", StringType(), False),
        ])

        def generator(self, ctx) -> dict:
            order = ctx.parent(models["order"])
            return {
                "id": ctx.sequence(),
                "order_id": order.key["order_id"],
                "order_region": order.key["region"],
            }

    try:
        with app_base.dataset(spark, seed=9) as dataset:
            Invoice.factory().create()
            invoices = dataset.dataframe("invoice_test_only")
            orders = dataset.dataframe("orders")

        joined = invoices.alias("invoice").join(
            orders.alias("order"),
            (F.col("invoice.order_id") == F.col("order.order_id"))
            & (F.col("invoice.order_region") == F.col("order.region")),
        )
        assert joined.count() == invoices.count()
    finally:
        app_base.registry.pop("invoice_test_only", None)


def test_custom_parent_role_supports_compound_keys(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    order_model = models["order"]
    order_line_model = models["order_line"]

    with app_base.dataset(spark, seed=20) as dataset:
        orders = (
            order_model
            .factory()
            .count(2)
            .has(order_line_model.factory().count(2), via="placed_order")
            .create()
        )
        orders_df = dataset.dataframe("orders")
        lines_df = dataset.dataframe("order_lines")

    assert len(orders) == 2
    assert orders_df.count() == 2
    assert lines_df.count() == 4
    joined = lines_df.alias("line").join(
        orders_df.alias("order"),
        (F.col("line.order_id") == F.col("order.order_id"))
        & (F.col("line.order_region") == F.col("order.region")),
    )
    assert joined.count() == lines_df.count()


def test_count_zero_is_an_explicit_no_op(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=1) as dataset:
        created = models["role"].factory().count(0).create()

    assert created == []
    assert dataset.dataframes == {}
