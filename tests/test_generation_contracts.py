"""End-to-end user scenarios for creating relational Spark fixtures."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality, assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import FactoryError, lazy


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


def test_lazy_overrides_can_use_row_seed_spark_and_siblings(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=123) as dataset:
        created = (
            models["role"]
            .factory()
            .where(
                id=lazy(lambda ctx: ctx.sequence()),
                name=lazy(lambda ctx: f"seed-{ctx.seed}-{ctx.spark is spark}"),
            )
            .create()
        )
        roles_df = dataset.dataframe("roles")

    assert created[0].name == "seed-123-True"
    assert roles_df.collect()[0].name == "seed-123-True"


def test_lazy_values_see_the_dataset_and_in_progress_row(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=3) as dataset:
        models["role"].create(name="admin")
        created = models["role"].create(
            name=lazy(lambda ctx: f"{ctx.dataset.count('roles')}-{ctx.row['id']}"),
        )

    assert created.name == "1-2"
    assert dataset.count("roles") == 2


def test_ctx_parent_can_create_default_parent_without_has(
    spark: SparkSession,
    app_base,
    models,
    temporary_model,
) -> None:
    @temporary_model
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


def test_count_zero_registers_an_empty_table_with_the_declared_schema(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    post_model = models["post"]

    with app_base.dataset(spark, seed=1) as dataset:
        created = models["role"].factory().count(0).has(post_model.factory()).create()
        roles_df = dataset.dataframe("roles")

    assert created == []
    assert dataset.table_names() == ["roles", "posts"]
    assert dataset.rows("roles") == []
    assert roles_df.count() == 0
    assert roles_df.schema == models["role"].__definition__
    assert dataset.dataframe("posts").count() == 0


def test_bulk_create_skips_the_generator_for_precomputed_rows(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    role_model = models["role"]
    rows = [{"id": index, "name": f"role-{index}"} for index in range(4)]

    with app_base.dataset(spark, seed=1) as dataset:
        created = role_model.bulk_create(rows)
        overridden = role_model.bulk_create(
            [{"id": 10, "name": "ignored"}],
            name="forced",
        )
        roles_df = dataset.dataframe("roles")

    assert [role.name for role in created] == ["role-0", "role-1", "role-2", "role-3"]
    assert [role.name for role in overridden] == ["forced"]
    assert dataset.count("roles") == 5
    assert roles_df.count() == 5
    assert rows[0] == {"id": 0, "name": "role-0"}


def test_from_rows_applies_overrides_and_children(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    user_model = models["user"]
    post_model = models["post"]
    rows = [
        {
            "id": index,
            "role_id": 1,
            "full_name": f"User {index}",
            "email": f"user{index}@example.com",
        }
        for index in range(2)
    ]

    with app_base.dataset(spark, seed=1, integrity="off") as dataset:
        created = (
            user_model
            .factory()
            .from_rows(rows)
            .where(status=lazy(lambda ctx: f"bulk-{ctx.index}"))
            .has(post_model.factory().count(2), via="author_id")
            .create()
        )

    assert [user.status for user in created] == ["bulk-0", "bulk-1"]
    assert dataset.count("users") == 2
    assert dataset.count("posts") == 4
    assert {row["author_id"] for row in dataset.rows("posts")} == {0, 1}


def test_factory_methods_return_copies_so_templates_are_reusable(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    role_model = models["role"]

    with app_base.dataset(spark, seed=11) as dataset:
        template = role_model.factory().count(1)
        tagged = template.where(name="tagged").create()[0]
        plain = template.create()[0]

    assert tagged.name == "tagged"
    assert plain.name != "tagged"
    assert dataset.count("roles") == 2


def test_from_rows_conflicts_with_count_and_variant(app_base, models) -> None:
    role_model = models["role"]
    user_model = models["user"]

    with pytest.raises(FactoryError, match="cannot be combined with count"):
        role_model.factory().count(2).from_rows([{"id": 1, "name": "a"}])

    with pytest.raises(FactoryError, match="cannot be combined with from_rows"):
        role_model.factory().from_rows([{"id": 1, "name": "a"}]).count(2)

    with pytest.raises(FactoryError, match="cannot be combined with variant"):
        user_model.factory().variant("churned").from_rows([])

    with pytest.raises(FactoryError, match="cannot be combined with from_rows"):
        user_model.factory().from_rows([]).variant("churned")
