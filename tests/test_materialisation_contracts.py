"""Spark materialisation, temp view and write contracts."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import ViewCollisionError, declarative_base

DEMO_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), False),
])


def _demo_model(base, table_name: str) -> type:
    class Demo(base):
        __table_name__ = table_name
        __primary_key__ = ("id",)
        __definition__ = DEMO_SCHEMA

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "name": f"row-{ctx.index}"}

    return Demo


def test_row_creation_costs_no_spark_round_trips(
    fake_spark,
    app_base,
    models,
) -> None:
    role_model = models["role"]
    user_model = models["user"]

    with app_base.dataset(fake_spark, seed=1) as dataset:
        for index in range(50):
            role_model.create(id=index, name=f"role-{index}")
        user_model.factory().count(10).create()

        assert fake_spark.stats["createDataFrame"] == 0
        assert fake_spark.stats["createOrReplaceTempView"] == 0
        assert dataset.count("roles") == 60

        dataset.dataframe("roles")
        dataset.dataframe("users")

    assert fake_spark.stats["createDataFrame"] == 2
    assert fake_spark.stats["createOrReplaceTempView"] == 2
    assert fake_spark.stats["rows_serialised"] == 70
    assert fake_spark.stats["views"] == {"roles", "users"}


@pytest.mark.parametrize("n", [100, 200, 400])
def test_serialisation_is_linear_in_row_count(fake_spark, app_base, models, n) -> None:
    with app_base.dataset(fake_spark, seed=1) as dataset:
        for index in range(n):
            models["role"].create(id=index, name="x")
        dataset.dataframe("roles")

    assert fake_spark.stats["createDataFrame"] == 1
    assert fake_spark.stats["createOrReplaceTempView"] == 1
    assert fake_spark.stats["rows_serialised"] == n


def test_reads_reuse_the_materialised_dataframe_until_rows_change(
    fake_spark,
    app_base,
    models,
) -> None:
    with app_base.dataset(fake_spark, seed=1) as dataset:
        models["role"].create(id=1, name="a")
        first = dataset.dataframe("roles")
        assert dataset.dataframe("roles") is first
        assert dataset.tables() == {"roles": first}
        assert fake_spark.stats["createDataFrame"] == 1

        models["role"].create(id=2, name="b")
        second = dataset.dataframe("roles")

    assert second is not first
    assert fake_spark.stats["createDataFrame"] == 2
    assert fake_spark.stats["rows_serialised"] == 3


def test_views_can_be_disabled(fake_spark, app_base, models) -> None:
    with app_base.dataset(fake_spark, seed=1, views=False) as dataset:
        models["role"].create(id=1, name="a")
        dataset.dataframe("roles")

    assert fake_spark.stats["createDataFrame"] == 1
    assert fake_spark.stats["createOrReplaceTempView"] == 0


def test_view_prefix_namespaces_registered_views(fake_spark, app_base, models) -> None:
    with app_base.dataset(fake_spark, seed=1, view_prefix="fixture_") as dataset:
        models["role"].create(id=1, name="a")
        assert dataset.view_name("roles") == "fixture_roles"
        dataset.flush()

    assert fake_spark.stats["views"] == {"fixture_roles"}


def test_same_view_name_from_two_bases_is_rejected(fake_spark) -> None:
    first_base = declarative_base("First")
    second_base = declarative_base("Second")
    first_model = _demo_model(first_base, "collision_demo")
    second_model = _demo_model(second_base, "collision_demo")

    with first_base.dataset(fake_spark, seed=1) as dataset:
        first_model.factory().create()
        dataset.flush()

    with pytest.raises(ViewCollisionError, match="already registered"):
        with second_base.dataset(fake_spark, seed=1) as dataset:
            second_model.factory().create()
            dataset.flush()

    with second_base.dataset(fake_spark, seed=1, view_prefix="second_") as dataset:
        second_model.factory().create()
        dataset.flush()

    assert fake_spark.stats["views"] == {"collision_demo", "second_collision_demo"}


def test_flush_targets_a_single_table(fake_spark, app_base, models) -> None:
    with app_base.dataset(fake_spark, seed=1) as dataset:
        models["role"].create(id=1, name="a")
        models["order"].create(order_id=1, region="eu")
        dataset.flush("roles")

        assert fake_spark.stats["createDataFrame"] == 1
        assert fake_spark.stats["views"] == {"roles"}

        dataset.flush()

    assert fake_spark.stats["views"] == {"roles", "orders"}


def test_rows_are_snapshots_of_the_generated_data(
    fake_spark,
    app_base,
    models,
) -> None:
    with app_base.dataset(fake_spark, seed=1) as dataset:
        models["role"].create(id=1, name="a")
        rows = dataset.rows("roles")
        rows[0]["name"] = "mutated"

    assert dataset.rows("roles") == [{"id": 1, "name": "a"}]
    assert dataset.table_names() == ["roles"]


def test_write_all_saves_every_table_to_a_path(
    spark: SparkSession,
    app_base,
    tmp_path,
    temporary_model,
) -> None:
    model = temporary_model(_demo_model(app_base, "write_path_test_only"))

    with app_base.dataset(spark, seed=1) as dataset:
        model.factory().count(3).create()
        destinations = dataset.write_all(
            format="parquet",
            options={"compression": "snappy"},
            path=lambda table: str(tmp_path / table.__table_name__),
        )

    assert destinations == {"write_path_test_only": str(tmp_path / model.fqn())}
    written = spark.read.parquet(destinations["write_path_test_only"])
    assert written.count() == 3


def test_write_all_saves_every_table_as_a_named_table(
    spark: SparkSession,
    app_base,
    temporary_model,
) -> None:
    model = temporary_model(_demo_model(app_base, "write_table_test_only"))

    with app_base.dataset(spark, seed=1) as dataset:
        model.factory().count(2).create()
        by_fqn = dataset.write_all()
        renamed = dataset.write_all(name=lambda table: f"renamed_{table.fqn()}")

    assert by_fqn == {"write_table_test_only": "write_table_test_only"}
    assert renamed == {"write_table_test_only": "renamed_write_table_test_only"}
    assert spark.table("spark_catalog.default.write_table_test_only").count() == 2
    renamed_table = spark.table("spark_catalog.default.renamed_write_table_test_only")
    assert renamed_table.count() == 2


def test_committing_no_rows_keeps_the_materialised_dataframe(
    fake_spark,
    app_base,
    models,
) -> None:
    with app_base.dataset(fake_spark, seed=1) as dataset:
        models["role"].create(id=1, name="a")
        first = dataset.dataframe("roles")
        models["role"].factory().count(0).create()

        assert dataset.dataframe("roles") is first

    assert fake_spark.stats["createDataFrame"] == 1
