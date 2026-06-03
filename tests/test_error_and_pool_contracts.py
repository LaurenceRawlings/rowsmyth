"""Integration coverage for user-visible error and pool behaviours."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import (
    CompoundPrimaryKeyError,
    DataframeNotFoundError,
    DatasetContextError,
    EmptyPoolError,
    FactoryError,
    MissingRequiredColumnError,
    PoolSampleError,
    UnknownColumnError,
    UnknownVariantError,
    UnresolvedPoolError,
    WrongDeclarativeBaseError,
    declarative_base,
)
from rowsmyth.pool import PoolChoice


def test_create_outside_dataset_raises_context_error(models) -> None:
    with pytest.raises(DatasetContextError, match=r"Base\.dataset"):
        models["role"].create(name="admin")


def test_dataframe_lookup_requires_created_table(spark: SparkSession, app_base) -> None:
    with app_base.dataset(spark) as dataset:
        with pytest.raises(DataframeNotFoundError, match="missing"):
            dataset.dataframe("missing")


def test_unknown_variant_and_invalid_counts_raise_domain_errors(models) -> None:
    with pytest.raises(UnknownVariantError, match="no variant 'missing'"):
        models["user"].factory().variant("missing")

    with pytest.raises(FactoryError, match="non-negative integer"):
        models["role"].factory().count(-1)

    with pytest.raises(FactoryError, match="non-negative integer"):
        models["role"].factory().count(1.5)  # type: ignore[arg-type]


def test_wrong_base_is_rejected_for_create_fk_and_parent(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    other_base = declarative_base()

    class OtherRole(other_base):
        __table_name__ = "roles"
        __primary_key__ = ("id",)
        __definition__ = models["role"].__definition__

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "name": "admin"}

    class OtherLine(other_base):
        __table_name__ = "other_lines"
        __primary_key__ = ("line_id",)
        __definition__ = models["order_line"].__definition__

        def generator(self, ctx) -> dict:
            order = ctx.parent(models["order"])
            return {
                "line_id": ctx.sequence(),
                "order_id": order.key["order_id"],
                "order_region": order.key["region"],
                "qty": 1,
            }

    with app_base.dataset(spark):
        with pytest.raises(
            WrongDeclarativeBaseError,
            match="different declarative base",
        ):
            OtherRole.create(name="admin")
        with pytest.raises(
            WrongDeclarativeBaseError,
            match="different declarative base",
        ):
            models["user"].factory().where(role_id=OtherRole.factory()).create()

    with other_base.dataset(spark):
        with pytest.raises(
            WrongDeclarativeBaseError,
            match="different declarative base",
        ):
            OtherLine.factory().create()


def test_validation_errors_happen_before_commit(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    class UnknownGenerated(app_base):
        __table_name__ = "unknown_generated_test_only"
        __primary_key__ = ("id",)
        __definition__ = StructType([StructField("id", LongType(), False)])

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "extra": "not in schema"}

    try:
        with app_base.dataset(spark) as dataset:
            with pytest.raises(UnknownColumnError, match="extra"):
                UnknownGenerated.factory().create()
            with pytest.raises(UnknownColumnError, match="unknown columns"):
                models["role"].create(name="admin", unknown=True)
            with pytest.raises(MissingRequiredColumnError, match="required_col"):
                models["missing_required"].factory().create()
            assert dataset.dataframes == {}
    finally:
        app_base.registry.pop("unknown_generated_test_only", None)


def test_nullable_columns_may_be_omitted(spark: SparkSession, app_base, models) -> None:
    with app_base.dataset(spark) as dataset:
        created = models["nullable_demo"].factory().count(1).create()
        nullable_demo = dataset.dataframe("nullable_demo")

    assert len(created) == 1
    assert nullable_demo.count() == 1


def test_compound_primary_key_factory_value_requires_ctx_parent(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark):
        with pytest.raises(CompoundPrimaryKeyError, match="compound keys"):
            models["bad_compound_fk"].factory().create()


def test_pool_sample_and_values_ignore_nulls(
    spark: SparkSession,
    app_base,
) -> None:
    schema = StructType([StructField("id", LongType(), True)])
    (
        spark.createDataFrame([(None,), (10,), (20,)], schema).createOrReplaceTempView(
            "roles"
        )
    )

    with app_base.dataset(spark, seed=4) as dataset:
        pool = dataset.pool("roles", "id")
        assert set(pool.values) == {10, 20}
        assert set(pool.sample(2)) == {10, 20}
        with pytest.raises(PoolSampleError, match="sample"):
            pool.sample(3)


def test_deferred_pool_choice_ignores_nulls(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    schema = StructType([StructField("id", LongType(), True)])
    spark.createDataFrame([(None,), (10,)], schema).createOrReplaceTempView("roles")

    with app_base.dataset(spark, seed=5) as dataset:
        created = models["pool_consumer"].factory().count(4).create()
        consumers = dataset.dataframe("pool_consumer")

    assert {item.role_id for item in created} == {10}
    assert {row.role_id for row in consumers.collect()} == {10}


def test_empty_pool_values_raise_for_no_non_null_values(
    spark: SparkSession,
    app_base,
) -> None:
    schema = StructType([StructField("id", LongType(), True)])
    spark.createDataFrame([(None,)], schema).createOrReplaceTempView("roles")

    with app_base.dataset(spark, seed=6) as dataset:
        with pytest.raises(EmptyPoolError, match="no non-null values"):
            _ = dataset.pool("roles", "id").values


def test_empty_deferred_pool_raises_for_no_non_null_values(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    schema = StructType([StructField("id", LongType(), True)])
    spark.createDataFrame([(None,)], schema).createOrReplaceTempView("roles")

    with app_base.dataset(spark, seed=6):
        with pytest.raises(EmptyPoolError, match="no non-null values"):
            models["pool_consumer"].factory().create()


def test_unresolved_pool_choice_raises_domain_error(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=7):
        models["role"].create(name="admin")
        with pytest.raises(UnresolvedPoolError, match="role_id"):
            (
                models["pool_consumer"]
                .factory()
                .where(
                    role_id=PoolChoice(
                        "roles",
                        "id",
                        None,  # type: ignore[arg-type]
                    )
                )
                .create()
            )


def test_required_column_cannot_be_none(
    spark: SparkSession,
    app_base,
) -> None:
    class NullRequired(app_base):
        __table_name__ = "null_required_test_only"
        __primary_key__ = ("id",)
        __definition__ = StructType([
            StructField("id", LongType(), False),
            StructField("required_col", StringType(), False),
        ])

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "required_col": None}

    try:
        with app_base.dataset(spark):
            with pytest.raises(MissingRequiredColumnError, match="required_col"):
                NullRequired.factory().create()
    finally:
        app_base.registry.pop("null_required_test_only", None)
