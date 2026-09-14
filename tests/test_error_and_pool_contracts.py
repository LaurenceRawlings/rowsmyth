"""Integration coverage for user-visible error and pool behaviours."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import (
    ColumnTypeError,
    CompoundPrimaryKeyError,
    DatasetConfigurationError,
    DatasetContextError,
    EmptyPoolError,
    FactoryError,
    LazyValueError,
    MissingRequiredColumnError,
    PoolSampleError,
    TableNotFoundError,
    UnknownColumnError,
    UnknownVariantError,
    WrongDeclarativeBaseError,
    declarative_base,
    lazy,
)


def test_create_outside_dataset_raises_context_error(models) -> None:
    with pytest.raises(DatasetContextError, match=r"Base\.dataset"):
        models["role"].create(name="admin")


def test_table_lookups_require_a_created_table(spark: SparkSession, app_base) -> None:
    with app_base.dataset(spark) as dataset:
        for lookup in (dataset.dataframe, dataset.rows, dataset.count, dataset.flush):
            with pytest.raises(TableNotFoundError, match="missing"):
                lookup("missing")


def test_unsupported_integrity_mode_is_rejected(spark: SparkSession, app_base) -> None:
    with pytest.raises(DatasetConfigurationError, match="integrity must be one of"):
        app_base.dataset(spark, integrity="strict")


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
        __table_name__ = "other_roles"
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
    temporary_model,
) -> None:
    @temporary_model
    class UnknownGenerated(app_base):
        __table_name__ = "unknown_generated_test_only"
        __primary_key__ = ("id",)
        __definition__ = StructType([StructField("id", LongType(), False)])

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "extra": "not in schema"}

    with app_base.dataset(spark) as dataset:
        with pytest.raises(UnknownColumnError, match="extra"):
            UnknownGenerated.factory().create()
        with pytest.raises(UnknownColumnError, match="unknown columns"):
            models["role"].create(name="admin", unknown=True)
        with pytest.raises(MissingRequiredColumnError, match="required_col"):
            models["missing_required"].factory().create()
        assert dataset.table_names() == []


def test_nullable_columns_may_be_omitted(spark: SparkSession, app_base, models) -> None:
    with app_base.dataset(spark) as dataset:
        created = models["nullable_demo"].factory().count(1).create()
        nullable_demo = dataset.dataframe("nullable_demo")

    assert len(created) == 1
    assert nullable_demo.count() == 1


def test_required_column_cannot_be_none(
    spark: SparkSession,
    app_base,
    models,
    temporary_model,
) -> None:
    @temporary_model
    class NullRequired(app_base):
        __table_name__ = "null_required_test_only"
        __primary_key__ = ("id",)
        __definition__ = models["missing_required"].__definition__

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "required_col": None}

    with app_base.dataset(spark):
        with pytest.raises(MissingRequiredColumnError, match="required_col"):
            NullRequired.factory().create()


def test_compound_primary_key_factory_value_requires_ctx_parent(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark):
        with pytest.raises(CompoundPrimaryKeyError, match="compound keys"):
            models["bad_compound_fk"].factory().create()


def test_wrong_column_types_are_reported_with_row_context(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    role_model = models["role"]

    with app_base.dataset(spark):
        with pytest.raises(ColumnTypeError, match=r"roles\.name: string column"):
            role_model.create(name=42)
        with pytest.raises(ColumnTypeError, match="bigint column expects int"):
            role_model.create(id="seven")


def test_bare_callables_are_data_and_point_at_lazy(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark):
        with pytest.raises(ColumnTypeError, match=r"wrap callables in rowsmyth\.lazy"):
            models["role"].create(name=str)


def test_lazy_requires_a_callable() -> None:
    with pytest.raises(LazyValueError, match="requires a callable"):
        lazy("not callable")  # type: ignore[arg-type]


def test_dataset_table_pools_read_generated_rows_without_spark(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=4) as dataset:
        models["role"].create(name="admin")
        pool = dataset.pool("roles", "id")
        assert pool.values == [1]

        models["role"].create(name="user")
        assert pool.values == [1, 2]
        assert sorted(pool.sample(2)) == [1, 2]
        assert pool.choice() in {1, 2}

        with pytest.raises(PoolSampleError, match="cannot sample"):
            pool.sample(3)


def test_dataset_table_pool_errors_are_specific(
    spark: SparkSession,
    app_base,
) -> None:
    with app_base.dataset(spark, seed=4) as dataset:
        with pytest.raises(EmptyPoolError, match="create roles rows"):
            _ = dataset.pool("roles", "id").values
        with pytest.raises(UnknownColumnError, match="not a column of roles"):
            _ = dataset.pool("roles", "nope").values


def test_external_view_pools_ignore_nulls_and_are_cached(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    schema = StructType([StructField("id", LongType(), True)])
    spark.createDataFrame([(None,), (20,), (10,)], schema).createOrReplaceTempView(
        "external_ids"
    )

    with app_base.dataset(spark, seed=5) as dataset:
        created = models["external_pool_consumer"].factory().count(4).create()
        assert dataset.pool("external_ids", "id").values == [10, 20]

        spark.createDataFrame([(99,)], schema).createOrReplaceTempView("external_ids")
        assert dataset.pool("external_ids", "id").values == [10, 20]

    assert {
        row.source_id for row in dataset.dataframe("external_pool_consumer").collect()
    } <= {10, 20}
    assert {item.source_id for item in created} <= {10, 20}


def test_external_view_pool_without_values_raises(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    schema = StructType([StructField("id", LongType(), True)])
    spark.createDataFrame([(None,)], schema).createOrReplaceTempView("external_ids")

    with app_base.dataset(spark, seed=6):
        with pytest.raises(EmptyPoolError, match="no non-null values in temp view"):
            models["external_pool_consumer"].factory().create()


def test_generated_tables_are_visible_to_spark_after_flush(
    spark: SparkSession,
    app_base,
    models,
    temporary_model,
) -> None:
    @temporary_model
    class FlushDemo(app_base):
        __table_name__ = "flush_demo_test_only"
        __primary_key__ = ("id",)
        __definition__ = StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), False),
        ])

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "name": "generated"}

    with app_base.dataset(spark, seed=1) as dataset:
        FlushDemo.factory().count(2).create()
        assert not spark.catalog.tableExists("flush_demo_test_only")

        dataset.flush()
        assert spark.catalog.tableExists("flush_demo_test_only")
        assert spark.table("flush_demo_test_only").count() == 2

        pooled = dataset.pool("flush_demo_test_only", "name").values

    assert pooled == ["generated"]
