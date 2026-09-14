"""Primary key and foreign key integrity contracts."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StructField, StructType

from rowsmyth import (
    DuplicatePrimaryKeyError,
    ForeignKeyViolationError,
    InvalidModelDefinitionError,
    RowsmythWarning,
    declarative_base,
)

CHILD_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("role_id", LongType(), True),
])


def test_duplicate_primary_keys_are_rejected_on_creation(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=1) as dataset:
        models["role"].create(id=7, name="first")
        with pytest.raises(DuplicatePrimaryKeyError, match=r"duplicate primary key"):
            models["role"].create(id=7, name="second")

    assert dataset.count("roles") == 1


def test_compound_primary_keys_are_compared_as_a_whole(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    order_model = models["order"]

    with app_base.dataset(spark, seed=1) as dataset:
        order_model.bulk_create([
            {"order_id": 1, "region": "eu"},
            {"order_id": 1, "region": "us"},
        ])
        with pytest.raises(DuplicatePrimaryKeyError, match="'region': 'us'"):
            order_model.create(order_id=1, region="us")

    assert dataset.count("orders") == 2


def test_declared_foreign_keys_are_validated_when_the_block_exits(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with pytest.raises(ForeignKeyViolationError, match=r"pool_consumer\.role_id"):
        with app_base.dataset(spark, seed=1) as dataset:
            models["role"].create(id=1, name="admin")
            models["pool_consumer"].bulk_create([
                {"id": 1, "role_id": 1, "fallback_role_id": 1},
                {"id": 2, "role_id": 404, "fallback_role_id": 1},
            ])

    assert dataset.count("pool_consumer") == 2


def test_children_may_be_created_before_their_parents(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=1) as dataset:
        models["pool_consumer"].bulk_create([
            {"id": 1, "role_id": 9, "fallback_role_id": 9},
        ])
        models["role"].create(id=9, name="late")
        dataset.check_integrity()

    assert dataset.count("roles") == 1


def test_compound_foreign_keys_are_validated_per_column(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=2) as dataset:
        models["order"].create(order_id=1, region="eu")
        models["order_line"].bulk_create([
            {"line_id": 1, "order_id": 1, "order_region": "eu", "qty": 2},
        ])
        dataset.check_integrity()

    assert dataset.count("order_lines") == 1

    with pytest.raises(
        ForeignKeyViolationError,
        match=r"order_lines\.order_region -> orders\.region",
    ):
        with app_base.dataset(spark, seed=2):
            models["order"].create(order_id=1, region="eu")
            models["order_line"].bulk_create([
                {"line_id": 1, "order_id": 1, "order_region": "apac", "qty": 2},
            ])


def test_warn_mode_reports_violations_without_raising(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with pytest.warns(RowsmythWarning) as recorded:
        with app_base.dataset(spark, seed=1, integrity="warn") as dataset:
            models["role"].create(id=1, name="first")
            models["role"].create(id=1, name="second")
            models["pool_consumer"].bulk_create([
                {"id": 1, "role_id": 404, "fallback_role_id": 1},
            ])

    messages = [str(warning.message) for warning in recorded]
    assert any("duplicate primary key" in message for message in messages)
    assert any("no matching parent row" in message for message in messages)
    assert dataset.count("roles") == 2


def test_off_mode_skips_every_integrity_check(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=1, integrity="off") as dataset:
        models["role"].create(id=1, name="first")
        models["role"].create(id=1, name="second")
        models["pool_consumer"].bulk_create([
            {"id": 1, "role_id": 404, "fallback_role_id": 1},
        ])
        dataset.check_integrity()

    assert dataset.count("roles") == 2
    assert dataset.count("pool_consumer") == 1


def test_integrity_is_not_checked_when_the_block_fails(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    sentinel = RuntimeError("fixture failed")

    with pytest.raises(RuntimeError, match="fixture failed"):
        with app_base.dataset(spark, seed=1) as dataset:
            models["pool_consumer"].bulk_create([
                {"id": 1, "role_id": 404, "fallback_role_id": 1},
            ])
            raise sentinel

    assert dataset.count("pool_consumer") == 1


def test_nullable_foreign_key_values_are_skipped(
    spark: SparkSession,
    app_base,
    temporary_model,
) -> None:
    @temporary_model
    class OptionalChild(app_base):
        __table_name__ = "optional_child_test_only"
        __primary_key__ = ("id",)
        __foreign_keys__ = {"role_id": "roles.id"}  # noqa: RUF012
        __definition__ = CHILD_SCHEMA

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence()}

    with app_base.dataset(spark, seed=1) as dataset:
        OptionalChild.factory().count(2).create()

    assert dataset.rows("optional_child_test_only") == [{"id": 1}, {"id": 2}]


def test_unknown_foreign_key_targets_are_reported(
    spark: SparkSession,
    app_base,
    temporary_model,
) -> None:
    @temporary_model
    class MissingTable(app_base):
        __table_name__ = "missing_table_fk_test_only"
        __primary_key__ = ("id",)
        __foreign_keys__ = {"role_id": "ghosts.id"}  # noqa: RUF012
        __definition__ = CHILD_SCHEMA

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "role_id": 1}

    @temporary_model
    class MissingColumn(app_base):
        __table_name__ = "missing_column_fk_test_only"
        __primary_key__ = ("id",)
        __foreign_keys__ = {"role_id": "roles.ghost"}  # noqa: RUF012
        __definition__ = CHILD_SCHEMA

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "role_id": 1}

    with pytest.raises(InvalidModelDefinitionError, match="target table 'ghosts'"):
        with app_base.dataset(spark, seed=1):
            MissingTable.factory().create()

    with pytest.raises(InvalidModelDefinitionError, match="target column 'ghost'"):
        with app_base.dataset(spark, seed=1):
            MissingColumn.factory().create()


def test_foreign_key_declarations_are_validated_at_class_definition() -> None:
    base = declarative_base()

    with pytest.raises(InvalidModelDefinitionError, match="not in __definition__"):

        class UnknownColumn(base):
            __table_name__ = "unknown_fk_column"
            __primary_key__ = ("id",)
            __foreign_keys__ = {"ghost_id": "roles.id"}  # noqa: RUF012
            __definition__ = CHILD_SCHEMA

    with pytest.raises(InvalidModelDefinitionError, match=r"must be 'table\.column'"):

        class BadTarget(base):
            __table_name__ = "bad_fk_target"
            __primary_key__ = ("id",)
            __foreign_keys__ = {"role_id": "roles"}  # noqa: RUF012
            __definition__ = CHILD_SCHEMA

    with pytest.raises(InvalidModelDefinitionError, match=r"must be 'table\.column'"):

        class EmptyTarget(base):
            __table_name__ = "empty_fk_target"
            __primary_key__ = ("id",)
            __foreign_keys__ = {"role_id": "roles."}  # noqa: RUF012
            __definition__ = CHILD_SCHEMA


def test_foreign_keys_hold_for_generated_relationships(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=3) as dataset:
        models["user"].factory().count(3).has(
            models["post"].factory().count(2),
            via="author_id",
        ).create()
        dataset.check_integrity()

    assert dataset.count("posts") == 6
    assert {row["author_id"] for row in dataset.rows("posts")} == {
        row["id"] for row in dataset.rows("users")
    }


def test_one_parent_index_serves_every_child_table(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    with app_base.dataset(spark, seed=4) as dataset:
        models["role"].create(id=1, name="admin")
        models["user"].bulk_create([
            {
                "id": 1,
                "role_id": 1,
                "full_name": "Ada Lovelace",
                "email": "ada@example.com",
                "status": "active",
            }
        ])
        models["pool_consumer"].bulk_create([
            {"id": 1, "role_id": 1, "fallback_role_id": 1},
        ])
        dataset.check_integrity()

    assert dataset.table_names() == ["roles", "users", "pool_consumer"]


def test_a_rejected_batch_leaves_the_dataset_unchanged(
    spark: SparkSession,
    app_base,
    models,
) -> None:
    role_model = models["role"]
    duplicated = [{"id": 1, "name": "first"}, {"id": 1, "name": "second"}]

    with app_base.dataset(spark, seed=1) as dataset:
        with pytest.raises(DuplicatePrimaryKeyError, match="duplicate primary key"):
            role_model.bulk_create(duplicated)

        assert dataset.table_names() == []

        role_model.bulk_create([{"id": 1, "name": "first"}])

    assert dataset.rows("roles") == [{"id": 1, "name": "first"}]
