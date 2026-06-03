"""Tests for NOT NULL validation."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from rowsmyth import MissingRequiredColumnError, UnknownColumnError


def test_missing_required_column(
    spark: SparkSession, app_base, missing_required_model
) -> None:
    with app_base.dataset(spark, seed=40):
        with pytest.raises(MissingRequiredColumnError, match="required_col"):
            missing_required_model.factory().count(1).create()


def test_nullable_ok_without_optional(
    spark: SparkSession,
    app_base,
    nullable_demo_model,
) -> None:
    with app_base.dataset(spark, seed=41) as dataset:
        created = nullable_demo_model.factory().count(1).create()
        nullable_demo = dataset.dataframe("nullable_demo")
    assert len(created) == 1
    assert nullable_demo.count() == 1


def test_all_rows_are_validated(
    spark: SparkSession,
    app_base,
) -> None:
    class MissingOnSecondRow(app_base):
        __table_name__ = "missing_on_second_row"
        __primary_key__ = ("id",)
        __definition__ = StructType([
            StructField("id", LongType(), False),
            StructField("required_col", StringType(), False),
        ])

        def generator(self, ctx) -> dict:
            attrs = {"id": ctx.sequence()}
            if ctx.index == 0:
                attrs["required_col"] = "present"
            return attrs

    try:
        with app_base.dataset(spark, seed=42):
            with pytest.raises(MissingRequiredColumnError, match="required_col"):
                MissingOnSecondRow.factory().count(2).create()
    finally:
        app_base.registry.pop("missing_on_second_row", None)


def test_required_column_cannot_be_none(
    spark: SparkSession,
    app_base,
) -> None:
    class NullRequired(app_base):
        __table_name__ = "null_required"
        __primary_key__ = ("id",)
        __definition__ = StructType([
            StructField("id", LongType(), False),
            StructField("required_col", StringType(), False),
        ])

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "required_col": None}

    try:
        with app_base.dataset(spark, seed=43):
            with pytest.raises(MissingRequiredColumnError, match="required_col"):
                NullRequired.factory().create()
    finally:
        app_base.registry.pop("null_required", None)


def test_model_create_validates_generated_required_none(
    spark: SparkSession,
    app_base,
) -> None:
    class ModelCreateNullRequired(app_base):
        __table_name__ = "model_create_null_required"
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
                ModelCreateNullRequired.create()
    finally:
        app_base.registry.pop("model_create_null_required", None)


def test_generated_unknown_column_raises_before_commit(
    spark: SparkSession,
    app_base,
) -> None:
    class UnknownGenerated(app_base):
        __table_name__ = "unknown_generated"
        __primary_key__ = ("id",)
        __definition__ = StructType([StructField("id", LongType(), False)])

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "extra": "not in schema"}

    try:
        with app_base.dataset(spark):
            with pytest.raises(UnknownColumnError, match="extra"):
                UnknownGenerated.factory().create()
    finally:
        app_base.registry.pop("unknown_generated", None)
