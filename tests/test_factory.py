"""Tests for Factory fluent API."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rowsmyth import Factory, generate


def test_count_and_where_scalar(spark: SparkSession, user_model) -> None:
    with generate(spark, seed=2) as gen:
        created = user_model.factory().count(2).where(status="frozen").create()
        df = gen.dataframe("users").withColumn("expected_status", F.lit("frozen"))
    assert len(created) == 2
    assert all(isinstance(user, user_model) for user in created)
    assert all(user.status == "frozen" for user in created)
    assert_column_equality(df, "status", "expected_status")
    assert df.count() == 2


def test_where_callable(spark: SparkSession, user_model) -> None:
    with generate(spark, seed=3) as gen:
        created = (
            user_model
            .factory()
            .count(1)
            .where(status="active")
            .where(email=lambda ctx: f"{ctx.row['status']}@example.com")
            .create()
        )
    row = gen.dataframe("users").collect()[0]
    assert created[0].email == "active@example.com"
    assert row.status == "active"
    assert row.email == "active@example.com"


def test_create_outside_generate_raises(user_model) -> None:
    with pytest.raises(RuntimeError, match="inside generate"):
        user_model.factory().create()


def test_factory_type(spark: SparkSession, user_model) -> None:
    with generate(spark):
        f: Factory = user_model.factory()
        assert f.count(1) is f
