"""Tests for Factory fluent API."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rowsmyth import (
    DatasetContextError,
    Factory,
    WrongDeclarativeBaseError,
    declarative_base,
)


def test_count_and_where_scalar(spark: SparkSession, app_base, user_model) -> None:
    with app_base.dataset(spark, seed=2) as dataset:
        created = user_model.factory().count(2).where(status="frozen").create()
        df = dataset.dataframe("users").withColumn("expected_status", F.lit("frozen"))
    assert len(created) == 2
    assert all(isinstance(user, user_model) for user in created)
    assert all(user.status == "frozen" for user in created)
    assert_column_equality(df, "status", "expected_status")
    assert df.count() == 2


def test_where_callable(spark: SparkSession, app_base, user_model) -> None:
    with app_base.dataset(spark, seed=3) as dataset:
        created = (
            user_model
            .factory()
            .count(1)
            .where(status="active")
            .where(email=lambda ctx: f"{ctx.row['status']}@example.com")
            .create()
        )
    row = dataset.dataframe("users").collect()[0]
    assert created[0].email == "active@example.com"
    assert row.status == "active"
    assert row.email == "active@example.com"


def test_create_outside_dataset_raises(user_model) -> None:
    with pytest.raises(DatasetContextError, match="inside dataset"):
        user_model.factory().create()


def test_factory_type(spark: SparkSession, app_base, user_model) -> None:
    with app_base.dataset(spark):
        f: Factory = user_model.factory()
        assert f.count(1) is f


def test_factory_create_wrong_base_raises(
    spark: SparkSession, app_base, user_model
) -> None:
    OtherBase = declarative_base()

    class OtherUser(OtherBase):
        __table_name__ = "users"
        __primary_key__ = ("id",)
        __definition__ = user_model.__definition__

        def generator(self, ctx) -> dict:
            return {
                "id": ctx.sequence(),
                "role_id": 1,
                "full_name": "Other User",
                "email": "other@example.com",
                "status": "active",
            }

    with app_base.dataset(spark):
        with pytest.raises(
            WrongDeclarativeBaseError, match="different declarative base"
        ):
            OtherUser.factory().create()
