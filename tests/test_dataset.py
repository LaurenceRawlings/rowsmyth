"""Tests for Base.dataset() and determinism."""

from __future__ import annotations

import random

import pytest
from chispa import assert_df_equality
from faker import Faker
from pyspark.sql import SparkSession

from rowsmyth import Dataset


def test_deterministic_with_seed(
    spark: SparkSession,
    monkeypatch,
    app_base,
    user_model,
) -> None:
    def fail_global_seed(*_args, **_kwargs) -> None:
        msg = "global seed must not be used"
        raise AssertionError(msg)

    monkeypatch.setattr(random, "seed", fail_global_seed)
    monkeypatch.setattr(Faker, "seed", fail_global_seed)

    with app_base.dataset(spark, seed=99) as dataset:
        assert isinstance(dataset, Dataset)
        assert dataset.base is app_base
        assert dataset.registry is app_base.registry
        assert dataset.seed == 99
        assert dataset.spark is spark
        user_model.factory().count(3).create()
        data1 = dataset.dataframe("users")
    with app_base.dataset(spark, seed=99) as dataset2:
        user_model.factory().count(3).create()
        data2 = dataset2.dataframe("users")
    assert_df_equality(data1, data2, ignore_row_order=True)


def test_no_seed(spark: SparkSession, app_base, user_model) -> None:
    with app_base.dataset(spark) as dataset:
        assert dataset.seed is None
        user_model.factory().count(1).create()


def test_dataframe_missing_raises(spark: SparkSession, app_base) -> None:
    with app_base.dataset(spark) as dataset:
        with pytest.raises(KeyError, match="missing"):
            dataset.dataframe("missing")


def test_model_key_and_pk(spark: SparkSession, app_base, user_model) -> None:
    with app_base.dataset(spark, seed=1) as dataset:
        created = user_model.factory().count(1).create()
        row = dataset.dataframe("users").collect()[0]
    user = user_model(
        id=row.id,
        role_id=1,
        full_name="x",
        email="a@b.c",
        status="active",
    )
    assert created[0].id == row.id
    assert isinstance(created[0], user_model)
    assert user.key == {"id": row.id}
    assert user.pk == row.id
