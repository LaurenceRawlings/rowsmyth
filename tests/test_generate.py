"""Tests for generate() and determinism."""

from __future__ import annotations

import random

import pytest
from chispa import assert_df_equality
from faker import Faker
from pyspark.sql import SparkSession

from rowsmyth import generate


def test_deterministic_with_seed(spark: SparkSession, monkeypatch, user_model) -> None:
    def fail_global_seed(*_args, **_kwargs) -> None:
        msg = "global seed must not be used"
        raise AssertionError(msg)

    monkeypatch.setattr(random, "seed", fail_global_seed)
    monkeypatch.setattr(Faker, "seed", fail_global_seed)

    with generate(spark, seed=99) as g:
        assert g.seed == 99
        assert g.spark is spark
        user_model.factory().count(3).create()
        data1 = g.dataframe("users")
    with generate(spark, seed=99) as g2:
        user_model.factory().count(3).create()
        data2 = g2.dataframe("users")
    assert_df_equality(data1, data2, ignore_row_order=True)


def test_no_seed(spark: SparkSession, user_model) -> None:
    with generate(spark) as g:
        assert g.seed is None
        user_model.factory().count(1).create()


def test_dataframe_missing_raises(spark: SparkSession) -> None:
    with generate(spark) as g:
        with pytest.raises(KeyError, match="missing"):
            g.dataframe("missing")


def test_model_key_and_pk(spark: SparkSession, user_model) -> None:
    with generate(spark, seed=1) as gen:
        created = user_model.factory().count(1).create()
        row = gen.dataframe("users").collect()[0]
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
