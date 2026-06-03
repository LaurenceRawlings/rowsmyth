"""Tests for ctx.pool()."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from rowsmyth import WrongDeclarativeBaseError, declarative_base


def test_pool_choice(
    spark: SparkSession, app_base, pool_consumer_model, role_model
) -> None:
    with app_base.dataset(spark, seed=30) as dataset:
        admin = role_model.create(name="admin")
        user = role_model.create(name="user")
        created = pool_consumer_model.factory().count(5).create()
        pool_df = dataset.dataframe("pool_consumer")
    role_ids = {admin.id, user.id}
    assert role_ids == {r.id for r in spark.table("roles").collect()}
    assert all("__rowsmyth_" not in col for col in pool_df.columns)
    assert all(item.role_id in role_ids for item in created)
    for row in pool_df.collect():
        assert row.role_id in role_ids


def test_pool_empty_raises(spark: SparkSession, app_base, pool_consumer_model) -> None:
    from pyspark.sql.types import LongType, StructField, StructType

    empty_schema = StructType([StructField("id", LongType(), True)])
    spark.createDataFrame([], empty_schema).createOrReplaceTempView("roles")
    with app_base.dataset(spark, seed=31):
        with pytest.raises(ValueError, match="no values"):
            pool_consumer_model.factory().count(1).create()


def test_pool_sample(spark: SparkSession, app_base, role_model) -> None:
    with app_base.dataset(spark, seed=32) as dataset:
        role_model.factory().count(5).create()
        pool = dataset.pool("roles", "id")
        sample = pool.sample(2)
        assert len(sample) == 2
        assert len(set(sample)) == 2


def test_model_create_outside_dataset_raises(role_model) -> None:
    with pytest.raises(RuntimeError, match="inside dataset"):
        role_model.create(name="admin")


def test_model_create_unknown_column_raises(
    spark: SparkSession,
    app_base,
    role_model,
) -> None:
    with app_base.dataset(spark):
        with pytest.raises(ValueError, match="unknown columns"):
            role_model.create(name="admin", unknown=True)


def test_model_constructor_is_not_persisted(
    spark: SparkSession,
    app_base,
    role_model,
) -> None:
    role = role_model(id=1, name="admin")
    assert role.key == {"id": 1}
    assert role.pk == 1
    with app_base.dataset(spark) as dataset:
        with pytest.raises(KeyError, match="roles"):
            dataset.dataframe("roles")


def test_model_create_wrong_base_raises(spark: SparkSession, app_base) -> None:
    OtherBase = declarative_base()

    class OtherRole(OtherBase):
        __table_name__ = "roles"
        __primary_key__ = ("id",)
        __definition__ = app_base.registry["roles"].__definition__

        def generator(self, ctx) -> dict:
            return {"id": ctx.sequence(), "name": "admin"}

    with app_base.dataset(spark):
        with pytest.raises(
            WrongDeclarativeBaseError, match="different declarative base"
        ):
            OtherRole.create(name="admin")
