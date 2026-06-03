"""Tests for foreign-key resolution."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rowsmyth import (
    CompoundPrimaryKeyError,
    WrongDeclarativeBaseError,
    declarative_base,
)


def test_fk_auto_creates_parent(spark: SparkSession, app_base, user_model) -> None:
    with app_base.dataset(spark, seed=10) as dataset:
        created = user_model.factory().count(2).create()
        roles = dataset.dataframe("roles")
        users = dataset.dataframe("users")
    assert roles.count() == 2
    assert users.count() == 2
    assert len(created) == 2
    joined = (
        users
        .alias("u")
        .join(
            roles.alias("r"),
            F.col("u.role_id") == F.col("r.id"),
        )
        .select(F.col("u.role_id"), F.col("r.id").alias("role_id_from_parent"))
    )
    assert joined.count() == users.count()
    assert_column_equality(joined, "role_id", "role_id_from_parent")


def test_has_injects_parent(
    spark: SparkSession, app_base, post_model, user_model
) -> None:
    with app_base.dataset(spark, seed=11) as dataset:
        created = (
            user_model
            .factory()
            .count(2)
            .has(post_model.factory().count(3), via="author_id")
            .create()
        )
        users = dataset.dataframe("users")
        posts = dataset.dataframe("posts")
    assert len(created) == 2
    assert users.count() == 2
    assert posts.count() == 6
    joined = (
        posts
        .alias("p")
        .join(
            users.alias("u"),
            F.col("p.author_id") == F.col("u.id"),
        )
        .select(F.col("p.author_id"), F.col("u.id").alias("user_id"))
    )
    assert_column_equality(joined, "author_id", "user_id")


def test_compound_fk_factory_raises(
    spark: SparkSession,
    app_base,
    bad_compound_fk_model,
) -> None:
    with (
        app_base.dataset(spark, seed=12),
        pytest.raises(CompoundPrimaryKeyError, match="compound keys"),
    ):
        bad_compound_fk_model.factory().count(1).create()


def test_roles_only(spark: SparkSession, app_base, role_model) -> None:
    with app_base.dataset(spark, seed=13) as dataset:
        created = role_model.factory().count(5).create()
        roles = dataset.dataframe("roles")
    assert len(created) == 5
    assert roles.count() == 5
    assert spark.catalog.tableExists("roles")


def test_fk_factory_wrong_base_raises(
    spark: SparkSession, app_base, user_model
) -> None:
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
            user_model.factory().where(role_id=OtherRole.factory()).create()
