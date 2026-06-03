"""Tests for foreign-key resolution."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rowsmyth import generate


def test_fk_auto_creates_parent(spark: SparkSession, user_model) -> None:
    with generate(spark, seed=10) as gen:
        created = user_model.factory().count(2).create()
        roles = gen.dataframe("roles").count()
        users = gen.dataframe("users").count()
    assert roles >= 1
    assert users == 2
    assert len(created) == 2


def test_has_injects_parent(spark: SparkSession, post_model, user_model) -> None:
    with generate(spark, seed=11) as gen:
        created = (
            user_model
            .factory()
            .count(2)
            .has(post_model.factory().count(3), via="author_id")
            .create()
        )
        users = gen.dataframe("users")
        posts = gen.dataframe("posts")
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


def test_compound_fk_factory_raises(spark: SparkSession, bad_compound_fk_model) -> None:
    with generate(spark, seed=12), pytest.raises(TypeError, match="compound keys"):
        bad_compound_fk_model.factory().count(1).create()


def test_roles_only(spark: SparkSession, role_model) -> None:
    with generate(spark, seed=13) as gen:
        created = role_model.factory().count(5).create()
        roles = gen.dataframe("roles")
    assert len(created) == 5
    assert roles.count() == 5
    assert spark.catalog.tableExists("roles")
