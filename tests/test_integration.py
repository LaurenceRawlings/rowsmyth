"""End-to-end integration tests."""

from __future__ import annotations

from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_users_posts_temp_views(
    spark: SparkSession, app_base, post_model, user_model
) -> None:
    with app_base.dataset(spark, seed=50) as dataset:
        users = (
            user_model
            .factory()
            .count(2)
            .has(post_model.factory().count(2).where(published=True), via="author_id")
            .create()
        )
        posts = dataset.dataframe("posts")
    assert spark.catalog.tableExists("users")
    assert spark.catalog.tableExists("posts")
    assert len(users) == 2
    assert posts.count() == 4
    published = posts.withColumn("expected", F.lit(True))
    assert_column_equality(published, "published", "expected")


def test_where_factory_fk(
    spark: SparkSession, app_base, role_model, user_model
) -> None:
    with app_base.dataset(spark, seed=51) as dataset:
        user_model.factory().count(1).where(role_id=role_model.factory()).create()
        users = dataset.dataframe("users")
        roles = dataset.dataframe("roles")
    joined = (
        users
        .alias("u")
        .join(
            roles.alias("r"),
            F.col("u.role_id") == F.col("r.id"),
        )
        .select(F.col("u.role_id"), F.col("r.id").alias("role_pk"))
    )
    assert_column_equality(joined, "role_id", "role_pk")
