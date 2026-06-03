"""Tests for variant decorator and merge."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_variant_override(spark: SparkSession, app_base, user_model) -> None:
    with app_base.dataset(spark, seed=1) as dataset:
        created = user_model.factory().count(3).variant("churned").create()
        df = dataset.dataframe("users").withColumn("expected_status", F.lit("inactive"))
    assert all(user.status == "inactive" for user in created)
    assert_column_equality(df, "status", "expected_status")


def test_unknown_variant_raises(user_model) -> None:
    with pytest.raises(KeyError, match="no variant 'missing'"):
        user_model.factory().variant("missing")
