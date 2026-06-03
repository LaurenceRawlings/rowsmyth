"""Tests for variant decorator and merge."""

from __future__ import annotations

import pytest
from chispa import assert_column_equality
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rowsmyth import generate


def test_variant_override(spark: SparkSession, user_model) -> None:
    with generate(spark, seed=1) as gen:
        created = user_model.factory().count(3).variant("churned").create()
        df = gen.dataframe("users").withColumn("expected_status", F.lit("inactive"))
    assert all(user.status == "inactive" for user in created)
    assert_column_equality(df, "status", "expected_status")


def test_unknown_variant_raises(user_model) -> None:
    with pytest.raises(KeyError, match="no variant 'missing'"):
        user_model.factory().variant("missing")
