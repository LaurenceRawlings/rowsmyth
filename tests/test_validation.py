"""Tests for NOT NULL validation."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


def test_missing_required_column(
    spark: SparkSession, app_base, missing_required_model
) -> None:
    with app_base.dataset(spark, seed=40):
        with pytest.raises(ValueError, match="required_col"):
            missing_required_model.factory().count(1).create()


def test_nullable_ok_without_optional(
    spark: SparkSession,
    app_base,
    nullable_demo_model,
) -> None:
    with app_base.dataset(spark, seed=41) as dataset:
        created = nullable_demo_model.factory().count(1).create()
        nullable_demo = dataset.dataframe("nullable_demo")
    assert len(created) == 1
    assert nullable_demo.count() == 1


def test_validation_runs_once(
    spark: SparkSession,
    app_base,
    nullable_demo_model,
) -> None:
    """Second row skips re-validation (coverage for validate_once early return)."""
    with app_base.dataset(spark, seed=42) as dataset:
        created = nullable_demo_model.factory().count(3).create()
        nullable_demo = dataset.dataframe("nullable_demo")
    assert len(created) == 3
    assert nullable_demo.count() == 3
