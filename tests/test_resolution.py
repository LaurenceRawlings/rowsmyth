"""Tests for resolution helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession

from rowsmyth import generate, variant
from rowsmyth.resolution import apply_variant


def test_apply_variant(spark: SparkSession, user_model) -> None:
    with generate(spark, seed=60):
        from rowsmyth.context import RowCtx, require_active

        gen = require_active()
        obj = user_model()
        ctx = RowCtx(gen, user_model, 0, {}, {})
        result = apply_variant(user_model, obj, "churned", ctx)
        assert result == {"status": "inactive"}


def test_variant_decorator_sets_name() -> None:
    @variant
    def sample(_self, _ctx) -> dict:
        return {}

    assert sample.__variant__ == "sample"  # type: ignore[attr-defined]
