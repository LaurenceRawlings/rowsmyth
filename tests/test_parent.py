"""Tests for ctx.parent() and compound keys."""

from __future__ import annotations

from pyspark.sql import SparkSession

from rowsmyth import generate


def test_parent_compound_key(
    spark: SparkSession, order_line_model, order_model
) -> None:
    with generate(spark, seed=20) as gen:
        created = (
            order_model
            .factory()
            .count(2)
            .has(order_line_model.factory().count(2))
            .create()
        )
        orders = gen.dataframe("orders")
        order_lines = gen.dataframe("order_lines")
    assert len(created) == 2
    assert orders.count() == 2
    assert order_lines.count() == 4
    for line in order_lines.collect():
        assert line.order_id is not None
        assert line.order_region in ("eu", "us")
