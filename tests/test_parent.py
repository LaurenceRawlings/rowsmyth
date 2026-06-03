"""Tests for ctx.parent() and compound keys."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rowsmyth import WrongDeclarativeBaseError, declarative_base


def test_parent_compound_key(
    spark: SparkSession, app_base, order_line_model, order_model
) -> None:
    with app_base.dataset(spark, seed=20) as dataset:
        created = (
            order_model
            .factory()
            .count(2)
            .has(order_line_model.factory().count(2))
            .create()
        )
        orders = dataset.dataframe("orders")
        order_lines = dataset.dataframe("order_lines")
    assert len(created) == 2
    assert orders.count() == 2
    assert order_lines.count() == 4
    joined = order_lines.alias("line").join(
        orders.alias("order"),
        (F.col("line.order_id") == F.col("order.order_id"))
        & (F.col("line.order_region") == F.col("order.region")),
    )
    assert joined.count() == order_lines.count()


def test_parent_wrong_base_raises(spark: SparkSession, app_base, order_model) -> None:
    OtherBase = declarative_base()

    class OtherLine(OtherBase):
        __table_name__ = "other_lines"
        __primary_key__ = ("line_id",)
        __definition__ = app_base.registry["order_lines"].__definition__

        def generator(self, ctx) -> dict:
            order = ctx.parent(order_model)
            return {
                "line_id": ctx.sequence(),
                "order_id": order.key["order_id"],
                "order_region": order.key["region"],
                "qty": 1,
            }

    with OtherBase.dataset(spark):
        with pytest.raises(
            WrongDeclarativeBaseError, match="different declarative base"
        ):
            OtherLine.factory().create()
