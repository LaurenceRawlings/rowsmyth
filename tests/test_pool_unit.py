"""Unit tests for pool helpers."""

from __future__ import annotations

import random
from typing import Any
from unittest.mock import MagicMock

import pytest

from rowsmyth import EmptyPoolError
from rowsmyth.pool import Pool, PoolChoice


class _Row:
    def __init__(self, value: Any) -> None:
        self._value = value

    def __getitem__(self, index: int) -> Any:
        return self._value


def test_pool_choice_and_sample() -> None:
    rng = random.Random(0)
    distinct = MagicMock()
    distinct.collect.return_value = [_Row(v) for v in [1, 2, 3, 4, 5]]
    select = MagicMock()
    select.distinct.return_value = distinct
    table_df = MagicMock()
    table_df.select.return_value = select
    spark = MagicMock()
    spark.table.return_value = table_df

    pool = Pool(spark, "roles", "id", rng)
    choice = pool.choice()
    assert isinstance(choice, PoolChoice)
    assert choice.view == "roles"
    assert choice.column == "id"
    sample = pool.sample(3)
    assert len(sample) == 3
    assert set(sample).issubset({1, 2, 3, 4, 5})
    assert pool.values == [1, 2, 3, 4, 5]


def test_pool_values_empty_raises() -> None:
    distinct = MagicMock()
    distinct.collect.return_value = []
    select = MagicMock()
    select.distinct.return_value = distinct
    table_df = MagicMock()
    table_df.select.return_value = select
    spark = MagicMock()
    spark.table.return_value = table_df

    pool = Pool(spark, "roles", "id", random.Random(0))
    with pytest.raises(EmptyPoolError, match="no values"):
        _ = pool.values
