"""Pool value helpers produced during row generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from rowsmyth.errors import EmptyPoolError, PoolSampleError

if TYPE_CHECKING:
    import random

    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class PoolChoice:
    """Deferred Spark-resolved choice from a temp view column."""

    view: str
    column: str
    seed: int


def _empty_pool_message(view: str, column: str) -> str:
    return f"pool({view!r}, {column!r}): no non-null values in temp view"


class Pool:
    """Distinct column values from a session temp view."""

    __slots__ = ("_rng", "_spark", "column", "view")

    def __init__(
        self,
        spark: SparkSession,
        view: str,
        column: str,
        rng: random.Random,
    ) -> None:
        self._spark = spark
        self.view = view
        self.column = column
        self._rng = rng

    @property
    def values(self) -> list[Any]:
        """Return current distinct values from Spark."""
        return self._values()

    def choice(self) -> PoolChoice:
        """Return a deferred uniformly random Spark pool choice."""
        return PoolChoice(
            view=self.view,
            column=self.column,
            seed=self._rng.randrange(0, 2**31),
        )

    def sample(self, k: int) -> list[Any]:
        """Pick k distinct values without replacement from Spark."""
        values = self._values()
        try:
            return self._rng.sample(values, k)
        except ValueError as exc:
            msg = (
                f"pool({self.view!r}, {self.column!r}): cannot sample "
                f"{k} values from {len(values)} available values"
            )
            raise PoolSampleError(msg) from exc

    def _values(self) -> list[Any]:
        from pyspark.sql import functions as F

        rows = (
            self._spark
            .table(self.view)
            .select(self.column)
            .where(F.col(self.column).isNotNull())
            .distinct()
            .collect()
        )
        values = [row[0] for row in rows]
        if not values:
            raise EmptyPoolError(_empty_pool_message(self.view, self.column))
        return values
