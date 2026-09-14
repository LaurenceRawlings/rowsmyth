"""Pool value helpers produced during row generation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rowsmyth.errors import EmptyPoolError, PoolSampleError

if TYPE_CHECKING:
    from collections.abc import Iterable

    from rowsmyth.dataset import Dataset


class PoolIndex:
    """Incremental distinct-value index for one pooled column."""

    __slots__ = ("cursor", "external", "loaded", "seen", "values")

    def __init__(self, *, external: bool) -> None:
        self.external = external
        self.loaded = False
        self.cursor = 0
        self.seen: set[Any] = set()
        self.values: list[Any] = []

    def extend(self, values: Iterable[Any]) -> None:
        """Add distinct non-null values, preserving first-seen order."""
        for value in values:
            if value is None or value in self.seen:
                continue
            self.seen.add(value)
            self.values.append(value)


class Pool:
    """Distinct values from a dataset table or an existing Spark temp view."""

    __slots__ = ("_dataset", "column", "view")

    def __init__(self, dataset: Dataset, view: str, column: str) -> None:
        self._dataset = dataset
        self.view = view
        self.column = column

    @property
    def values(self) -> list[Any]:
        """Distinct non-null values currently available in the pool."""
        return list(self._values())

    def choice(self) -> Any:
        """Return one uniformly random value from the pool."""
        return self._dataset.random.choice(self._values())

    def sample(self, k: int) -> list[Any]:
        """Pick k distinct values without replacement."""
        values = self._values()
        try:
            return self._dataset.random.sample(values, k)
        except ValueError as exc:
            msg = (
                f"pool({self.view!r}, {self.column!r}): cannot sample "
                f"{k} values from {len(values)} available values"
            )
            raise PoolSampleError(msg) from exc

    def _values(self) -> list[Any]:
        return self._dataset._pool_values(self.view, self.column)


def empty_pool_error(view: str, column: str, *, external: bool) -> EmptyPoolError:
    """Build the error raised when a pool holds no usable values."""
    if external:
        msg = f"pool({view!r}, {column!r}): no non-null values in temp view"
    else:
        msg = (
            f"pool({view!r}, {column!r}): no non-null values; create {view} rows "
            "before sampling from them"
        )
    return EmptyPoolError(msg)
