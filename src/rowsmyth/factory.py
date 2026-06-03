"""Fluent factory for building Spark DataFrames row by row."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rowsmyth.model import Model

from rowsmyth.dataset import Dataset, RowCtx, require_active
from rowsmyth.errors import FactoryError, UnknownVariantError
from rowsmyth.model import validate_dataset_base
from rowsmyth.resolution import (
    apply_variant,
    resolve_row_values,
    validate_row,
)


class Factory:
    """Builder for generating rows of a single model (and related children)."""

    __slots__ = ("_children", "_model", "_n", "_variant", "_where")

    def __init__(self, table: type[Model]) -> None:
        self._model = table
        self._n = 1
        self._variant: str | None = None
        self._where: dict[str, Any] = {}
        self._children: list[tuple[Factory, str | None]] = []

    def count(self, n: int) -> Factory:
        """Number of rows to generate."""
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            msg = "Factory.count() requires a non-negative integer"
            raise FactoryError(msg)
        self._n = n
        return self

    def where(self, **overrides: Any) -> Factory:
        """Column overrides (scalars, Factory FKs, or callables)."""
        self._where.update(overrides)
        return self

    def has(self, child: Factory, via: str | None = None) -> Factory:
        """Attach child rows per parent row (injected parent via ``via``)."""
        self._children.append((child, via))
        return self

    def variant(self, name: str) -> Factory:
        """Apply a named :func:`rowsmyth.variant` partial override."""
        if name not in self._model._variants:
            msg = f"{self._model.__table_name__} has no variant {name!r}"
            raise UnknownVariantError(msg)
        self._variant = name
        return self

    def create(self) -> list[Model]:
        """Generate rows, register temp views and return root models."""
        dataset = require_active()
        validate_dataset_base(self._model, dataset)
        acc: dict[str, list[dict[str, Any]]] = {}
        self._generate(dataset, acc, injected={})
        dataset._commit(acc)
        return [
            self._model(**attrs) for attrs in acc.get(self._model.__table_name__, [])
        ]

    def _generate(
        self,
        dataset: Dataset,
        acc: dict[str, list[dict[str, Any]]],
        injected: dict[str, Model],
    ) -> None:
        """Generate ``_n`` rows and recurse into children."""
        validate_dataset_base(self._model, dataset)
        for i in range(self._n):
            attrs = self._row(dataset, acc, index=i, injected=injected)
            acc.setdefault(self._model.__table_name__, []).append(attrs)
            inst = self._model(**attrs)
            for child, via in self._children:
                slot = via or self._model.__table_name__
                child._generate(dataset, acc, {**injected, slot: inst})

    def _row(
        self,
        dataset: Dataset,
        acc: dict[str, list[dict[str, Any]]],
        *,
        index: int,
        injected: dict[str, Model],
    ) -> dict[str, Any]:
        """Materialise one row (used by :meth:`create` and parent creation)."""
        obj = self._model()
        ctx = RowCtx(dataset, self._model, index, dict(injected), acc)
        attrs = dict(obj.generator(ctx))
        if self._variant is not None:
            attrs.update(apply_variant(self._model, obj, self._variant, ctx))
        attrs.update(self._where)
        ctx.row = attrs
        resolve_row_values(attrs, ctx)
        validate_row(self._model, attrs)
        return attrs
