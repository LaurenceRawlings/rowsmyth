"""Fluent factory for building Spark DataFrames row by row."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from rowsmyth.table import Model

from rowsmyth.context import Generation, RowCtx, require_active
from rowsmyth.resolution import (
    apply_variant,
    resolve_row_values,
    validate_once,
)


class Factory:
    """Builder for generating rows of a single model (and related children)."""

    __slots__ = ("_children", "_n", "_variant", "_where", "table")

    def __init__(self, table: type[Model]) -> None:
        self.table = table
        self._n = 1
        self._variant: str | None = None
        self._where: dict[str, Any] = {}
        self._children: list[tuple[Factory, str | None]] = []

    def count(self, n: int) -> Factory:
        """Number of rows to generate."""
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
        if name not in self.table._variants:
            msg = f"{self.table.__table_name__} has no variant {name!r}"
            raise KeyError(msg)
        self._variant = name
        return self

    def create(self) -> list[Model]:
        """Generate rows, register temp views and return root models."""
        gen = require_active()
        acc: dict[str, list[dict[str, Any]]] = {}
        self.generate(gen, acc, injected={})
        gen._commit(acc)
        return [self.table(**attrs) for attrs in acc.get(self.table.__table_name__, [])]

    def generate(
        self,
        gen: Generation,
        acc: dict[str, list[dict[str, Any]]],
        injected: dict[str, Model],
    ) -> None:
        """Generate ``_n`` rows and recurse into children."""
        for i in range(self._n):
            attrs = self.row(gen, acc, index=i, injected=injected)
            acc.setdefault(self.table.__table_name__, []).append(attrs)
            inst = self.table(**attrs)
            for child, via in self._children:
                slot = via or self.table.__table_name__
                child.generate(gen, acc, {**injected, slot: inst})

    def row(
        self,
        gen: Generation,
        acc: dict[str, list[dict[str, Any]]],
        *,
        index: int,
        injected: dict[str, Model],
    ) -> dict[str, Any]:
        """Materialise one row (used by :meth:`create` and parent creation)."""
        obj = self.table()
        ctx = RowCtx(gen, self.table, index, dict(injected), acc)
        attrs = dict(obj.generator(ctx))
        if self._variant is not None:
            attrs.update(apply_variant(self.table, obj, self._variant, ctx))
        attrs.update(self._where)
        ctx.row = attrs
        resolve_row_values(attrs, ctx)
        validate_once(gen, self.table, attrs)
        return attrs
