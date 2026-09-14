"""Fluent factory for building rows of a model and its relations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

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
    """
    Builder for generating rows of a single model (and related children).

    Every method returns a new factory, so a configured factory is a reusable
    template: refining it never mutates the factory it was derived from.
    """

    __slots__ = ("_children", "_model", "_n", "_presets", "_variant", "_where")

    def __init__(self, table: type[Model]) -> None:
        self._model = table
        self._n: int | None = None
        self._variant: str | None = None
        self._where: dict[str, Any] = {}
        self._children: list[tuple[Factory, str | None]] = []
        self._presets: list[dict[str, Any]] | None = None

    def count(self, n: int) -> Factory:
        """Number of rows to generate (default 1)."""
        if not isinstance(n, int) or isinstance(n, bool) or n < 0:
            msg = "Factory.count() requires a non-negative integer"
            raise FactoryError(msg)
        if self._presets is not None:
            msg = "Factory.count() cannot be combined with from_rows()"
            raise FactoryError(msg)
        clone = self._copy()
        clone._n = n
        return clone

    def from_rows(self, rows: Iterable[dict[str, Any]]) -> Factory:
        """
        Generate one row per mapping, bypassing :meth:`Model.generator`.

        Overrides from :meth:`where` still apply, and every row is validated as
        usual. Cannot be combined with :meth:`count` or :meth:`variant`.
        """
        if self._n is not None:
            msg = "Factory.from_rows() cannot be combined with count()"
            raise FactoryError(msg)
        if self._variant is not None:
            msg = "Factory.from_rows() cannot be combined with variant()"
            raise FactoryError(msg)
        clone = self._copy()
        clone._presets = [dict(row) for row in rows]
        return clone

    def where(self, **overrides: Any) -> Factory:
        """Column overrides (scalars, Factory FKs, or :func:`rowsmyth.lazy`)."""
        clone = self._copy()
        clone._where.update(overrides)
        return clone

    def has(self, child: Factory, via: str | None = None) -> Factory:
        """Attach child rows per parent row (injected parent via ``via``)."""
        clone = self._copy()
        clone._children.append((child, via))
        return clone

    def variant(self, name: str) -> Factory:
        """Apply a named :func:`rowsmyth.variant` partial override."""
        if name not in self._model._variants:
            msg = f"{self._model.__table_name__} has no variant {name!r}"
            raise UnknownVariantError(msg)
        if self._presets is not None:
            msg = "Factory.variant() cannot be combined with from_rows()"
            raise FactoryError(msg)
        clone = self._copy()
        clone._variant = name
        return clone

    def create(self) -> list[Model]:
        """Generate rows into the active dataset and return root models."""
        dataset = require_active()
        validate_dataset_base(self._model, dataset)
        acc: dict[str, list[dict[str, Any]]] = {}
        self._generate(dataset, acc, injected={})
        dataset._commit(acc)
        return [
            self._model(**attrs) for attrs in acc.get(self._model.__table_name__, [])
        ]

    def _copy(self) -> Factory:
        clone = Factory(self._model)
        clone._n = self._n
        clone._variant = self._variant
        clone._where = dict(self._where)
        clone._children = list(self._children)
        clone._presets = self._presets
        return clone

    def _touch(self, acc: dict[str, list[dict[str, Any]]]) -> None:
        """Register every table in this factory tree, even with no rows."""
        acc.setdefault(self._model.__table_name__, [])
        for child, _ in self._children:
            child._touch(acc)

    def _sources(self) -> list[dict[str, Any] | None]:
        if self._presets is not None:
            return list(self._presets)
        return [None] * (1 if self._n is None else self._n)

    def _generate(
        self,
        dataset: Dataset,
        acc: dict[str, list[dict[str, Any]]],
        injected: dict[str, Model],
    ) -> None:
        """Generate this factory's rows and recurse into children."""
        validate_dataset_base(self._model, dataset)
        self._touch(acc)
        for index, preset in enumerate(self._sources()):
            attrs = self._row(
                dataset,
                acc,
                index=index,
                injected=injected,
                preset=preset,
            )
            acc[self._model.__table_name__].append(attrs)
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
        preset: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Materialise one row (used by :meth:`create` and parent creation)."""
        ctx = RowCtx(dataset, self._model, index, dict(injected), acc)
        if preset is None:
            obj = self._model()
            attrs = dict(obj.generator(ctx))
            if self._variant is not None:
                attrs.update(apply_variant(self._model, obj, self._variant, ctx))
        else:
            attrs = dict(preset)
        attrs.update(self._where)
        ctx.row = attrs
        resolve_row_values(attrs, ctx)
        validate_row(self._model, attrs)
        return attrs
