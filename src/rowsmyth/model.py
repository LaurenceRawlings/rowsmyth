"""Declarative model base and variant decorator."""

from __future__ import annotations

import types
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from rowsmyth.dataset import Dataset, RowCtx
    from rowsmyth.factory import Factory


class WrongDeclarativeBaseError(RuntimeError):
    """Raised when a model is used with a dataset for another declarative base."""


def variant(fn: Callable[..., dict[str, Any]]) -> Callable[..., dict[str, Any]]:
    """Mark a method as a named partial override for :meth:`Model.generator`."""
    if isinstance(fn, types.FunctionType):
        object.__setattr__(fn, "__variant__", fn.__name__)
    return fn


class Model:
    """
    Declarative base for a relational model.

    Subclasses declare ``__table_name__``, ``__definition__`` and
    ``__primary_key__``, then implement :meth:`generator` to produce one row.
    """

    registry: ClassVar[dict[str, type[Model]]] = {}
    __rowsmyth_base__: ClassVar[type[Model] | None] = None

    __table_name__: ClassVar[str]
    __definition__: ClassVar[StructType]
    __primary_key__: ClassVar[tuple[str, ...]]
    __catalog__: ClassVar[str | None] = None
    __schema__: ClassVar[str | None] = None
    __comment__: ClassVar[str | None] = None
    __table_tags__: ClassVar[dict[str, str]] = {}
    __expectations__: ClassVar[dict[str, str]] = {}

    _variants: ClassVar[dict[str, Callable[..., dict[str, Any]]]]

    @classmethod
    def _field_names(cls) -> set[str]:
        return {field.name for field in cls.__definition__.fields}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "__table_name__", None):
            return
        base = declarative_base_for(cls)
        internal = [
            field.name
            for field in cls.__definition__.fields
            if field.name.startswith("__rowsmyth_")
        ]
        if internal:
            msg = f"{cls.__table_name__}: reserved rowsmyth columns: {internal}"
            raise ValueError(msg)
        cls._variants = {
            m.__variant__: m  # type: ignore[attr-defined]
            for m in vars(cls).values()
            if callable(m) and hasattr(m, "__variant__")
        }
        base.registry[cls.__table_name__] = cls

    def __init__(self, **attrs: Any) -> None:
        unknown = set(attrs) - type(self)._field_names()
        if unknown:
            msg = f"{type(self).__table_name__}: unknown columns: {sorted(unknown)}"
            raise ValueError(msg)
        object.__setattr__(self, "attrs", dict(attrs))
        for key, value in attrs.items():
            object.__setattr__(self, key, value)

    @property
    def table(self) -> type[Model]:
        """Return the model class for compatibility with parent internals."""
        return type(self)

    def __getattr__(self, name: str) -> Any:
        try:
            return self.attrs[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    @property
    def key(self) -> dict[str, Any]:
        """Primary key columns as a dict."""
        return {k: self.attrs[k] for k in type(self).__primary_key__}

    @property
    def pk(self) -> Any:
        """Scalar primary key (single-column PK only)."""
        (col,) = type(self).__primary_key__
        return self.attrs[col]

    def generator(self, ctx: RowCtx) -> dict[str, Any]:
        """Return attribute values for one row."""
        msg = f"{type(self).__name__} must implement generator()"
        raise NotImplementedError(msg)

    @classmethod
    def create(cls, **cols: Any) -> Model:
        """Create one row in the active dataset and return its model."""
        from rowsmyth.dataset import RowCtx, require_active
        from rowsmyth.resolution import resolve_row_values, validate_once

        unknown = set(cols) - cls._field_names()
        if unknown:
            msg = f"{cls.__table_name__}: unknown columns: {sorted(unknown)}"
            raise ValueError(msg)

        gen = require_active()
        validate_dataset_base(cls, gen)
        acc: dict[str, list[dict[str, Any]]] = {}
        index = len(gen._rows.get(cls.__table_name__, []))
        obj = cls()
        ctx = RowCtx(gen, cls, index, {}, acc)
        attrs = dict(obj.generator(ctx))
        attrs.update(cols)
        ctx.row = attrs
        resolve_row_values(attrs, ctx)
        validate_once(gen, cls, attrs)
        acc.setdefault(cls.__table_name__, []).append(attrs)
        gen._commit(acc)
        return cls(**attrs)

    @classmethod
    def factory(cls) -> Factory:
        """Return a fluent factory for this table."""
        from rowsmyth.factory import Factory

        return Factory(cls)

    @classmethod
    def dataset(
        cls,
        spark: SparkSession,
        seed: int | None = None,
    ) -> Dataset:
        """Return a dataset context manager bound to this declarative base."""
        from rowsmyth.dataset import dataset

        base = declarative_base_for(cls)
        return dataset(spark, base, seed)

    @classmethod
    def fqn(cls) -> str:
        """Fully qualified table name (catalog.schema.table)."""
        parts = (cls.__catalog__, cls.__schema__, cls.__table_name__)
        return ".".join(p for p in parts if p)

    @classmethod
    def column_tags(cls) -> dict[str, dict[str, str]]:
        """Column-level Unity Catalog tags from Spark schema metadata."""
        result: dict[str, dict[str, str]] = {}
        for field in cls.__definition__.fields:
            tags = field.metadata.get("tags", {})
            if tags:
                result[field.name] = dict(tags)
        return result

    @classmethod
    def column_comments(cls) -> dict[str, str]:
        """Column comments from Spark schema metadata."""
        result: dict[str, str] = {}
        for field in cls.__definition__.fields:
            comment = field.metadata.get("comment")
            if comment:
                result[field.name] = str(comment)
        return result

    @classmethod
    def uc_tag_sql(cls) -> list[str]:
        """SQL statements needed to apply Unity Catalog table and column tags."""
        statements: list[str] = []
        table_name = _quote_fqn(cls.fqn())
        if cls.__table_tags__:
            statements.append(
                f"ALTER TABLE {table_name} SET TAGS ({_tag_pairs(cls.__table_tags__)})"
            )
        for column, tags in cls.column_tags().items():
            statements.append(
                "ALTER TABLE "
                f"{table_name} ALTER COLUMN {_quote_identifier(column)} "
                f"SET TAGS ({_tag_pairs(tags)})"
            )
        return statements


def _quote_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _quote_fqn(value: str) -> str:
    return ".".join(_quote_identifier(part) for part in value.split("."))


def _tag_pairs(tags: dict[str, str]) -> str:
    return ", ".join(
        f"{_quote_literal(key)} = {_quote_literal(value)}"
        for key, value in tags.items()
    )


def declarative_base(name: str = "Base") -> type[Model]:
    """Create a scoped declarative base for rowsmyth models."""

    class Base(Model):
        registry: ClassVar[dict[str, type[Model]]] = {}

    Base.__name__ = name
    Base.__qualname__ = name
    Base.__rowsmyth_base__ = Base
    return Base


def declarative_base_for(model: type[Model]) -> type[Model]:
    """Return the declarative base for a rowsmyth model class."""
    base = getattr(model, "__rowsmyth_base__", None)
    if base is None:
        msg = (
            f"{model.__name__} must extend a rowsmyth declarative base created by "
            "declarative_base()"
        )
        raise TypeError(msg)
    return base


def validate_dataset_base(model: type[Model], dataset: Dataset) -> None:
    """Raise if ``model`` belongs to a different base than ``dataset``."""
    model_base = declarative_base_for(model)
    if model_base is dataset.base:
        return
    msg = (
        f"{model.__name__} belongs to a different declarative base than the "
        "active dataset"
    )
    raise WrongDeclarativeBaseError(msg)
