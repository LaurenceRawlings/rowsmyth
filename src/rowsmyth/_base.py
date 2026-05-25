from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import registry as _registry

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import MetaData

    from ._builder import FactoryBuilder
    from ._dataset import Dataset


# SQLAlchemy's declared_attr only works for instance-level access, so we use a
# custom descriptor to expose table metadata as class-level read-only properties.
class _classproperty:  # noqa: N801
    def __init__(self, func: Callable[..., Any]) -> None:
        self.func = func

    def __get__(self, obj: Any, cls: type | None = None) -> Any:
        return self.func(cls if cls is not None else type(obj))


class TableSpecMixin:
    __variants__: dict[str, Callable[..., Any]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.__variants__ = {
            name: val
            for name, val in vars(cls).items()
            if callable(val) and getattr(val, "_is_variant", False)
        }

    @_classproperty
    def __comment__(cls) -> str | None:  # noqa: N805
        return cls.__table__.comment  # ty: ignore[unresolved-attribute]

    @_classproperty
    def __table_info__(cls) -> dict[str, Any]:  # noqa: N805
        return cls.__table__.info  # ty: ignore[unresolved-attribute]

    @_classproperty
    def __column_info__(cls) -> dict[str, dict[str, Any]]:  # noqa: N805
        return {
            prop.key: col.info
            for prop in cls.__mapper__.column_attrs  # ty: ignore[unresolved-attribute]
            for col in prop.columns
        }

    @_classproperty
    def __expectations__(cls) -> dict[str, str]:  # noqa: N805
        from sqlalchemy import CheckConstraint

        return {
            c.name: str(c.sqltext)
            for c in cls.__table__.constraints  # ty: ignore[unresolved-attribute]
            if isinstance(c, CheckConstraint) and c.name is not None
        }

    @_classproperty
    def __spark_schema__(cls) -> Any:  # noqa: N805
        from ._spark import to_spark_schema

        return to_spark_schema(cls)  # ty: ignore[invalid-argument-type]

    def __repr__(self) -> str:
        attrs = ", ".join(
            f"{c.key}={getattr(self, c.key)!r}"
            for c in self.__mapper__.columns  # ty: ignore[unresolved-attribute]
        )
        return f"{self.__class__.__name__}({attrs})"

    @classmethod
    def generators(cls) -> dict[Any, Any]:
        """Override to supply factory-boy declarations for column generation.

        Returns:
            A dict mapping column attributes (or string names) to factory-boy
            declarations (e.g. ``factory.Faker(...)``). Keys can be the column
            attribute itself (``cls.name``) or a plain string (``"name"``).

        Example:
            >>> TableSpecMixin.generators()
            {}
        """
        return {}

    @classmethod
    def factory(cls, count: int = 1, max_count: int | None = None) -> FactoryBuilder:
        """Return a FactoryBuilder for this model.

        Args:
            count: Exact number of instances to generate, or minimum when
                ``max_count`` is also provided.
            max_count: Upper bound for a random count in
                ``[count, max_count]`` inclusive. ``None`` means exact.

        Returns:
            A ``FactoryBuilder`` ready to be configured with ``.has()``,
            ``.mix()``, ``.where()`` and ``.create()``.

        Example:
            >>> # User.factory(10)         # exactly 10
            >>> # User.factory(1, 5)       # random 1-5 per parent
            >>> # User.factory(5).create() # generate and persist
        """  # doctest: +SKIP
        from ._builder import FactoryBuilder

        return FactoryBuilder(cls, count, max_count)

    @classmethod
    def dataset(cls, *builders: FactoryBuilder) -> Dataset:
        """Return a Dataset generating one row per registered model.

        Args:
            *builders: Optional ``FactoryBuilder`` overrides for specific
                models. Models without an override get one instance using
                their default generators.

        Returns:
            A ``Dataset`` whose ``.create()`` returns a dict keyed by table
            name, values are lists of persisted model instances.

        Raises:
            TypeError: If any argument is not a ``FactoryBuilder``.
            ValueError: If a builder's model is not registered to this Base.

        Example:
            >>> # result = Base.dataset(User.factory(10)).create()
            >>> # result["users"]  # -> list of 10 User instances
        """  # doctest: +SKIP
        from ._builder import FactoryBuilder
        from ._dataset import Dataset

        base_models = {mapper.class_ for mapper in cls.registry.mappers}  # ty: ignore[unresolved-attribute]
        for b in builders:
            if not isinstance(b, FactoryBuilder):
                msg = f"Expected a FactoryBuilder, got {type(b).__name__}"
                raise TypeError(msg)
            if b.model not in base_models:
                msg = f"{b.model.__name__} is not registered to this Base."
                raise ValueError(msg)
        overrides = {b.model: b for b in builders}
        return Dataset(cls, overrides)


def declarative_base(
    metadata: MetaData | None = None,
    type_annotation_map: dict[Any, Any] | None = None,
    registry: _registry | None = None,
) -> type[TableSpecMixin]:
    """Create a new SQLAlchemy declarative base with rowsmyth capabilities.

    Args:
        metadata: Optional ``MetaData`` instance shared by all models that
            subclass the returned base. A new ``MetaData`` is created if
            not provided.
        type_annotation_map: Optional mapping of Python types to SQLAlchemy
            ``TypeEngine`` classes or instances, used by ``Mapped[]``
            annotations.
        registry: Optional pre-existing ``registry`` instance. When provided,
            models using this base share the mapper registry with other bases
            that reference the same registry.

    Returns:
        A new declarative base class with ``TableSpecMixin`` mixed in.

    Example:
        >>> from rowsmyth import declarative_base
        >>> Base = declarative_base()
        >>> issubclass(Base, TableSpecMixin)
        True
    """
    attrs: dict[str, Any] = {}
    if metadata is not None:
        attrs["metadata"] = metadata
    if type_annotation_map is not None:
        attrs["type_annotation_map"] = type_annotation_map
    if registry is not None:
        attrs["registry"] = registry
    return type("Base", (TableSpecMixin, DeclarativeBase), attrs)
